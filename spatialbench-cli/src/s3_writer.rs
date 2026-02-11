// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! S3 writer support for writing generated data directly to S3

use crate::plan::PARQUET_BUFFER_SIZE;
use bytes::Bytes;
use log::{debug, info};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::io::{self, Write};
use std::sync::Arc;
use url::Url;

/// Minimum part size enforced by AWS S3 for multipart uploads (except last part)
const S3_MIN_PART_SIZE: usize = 5 * 1024 * 1024; // 5MB

/// A writer that buffers data parts in memory and uploads to S3 when finished
///
/// This implementation avoids nested runtime issues by deferring all async
/// operations to the finish() method. Parts are accumulated in memory during
/// write() calls and uploaded in a batch during finish().
pub struct S3Writer {
    /// The S3 client
    client: Arc<dyn ObjectStore>,
    /// The path in S3 to write to
    path: ObjectPath,
    /// Current buffer for accumulating data
    buffer: Vec<u8>,
    /// Completed parts ready for upload (each is at least MIN_PART_SIZE)
    parts: Vec<Bytes>,
    /// Total bytes written
    total_bytes: usize,
}

impl S3Writer {
    /// Create a new S3 writer for the given S3 URI
    ///
    /// The URI should be in the format: s3://bucket/path/to/object
    ///
    /// Authentication is handled through AWS environment variables:
    /// - AWS_ACCESS_KEY_ID
    /// - AWS_SECRET_ACCESS_KEY
    /// - AWS_REGION (optional, defaults to us-east-1)
    /// - AWS_SESSION_TOKEN (optional, for temporary credentials)
    /// - AWS_ENDPOINT (optional, for S3-compatible services)
    pub fn new(uri: &str) -> Result<Self, io::Error> {
        let url = Url::parse(uri).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid S3 URI: {}", e),
            )
        })?;

        if url.scheme() != "s3" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Expected s3:// URI, got: {}", url.scheme()),
            ));
        }

        let bucket = url.host_str().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "S3 URI missing bucket name")
        })?;

        let path = url.path().trim_start_matches('/');

        debug!(
            "Creating S3 streaming writer for bucket: {}, path: {}",
            bucket, path
        );

        // Build the S3 client using environment variables
        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

        // Try to get credentials from environment variables
        if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
            builder = builder.with_access_key_id(access_key);
        }

        if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_key);
        }

        if let Ok(region) = std::env::var("AWS_REGION") {
            builder = builder.with_region(region);
        }

        if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
            builder = builder.with_token(session_token);
        }

        if let Ok(endpoint) = std::env::var("AWS_ENDPOINT") {
            builder = builder.with_endpoint(endpoint);
        }

        let client = builder
            .build()
            .map_err(|e| io::Error::other(format!("Failed to create S3 client: {}", e)))?;

        info!(
            "S3 streaming writer created successfully for bucket: {}",
            bucket
        );

        Ok(Self {
            client: Arc::new(client),
            path: ObjectPath::from(path),
            buffer: Vec::with_capacity(S3_MIN_PART_SIZE),
            parts: Vec::new(),
            total_bytes: 0,
        })
    }

    /// Complete the upload by sending all buffered data to S3
    ///
    /// This method performs all async operations, uploading parts and completing
    /// the multipart upload. It must be called from an async context.
    pub async fn finish(mut self) -> Result<usize, io::Error> {
        debug!("Completing S3 upload: {} bytes total", self.total_bytes);

        // Add any remaining buffer data as the final part
        if !self.buffer.is_empty() {
            self.parts
                .push(Bytes::from(std::mem::take(&mut self.buffer)));
        }

        // Handle small files with simple PUT
        if self.parts.len() == 1 && self.parts[0].len() < S3_MIN_PART_SIZE {
            debug!(
                "Using simple PUT for small file: {} bytes",
                self.total_bytes
            );
            let data = self.parts.into_iter().next().unwrap();
            self.client
                .put(&self.path, data.into())
                .await
                .map_err(|e| io::Error::other(format!("Failed to upload to S3: {}", e)))?;
            info!("Successfully uploaded {} bytes to S3", self.total_bytes);
            return Ok(self.total_bytes);
        }

        // Use multipart upload for larger files
        debug!("Starting multipart upload for {} parts", self.parts.len());
        let mut upload =
            self.client.put_multipart(&self.path).await.map_err(|e| {
                io::Error::other(format!("Failed to start multipart upload: {}", e))
            })?;

        // Upload all parts
        for (i, part_data) in self.parts.into_iter().enumerate() {
            debug!("Uploading part {} ({} bytes)", i + 1, part_data.len());
            upload
                .put_part(part_data.into())
                .await
                .map_err(|e| io::Error::other(format!("Failed to upload part {}: {}", i + 1, e)))?;
        }

        // Complete the multipart upload
        upload
            .complete()
            .await
            .map_err(|e| io::Error::other(format!("Failed to complete multipart upload: {}", e)))?;

        info!(
            "Successfully uploaded {} bytes to S3 using multipart upload",
            self.total_bytes
        );
        Ok(self.total_bytes)
    }

    /// Get the total bytes written so far
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Get the buffer size (for compatibility)
    pub fn buffer_size(&self) -> usize {
        self.total_bytes
    }
}

impl Write for S3Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.total_bytes += buf.len();
        self.buffer.extend_from_slice(buf);

        // When buffer reaches our target part size (32MB), save it as a completed part
        // No async operations here - we just move data to the parts vec
        if self.buffer.len() >= PARQUET_BUFFER_SIZE {
            let part_data =
                std::mem::replace(&mut self.buffer, Vec::with_capacity(PARQUET_BUFFER_SIZE));
            self.parts.push(Bytes::from(part_data));
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // No-op: all data will be uploaded in finish()
        Ok(())
    }
}

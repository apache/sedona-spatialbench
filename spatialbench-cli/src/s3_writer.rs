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

//! S3 writer that streams multipart uploads instead of buffering in memory.
//!
//! Data is buffered in 32 MB chunks (matching [`PARQUET_BUFFER_SIZE`]). When a
//! chunk is full it is sent through an [`mpsc`] channel to a background tokio
//! task that uploads it immediately via `MultipartUpload::put_part`. This
//! keeps peak memory usage roughly constant regardless of total file size.
//!
//! [`mpsc`]: tokio::sync::mpsc

use crate::plan::PARQUET_BUFFER_SIZE;
use bytes::Bytes;
use log::{debug, info};
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use url::Url;

/// Parse an S3 URI into its (bucket, path) components.
///
/// The URI should be in the format: `s3://bucket/path/to/object`
pub fn parse_s3_uri(uri: &str) -> Result<(String, String), io::Error> {
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

    let bucket = url
        .host_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "S3 URI missing bucket name"))?
        .to_string();

    let path = url.path().trim_start_matches('/').to_string();

    Ok((bucket, path))
}

/// Build an S3 [`ObjectStore`] client for the given bucket using environment variables.
///
/// Uses [`AmazonS3Builder::from_env`] which reads all standard AWS environment
/// variables including `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
/// `AWS_DEFAULT_REGION`, `AWS_REGION`, `AWS_SESSION_TOKEN`, `AWS_ENDPOINT`, etc.
pub fn build_s3_client(bucket: &str) -> Result<Arc<dyn ObjectStore>, io::Error> {
    debug!("Building S3 client for bucket: {}", bucket);
    let client = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .build()
        .map_err(|e| io::Error::other(format!("Failed to create S3 client: {}", e)))?;
    info!("S3 client created successfully for bucket: {}", bucket);
    Ok(Arc::new(client))
}

/// Message sent from the writer thread to the background upload task.
enum UploadMessage {
    /// A completed part ready for upload.
    Part(Bytes),
    /// All parts have been sent; the upload should be completed.
    Finish,
}

/// A writer that streams data to S3 via multipart upload.
///
/// Internally, a background tokio task is spawned that starts the multipart
/// upload eagerly and uploads each part as it arrives through a channel.
/// The [`Write`] implementation buffers data in 32 MB chunks and sends
/// completed chunks to the background task via [`mpsc::Sender::blocking_send`]
/// (safe because all callers run inside [`tokio::task::spawn_blocking`]).
///
/// On [`finish`](S3Writer::finish), any remaining buffered data is sent as the
/// final part, the channel is closed, and we wait for the background task to
/// call `complete()` on the multipart upload. If any part upload fails, the
/// multipart upload is aborted to avoid orphaned uploads accruing S3 storage
/// costs.
///
/// For small files (< 5 MB total) a simple PUT is used instead of multipart.
pub struct S3Writer {
    /// The S3 client (kept for the small-file PUT fallback)
    client: Arc<dyn ObjectStore>,
    /// The object path in S3
    path: ObjectPath,
    /// Current buffer for accumulating data before sending as a part
    buffer: Vec<u8>,
    /// Total bytes written through [`Write::write`]
    total_bytes: usize,
    /// Channel to send parts to the background upload task.
    ///
    /// Set to `None` after the first part is sent (at which point the
    /// background task is spawned and this is replaced by `upload_tx`).
    /// Before any parts are sent this is `None` and parts accumulate in
    /// `pending_parts` for the small-file optimization.
    upload_tx: Option<mpsc::Sender<UploadMessage>>,
    /// Receives the final result (total bytes) from the background upload task.
    result_rx: Option<oneshot::Receiver<Result<(), io::Error>>>,
    /// Parts accumulated before we decide whether to use simple PUT or
    /// multipart upload. Once we exceed [`PARQUET_BUFFER_SIZE`] total, we switch
    /// to the streaming multipart path.
    pending_parts: Vec<Bytes>,
    /// Whether the streaming multipart upload has been started.
    multipart_started: bool,
}

impl S3Writer {
    /// Create a new S3 writer for the given S3 URI, building a fresh client.
    ///
    /// Prefer [`S3Writer::with_client`] when writing multiple files to reuse
    /// the same client.
    ///
    /// Authentication is handled through standard AWS environment variables
    /// via [`AmazonS3Builder::from_env`].
    pub fn new(uri: &str) -> Result<Self, io::Error> {
        let (bucket, path) = parse_s3_uri(uri)?;
        let client = build_s3_client(&bucket)?;
        Ok(Self::with_client(client, &path))
    }

    /// Create a new S3 writer using an existing [`ObjectStore`] client.
    ///
    /// This avoids creating a new client per file, which is important when
    /// generating many partitioned files.
    pub fn with_client(client: Arc<dyn ObjectStore>, path: &str) -> Self {
        debug!("Creating S3 writer for path: {}", path);
        Self {
            client,
            path: ObjectPath::from(path),
            buffer: Vec::with_capacity(PARQUET_BUFFER_SIZE),
            total_bytes: 0,
            upload_tx: None,
            result_rx: None,
            pending_parts: Vec::new(),
            multipart_started: false,
        }
    }

    /// Start the background multipart upload task, draining any pending parts.
    ///
    /// This is called lazily when we accumulate enough data to exceed the
    /// simple-PUT threshold. From this point on, every completed buffer is
    /// sent directly to the background task for immediate upload.
    fn start_multipart_upload(&mut self) {
        debug_assert!(!self.multipart_started, "multipart upload already started");
        self.multipart_started = true;

        // Channel capacity of 2: one part being uploaded, one buffered and ready.
        // This keeps memory bounded while allowing overlap between buffering and
        // uploading.
        let (tx, rx) = mpsc::channel::<UploadMessage>(2);
        let (result_tx, result_rx) = oneshot::channel();

        let client = Arc::clone(&self.client);
        let path = self.path.clone();
        let pending = std::mem::take(&mut self.pending_parts);

        tokio::spawn(async move {
            let result = run_multipart_upload(client, path, pending, rx).await;
            // Ignore send error — the receiver may have been dropped if the
            // writer was abandoned.
            let _ = result_tx.send(result);
        });

        self.upload_tx = Some(tx);
        self.result_rx = Some(result_rx);
    }

    /// Send a completed buffer chunk to the background upload task.
    ///
    /// If the channel is closed (because the background task failed), this
    /// attempts to retrieve the real error from `result_rx` so the caller
    /// sees the underlying S3 error rather than a generic "channel closed"
    /// message.
    fn send_part(&mut self, part: Bytes) -> io::Result<()> {
        if let Some(tx) = &self.upload_tx {
            if tx.blocking_send(UploadMessage::Part(part)).is_err() {
                // The background task has terminated — try to retrieve the
                // real error it reported before falling back to a generic msg.
                if let Some(rx) = &mut self.result_rx {
                    if let Ok(Err(e)) = rx.try_recv() {
                        return Err(io::Error::other(format!("S3 upload failed: {e}")));
                    }
                }
                return Err(io::Error::other(
                    "Background upload task terminated unexpectedly",
                ));
            }
        }
        Ok(())
    }

    /// Complete the upload by sending any remaining data and waiting for the
    /// background task to finish.
    ///
    /// For small files (total data < [`PARQUET_BUFFER_SIZE`] and fits in a single
    /// part), a simple PUT is used instead of multipart upload.
    ///
    /// This method must be called from an async context (it is typically called
    /// via [`block_on`](tokio::runtime::Handle::block_on) from inside
    /// [`spawn_blocking`](tokio::task::spawn_blocking)).
    pub async fn finish(mut self) -> Result<usize, io::Error> {
        let total = self.total_bytes;
        debug!("Completing S3 upload: {} bytes total", total);

        // Flush any remaining buffer data
        if !self.buffer.is_empty() {
            let remaining = Bytes::from(std::mem::take(&mut self.buffer));

            if self.multipart_started {
                // Send as the last part
                if let Some(tx) = &self.upload_tx {
                    tx.send(UploadMessage::Part(remaining)).await.map_err(|_| {
                        io::Error::other("Background upload task terminated unexpectedly")
                    })?;
                }
            } else {
                self.pending_parts.push(remaining);
            }
        }

        if self.multipart_started {
            // Signal the background task that we are done
            if let Some(tx) = self.upload_tx.take() {
                let _ = tx.send(UploadMessage::Finish).await;
            }
            // Wait for the background task result
            if let Some(rx) = self.result_rx.take() {
                rx.await.map_err(|_| {
                    io::Error::other("Upload task dropped without sending result")
                })??;
            }
        } else {
            // Small file path — use a simple PUT
            let data: Vec<u8> = self
                .pending_parts
                .into_iter()
                .flat_map(|b| b.to_vec())
                .collect();

            if data.is_empty() {
                debug!("No data to upload");
                return Ok(0);
            }

            debug!("Using simple PUT for small file: {} bytes", data.len());
            self.client
                .put(&self.path, Bytes::from(data).into())
                .await
                .map_err(|e| io::Error::other(format!("Failed to upload to S3: {}", e)))?;
        }

        info!("Successfully uploaded {} bytes to S3", total);
        Ok(total)
    }

    /// Get the total bytes written so far
    #[allow(dead_code)] // used by zone module in a later commit
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Get the buffer size (for compatibility)
    #[allow(dead_code)] // used by zone module in a later commit
    pub fn buffer_size(&self) -> usize {
        self.total_bytes
    }
}

/// Background task that runs the multipart upload.
///
/// Starts the upload, drains any pre-accumulated pending parts, then
/// continuously receives new parts from the channel and uploads them. On
/// any upload error the multipart upload is aborted to avoid orphaned
/// uploads accruing S3 storage costs.
async fn run_multipart_upload(
    client: Arc<dyn ObjectStore>,
    path: ObjectPath,
    pending_parts: Vec<Bytes>,
    mut rx: mpsc::Receiver<UploadMessage>,
) -> Result<(), io::Error> {
    debug!("Starting multipart upload for {:?}", path);
    let mut upload = client
        .put_multipart(&path)
        .await
        .map_err(|e| io::Error::other(format!("Failed to start multipart upload: {}", e)))?;

    let mut part_number: usize = 0;

    // Upload any parts that were accumulated before the task started
    for part_data in pending_parts {
        part_number += 1;
        debug!(
            "Uploading pending part {} ({} bytes)",
            part_number,
            part_data.len()
        );
        if let Err(e) = upload.put_part(part_data.into()).await {
            debug!("Part upload failed, aborting multipart upload");
            let _ = upload.abort().await;
            return Err(io::Error::other(format!(
                "Failed to upload part {}: {}",
                part_number, e
            )));
        }
    }

    // Receive and upload parts from the channel
    while let Some(msg) = rx.recv().await {
        match msg {
            UploadMessage::Part(part_data) => {
                part_number += 1;
                debug!("Uploading part {} ({} bytes)", part_number, part_data.len());
                if let Err(e) = upload.put_part(part_data.into()).await {
                    debug!("Part upload failed, aborting multipart upload");
                    let _ = upload.abort().await;
                    return Err(io::Error::other(format!(
                        "Failed to upload part {}: {}",
                        part_number, e
                    )));
                }
            }
            UploadMessage::Finish => {
                break;
            }
        }
    }

    // Complete the multipart upload
    debug!("Completing multipart upload ({} parts)", part_number);
    if let Err(e) = upload.complete().await {
        debug!("Multipart complete failed, aborting");
        // complete() consumes the upload, so we can't abort here — the upload
        // will be cleaned up by S3's lifecycle rules for incomplete uploads.
        return Err(io::Error::other(format!(
            "Failed to complete multipart upload: {}",
            e
        )));
    }

    debug!(
        "Multipart upload completed successfully ({} parts)",
        part_number
    );
    Ok(())
}

impl Write for S3Writer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.total_bytes += buf.len();
        self.buffer.extend_from_slice(buf);

        // When buffer reaches our target part size (32MB), send it as a part
        if self.buffer.len() >= PARQUET_BUFFER_SIZE {
            let part_data = Bytes::from(std::mem::replace(
                &mut self.buffer,
                Vec::with_capacity(PARQUET_BUFFER_SIZE),
            ));

            if self.multipart_started {
                // Stream directly to the background upload task
                self.send_part(part_data)?;
            } else {
                // Accumulate until we know whether this will be a small file
                self.pending_parts.push(part_data);

                // We now have at least 32MB, which exceeds the 5MB simple PUT
                // threshold — switch to streaming multipart upload
                self.start_multipart_upload();
            }
        }

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        // No-op: all data will be uploaded in finish()
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    // ---- parse_s3_uri tests ----

    #[test]
    fn parse_s3_uri_valid() {
        let (bucket, path) = parse_s3_uri("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(path, "path/to/file.parquet");
    }

    #[test]
    fn parse_s3_uri_nested_path() {
        let (bucket, path) = parse_s3_uri("s3://bucket/a/b/c/d/file.parquet").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(path, "a/b/c/d/file.parquet");
    }

    #[test]
    fn parse_s3_uri_no_path() {
        let (bucket, path) = parse_s3_uri("s3://bucket").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(path, "");
    }

    #[test]
    fn parse_s3_uri_trailing_slash() {
        let (bucket, path) = parse_s3_uri("s3://bucket/prefix/").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(path, "prefix/");
    }

    #[test]
    fn parse_s3_uri_wrong_scheme() {
        let err = parse_s3_uri("https://bucket/path").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("Expected s3://"));
    }

    #[test]
    fn parse_s3_uri_invalid_uri() {
        let err = parse_s3_uri("not a uri at all").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("Invalid S3 URI"));
    }

    // ---- S3Writer tests using InMemory object store ----

    #[tokio::test]
    async fn write_small_file() {
        let store = Arc::new(InMemory::new());
        let mut writer = S3Writer::with_client(store.clone(), "output/test.parquet");

        let data = b"hello world";
        writer.write_all(data).unwrap();

        let total = writer.finish().await.unwrap();
        assert_eq!(total, data.len());

        // Verify the data arrived in the store
        let result = store
            .get(&ObjectPath::from("output/test.parquet"))
            .await
            .unwrap();
        let stored = result.bytes().await.unwrap();
        assert_eq!(stored.as_ref(), data);
    }

    #[tokio::test]
    async fn write_empty_file() {
        let store = Arc::new(InMemory::new());
        let writer = S3Writer::with_client(store.clone(), "output/empty.parquet");

        let total = writer.finish().await.unwrap();
        assert_eq!(total, 0);

        // Nothing should be written to the store
        let result = store.get(&ObjectPath::from("output/empty.parquet")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn write_large_file_triggers_multipart() {
        let store = Arc::new(InMemory::new());
        let mut writer = S3Writer::with_client(store.clone(), "output/large.parquet");

        // Write more than PARQUET_BUFFER_SIZE (32MB) to trigger multipart
        let chunk = vec![0xABu8; 1024 * 1024]; // 1MB chunks
        let num_chunks = 34; // 34MB total > 32MB threshold
        for _ in 0..num_chunks {
            writer.write_all(&chunk).unwrap();
        }

        let total = writer.finish().await.unwrap();
        assert_eq!(total, num_chunks * chunk.len());

        // Verify the data arrived in the store and is correct size
        let result = store
            .get(&ObjectPath::from("output/large.parquet"))
            .await
            .unwrap();
        let stored = result.bytes().await.unwrap();
        assert_eq!(stored.len(), num_chunks * chunk.len());
        // Verify all bytes are correct
        assert!(stored.iter().all(|&b| b == 0xAB));
    }

    #[tokio::test]
    async fn write_multiple_small_writes() {
        let store = Arc::new(InMemory::new());
        let mut writer = S3Writer::with_client(store.clone(), "output/multi.parquet");

        // Simulate many small writes (like a Parquet encoder would produce)
        for i in 0u8..100 {
            writer.write_all(&[i]).unwrap();
        }

        let total = writer.finish().await.unwrap();
        assert_eq!(total, 100);

        let result = store
            .get(&ObjectPath::from("output/multi.parquet"))
            .await
            .unwrap();
        let stored = result.bytes().await.unwrap();
        let expected: Vec<u8> = (0u8..100).collect();
        assert_eq!(stored.as_ref(), expected.as_slice());
    }

    #[tokio::test]
    async fn total_bytes_tracks_writes() {
        let store = Arc::new(InMemory::new());
        let mut writer = S3Writer::with_client(store, "output/track.parquet");

        assert_eq!(writer.total_bytes(), 0);

        writer.write_all(&[1, 2, 3]).unwrap();
        assert_eq!(writer.total_bytes(), 3);

        writer.write_all(&[4, 5]).unwrap();
        assert_eq!(writer.total_bytes(), 5);
    }

    /// Verify that `std::io::Write::flush()` does NOT upload data to S3.
    /// Data is only uploaded when `finish()` is called. This test guards
    /// against the bug where CSV/TBL writes were silently lost because
    /// the `WriterSink` called `flush()` (a no-op) but never `finish()`.
    #[tokio::test]
    async fn flush_does_not_upload_without_finish() {
        let store = Arc::new(InMemory::new());
        let mut writer = S3Writer::with_client(store.clone(), "output/flush_test.csv");

        let data = b"col1,col2\nfoo,bar\n";
        writer.write_all(data).unwrap();
        writer.flush().unwrap();

        // Data should NOT be in the store yet — flush is a no-op
        let result = store.get(&ObjectPath::from("output/flush_test.csv")).await;
        assert!(
            result.is_err(),
            "data should not be uploaded before finish()"
        );

        // Now call finish — data should appear
        let total = writer.finish().await.unwrap();
        assert_eq!(total, data.len());

        let result = store
            .get(&ObjectPath::from("output/flush_test.csv"))
            .await
            .unwrap();
        let stored = result.bytes().await.unwrap();
        assert_eq!(stored.as_ref(), data);
    }

    /// Simulate the `--mb-per-file 256` scenario: a large file with multiple
    /// multipart parts streamed through the channel after the initial pending
    /// parts are drained. This exercises the `send_part` → channel → background
    /// task path with several parts (like 6 × 32 MB for a ~185 MB file).
    ///
    /// Writes are done from `spawn_blocking` to match the real Parquet write
    /// path — `blocking_send` requires a non-async context.
    #[tokio::test]
    async fn write_many_parts_triggers_streaming_multipart() {
        let store = Arc::new(InMemory::new());
        let writer = S3Writer::with_client(store.clone(), "output/many_parts.parquet");

        // Write 192 MB from a blocking task. The first 32 MB goes to
        // pending_parts, then start_multipart_upload is called, and the
        // remaining 5 parts are streamed through the channel.
        let writer = tokio::task::spawn_blocking(move || {
            let mut writer = writer;
            let chunk = vec![0xCDu8; 1024 * 1024]; // 1 MB
            let total_mb = 192;
            for _ in 0..total_mb {
                writer.write_all(&chunk).unwrap();
            }
            writer
        })
        .await
        .unwrap();

        let total_mb = 192;
        let total = writer.finish().await.unwrap();
        assert_eq!(total, total_mb * 1024 * 1024);

        let result = store
            .get(&ObjectPath::from("output/many_parts.parquet"))
            .await
            .unwrap();
        let stored = result.bytes().await.unwrap();
        assert_eq!(stored.len(), total_mb * 1024 * 1024);
        assert!(stored.iter().all(|&b| b == 0xCD));
    }

    /// Write from inside `spawn_blocking` to match the real Parquet write
    /// path, where `S3Writer::write()` is called from a blocking thread and
    /// `finish()` is awaited after the blocking task returns.
    #[tokio::test]
    async fn write_from_spawn_blocking() {
        let store = Arc::new(InMemory::new());
        let writer = S3Writer::with_client(store.clone(), "output/blocking.parquet");

        // Write 96 MB (3 × 32 MB parts) from a blocking task
        let writer = tokio::task::spawn_blocking(move || {
            let mut writer = writer;
            let chunk = vec![0xEFu8; 1024 * 1024]; // 1 MB
            for _ in 0..96 {
                writer.write_all(&chunk).unwrap();
            }
            writer
        })
        .await
        .unwrap();

        let total = writer.finish().await.unwrap();
        assert_eq!(total, 96 * 1024 * 1024);

        let result = store
            .get(&ObjectPath::from("output/blocking.parquet"))
            .await
            .unwrap();
        let stored = result.bytes().await.unwrap();
        assert_eq!(stored.len(), 96 * 1024 * 1024);
        assert!(stored.iter().all(|&b| b == 0xEF));
    }
}

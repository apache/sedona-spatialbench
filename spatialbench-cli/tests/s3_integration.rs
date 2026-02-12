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

//! Integration tests for S3 output using MinIO.
//!
//! These tests are `#[ignore]`d by default and only run in CI where a MinIO
//! service container is available. They verify end-to-end S3 write support
//! by running the CLI binary against a real S3-compatible object store.
//!
//! Required environment variables (set by CI):
//! - `AWS_ACCESS_KEY_ID`
//! - `AWS_SECRET_ACCESS_KEY`
//! - `AWS_ENDPOINT` — MinIO endpoint (e.g. `http://localhost:9000`)
//! - `AWS_REGION`
//! - `AWS_ALLOW_HTTP=true`
//! - `S3_TEST_BUCKET` — bucket name to write to (must already exist)

use assert_cmd::Command;
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use std::sync::Arc;

/// Build an S3 client pointing at the MinIO instance, using the same env
/// vars that `spatialbench-cli` uses internally.
fn minio_client(bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("Failed to build MinIO client from env"),
    )
}

/// Return the test bucket name from the environment.
fn test_bucket() -> String {
    std::env::var("S3_TEST_BUCKET").expect("S3_TEST_BUCKET not set")
}

/// List all object keys under the given prefix.
async fn list_keys(client: &dyn ObjectStore, prefix: &str) -> Vec<String> {
    use futures::TryStreamExt;
    let prefix = object_store::path::Path::from(prefix);
    client
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .expect("Failed to list objects")
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect()
}

/// "Is it plugged in" check: generate Parquet output to S3 and verify the
/// files land in the bucket.
#[tokio::test]
#[ignore]
async fn s3_parquet_output() {
    let bucket = test_bucket();
    let prefix = "s3-integration-test/parquet";
    let output_dir = format!("s3://{bucket}/{prefix}/");

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .args([
            "--scale-factor",
            "0.001",
            "--tables",
            "trip",
            "--format",
            "parquet",
            "--output-dir",
            &output_dir,
        ])
        .assert()
        .success();

    let client = minio_client(&bucket);
    let keys = list_keys(client.as_ref(), prefix).await;
    assert!(
        keys.iter().any(|k| k.ends_with(".parquet")),
        "Expected at least one .parquet file in {output_dir}, found: {keys:?}"
    );

    // Verify the file is non-empty
    for key in &keys {
        let path = object_store::path::Path::from(key.as_str());
        let meta = client.head(&path).await.expect("Failed to HEAD object");
        assert!(meta.size > 0, "File {key} should be non-empty");
    }
}

/// Verify CSV output to S3 works (exercises the WriterSink → finish() path,
/// which is different from the Parquet AsyncFinalize path).
#[tokio::test]
#[ignore]
async fn s3_csv_output() {
    let bucket = test_bucket();
    let prefix = "s3-integration-test/csv";
    let output_dir = format!("s3://{bucket}/{prefix}/");

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .args([
            "--scale-factor",
            "0.001",
            "--tables",
            "trip",
            "--format",
            "csv",
            "--output-dir",
            &output_dir,
        ])
        .assert()
        .success();

    let client = minio_client(&bucket);
    let keys = list_keys(client.as_ref(), prefix).await;
    assert!(
        keys.iter().any(|k| k.ends_with(".csv")),
        "Expected at least one .csv file in {output_dir}, found: {keys:?}"
    );

    for key in &keys {
        let path = object_store::path::Path::from(key.as_str());
        let meta = client.head(&path).await.expect("Failed to HEAD object");
        assert!(meta.size > 0, "File {key} should be non-empty");
    }
}

/// Verify multi-part file generation works with S3 output.
#[tokio::test]
#[ignore]
async fn s3_parquet_multi_part_output() {
    let bucket = test_bucket();
    let prefix = "s3-integration-test/parquet-parts";
    let output_dir = format!("s3://{bucket}/{prefix}/");

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .args([
            "--scale-factor",
            "0.001",
            "--tables",
            "trip",
            "--format",
            "parquet",
            "--parts",
            "2",
            "--output-dir",
            &output_dir,
        ])
        .assert()
        .success();

    let client = minio_client(&bucket);
    let keys = list_keys(client.as_ref(), prefix).await;
    let parquet_keys: Vec<_> = keys.iter().filter(|k| k.ends_with(".parquet")).collect();
    assert_eq!(
        parquet_keys.len(),
        2,
        "Expected 2 .parquet files with --parts 2, found: {parquet_keys:?}"
    );
}

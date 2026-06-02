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

//! Parallel raster COG generation pipeline.
//!
//! Uses a semaphore-bounded pool of `spawn_blocking` workers, each with a
//! thread-local pixel buffer reused across COGs. Manifest entries flow back
//! through a bounded `mpsc` channel.
//!
//! For S3 output, COGs are encoded to `Vec<u8>` in memory and uploaded via
//! `object_store::put()`. For local output, bytes are written to files.

use crate::s3_writer::{build_s3_client, parse_s3_uri};

use spatialbench_raster::cog::{write_cog_bytes, CogConfig, PixelBuffer};
use spatialbench_raster::footprint::Footprint;
use spatialbench_raster::scaling::ScalingTier;
use spatialbench_raster::ManifestEntry;

use bytes::Bytes;
use log::info;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tokio::sync::{mpsc, Semaphore};

use std::cell::RefCell;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A single unit of work: generate one COG.
#[derive(Clone)]
struct CogWorkItem {
    footprint: Footprint,
    cog_id: u32,
    /// Relative path within the output directory (e.g., "pile/00000/0000.tif").
    key: String,
    config: CogConfig,
}

/// Generate COGs for all footprints in parallel and return manifest entries.
///
/// Pipeline:
/// 1. Pre-create all footprint directories (skipped for S3 output).
/// 2. Spawn all work items eagerly; semaphore gates concurrency to `num_threads`.
/// 3. Each `spawn_blocking` worker uses a `thread_local!` pixel buffer.
/// 4. COG bytes are written to local files or uploaded to S3 via `object_store`.
/// 5. Manifest entries flow through a bounded `mpsc` channel.
/// 6. A collector task drains the channel into `Vec<ManifestEntry>`.
pub async fn run_raster(
    footprints: &[Footprint],
    tier: &ScalingTier,
    cog_config: &CogConfig,
    output_dir: &str,
    num_threads: usize,
) -> io::Result<Vec<ManifestEntry>> {
    let is_s3 = output_dir.starts_with("s3://");
    let total_cogs = footprints.len() as u64 * tier.scenes_per_footprint as u64;

    // Build S3 client if needed, otherwise prepare local pile directory
    let s3 = if is_s3 {
        let (bucket, prefix) = parse_s3_uri(output_dir.trim_end_matches('/'))?;
        let client = build_s3_client(&bucket)?;
        Some((client, format!("{}/pile", prefix)))
    } else {
        None
    };

    let local_pile_dir = if !is_s3 {
        let p = Path::new(output_dir).join("pile");
        Some(p.to_string_lossy().into_owned())
    } else {
        None
    };

    // Phase 1: Pre-create directories (local only; S3 has no directories)
    if let Some(ref pile_dir) = local_pile_dir {
        for fp in footprints {
            std::fs::create_dir_all(format!("{}/{:05}", pile_dir, fp.id))?;
        }
    }

    // Phase 2: Build work items
    let mut work_items = Vec::with_capacity(total_cogs as usize);
    for fp in footprints {
        for cog_id in 0..tier.scenes_per_footprint {
            work_items.push(CogWorkItem {
                footprint: *fp,
                cog_id,
                key: format!("{:05}/{:04}.tif", fp.id, cog_id),
                config: *cog_config,
            });
        }
    }

    // Phase 3: Parallel execution
    let semaphore = Arc::new(Semaphore::new(num_threads));
    let counter = Arc::new(AtomicU64::new(0));
    let (tx, mut rx) = mpsc::channel::<ManifestEntry>(num_threads * 2);
    // Collector task
    let collector = tokio::spawn(async move {
        let mut manifest = Vec::with_capacity(total_cogs as usize);
        while let Some(entry) = rx.recv().await {
            manifest.push(entry);
        }
        manifest
    });

    // Spawn work items with backpressure; semaphore acquired before spawn
    // to prevent eagerly queuing millions of tasks in memory.
    let mut join_handles = Vec::with_capacity(work_items.len());
    for item in work_items {
        // Acquire permit BEFORE spawning to apply backpressure to the loop
        let permit = Arc::clone(&semaphore)
            .acquire_owned()
            .await
            .map_err(|e| io::Error::other(format!("semaphore closed: {e}")))?;

        let tx = tx.clone();
        let counter = Arc::clone(&counter);
        let total = total_cogs;
        let s3 = s3.clone();
        let local_pile_dir = local_pile_dir.clone();

        let handle = tokio::spawn(async move {
            let cog_output = tokio::task::spawn_blocking(move || {
                thread_local! {
                    static PIXEL_BUF: RefCell<Option<PixelBuffer>> = const { RefCell::new(None) };
                }

                PIXEL_BUF.with(|cell| {
                    let mut opt = cell.borrow_mut();
                    let buf = opt.get_or_insert_with(|| PixelBuffer::new(item.config.dtype));
                    let bytes = write_cog_bytes(&item.config, &item.footprint, item.cog_id, buf)?;

                    Ok::<(Vec<u8>, String, ManifestEntry), io::Error>((
                        bytes,
                        item.key,
                        ManifestEntry {
                            footprint_id: item.footprint.id,
                            cog_id: item.cog_id,
                            bbox_4326: item.footprint.bbox_4326,
                            epsg: item.footprint.epsg,
                        },
                    ))
                })
            })
            .await
            .map_err(|e| io::Error::other(format!("blocking task panicked: {e}")))??;

            // Release permit after generation, before I/O — lets other workers
            // start generating while this task uploads.
            drop(permit);

            let (bytes, key, entry) = cog_output;

            // Write output: S3 upload or local file
            if let Some((ref client, ref prefix)) = s3 {
                let path = ObjectPath::from(format!("{}/{}", prefix, key));
                let payload = Bytes::from(bytes);
                client
                    .put(&path, payload.into())
                    .await
                    .map_err(|e| io::Error::other(format!("S3 PUT failed: {e}")))?;
            } else if let Some(ref pile_dir) = local_pile_dir {
                let path = format!("{}/{}", pile_dir, key);
                tokio::fs::write(&path, &bytes).await?;
            }

            let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
            if n.is_multiple_of(100) {
                info!("generated {n}/{total} COGs");
            }

            tx.send(entry)
                .await
                .map_err(|e| io::Error::other(format!("channel send failed: {e}")))?;

            Ok::<(), io::Error>(())
        });
        join_handles.push(handle);
    }

    // Wait for all workers, propagating first error
    for handle in join_handles {
        handle
            .await
            .map_err(|e| io::Error::other(format!("task join failed: {e}")))??;
    }

    // Drop sender so collector finishes
    drop(tx);
    let manifest = collector
        .await
        .map_err(|e| io::Error::other(format!("collector task failed: {e}")))?;

    info!(
        "generated {}/{} COGs (complete)",
        counter.load(Ordering::Relaxed),
        total_cogs
    );

    Ok(manifest)
}

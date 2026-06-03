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
//! A bounded two-stage `futures` stream: a CPU stage generates COG bytes in
//! `spawn_blocking` workers (each with a thread-local pixel buffer reused
//! across COGs), and an I/O stage writes them to a local directory or uploads
//! to S3. The two stages have independent concurrency limits, so peak memory
//! is a function of concurrency — not the total number of COGs — making
//! multi-TB runs memory-safe regardless of size.
//!
//! With `resume`, COGs already present at the destination are skipped (both
//! generation and upload). The returned manifest is always built from the full
//! work list, so the STAC catalog covers the whole pile even on a resume that
//! generates nothing.

use crate::s3_writer::{build_s3_client, parse_s3_uri};

use spatialbench_raster::cog::{write_cog_bytes, CogConfig};
use spatialbench_raster::footprint::Footprint;
use spatialbench_raster::scaling::ScalingTier;
use spatialbench_raster::ManifestEntry;

use bytes::Bytes;
use futures::stream::{StreamExt, TryStreamExt};
use log::info;
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore};

use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Default memory budget for in-flight COG buffers when none is supplied: 8 GiB.
const DEFAULT_RASTER_MEMORY_BUDGET_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Multipart part size for S3 uploads (matches the parquet path's 32 MiB chunk).
/// COGs larger than this upload as multipart; smaller ones use a single PUT.
const COG_UPLOAD_PART_SIZE: usize = 32 * 1024 * 1024;

/// A single unit of work: generate one COG.
#[derive(Clone)]
struct CogWorkItem {
    footprint: Footprint,
    cog_id: u32,
    /// Pile-relative key, e.g. "00000/0000.tif".
    key: String,
    config: CogConfig,
}

/// Destination for generated COG bytes. Keys are relative to the pile root,
/// formatted as "{fp:05}/{cog:04}.tif" (matches [`CogWorkItem::key`]).
enum OutputSink {
    Local {
        /// Path to the `pile` directory.
        pile_dir: String,
    },
    S3 {
        client: Arc<dyn ObjectStore>,
        /// Object-key prefix up to (not including) the trailing slash,
        /// e.g. "output/raster/pile".
        prefix: String,
    },
    /// Instrumented sink for pipeline tests: counts puts, tracks concurrent
    /// in-flight puts, and can inject latency or a failure.
    #[cfg(test)]
    Test {
        inflight: Arc<std::sync::atomic::AtomicUsize>,
        max_inflight: Arc<std::sync::atomic::AtomicUsize>,
        put_count: Arc<AtomicU64>,
        /// 1-based put index that should return an error (`None` = never).
        fail_at: Option<u64>,
        delay_ms: u64,
    },
}

impl OutputSink {
    /// Write one COG. `key` is the pile-relative key, e.g. "00007/0003.tif".
    async fn put(&self, key: &str, bytes: Vec<u8>) -> io::Result<()> {
        match self {
            OutputSink::Local { pile_dir } => {
                let path = format!("{pile_dir}/{key}");
                tokio::fs::write(&path, &bytes).await
            }
            OutputSink::S3 { client, prefix } => {
                let path = ObjectPath::from(format!("{prefix}/{key}"));
                upload_s3(client, &path, Bytes::from(bytes)).await
            }
            #[cfg(test)]
            OutputSink::Test {
                inflight,
                max_inflight,
                put_count,
                fail_at,
                delay_ms,
            } => {
                let cur = inflight.fetch_add(1, Ordering::SeqCst) + 1;
                max_inflight.fetch_max(cur, Ordering::SeqCst);
                if *delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
                }
                let n = put_count.fetch_add(1, Ordering::SeqCst) + 1;
                inflight.fetch_sub(1, Ordering::SeqCst);
                if Some(n) == *fail_at {
                    return Err(io::Error::other("injected put failure"));
                }
                Ok(())
            }
        }
    }

    /// Return the set of pile-relative keys ("{fp:05}/{cog:04}.tif") that
    /// already exist at this destination. Used only for `resume`.
    async fn existing_keys(&self) -> io::Result<HashSet<String>> {
        match self {
            OutputSink::Local { pile_dir } => local_existing_keys(pile_dir),
            OutputSink::S3 { client, prefix } => s3_existing_keys(client, prefix).await,
            #[cfg(test)]
            OutputSink::Test { .. } => Ok(HashSet::new()),
        }
    }
}

/// List existing pile objects in S3 with one paginated LIST and strip the
/// prefix down to pile-relative `.tif` keys.
async fn s3_existing_keys(
    client: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> io::Result<HashSet<String>> {
    let list_prefix = ObjectPath::from(prefix);
    let mut stream = client.list(Some(&list_prefix));
    let strip = format!("{prefix}/");
    let mut keys = HashSet::new();
    while let Some(meta) = stream.next().await {
        let meta = meta.map_err(|e| io::Error::other(format!("S3 LIST failed: {e}")))?;
        if let Some(rel) = meta.location.as_ref().strip_prefix(&strip) {
            if rel.ends_with(".tif") {
                keys.insert(rel.to_string());
            }
        }
    }
    Ok(keys)
}

/// Upload one COG to S3. Objects larger than [`COG_UPLOAD_PART_SIZE`] use a
/// multipart upload with sequential parts, so each request is small enough to
/// complete under the per-request retry timeout and is independently
/// retryable — the same resilience the parquet table path gets from `S3Writer`.
/// A single 124 MB PUT, by contrast, fails as a whole on a slow/contended link.
async fn upload_s3(
    client: &Arc<dyn ObjectStore>,
    path: &ObjectPath,
    data: Bytes,
) -> io::Result<()> {
    if data.len() <= COG_UPLOAD_PART_SIZE {
        return client
            .put(path, data.into())
            .await
            .map(|_| ())
            .map_err(|e| io::Error::other(format!("S3 PUT failed: {e}")));
    }

    let mut upload = client
        .put_multipart(path)
        .await
        .map_err(|e| io::Error::other(format!("S3 multipart init failed: {e}")))?;

    let mut offset = 0;
    while offset < data.len() {
        let end = (offset + COG_UPLOAD_PART_SIZE).min(data.len());
        let part = data.slice(offset..end);
        if let Err(e) = upload.put_part(part.into()).await {
            // Abort so the incomplete upload doesn't accrue storage cost.
            let _ = upload.abort().await;
            return Err(io::Error::other(format!("S3 part upload failed: {e}")));
        }
        offset = end;
    }
    upload
        .complete()
        .await
        .map(|_| ())
        .map_err(|e| io::Error::other(format!("S3 multipart complete failed: {e}")))
}

/// Walk a local pile directory (`pile/{fp}/{cog}.tif`) collecting existing
/// pile-relative keys. A missing pile directory yields an empty set.
fn local_existing_keys(pile_dir: &str) -> io::Result<HashSet<String>> {
    let mut keys = HashSet::new();
    let root = Path::new(pile_dir);
    if !root.exists() {
        return Ok(keys);
    }
    for fp_entry in std::fs::read_dir(root)? {
        let fp_entry = fp_entry?;
        if !fp_entry.file_type()?.is_dir() {
            continue;
        }
        let fp_name = fp_entry.file_name();
        let fp_name = fp_name.to_string_lossy();
        for cog_entry in std::fs::read_dir(fp_entry.path())? {
            let cog_entry = cog_entry?;
            let cog_name = cog_entry.file_name();
            let cog_name = cog_name.to_string_lossy();
            if cog_name.ends_with(".tif") {
                keys.insert(format!("{fp_name}/{cog_name}"));
            }
        }
    }
    Ok(keys)
}

/// Resolved concurrency limits for the two pipeline stages.
#[derive(Debug, Clone, Copy)]
struct RasterConcurrency {
    /// CPU-bound generation stage.
    gen: usize,
    /// Network/memory-bound output stage.
    upload: usize,
}

impl RasterConcurrency {
    /// Derive limits.
    ///
    /// - `gen` = `num_threads`.
    /// - `upload` = `clamp(memory_budget / est_cog_bytes, gen, 4 * gen)`,
    ///   unless `upload_override` is `Some`, which wins (clamped to `>= 1`).
    ///
    /// Upload concurrency is governed by the memory budget rather than a fixed
    /// network cap: multipart upload (see [`upload_s3`]) keeps each request
    /// small, so many concurrent uploads no longer risk per-request timeouts on
    /// a slow link — the binding constraint reverts to in-flight memory. The
    /// `4 * gen` ceiling is plenty to saturate an in-region EC2→S3 pipe;
    /// bandwidth-limited hosts can lower it via `--raster-upload-concurrency`.
    ///
    /// `est_cog_bytes` is the conservative per-in-flight-COG memory unit (raw
    /// pixel size); raw ≥ compressed output, so this keeps peak memory under
    /// budget even for incompressible data.
    fn resolve(
        num_threads: usize,
        est_cog_bytes: u64,
        memory_budget_bytes: u64,
        upload_override: Option<usize>,
    ) -> Self {
        let gen = num_threads.max(1);
        let upload = match upload_override {
            Some(u) => u.max(1),
            None => ((memory_budget_bytes / est_cog_bytes.max(1)) as usize).clamp(gen, gen * 4),
        };
        Self { gen, upload }
    }
}

/// Raw bytes of one COG's pixel buffer — the dominant in-flight memory unit.
fn est_cog_bytes(cog: &CogConfig) -> u64 {
    cog.raster.cog_width as u64 * cog.raster.cog_height as u64 * cog.dtype.bytes_per_pixel() as u64
}

/// Build the full work list and the full deterministic manifest.
///
/// The manifest is independent of which COGs are later skipped or generated,
/// so the STAC catalog always describes the entire pile.
fn build_work_and_manifest(
    footprints: &[Footprint],
    tier: &ScalingTier,
    cog_config: &CogConfig,
) -> (Vec<CogWorkItem>, Vec<ManifestEntry>) {
    let cap = footprints.len() * tier.scenes_per_footprint as usize;
    let mut work_items = Vec::with_capacity(cap);
    let mut manifest = Vec::with_capacity(cap);
    for fp in footprints {
        for cog_id in 0..tier.scenes_per_footprint {
            work_items.push(CogWorkItem {
                footprint: *fp,
                cog_id,
                key: format!("{:05}/{:04}.tif", fp.id, cog_id),
                config: *cog_config,
            });
            manifest.push(ManifestEntry {
                footprint_id: fp.id,
                cog_id,
                bbox_4326: fp.bbox_4326,
                epsg: fp.epsg,
            });
        }
    }
    (work_items, manifest)
}

/// Drop work items whose key already exists at the destination.
fn filter_pending(work: Vec<CogWorkItem>, existing: &HashSet<String>) -> Vec<CogWorkItem> {
    work.into_iter()
        .filter(|w| !existing.contains(&w.key))
        .collect()
}

/// Generate one COG's bytes. Noise is generated per tile inside
/// `write_cog_bytes`, so there is no large per-COG buffer to pool.
fn generate_cog(item: CogWorkItem) -> io::Result<(Vec<u8>, String)> {
    let bytes = write_cog_bytes(&item.config, &item.footprint, item.cog_id)?;
    Ok((bytes, item.key))
}

/// Run the bounded two-stage pipeline over `items`, writing to `sink`.
///
/// Stage 1 (CPU) generates at most `conc.gen` COGs concurrently; stage 2 (I/O)
/// writes at most `conc.upload` concurrently. Because the stages are chained,
/// the CPU stage cannot run more than ~`conc.gen` ahead of the I/O stage, so
/// peak in-flight COGs ≈ `gen + upload` — independent of `items.len()`.
/// Bytes written by [`run_pipeline`], for throughput reporting.
#[derive(Debug)]
struct UploadStats {
    cogs: u64,
    bytes: u64,
}

async fn run_pipeline(
    sink: Arc<OutputSink>,
    items: Vec<CogWorkItem>,
    conc: RasterConcurrency,
    total_cogs: u64,
) -> io::Result<UploadStats> {
    let counter = Arc::new(AtomicU64::new(0));
    let bytes_total = Arc::new(AtomicU64::new(0));

    futures::stream::iter(items.into_iter())
        // Stage 1 (CPU): generate bytes off the async runtime.
        .map(|item| async move {
            tokio::task::spawn_blocking(move || generate_cog(item))
                .await
                .map_err(|e| io::Error::other(format!("blocking task panicked: {e}")))?
        })
        .buffer_unordered(conc.gen)
        // Stage 2 (I/O): write/upload, bounded independently.
        .map(|res: io::Result<(Vec<u8>, String)>| {
            let sink = Arc::clone(&sink);
            let counter = Arc::clone(&counter);
            let bytes_total = Arc::clone(&bytes_total);
            async move {
                let (bytes, key) = res?;
                let len = bytes.len() as u64;
                sink.put(&key, bytes).await?;
                bytes_total.fetch_add(len, Ordering::Relaxed);
                let n = counter.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_multiple_of(16) {
                    info!("generated {n}/{total_cogs} COGs");
                }
                Ok::<(), io::Error>(())
            }
        })
        .buffer_unordered(conc.upload)
        .try_for_each(|()| async { Ok(()) })
        .await?;

    Ok(UploadStats {
        cogs: counter.load(Ordering::Relaxed),
        bytes: bytes_total.load(Ordering::Relaxed),
    })
}

/// Parameters for [`run_raster`].
pub struct RunRasterArgs<'a> {
    pub footprints: &'a [Footprint],
    pub tier: &'a ScalingTier,
    pub cog_config: &'a CogConfig,
    pub output_dir: &'a str,
    pub num_threads: usize,
    /// Skip COGs already present at the destination (LIST/walk on start).
    pub resume: bool,
    /// Explicit output-stage concurrency; `None` auto-derives from budget.
    pub upload_concurrency: Option<usize>,
    /// In-flight memory budget in bytes; `None` uses the 8 GiB default.
    pub memory_budget_bytes: Option<u64>,
}

/// Generate COGs for all footprints and return manifest entries.
///
/// See the module docs for the pipeline shape. Output goes to a local `pile`
/// directory or, for `s3://` URIs, to S3.
pub async fn run_raster(args: RunRasterArgs<'_>) -> io::Result<Vec<ManifestEntry>> {
    let RunRasterArgs {
        footprints,
        tier,
        cog_config,
        output_dir,
        num_threads,
        resume,
        upload_concurrency,
        memory_budget_bytes,
    } = args;

    // Build the output sink.
    let is_s3 = output_dir.starts_with("s3://");
    let sink = if is_s3 {
        let (bucket, prefix) = parse_s3_uri(output_dir.trim_end_matches('/'))?;
        let client = build_s3_client(&bucket)?;
        OutputSink::S3 {
            client,
            prefix: format!("{prefix}/pile"),
        }
    } else {
        let pile_dir = Path::new(output_dir)
            .join("pile")
            .to_string_lossy()
            .into_owned();
        OutputSink::Local { pile_dir }
    };

    // Pre-create local footprint directories (S3 has no directories).
    if let OutputSink::Local { ref pile_dir } = sink {
        for fp in footprints {
            std::fs::create_dir_all(format!("{pile_dir}/{:05}", fp.id))?;
        }
    }

    let (work_items, manifest) = build_work_and_manifest(footprints, tier, cog_config);
    let total_cogs = work_items.len() as u64;

    // Resume: drop already-present COGs.
    let pending = if resume {
        let existing = sink.existing_keys().await?;
        let before = work_items.len();
        let pending = filter_pending(work_items, &existing);
        info!(
            "resume: {} of {} COGs already present, generating {}",
            before - pending.len(),
            before,
            pending.len()
        );
        pending
    } else {
        work_items
    };

    let conc = RasterConcurrency::resolve(
        num_threads,
        est_cog_bytes(cog_config),
        memory_budget_bytes.unwrap_or(DEFAULT_RASTER_MEMORY_BUDGET_BYTES),
        upload_concurrency,
    );
    info!(
        "raster pipeline: gen_concurrency={}, upload_concurrency={}, pending={}",
        conc.gen,
        conc.upload,
        pending.len()
    );

    let start = Instant::now();
    let stats = run_pipeline(Arc::new(sink), pending, conc, total_cogs).await?;
    let elapsed = start.elapsed();

    // End-of-run throughput summary — the headline metric for S3 generation.
    let secs = elapsed.as_secs_f64().max(1e-9);
    info!(
        "raster complete: {} COGs, {:.1} GB in {:.0?} — {:.1} COGs/s, {:.0} MB/s",
        stats.cogs,
        stats.bytes as f64 / 1e9,
        elapsed,
        stats.cogs as f64 / secs,
        stats.bytes as f64 / 1e6 / secs,
    );
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use spatialbench_raster::cog::RasterDtype;
    use spatialbench_raster::footprint::FootprintConfig;
    use std::sync::atomic::AtomicUsize;

    fn tiny_cog_config() -> CogConfig {
        CogConfig {
            raster: FootprintConfig {
                cog_width: 64,
                cog_height: 64,
                resolution: 60,
            },
            tile_size: 32,
            noise_frequency: 4.0,
            dtype: RasterDtype::UInt8,
            zstd_level: 1,
        }
    }

    fn footprint(id: u32) -> Footprint {
        Footprint {
            id,
            epsg: 32614,
            origin: (500_000.0, 4_000_000.0),
            bbox_4326: [-100.0, 35.0, -99.0, 36.0],
        }
    }

    fn tier(scenes: u32) -> ScalingTier {
        ScalingTier {
            sf: 1,
            scenes_per_footprint: scenes,
            temporal: (1, scenes),
            balanced: (1, scenes),
            wide: (1, scenes),
        }
    }

    // ---- RasterConcurrency::resolve ----

    #[test]
    fn resolve_budget_caps_low() {
        // 1 GiB / 241 MB ≈ 4, clamped up to gen.
        let c = RasterConcurrency::resolve(12, 241_000_000, 1024 * 1024 * 1024, None);
        assert_eq!(c.gen, 12);
        assert_eq!(c.upload, 12);
    }

    #[test]
    fn resolve_budget_mid() {
        let c = RasterConcurrency::resolve(12, 241_000_000, 8 * 1024 * 1024 * 1024, None);
        assert_eq!(c.upload, 35); // 8 GiB / 241 MB
    }

    #[test]
    fn resolve_budget_huge_clamps_to_4x() {
        let c = RasterConcurrency::resolve(12, 241_000_000, 1024u64.pow(4), None);
        assert_eq!(c.upload, 48);
    }

    #[test]
    fn resolve_override_wins() {
        let c = RasterConcurrency::resolve(12, 241_000_000, 8 * 1024 * 1024 * 1024, Some(5));
        assert_eq!(c.upload, 5);
    }

    #[test]
    fn resolve_override_floor_is_one() {
        let c = RasterConcurrency::resolve(12, 241_000_000, 8 * 1024 * 1024 * 1024, Some(0));
        assert_eq!(c.upload, 1);
    }

    #[test]
    fn resolve_est_zero_no_panic() {
        let c = RasterConcurrency::resolve(12, 0, 8 * 1024 * 1024 * 1024, None);
        assert_eq!(c.upload, 48);
    }

    // ---- OutputSink round-trips ----

    #[tokio::test]
    async fn s3_put_and_list_roundtrip() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = OutputSink::S3 {
            client: Arc::clone(&store),
            prefix: "out/raster/pile".to_string(),
        };
        sink.put("00000/0000.tif", vec![1, 2, 3]).await.unwrap();
        // A non-.tif object under the prefix must be excluded.
        store
            .put(
                &ObjectPath::from("out/raster/pile/_marker.txt"),
                Bytes::from_static(b"x").into(),
            )
            .await
            .unwrap();

        let keys = sink.existing_keys().await.unwrap();
        assert_eq!(keys, HashSet::from(["00000/0000.tif".to_string()]));
    }

    /// Objects larger than the part size take the multipart path; verify the
    /// reassembled object matches byte-for-byte.
    #[tokio::test]
    async fn s3_multipart_roundtrip() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = OutputSink::S3 {
            client: Arc::clone(&store),
            prefix: "out/pile".to_string(),
        };
        // 40 MB > 32 MB part size → multipart (32 MB + 8 MB).
        let data: Vec<u8> = (0..40 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        sink.put("00000/0000.tif", data.clone()).await.unwrap();

        let got = store
            .get(&ObjectPath::from("out/pile/00000/0000.tif"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(got.len(), data.len());
        assert_eq!(got.as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn local_put_and_walk_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let pile_dir = dir.path().join("pile").to_string_lossy().into_owned();
        std::fs::create_dir_all(format!("{pile_dir}/00000")).unwrap();
        std::fs::create_dir_all(format!("{pile_dir}/00001")).unwrap();
        let sink = OutputSink::Local {
            pile_dir: pile_dir.clone(),
        };
        sink.put("00000/0000.tif", vec![1]).await.unwrap();
        sink.put("00001/0003.tif", vec![2]).await.unwrap();
        // Stray non-.tif file must be excluded.
        std::fs::write(format!("{pile_dir}/00000/notes.txt"), b"x").unwrap();

        let keys = sink.existing_keys().await.unwrap();
        assert_eq!(
            keys,
            HashSet::from(["00000/0000.tif".to_string(), "00001/0003.tif".to_string()])
        );
    }

    #[tokio::test]
    async fn existing_keys_empty_destination() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let s3 = OutputSink::S3 {
            client: store,
            prefix: "out/pile".to_string(),
        };
        assert!(s3.existing_keys().await.unwrap().is_empty());

        let dir = tempfile::tempdir().unwrap();
        let local = OutputSink::Local {
            pile_dir: dir.path().join("pile").to_string_lossy().into_owned(),
        };
        assert!(local.existing_keys().await.unwrap().is_empty());
    }

    // ---- work list + manifest + resume filter ----

    #[test]
    fn work_and_manifest_cover_all_pairs() {
        let fps = [footprint(0), footprint(1)];
        let (work, manifest) = build_work_and_manifest(&fps, &tier(3), &tiny_cog_config());
        assert_eq!(work.len(), 6);
        assert_eq!(manifest.len(), 6);
        let keys: HashSet<_> = work.iter().map(|w| w.key.clone()).collect();
        for fp in 0..2 {
            for cog in 0..3 {
                assert!(keys.contains(&format!("{fp:05}/{cog:04}.tif")));
            }
        }
    }

    #[test]
    fn filter_pending_drops_existing_only() {
        let fps = [footprint(0)];
        let (work, _) = build_work_and_manifest(&fps, &tier(4), &tiny_cog_config());
        let existing = HashSet::from(["00000/0000.tif".to_string(), "00000/0002.tif".to_string()]);
        let pending = filter_pending(work, &existing);
        assert_eq!(pending.len(), 2);
        assert!(pending.iter().all(|w| !existing.contains(&w.key)));
    }

    // ---- run_raster end-to-end (local) ----

    #[tokio::test]
    async fn run_raster_local_generates_then_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().to_string_lossy().into_owned();
        let fps = [footprint(0), footprint(1)];
        let tier = tier(2);
        let cfg = tiny_cog_config();

        let args = |resume| RunRasterArgs {
            footprints: &fps,
            tier: &tier,
            cog_config: &cfg,
            output_dir: &out,
            num_threads: 2,
            resume,
            upload_concurrency: None,
            memory_budget_bytes: None,
        };

        let manifest = run_raster(args(false)).await.unwrap();
        assert_eq!(manifest.len(), 4);
        let pile = dir.path().join("pile");
        let count_tifs = || {
            let mut n = 0;
            for fp in std::fs::read_dir(&pile).unwrap() {
                for cog in std::fs::read_dir(fp.unwrap().path()).unwrap() {
                    if cog.unwrap().file_name().to_string_lossy().ends_with(".tif") {
                        n += 1;
                    }
                }
            }
            n
        };
        assert_eq!(count_tifs(), 4);

        // Resume: manifest still full, files still present (skipped).
        let manifest2 = run_raster(args(true)).await.unwrap();
        assert_eq!(manifest2.len(), 4);
        assert_eq!(count_tifs(), 4);
    }

    // ---- run_pipeline: backpressure + error propagation ----

    fn pipeline_items(n: u32) -> Vec<CogWorkItem> {
        let cfg = tiny_cog_config();
        let (work, _) = build_work_and_manifest(&[footprint(0)], &tier(n), &cfg);
        work
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipeline_upload_stage_is_bounded() {
        // Slow uploads + many items: the upload stage must never exceed its
        // configured concurrency, regardless of how many items exist. This is
        // the memory-safety invariant (output buffers live in the upload stage).
        let inflight = Arc::new(AtomicUsize::new(0));
        let max_inflight = Arc::new(AtomicUsize::new(0));
        let put_count = Arc::new(AtomicU64::new(0));
        let sink = Arc::new(OutputSink::Test {
            inflight: Arc::clone(&inflight),
            max_inflight: Arc::clone(&max_inflight),
            put_count: Arc::clone(&put_count),
            fail_at: None,
            delay_ms: 5,
        });
        let conc = RasterConcurrency { gen: 4, upload: 3 };
        let items = pipeline_items(60);
        run_pipeline(sink, items, conc, 60).await.unwrap();

        assert_eq!(put_count.load(Ordering::SeqCst), 60);
        assert!(
            max_inflight.load(Ordering::SeqCst) <= conc.upload,
            "upload in-flight {} exceeded limit {}",
            max_inflight.load(Ordering::SeqCst),
            conc.upload
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pipeline_propagates_put_error() {
        let sink = Arc::new(OutputSink::Test {
            inflight: Arc::new(AtomicUsize::new(0)),
            max_inflight: Arc::new(AtomicUsize::new(0)),
            put_count: Arc::new(AtomicU64::new(0)),
            fail_at: Some(3),
            delay_ms: 0,
        });
        let conc = RasterConcurrency { gen: 2, upload: 2 };
        let err = run_pipeline(sink, pipeline_items(20), conc, 20)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("injected put failure"));
    }
}

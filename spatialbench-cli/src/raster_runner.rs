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

//! Sequential raster COG generation runner.
//!
//! Generates COGs for all footprints × scenes, collecting [`ManifestEntry`]
//! metadata for downstream STAC catalog generation.

use spatialbench_raster::cog::{write_cog, CogConfig};
use spatialbench_raster::footprint::Footprint;
use spatialbench_raster::scaling::ScalingTier;

use log::info;

use std::io;
use std::path::Path;

/// Metadata for a single generated COG, collected for STAC catalog writing.
///
/// Does not store a `PathBuf` — the path is deterministic from
/// `(footprint_id, cog_id)` as `pile/{fp:05}/{cog:04}.tif` and
/// reconstructed when needed, avoiding heap allocations.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // Fields used in commit 4 (STAC writer)
pub struct ManifestEntry {
    /// Footprint ID (matches [`Footprint::id`]).
    pub footprint_id: u32,
    /// COG scene ID within this footprint.
    pub cog_id: u32,
    /// Bounding box in EPSG:4326: [west, south, east, north].
    pub bbox_4326: [f64; 4],
    /// EPSG code for the COG's CRS.
    pub epsg: u32,
}

/// Generate COGs for all footprints and return manifest entries.
///
/// This is the sequential version — commit 2 upgrades to parallel.
pub async fn run_raster(
    footprints: &[Footprint],
    tier: &ScalingTier,
    cog_config: &CogConfig,
    output_dir: &Path,
    _num_threads: usize,
) -> io::Result<Vec<ManifestEntry>> {
    let pile_dir = output_dir.join("pile");
    let total_cogs = footprints.len() as u64 * tier.scenes_per_footprint as u64;
    let mut manifest = Vec::with_capacity(total_cogs as usize);

    // Pre-create all footprint directories
    for fp in footprints {
        let fp_dir = pile_dir.join(format!("{:05}", fp.id));
        std::fs::create_dir_all(&fp_dir)?;
    }

    let mut count = 0u64;
    for fp in footprints {
        let fp_dir = pile_dir.join(format!("{:05}", fp.id));
        for cog_id in 0..tier.scenes_per_footprint {
            let path = fp_dir.join(format!("{:04}.tif", cog_id));
            write_cog(cog_config, fp, cog_id, &path)?;

            manifest.push(ManifestEntry {
                footprint_id: fp.id,
                cog_id,
                bbox_4326: fp.bbox_4326,
                epsg: fp.epsg,
            });

            count += 1;
            if count.is_multiple_of(100) {
                info!("generated {count}/{total_cogs} COGs");
            }
        }
    }

    info!("generated {count}/{total_cogs} COGs (complete)");
    Ok(manifest)
}

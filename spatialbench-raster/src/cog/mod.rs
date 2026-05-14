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

//! Cloud-Optimized GeoTIFF (COG) writer using GDAL.
//!
//! Generates single-band UInt8 COGs with:
//! - Per-footprint UTM CRS (EPSG:326xx)
//! - ZSTD compression
//! - 256×256 internal tiling
//! - Perlin noise pixel data for realistic compression ratios

use crate::footprint::Footprint;
use crate::noise::PerlinNoise;

use gdal::raster::RasterCreationOptions;
use gdal::spatial_ref::SpatialRef;
use gdal::{DriverManager, GeoTransform};

use std::io;
use std::path::Path;

/// Configuration for COG generation.
#[derive(Debug, Clone, Copy)]
pub struct CogConfig {
    /// Raster width in pixels.
    pub width: u32,
    /// Raster height in pixels.
    pub height: u32,
    /// Internal tile size (pixels per side).
    pub tile_size: u32,
    /// Pixel resolution in meters (for geotransform).
    pub resolution: f64,
    /// Perlin noise frequency (controls spatial detail per tile).
    pub noise_frequency: f64,
}

impl Default for CogConfig {
    fn default() -> Self {
        Self {
            width: 1830,
            height: 1830,
            resolution: 60.0,
            tile_size: 256,
            noise_frequency: 8.0,
        }
    }
}

/// Write a single COG file for a given footprint and scene ID.
///
/// The output is a Cloud-Optimized GeoTIFF with:
/// - Single band, UInt8, ZSTD compressed
/// - CRS set to the footprint's UTM zone
/// - Geotransform derived from the footprint's NW corner origin
/// - Deterministic Perlin noise pixel data seeded from `(footprint.id, cog_id)`
///
/// Internally creates a MEM dataset, populates it, then uses the COG driver's
/// `create_copy` to produce the final file (COG is a create-copy-only driver).
///
/// # Errors
///
/// Returns `io::Error` if GDAL fails to create the dataset or write raster data.
pub fn write_cog(
    config: &CogConfig,
    footprint: &Footprint,
    cog_id: u32,
    output_path: &Path,
) -> io::Result<()> {
    // Create in-memory dataset
    let mem_driver = DriverManager::get_driver_by_name("MEM").map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("GDAL MEM driver not available: {e}"),
        )
    })?;

    let mut mem_ds = mem_driver
        .create_with_band_type::<u8, _>("", config.width as usize, config.height as usize, 1)
        .map_err(|e| io::Error::other(format!("failed to create MEM dataset: {e}")))?;

    // Set CRS to footprint's UTM zone
    let srs = SpatialRef::from_epsg(footprint.epsg).map_err(|e| {
        io::Error::other(format!(
            "failed to create SRS for EPSG:{}: {e}",
            footprint.epsg
        ))
    })?;
    mem_ds
        .set_spatial_ref(&srs)
        .map_err(|e| io::Error::other(format!("failed to set CRS: {e}")))?;

    // Geotransform: origin is NW corner, positive x-res east, negative y-res south
    let gt: GeoTransform = [
        footprint.origin.0, // top-left x (easting)
        config.resolution,  // pixel width (meters)
        0.0,                // rotation
        footprint.origin.1, // top-left y (northing)
        0.0,                // rotation
        -config.resolution, // pixel height (negative = south)
    ];
    mem_ds
        .set_geo_transform(&gt)
        .map_err(|e| io::Error::other(format!("failed to set geotransform: {e}")))?;

    // Generate pixel data with Perlin noise
    let seed = (footprint.id as u64) << 32 | cog_id as u64;
    let noise = PerlinNoise::new(seed);
    let pixels = noise.generate_raster(config.width, config.height, config.noise_frequency);

    // Write raster band
    let mut band = mem_ds
        .rasterband(1)
        .map_err(|e| io::Error::other(format!("failed to get raster band: {e}")))?;
    let mut buffer =
        gdal::raster::Buffer::new((config.width as usize, config.height as usize), pixels);
    band.write::<u8>(
        (0, 0),
        (config.width as usize, config.height as usize),
        &mut buffer,
    )
    .map_err(|e| io::Error::other(format!("failed to write pixel data: {e}")))?;

    // Create COG via create_copy from the MEM dataset
    let cog_driver = DriverManager::get_driver_by_name("COG").map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("GDAL COG driver not available: {e}"),
        )
    })?;

    let cog_options = RasterCreationOptions::from_iter([
        "COMPRESS=ZSTD",
        &format!("BLOCKSIZE={}", config.tile_size),
        "LEVEL=3",
    ]);

    mem_ds
        .create_copy(&cog_driver, output_path, &cog_options)
        .map_err(|e| io::Error::other(format!("failed to create COG: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::footprint::{lon_to_utm_zone, lonlat_to_utm, utm_to_lonlat};

    use gdal::Dataset;
    use tempfile::tempdir;

    fn test_footprint() -> Footprint {
        let lon = -100.0_f64;
        let lat = 35.0_f64;
        let zone = lon_to_utm_zone(lon);
        let (e, n) = lonlat_to_utm(lon, lat, zone);
        let step = 109_800.0;
        let nw = utm_to_lonlat(e, n, zone, true);
        let se = utm_to_lonlat(e + step, n - step, zone, true);
        Footprint {
            id: 0,
            epsg: 32600 + zone,
            origin: (e, n),
            bbox_4326: [
                nw.0.min(se.0),
                nw.1.min(se.1),
                nw.0.max(se.0),
                nw.1.max(se.1),
            ],
        }
    }

    #[test]
    fn cog_roundtrip_default_config() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tif");
        let config = CogConfig::default();
        let fp = test_footprint();

        write_cog(&config, &fp, 0, &path).unwrap();

        // Verify with GDAL
        let ds = Dataset::open(&path).unwrap();
        let (w, h) = ds.raster_size();
        assert_eq!(w, 1830);
        assert_eq!(h, 1830);
        assert_eq!(ds.raster_count(), 1);

        // Verify CRS
        let srs = ds.spatial_ref().unwrap();
        assert_eq!(srs.auth_code().unwrap(), fp.epsg as i32);

        // Verify geotransform
        let gt = ds.geo_transform().unwrap();
        assert!((gt[0] - fp.origin.0).abs() < 1e-6);
        assert!((gt[3] - fp.origin.1).abs() < 1e-6);
        assert!((gt[1] - 60.0).abs() < 1e-6);
        assert!((gt[5] - (-60.0)).abs() < 1e-6);
    }

    #[test]
    fn cog_deterministic() {
        let dir = tempdir().unwrap();
        let config = CogConfig::default();
        let fp = test_footprint();

        let path_a = dir.path().join("a.tif");
        let path_b = dir.path().join("b.tif");
        write_cog(&config, &fp, 7, &path_a).unwrap();
        write_cog(&config, &fp, 7, &path_b).unwrap();

        let a = std::fs::read(&path_a).unwrap();
        let b = std::fs::read(&path_b).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn cog_different_seeds_differ() {
        let dir = tempdir().unwrap();
        let config = CogConfig::default();
        let fp = test_footprint();

        let path_a = dir.path().join("a.tif");
        let path_b = dir.path().join("b.tif");
        write_cog(&config, &fp, 0, &path_a).unwrap();
        write_cog(&config, &fp, 1, &path_b).unwrap();

        let a = std::fs::read(&path_a).unwrap();
        let b = std::fs::read(&path_b).unwrap();
        assert_ne!(a, b);
    }
}

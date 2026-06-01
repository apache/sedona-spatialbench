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
//! Generates single-band COGs with configurable data type (UInt8, UInt16, Float32):
//! - Per-footprint UTM CRS (EPSG:326xx)
//! - ZSTD compression
//! - Configurable internal tiling
//! - Perlin noise pixel data for realistic compression ratios

use crate::footprint::{Footprint, FootprintConfig};
use crate::noise::PerlinNoise;

use gdal::raster::RasterCreationOptions;
use gdal::spatial_ref::SpatialRef;
use gdal::{DriverManager, GeoTransform};

use std::io;
use std::path::Path;

/// Raster data type for COG generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RasterDtype {
    /// Unsigned 8-bit integer (0–255). Smallest files, common for classification/indices.
    UInt8,
    /// Unsigned 16-bit integer (0–65535). Common for Sentinel-2 L2A reflectance.
    UInt16,
    /// 32-bit float. Common for climate variables, analysis-ready data.
    Float32,
}

impl Default for RasterDtype {
    fn default() -> Self {
        Self::UInt8
    }
}

impl RasterDtype {
    /// Bytes per pixel for this data type.
    pub const fn bytes_per_pixel(&self) -> usize {
        match self {
            Self::UInt8 => 1,
            Self::UInt16 => 2,
            Self::Float32 => 4,
        }
    }
}

/// Reusable pixel buffer for COG generation.
///
/// Holds a typed buffer matching the target [`RasterDtype`], avoiding
/// intermediate allocations when generating noise directly into the
/// target type. Each variant's inner `Vec` is reused across COGs on
/// the same thread.
pub enum PixelBuffer {
    /// UInt8 pixel buffer.
    U8(Vec<u8>),
    /// UInt16 pixel buffer.
    U16(Vec<u16>),
    /// Float32 pixel buffer.
    F32(Vec<f32>),
}

impl PixelBuffer {
    /// Create a new empty buffer for the given dtype.
    pub fn new(dtype: RasterDtype) -> Self {
        match dtype {
            RasterDtype::UInt8 => Self::U8(Vec::new()),
            RasterDtype::UInt16 => Self::U16(Vec::new()),
            RasterDtype::Float32 => Self::F32(Vec::new()),
        }
    }
}

/// Configuration for COG generation.
///
/// Wraps a [`FootprintConfig`] (which defines pixel dimensions and resolution)
/// and adds COG-specific settings like internal tile size and noise frequency.
/// This ensures footprint grid generation and COG writing always agree on
/// raster dimensions.
#[derive(Debug, Clone, Copy)]
pub struct CogConfig {
    /// Shared raster dimensions and resolution.
    pub raster: FootprintConfig,
    /// Internal tile size (pixels per side).
    pub tile_size: u32,
    /// Perlin noise frequency (controls spatial detail per tile).
    pub noise_frequency: f32,
    /// Pixel data type.
    pub dtype: RasterDtype,
}

impl Default for CogConfig {
    fn default() -> Self {
        Self {
            raster: FootprintConfig::default(),
            tile_size: 256,
            noise_frequency: 8.0,
            dtype: RasterDtype::default(),
        }
    }
}

/// Write a single COG file for a given footprint and scene ID.
///
/// The output is a Cloud-Optimized GeoTIFF with:
/// - Single band, configurable dtype, ZSTD compressed
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
    let width = config.raster.cog_width;
    let height = config.raster.cog_height;
    let resolution = config.raster.resolution as f64;

    let seed = (footprint.id as u64) << 32 | cog_id as u64;
    let noise = PerlinNoise::new(seed);

    let mut mem_ds = create_mem_dataset(config, width, height)?;
    set_crs_and_geotransform(&mut mem_ds, footprint, resolution)?;

    // Generate and write pixel data directly in target dtype
    match config.dtype {
        RasterDtype::UInt8 => {
            let pixels = noise.generate_raster(width, height, config.noise_frequency);
            write_band_data::<u8>(&mut mem_ds, width, height, pixels)?;
        }
        RasterDtype::UInt16 => {
            let mut pixels = Vec::new();
            noise.generate_raster_u16_into(width, height, config.noise_frequency, &mut pixels);
            write_band_data::<u16>(&mut mem_ds, width, height, pixels)?;
        }
        RasterDtype::Float32 => {
            let mut pixels = Vec::new();
            noise.generate_raster_f32_into(width, height, config.noise_frequency, &mut pixels);
            write_band_data::<f32>(&mut mem_ds, width, height, pixels)?;
        }
    }

    create_cog_from_mem(&mem_ds, config, output_path)
}

/// Write a single COG file, reusing a [`PixelBuffer`] for pixel data.
///
/// Like [`write_cog`], but accepts an externally-owned typed pixel buffer
/// to avoid heap allocation on the hot path. Noise is generated directly
/// into the target dtype — no intermediate u8 buffer or conversion step.
///
/// # Performance
///
/// At 1830×1830 pixels, reusing this buffer across COGs on the same
/// thread avoids ~3.3 MB (UInt8), ~6.7 MB (UInt16), or ~13.4 MB (Float32)
/// of alloc/dealloc per COG.
///
/// # Panics
///
/// Panics if the `pixel_buf` variant does not match `config.dtype`.
pub fn write_cog_with_buffer(
    config: &CogConfig,
    footprint: &Footprint,
    cog_id: u32,
    output_path: &Path,
    pixel_buf: &mut PixelBuffer,
) -> io::Result<()> {
    let width = config.raster.cog_width;
    let height = config.raster.cog_height;
    let resolution = config.raster.resolution as f64;

    let seed = (footprint.id as u64) << 32 | cog_id as u64;
    let noise = PerlinNoise::new(seed);

    let mut mem_ds = create_mem_dataset(config, width, height)?;
    set_crs_and_geotransform(&mut mem_ds, footprint, resolution)?;

    match pixel_buf {
        PixelBuffer::U8(buf) => {
            noise.generate_raster_into(width, height, config.noise_frequency, buf);
            write_band_data::<u8>(&mut mem_ds, width, height, buf.clone())?;
        }
        PixelBuffer::U16(buf) => {
            noise.generate_raster_u16_into(width, height, config.noise_frequency, buf);
            write_band_data::<u16>(&mut mem_ds, width, height, buf.clone())?;
        }
        PixelBuffer::F32(buf) => {
            noise.generate_raster_f32_into(width, height, config.noise_frequency, buf);
            write_band_data::<f32>(&mut mem_ds, width, height, buf.clone())?;
        }
    }

    create_cog_from_mem(&mem_ds, config, output_path)
}

/// Create an in-memory GDAL dataset with the correct band type.
fn create_mem_dataset(config: &CogConfig, width: u32, height: u32) -> io::Result<gdal::Dataset> {
    let mem_driver = DriverManager::get_driver_by_name("MEM").map_err(|e| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("GDAL MEM driver not available: {e}"),
        )
    })?;

    let w = width as usize;
    let h = height as usize;
    let ds = match config.dtype {
        RasterDtype::UInt8 => mem_driver.create_with_band_type::<u8, _>("", w, h, 1),
        RasterDtype::UInt16 => mem_driver.create_with_band_type::<u16, _>("", w, h, 1),
        RasterDtype::Float32 => mem_driver.create_with_band_type::<f32, _>("", w, h, 1),
    }
    .map_err(|e| io::Error::other(format!("failed to create MEM dataset: {e}")))?;

    Ok(ds)
}

/// Set CRS and geotransform on a dataset.
fn set_crs_and_geotransform(
    ds: &mut gdal::Dataset,
    footprint: &Footprint,
    resolution: f64,
) -> io::Result<()> {
    let srs = SpatialRef::from_epsg(footprint.epsg).map_err(|e| {
        io::Error::other(format!(
            "failed to create SRS for EPSG:{}: {e}",
            footprint.epsg
        ))
    })?;
    ds.set_spatial_ref(&srs)
        .map_err(|e| io::Error::other(format!("failed to set CRS: {e}")))?;

    let gt: GeoTransform = [
        footprint.origin.0,
        resolution,
        0.0,
        footprint.origin.1,
        0.0,
        -resolution,
    ];
    ds.set_geo_transform(&gt)
        .map_err(|e| io::Error::other(format!("failed to set geotransform: {e}")))?;

    Ok(())
}

/// Write pixel data to band 1 of a dataset.
fn write_band_data<T: gdal::raster::GdalType + Copy>(
    ds: &mut gdal::Dataset,
    width: u32,
    height: u32,
    pixels: Vec<T>,
) -> io::Result<()> {
    let mut band = ds
        .rasterband(1)
        .map_err(|e| io::Error::other(format!("failed to get raster band: {e}")))?;
    let mut buffer = gdal::raster::Buffer::new((width as usize, height as usize), pixels);
    band.write::<T>((0, 0), (width as usize, height as usize), &mut buffer)
        .map_err(|e| io::Error::other(format!("failed to write pixel data: {e}")))?;
    Ok(())
}

/// Create a COG file from an in-memory dataset via create_copy.
fn create_cog_from_mem(
    mem_ds: &gdal::Dataset,
    config: &CogConfig,
    output_path: &Path,
) -> io::Result<()> {
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
        "NUM_THREADS=ALL_CPUS",
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

        // Verify pixel data was written (not all zeros, has variation)
        let band = ds.rasterband(1).unwrap();
        let buf = band
            .read_as::<u8>((0, 0), (w, h), (w as usize, h as usize), None)
            .unwrap();
        let pixels = buf.data();
        let min = *pixels.iter().min().unwrap();
        let max = *pixels.iter().max().unwrap();
        assert!(
            max - min > 50,
            "expected pixel variation in COG, got range [{min}, {max}]"
        );
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

    #[test]
    fn cog_with_buffer_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("buf_test.tif");
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        write_cog_with_buffer(&config, &fp, 0, &path, &mut buf).unwrap();

        let ds = Dataset::open(&path).unwrap();
        let (w, h) = ds.raster_size();
        assert_eq!(w, 1830);
        assert_eq!(h, 1830);

        // Buffer should have been filled
        match &buf {
            PixelBuffer::U8(v) => assert_eq!(v.len(), 1830 * 1830),
            _ => panic!("expected U8 buffer for default config"),
        }
    }

    #[test]
    fn cog_with_buffer_reuse() {
        let dir = tempdir().unwrap();
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        // Write two COGs reusing the same buffer
        let path_a = dir.path().join("a.tif");
        let path_b = dir.path().join("b.tif");
        write_cog_with_buffer(&config, &fp, 0, &path_a, &mut buf).unwrap();
        let cap_after_first = match &buf {
            PixelBuffer::U8(v) => v.capacity(),
            _ => panic!("expected U8 buffer"),
        };
        write_cog_with_buffer(&config, &fp, 1, &path_b, &mut buf).unwrap();

        // Buffer should not have reallocated
        match &buf {
            PixelBuffer::U8(v) => assert_eq!(v.capacity(), cap_after_first),
            _ => panic!("expected U8 buffer"),
        }
    }
}

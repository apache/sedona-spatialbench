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

//! Cloud-Optimized GeoTIFF (COG) writer using pure Rust.
//!
//! Generates single-band COGs with configurable data type (UInt8, UInt16, Float32):
//! - Per-footprint UTM CRS (EPSG:326xx) via GeoTIFF tags
//! - ZSTD tile compression (tag 50000)
//! - Configurable internal tiling (default 256×256)
//! - Perlin noise pixel data for realistic compression ratios
//!
//! No C dependencies — uses the [`tiff`] crate for TIFF structure and
//! [`zstd`] for compression. Output is `Vec<u8>` suitable for writing
//! to files or uploading directly to object storage.

use crate::footprint::{Footprint, FootprintConfig};
use crate::noise::PerlinNoise;

use tiff::encoder::{TiffEncoder, TiffKindStandard};
use tiff::tags::Tag;

use std::io::{self, Cursor};
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
    /// Perlin noise frequency. Higher values raise spatial detail (entropy),
    /// which lowers the achievable compression ratio. This is the primary
    /// knob for matching real-world raster compression ratios: at 10980×10980
    /// UInt16, ~128 yields ~2× (typical Sentinel-2 L2A 10m), whereas low
    /// values like 8–16 produce unrealistically compressible (~8×) data.
    pub noise_frequency: f32,
    /// Pixel data type.
    pub dtype: RasterDtype,
    /// ZSTD compression level (1=fastest .. 22). Controls generation speed and,
    /// secondarily, ratio. Once data entropy is realistic (high frequency),
    /// higher levels buy little ratio for significant time, so 6 is a good
    /// default; raise toward 9 only if squeezing the last few percent matters.
    pub zstd_level: i32,
}

impl Default for CogConfig {
    fn default() -> Self {
        Self {
            raster: FootprintConfig::default(),
            tile_size: 256,
            noise_frequency: 8.0,
            dtype: RasterDtype::default(),
            zstd_level: 6,
        }
    }
}

/// Encode a single-band COG as in-memory bytes.
///
/// Returns the complete COG file as a `Vec<u8>`. The caller decides
/// where to write it: local file, S3 via `object_store::put()`, or
/// keep it in memory.
///
/// The output is a Cloud-Optimized GeoTIFF with:
/// - Single band, tiled layout (256×256 default), ZSTD compressed
/// - GeoTIFF CRS tags for the footprint's UTM zone
/// - Deterministic Perlin noise pixels seeded from `(footprint.id, cog_id)`
///
/// # Errors
///
/// Returns `io::Error` if TIFF encoding or ZSTD compression fails.
pub fn write_cog_bytes(
    config: &CogConfig,
    footprint: &Footprint,
    cog_id: u32,
) -> io::Result<Vec<u8>> {
    let seed = (footprint.id as u64) << 32 | cog_id as u64;
    let noise = PerlinNoise::new(seed);
    encode_cog(config, footprint, &noise)
}

/// Write a single COG file to a local path.
///
/// Convenience wrapper around [`write_cog_bytes`] that writes the
/// result to a file.
///
/// # Errors
///
/// Returns `io::Error` if encoding or file writing fails.
pub fn write_cog(
    config: &CogConfig,
    footprint: &Footprint,
    cog_id: u32,
    output_path: &Path,
) -> io::Result<()> {
    let bytes = write_cog_bytes(config, footprint, cog_id)?;
    std::fs::write(output_path, &bytes)?;
    Ok(())
}

/// Encode a COG byte buffer from a seeded noise field.
///
/// Core encoding function that generates pixels per tile (no whole-image
/// buffer) and produces a complete TIFF file in memory using the `tiff`
/// crate's `DirectoryEncoder`.
fn encode_cog(
    config: &CogConfig,
    footprint: &Footprint,
    noise: &PerlinNoise,
) -> io::Result<Vec<u8>> {
    // Pre-allocate output buffer (~1 MB typical for 1830×1830 UInt8 ZSTD)
    let mut cursor = Cursor::new(Vec::with_capacity(1024 * 1024));
    let mut encoder = TiffEncoder::new(&mut cursor)
        .map_err(|e| io::Error::other(format!("TIFF encoder init failed: {e}")))?;
    let mut dir = encoder
        .image_directory()
        .map_err(|e| io::Error::other(format!("TIFF directory init failed: {e}")))?;

    // Write image structure tags
    write_image_tags(&mut dir, config)?;

    // Write GeoTIFF tags
    write_geotiff_tags(&mut dir, config, footprint)?;

    // Compress and write tiles, collecting offsets and byte counts
    let (tile_offsets, tile_byte_counts) = write_tiles(&mut dir, config, noise)?;

    // Write tile offset/count arrays
    dir.write_tag(Tag::TileOffsets, &tile_offsets[..])
        .map_err(|e| io::Error::other(format!("failed to write tile offsets: {e}")))?;
    dir.write_tag(Tag::TileByteCounts, &tile_byte_counts[..])
        .map_err(|e| io::Error::other(format!("failed to write tile byte counts: {e}")))?;

    dir.finish()
        .map_err(|e| io::Error::other(format!("failed to finalize IFD: {e}")))?;

    let raw = cursor.into_inner();
    let reordered = reorder_ifd_before_data(raw)?;
    prepend_ghost_header(reordered)
}

/// Build the GDAL_STRUCTURAL_METADATA ghost header block.
///
/// This is a text block that GDAL writes at the start of COG files to enable
/// efficient range-request access. It contains the layout type and version,
/// plus the offset to the ghost IFD. GDAL readers use this to skip directly
/// to the real IFD.
///
/// Format: `GDAL_STRUCTURAL_METADATA_SIZE=000140 bytes\n` followed by the
/// metadata XML, padded to the declared size with null bytes.
fn prepend_ghost_header(mut data: Vec<u8>) -> io::Result<Vec<u8>> {
    // Ghost header content (matches GDAL COG driver output)
    let layout_info = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nLEADER_SIZE_AS_UINT4=0\nTRAILER_SIZE_AS_UINT4=0\nKNOWN_INCOMPATIBLE_EDITION=NO\n";

    // Pad to a fixed size for alignment (GDAL uses 140 bytes of content)
    let content_size = 140;
    let mut ghost_content = layout_info.as_bytes().to_vec();
    ghost_content.resize(content_size, 0);

    // The full ghost block: size declaration line + content
    let size_line = format!("GDAL_STRUCTURAL_METADATA_SIZE={content_size:06} bytes\n");
    let ghost_block_size = size_line.len() + content_size;

    // Shift all offsets in the existing data by ghost_block_size
    let shift = ghost_block_size;

    // Patch IFD offset in header (bytes 4-7)
    let old_ifd_offset = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let new_ifd_offset = old_ifd_offset + shift;
    data[4..8].copy_from_slice(&(new_ifd_offset as u32).to_le_bytes());

    // Patch all offsets inside the IFD
    let ifd_start = old_ifd_offset; // IFD position in the current (pre-shift) data
    if ifd_start + 2 > data.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IFD truncated in ghost header",
        ));
    }
    let tag_count = u16::from_le_bytes([data[ifd_start], data[ifd_start + 1]]) as usize;

    for i in 0..tag_count {
        let entry_offset = ifd_start + 2 + i * 12;
        if entry_offset + 12 > data.len() {
            break;
        }

        let field_type = u16::from_le_bytes([data[entry_offset + 2], data[entry_offset + 3]]);
        let count = u32::from_le_bytes([
            data[entry_offset + 4],
            data[entry_offset + 5],
            data[entry_offset + 6],
            data[entry_offset + 7],
        ]) as usize;

        let type_size = match field_type {
            1 | 2 | 6 | 7 => 1,
            3 | 8 => 2,
            4 | 9 | 11 => 4,
            5 | 10 | 12 => 8,
            _ => 4,
        };

        if count * type_size <= 4 {
            continue;
        }

        // Patch the out-of-line value offset
        let val_off_pos = entry_offset + 8;
        let old_offset = u32::from_le_bytes([
            data[val_off_pos],
            data[val_off_pos + 1],
            data[val_off_pos + 2],
            data[val_off_pos + 3],
        ]);
        let new_offset = old_offset as usize + shift;
        data[val_off_pos..val_off_pos + 4].copy_from_slice(&(new_offset as u32).to_le_bytes());

        // Patch TileOffsets/StripOffsets values
        let tag_id = u16::from_le_bytes([data[entry_offset], data[entry_offset + 1]]);
        if tag_id == 324 || tag_id == 273 {
            let arr_offset = new_offset - shift; // position in current data
            for j in 0..count {
                let pos = arr_offset + j * 4;
                if pos + 4 > data.len() {
                    break;
                }
                let old_tile_off =
                    u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]);
                let new_tile_off = old_tile_off as usize + shift;
                data[pos..pos + 4].copy_from_slice(&(new_tile_off as u32).to_le_bytes());
            }
        }
    }

    // Build final output: header(8) + ghost_block + rest_of_data(IFD + tiles)
    let mut out = Vec::with_capacity(data.len() + ghost_block_size);
    out.extend_from_slice(&data[..8]); // TIFF header with patched IFD offset
    out.extend_from_slice(size_line.as_bytes());
    out.extend_from_slice(&ghost_content);
    out.extend_from_slice(&data[8..]); // IFD + tile data
    Ok(out)
}

/// Reorder a TIFF so the IFD appears immediately after the 8-byte header,
/// before all tile data. This makes the file a strict Cloud-Optimized GeoTIFF
/// where metadata can be fetched with a single HTTP range request.
///
/// The `tiff` crate writes tile data first, then the IFD at the end.
/// This function rearranges the layout to: header → IFD → tile data,
/// patching all file offsets accordingly.
fn reorder_ifd_before_data(raw: Vec<u8>) -> io::Result<Vec<u8>> {
    if raw.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "TIFF too short"));
    }

    // Read IFD offset from header (bytes 4-7, little-endian u32)
    let ifd_offset = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]) as usize;

    // If IFD is already right after the header (offset == 8), no reordering needed
    if ifd_offset == 8 {
        return Ok(raw);
    }

    // Data between header and IFD is tile data + tag overflow values
    let header = &raw[..8];
    let tile_data = &raw[8..ifd_offset];
    let ifd_and_tail = &raw[ifd_offset..];
    let ifd_len = ifd_and_tail.len();
    let shift = ifd_len; // IFD moves forward, tile data shifts back

    // Build reordered buffer: header → IFD → tile data
    let mut out = Vec::with_capacity(raw.len());
    out.extend_from_slice(header);
    out.extend_from_slice(ifd_and_tail);
    out.extend_from_slice(tile_data);

    // Patch header: IFD now starts at offset 8
    out[4..8].copy_from_slice(&8u32.to_le_bytes());

    // Parse the IFD to patch all offset values that point into tile data.
    // IFD layout: u16 tag_count, then tag_count × 12-byte entries, then u32 next_ifd.
    let ifd_start = 8usize; // new IFD position
    if out.len() < ifd_start + 2 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "IFD truncated"));
    }
    let tag_count = u16::from_le_bytes([out[ifd_start], out[ifd_start + 1]]) as usize;

    for i in 0..tag_count {
        let entry_offset = ifd_start + 2 + i * 12;
        if entry_offset + 12 > out.len() {
            break;
        }

        let field_type = u16::from_le_bytes([out[entry_offset + 2], out[entry_offset + 3]]);
        let count = u32::from_le_bytes([
            out[entry_offset + 4],
            out[entry_offset + 5],
            out[entry_offset + 6],
            out[entry_offset + 7],
        ]) as usize;

        // Determine byte size of the value
        let type_size = match field_type {
            1 | 2 | 6 | 7 => 1, // BYTE, ASCII, SBYTE, UNDEFINED
            3 | 8 => 2,         // SHORT, SSHORT
            4 | 9 | 11 => 4,    // LONG, SLONG, FLOAT
            5 | 10 | 12 => 8,   // RATIONAL, SRATIONAL, DOUBLE
            _ => 4,
        };
        let total_bytes = count * type_size;

        if total_bytes <= 4 {
            // Value is inline in the IFD entry (no offset to patch)
            continue;
        }

        // Value is stored out-of-line; bytes 8-11 of the entry contain the file offset
        let val_off_pos = entry_offset + 8;
        let old_offset = u32::from_le_bytes([
            out[val_off_pos],
            out[val_off_pos + 1],
            out[val_off_pos + 2],
            out[val_off_pos + 3],
        ]) as usize;

        // Determine new offset: if it was in the IFD region, it moved to
        // the beginning; if it was in tile data, it shifted by ifd_len.
        let new_offset = if old_offset >= ifd_offset {
            // Was in IFD/tail region → now at (8 + offset_within_ifd_region)
            8 + (old_offset - ifd_offset)
        } else {
            // Was in tile data region → shifted forward by ifd_len
            old_offset + shift
        };

        out[val_off_pos..val_off_pos + 4].copy_from_slice(&(new_offset as u32).to_le_bytes());

        // If this is TileOffsets (tag 324) or StripOffsets (tag 273),
        // patch the individual offset values too (they point into tile data)
        let tag_id = u16::from_le_bytes([out[entry_offset], out[entry_offset + 1]]);
        if tag_id == 324 || tag_id == 273 {
            // The offset array is stored at new_offset
            for j in 0..count {
                let pos = new_offset + j * 4;
                if pos + 4 > out.len() {
                    break;
                }
                let old_tile_off =
                    u32::from_le_bytes([out[pos], out[pos + 1], out[pos + 2], out[pos + 3]]);
                // Tile data shifted by ifd_len bytes
                let new_tile_off = old_tile_off as usize + shift;
                out[pos..pos + 4].copy_from_slice(&(new_tile_off as u32).to_le_bytes());
            }
        }
    }

    Ok(out)
}

/// Write TIFF image structure tags to the directory.
fn write_image_tags(
    dir: &mut tiff::encoder::DirectoryEncoder<&mut Cursor<Vec<u8>>, TiffKindStandard>,
    config: &CogConfig,
) -> io::Result<()> {
    let map_err = |e| io::Error::other(format!("failed to write image tag: {e}"));

    dir.write_tag(Tag::ImageWidth, config.raster.cog_width)
        .map_err(map_err)?;
    dir.write_tag(Tag::ImageLength, config.raster.cog_height)
        .map_err(map_err)?;
    dir.write_tag(Tag::TileWidth, config.tile_size)
        .map_err(map_err)?;
    dir.write_tag(Tag::TileLength, config.tile_size)
        .map_err(map_err)?;

    let (bits_per_sample, sample_format): (&[u16], &[u16]) = match config.dtype {
        RasterDtype::UInt8 => (&[8], &[1]),
        RasterDtype::UInt16 => (&[16], &[1]),
        RasterDtype::Float32 => (&[32], &[3]),
    };

    dir.write_tag(Tag::BitsPerSample, bits_per_sample)
        .map_err(map_err)?;
    dir.write_tag(Tag::SamplesPerPixel, 1u16).map_err(map_err)?;
    // ZSTD compression tag = 50000 (0xC350)
    dir.write_tag(Tag::Compression, 50000u16).map_err(map_err)?;
    // MinIsBlack
    dir.write_tag(Tag::PhotometricInterpretation, 1u16)
        .map_err(map_err)?;
    dir.write_tag(Tag::SampleFormat, sample_format)
        .map_err(map_err)?;
    // Chunky (contiguous) planar configuration
    dir.write_tag(Tag::PlanarConfiguration, 1u16)
        .map_err(map_err)?;

    // Horizontal differencing predictor (2 = integer, 3 = floating-point)
    let predictor: u16 = match config.dtype {
        RasterDtype::UInt8 | RasterDtype::UInt16 => 2,
        RasterDtype::Float32 => 3,
    };
    dir.write_tag(Tag::Predictor, predictor).map_err(map_err)?;

    Ok(())
}

/// Write GeoTIFF CRS tags to the directory.
fn write_geotiff_tags(
    dir: &mut tiff::encoder::DirectoryEncoder<&mut Cursor<Vec<u8>>, TiffKindStandard>,
    config: &CogConfig,
    footprint: &Footprint,
) -> io::Result<()> {
    let map_err = |e| io::Error::other(format!("failed to write GeoTIFF tag: {e}"));

    // ModelPixelScaleTag (33550): [ScaleX, ScaleY, ScaleZ]
    let resolution = config.raster.resolution as f64;
    dir.write_tag(
        Tag::ModelPixelScaleTag,
        &[resolution, resolution, 0.0f64][..],
    )
    .map_err(map_err)?;

    // ModelTiepointTag (33922): [I, J, K, X, Y, Z]
    dir.write_tag(
        Tag::ModelTiepointTag,
        &[
            0.0f64,
            0.0,
            0.0,
            footprint.origin.0,
            footprint.origin.1,
            0.0,
        ][..],
    )
    .map_err(map_err)?;

    // GeoKeyDirectoryTag (34735): encodes CRS as GeoTIFF key entries
    #[allow(clippy::cast_possible_truncation)]
    let geo_keys: [u16; 16] = [
        1,
        1,
        0,
        3, // GeoTIFF 1.1.0, 3 keys
        1024,
        0,
        1,
        1, // GTModelTypeGeoKey = ModelTypeProjected (1)
        1025,
        0,
        1,
        1, // GTRasterTypeGeoKey = RasterPixelIsArea (1)
        3072,
        0,
        1,
        footprint.epsg as u16, // ProjectedCSTypeGeoKey = EPSG code
    ];
    dir.write_tag(Tag::GeoKeyDirectoryTag, &geo_keys[..])
        .map_err(map_err)?;

    Ok(())
}

/// Frequency search domain for ratio calibration.
const CALIB_FREQ_MIN: f32 = 2.0;
const CALIB_FREQ_MAX: f32 = 2048.0;
/// Geometric step for the coarse sweep that maps the descending arm.
const CALIB_SWEEP_STEP: f32 = 1.3;
/// Sample an N×N block of full interior tiles per probe (cheap, representative).
const CALIB_TILES: u32 = 4;
/// Bisection iterations to refine within the bracketed sub-interval.
const CALIB_ITERS: usize = 12;

/// Result of calibrating `noise_frequency` to a target compression ratio.
#[derive(Debug, Clone, Copy)]
pub struct CalibrationResult {
    /// Resolved frequency to use for generation.
    pub frequency: f32,
    /// Ratio actually achieved by the sample at `frequency`.
    pub achieved_ratio: f32,
    /// True if `target_ratio` was outside the achievable range and `frequency`
    /// was clamped to the nearest end of the achievable (smooth-noise) arm.
    pub clamped: bool,
}

/// Resolve the `noise_frequency` that yields ~`target_ratio` for this config's
/// dtype, dimensions, tile size, and zstd level.
///
/// Ratio vs frequency is **not globally monotonic**: it falls as smooth-gradient
/// detail rises, reaches a minimum, then rises again once the frequency is high
/// enough that aliasing/periodicity sets in (and collapses entirely at
/// integer-lattice frequencies, where Perlin is 0). Only the initial *descending
/// arm* is genuine smooth-noise — the realistic regime. So we walk that arm with
/// a coarse geometric sweep, stop when the ratio turns back up, and bisect within
/// it. At realistic dimensions (e.g. 10980²) the arm spans the whole useful
/// range (~1.1×–6×); small images have a shorter arm (higher minimum ratio), in
/// which case an out-of-range target clamps to the arm end.
///
/// Deterministic: fixed seed (0), fixed sample tiles, fixed sweep/bisection —
/// the same `(config, target_ratio)` always yields the same frequency. The
/// caller should log the result. Cost is a few dozen small-tile compressions
/// (sub-second), negligible vs a real run.
///
/// # Errors
///
/// Returns `io::Error` if ZSTD compression fails during a probe.
pub fn calibrate_frequency(config: &CogConfig, target_ratio: f32) -> io::Result<CalibrationResult> {
    // Coarse geometric sweep along the descending arm, stopping as soon as we
    // bracket the target (or leave the smooth arm / hit the cap). Stopping early
    // keeps both startup and tests cheap.
    let mut prev: Option<(f32, f32)> = None;
    let mut freq = CALIB_FREQ_MIN;
    loop {
        let r = measure_ratio(config, freq)?;
        match prev {
            // First probe: if the target is already at/above the highest ratio,
            // the lowest frequency is the best we can do.
            None => {
                if target_ratio >= r {
                    return Ok(CalibrationResult {
                        frequency: freq,
                        achieved_ratio: r,
                        clamped: true,
                    });
                }
            }
            Some((pf, pr)) => {
                if r >= pr {
                    // Ratio turned back up → end of the smooth arm at `prev`, and
                    // the target is below the arm's minimum → clamp to the arm end.
                    return Ok(CalibrationResult {
                        frequency: pf,
                        achieved_ratio: pr,
                        clamped: true,
                    });
                }
                if r <= target_ratio {
                    // Bracketed: ratio(pf) > target >= ratio(freq). Bisect within
                    // [pf, freq] — monotonic on the arm, so this is well-defined.
                    let (mut lo, mut hi) = (pf, freq);
                    let (mut mid, mut mid_ratio) = (freq, r);
                    for _ in 0..CALIB_ITERS {
                        mid = 0.5 * (lo + hi);
                        mid_ratio = measure_ratio(config, mid)?;
                        if mid_ratio > target_ratio {
                            lo = mid; // ratio too high → need more frequency
                        } else {
                            hi = mid; // ratio too low → back off frequency
                        }
                    }
                    return Ok(CalibrationResult {
                        frequency: mid,
                        achieved_ratio: mid_ratio,
                        clamped: false,
                    });
                }
            }
        }
        if freq >= CALIB_FREQ_MAX {
            // Hit the cap with ratio still above target → clamp to the cap.
            return Ok(CalibrationResult {
                frequency: freq,
                achieved_ratio: r,
                clamped: true,
            });
        }
        prev = Some((freq, r));
        freq = (freq * CALIB_SWEEP_STEP).min(CALIB_FREQ_MAX);
    }
}

/// Average compression ratio (raw / compressed) over a small block of full
/// interior tiles, using the exact per-tile pipeline as [`write_tiles`]
/// (noise → predictor → ZSTD). The Perlin field is statistically homogeneous,
/// so a fixed seed and any interior tiles are representative.
fn measure_ratio(config: &CogConfig, freq: f32) -> io::Result<f32> {
    let noise = PerlinNoise::new(0);
    let (w, h, ts) = (
        config.raster.cog_width,
        config.raster.cog_height,
        config.tile_size,
    );
    let bpp = config.dtype.bytes_per_pixel();
    // Clamp the sample block to the available grid (tiny configs / tests).
    let nx = CALIB_TILES.min(w.div_ceil(ts));
    let ny = CALIB_TILES.min(h.div_ceil(ts));
    let mut tile_buf = vec![0u8; ts as usize * ts as usize * bpp];
    let mut compressor = zstd::bulk::Compressor::new(config.zstd_level)
        .map_err(|e| io::Error::other(format!("ZSTD compressor init failed: {e}")))?;
    let (mut raw, mut comp) = (0usize, 0usize);
    for ty in 0..ny {
        for tx in 0..nx {
            match config.dtype {
                RasterDtype::UInt8 => {
                    noise.generate_tile_u8_into(w, h, freq, ts, tx, ty, &mut tile_buf)
                }
                RasterDtype::UInt16 => {
                    noise.generate_tile_u16_into(w, h, freq, ts, tx, ty, &mut tile_buf)
                }
                RasterDtype::Float32 => {
                    noise.generate_tile_f32_into(w, h, freq, ts, tx, ty, &mut tile_buf)
                }
            }
            apply_predictor(&mut tile_buf, ts as usize, ts as usize, config.dtype);
            let c = compressor
                .compress(&tile_buf)
                .map_err(|e| io::Error::other(format!("ZSTD compression failed: {e}")))?;
            raw += tile_buf.len();
            comp += c.len();
        }
    }
    Ok(raw as f32 / comp.max(1) as f32)
}

/// Compress and write all tiles, returning offset and byte count arrays.
///
/// Compression uses ZSTD (TIFF tag 50000) at `config.zstd_level` with a
/// horizontal differencing predictor (Predictor=2 for integers, 3 for floats).
/// Note this is more aggressive than GDAL's COG defaults (LZW, no predictor);
/// ZSTD + predictor better mimics analysis-ready imagery pipelines. The
/// achievable ratio is governed by data entropy (`noise_frequency`), not the
/// level — see [`CogConfig::noise_frequency`].
fn write_tiles(
    dir: &mut tiff::encoder::DirectoryEncoder<&mut Cursor<Vec<u8>>, TiffKindStandard>,
    config: &CogConfig,
    noise: &PerlinNoise,
) -> io::Result<(Vec<u32>, Vec<u32>)> {
    let width = config.raster.cog_width;
    let height = config.raster.cog_height;
    let ts = config.tile_size;
    let freq = config.noise_frequency;
    let bpp = config.dtype.bytes_per_pixel();
    let tiles_across = width.div_ceil(ts);
    let tiles_down = height.div_ceil(ts);
    let num_tiles = (tiles_across * tiles_down) as usize;

    let mut tile_offsets = Vec::with_capacity(num_tiles);
    let mut tile_byte_counts = Vec::with_capacity(num_tiles);

    // Pre-allocate tile buffer and ZSTD compressor (reused across all tiles).
    // Noise is generated directly into each tile, so no whole-image buffer is
    // ever materialized.
    let tile_byte_len = ts as usize * ts as usize * bpp;
    let mut tile_buf = vec![0u8; tile_byte_len];
    let mut compressor = zstd::bulk::Compressor::new(config.zstd_level)
        .map_err(|e| io::Error::other(format!("ZSTD compressor init failed: {e}")))?;

    for ty in 0..tiles_down {
        for tx in 0..tiles_across {
            match config.dtype {
                RasterDtype::UInt8 => {
                    noise.generate_tile_u8_into(width, height, freq, ts, tx, ty, &mut tile_buf)
                }
                RasterDtype::UInt16 => {
                    noise.generate_tile_u16_into(width, height, freq, ts, tx, ty, &mut tile_buf)
                }
                RasterDtype::Float32 => {
                    noise.generate_tile_f32_into(width, height, freq, ts, tx, ty, &mut tile_buf)
                }
            }
            apply_predictor(&mut tile_buf, ts as usize, ts as usize, config.dtype);
            let compressed = compressor
                .compress(&tile_buf)
                .map_err(|e| io::Error::other(format!("ZSTD compression failed: {e}")))?;

            let offset = dir
                .write_data(&compressed[..])
                .map_err(|e| io::Error::other(format!("failed to write tile data: {e}")))?;

            #[allow(clippy::cast_possible_truncation)]
            {
                tile_offsets.push(offset as u32);
                tile_byte_counts.push(compressed.len() as u32);
            }
        }
    }

    Ok((tile_offsets, tile_byte_counts))
}

/// Apply horizontal differencing predictor in-place.
///
/// For integer types (Predictor=2): each sample is replaced by the difference
/// from its left neighbor. The first sample in each row is unchanged.
///
/// For float types (Predictor=3): floating-point predictor splits bytes by
/// significance — first all MSBs, then next bytes, etc. — then applies
/// horizontal differencing on each byte plane. This is the standard TIFF
/// floating-point predictor that GDAL uses.
///
/// Both predictors produce data with lower entropy, improving ZSTD compression
/// by 20-40% for spatially correlated raster data.
fn apply_predictor(tile: &mut [u8], tile_width: usize, tile_height: usize, dtype: RasterDtype) {
    match dtype {
        RasterDtype::UInt8 => {
            // Predictor=2: horizontal differencing on bytes, right-to-left
            for row in 0..tile_height {
                let row_start = row * tile_width;
                for col in (1..tile_width).rev() {
                    let idx = row_start + col;
                    tile[idx] = tile[idx].wrapping_sub(tile[idx - 1]);
                }
            }
        }
        RasterDtype::UInt16 => {
            let bpp = 2;
            let row_bytes = tile_width * bpp;
            for row in 0..tile_height {
                let row_start = row * row_bytes;
                // Work right-to-left on 2-byte samples
                for col in (1..tile_width).rev() {
                    let cur = row_start + col * bpp;
                    let prev = row_start + (col - 1) * bpp;
                    let cur_val = u16::from_le_bytes([tile[cur], tile[cur + 1]]);
                    let prev_val = u16::from_le_bytes([tile[prev], tile[prev + 1]]);
                    let diff = cur_val.wrapping_sub(prev_val);
                    tile[cur..cur + 2].copy_from_slice(&diff.to_le_bytes());
                }
            }
        }
        RasterDtype::Float32 => {
            // Predictor=3: floating-point horizontal differencing
            // Step 1: byte-swap to big-endian (MSB first) per sample
            // Step 2: rearrange so all MSBs come first, then 2nd bytes, etc.
            // Step 3: horizontal differencing on the byte-reordered row
            let bpp = 4;
            let row_bytes = tile_width * bpp;
            let mut row_tmp = vec![0u8; row_bytes];
            for row in 0..tile_height {
                let row_start = row * row_bytes;
                let row_slice = &tile[row_start..row_start + row_bytes];

                // Rearrange: collect byte 0 of all pixels, then byte 1, etc.
                // Using big-endian byte order (MSB first) per TIFF spec
                for col in 0..tile_width {
                    let src = col * bpp;
                    let f = f32::from_le_bytes([
                        row_slice[src],
                        row_slice[src + 1],
                        row_slice[src + 2],
                        row_slice[src + 3],
                    ]);
                    let be = f.to_be_bytes();
                    for b in 0..bpp {
                        row_tmp[b * tile_width + col] = be[b];
                    }
                }

                // Horizontal differencing on the rearranged bytes
                for i in (1..row_bytes).rev() {
                    row_tmp[i] = row_tmp[i].wrapping_sub(row_tmp[i - 1]);
                }

                tile[row_start..row_start + row_bytes].copy_from_slice(&row_tmp);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::footprint::{lon_to_utm_zone, lonlat_to_utm, utm_to_lonlat};

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

    /// Encode a COG, decode it with the `tiff` crate's decoder, verify
    /// dimensions and tile layout.
    #[test]
    fn cog_bytes_valid_tiff() {
        let config = CogConfig::default();
        let fp = test_footprint();

        let bytes = write_cog_bytes(&config, &fp, 0).unwrap();

        // Verify TIFF header magic bytes (little-endian: 0x49 0x49 0x2A 0x00)
        assert!(bytes.len() > 8);
        assert_eq!(bytes[0], 0x49); // 'I' - little-endian
        assert_eq!(bytes[1], 0x49); // 'I'
        assert_eq!(bytes[2], 0x2A); // TIFF magic
        assert_eq!(bytes[3], 0x00);
    }

    /// Same inputs → same bytes.
    #[test]
    fn cog_bytes_deterministic() {
        let config = CogConfig::default();
        let fp = test_footprint();

        let a = write_cog_bytes(&config, &fp, 7).unwrap();
        let b = write_cog_bytes(&config, &fp, 7).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn cog_bytes_different_seeds_differ() {
        let config = CogConfig::default();
        let fp = test_footprint();

        let a = write_cog_bytes(&config, &fp, 0).unwrap();
        let b = write_cog_bytes(&config, &fp, 1).unwrap();
        assert_ne!(a, b);
    }

    /// Byte-for-byte guard against the pre-per-tile implementation. Hashes are
    /// captured from `main` (whole-image generate + extract path) for a fixed
    /// config; per-tile generation must reproduce them exactly. If these break,
    /// the COG output changed — investigate before updating the constants.
    #[test]
    fn cog_bytes_golden() {
        use std::hash::{Hash, Hasher};
        let fp = Footprint {
            id: 3,
            epsg: 32614,
            origin: (500_000.0, 4_000_000.0),
            bbox_4326: [-100.0, 35.0, -99.0, 36.0],
        };
        let cases = [
            (RasterDtype::UInt8, 7826usize, 0x3722b80ef66da018u64),
            (RasterDtype::UInt16, 19109, 0x9d8a2712b512d47c),
            (RasterDtype::Float32, 32448, 0x14616d811c77befe),
        ];
        for (dtype, len, hash) in cases {
            let config = CogConfig {
                raster: FootprintConfig {
                    cog_width: 100,
                    cog_height: 100,
                    resolution: 60,
                },
                tile_size: 32,
                noise_frequency: 8.0,
                dtype,
                zstd_level: 6,
            };
            let bytes = write_cog_bytes(&config, &fp, 5).unwrap();
            assert_eq!(bytes.len(), len, "{dtype:?} length changed");
            let mut h = std::collections::hash_map::DefaultHasher::new();
            bytes.hash(&mut h);
            assert_eq!(h.finish(), hash, "{dtype:?} bytes changed");
        }
    }

    fn calib_config(dtype: RasterDtype) -> CogConfig {
        // 2048×2048 / tile 512 → a 4×4 block of full interior tiles; fast.
        CogConfig {
            // Real Sentinel-2-sized image: the descending (smooth-noise) arm
            // spans the whole realistic ratio range here. measure_ratio samples
            // a fixed 4×4 tile block regardless of dimensions, so this is fast.
            raster: FootprintConfig {
                cog_width: 10980,
                cog_height: 10980,
                resolution: 10,
            },
            tile_size: 512,
            noise_frequency: 8.0, // ignored by calibration
            dtype,
            zstd_level: 3,
        }
    }

    #[test]
    fn calibrate_hits_target() {
        let cal = calibrate_frequency(&calib_config(RasterDtype::UInt16), 2.0).unwrap();
        assert!(!cal.clamped, "2.0x should be reachable at 10980²");
        assert!(
            (cal.achieved_ratio - 2.0).abs() / 2.0 < 0.10,
            "achieved {} not within 10% of 2.0",
            cal.achieved_ratio
        );
        assert!(
            cal.frequency > CALIB_FREQ_MIN && cal.frequency < CALIB_FREQ_MAX,
            "frequency {} should be interior",
            cal.frequency
        );
    }

    #[test]
    fn calibrate_is_deterministic() {
        let cfg = calib_config(RasterDtype::UInt16);
        let a = calibrate_frequency(&cfg, 2.0).unwrap();
        let b = calibrate_frequency(&cfg, 2.0).unwrap();
        assert_eq!(a.frequency.to_bits(), b.frequency.to_bits());
    }

    #[test]
    fn calibrate_clamps_unreachable_high() {
        // No frequency makes smooth Perlin compress 100x → clamp to the arm
        // start (lowest frequency, highest ratio).
        let cal = calibrate_frequency(&calib_config(RasterDtype::UInt16), 100.0).unwrap();
        assert!(cal.clamped);
        assert_eq!(cal.frequency.to_bits(), CALIB_FREQ_MIN.to_bits());
    }

    #[test]
    fn calibrate_clamps_unreachable_low() {
        // ~1.02x is below the arm's minimum ratio → clamp to the arm end
        // (highest frequency reached). Achieved ratio stays above the target.
        let cal = calibrate_frequency(&calib_config(RasterDtype::UInt16), 1.02).unwrap();
        assert!(cal.clamped);
        assert!(
            cal.frequency > CALIB_FREQ_MIN,
            "should clamp to the high end"
        );
        assert!(cal.achieved_ratio > 1.02);
    }

    #[test]
    fn calibrate_dtype_dependent() {
        // Same target, different dtype → different resolved frequency. This is
        // the footgun fix: a fixed noise_frequency is not portable across dtypes.
        let u8_freq = calibrate_frequency(&calib_config(RasterDtype::UInt8), 2.0)
            .unwrap()
            .frequency;
        let f32_freq = calibrate_frequency(&calib_config(RasterDtype::Float32), 2.0)
            .unwrap()
            .frequency;
        assert_ne!(u8_freq.to_bits(), f32_freq.to_bits());
    }

    #[test]
    fn measure_ratio_decreases_in_smooth_regime() {
        // On the descending arm, more frequency = more detail = lower ratio.
        let cfg = calib_config(RasterDtype::UInt16);
        assert!(measure_ratio(&cfg, 16.0).unwrap() > measure_ratio(&cfg, 256.0).unwrap());
    }

    /// Test with dimensions that aren't a multiple of tile_size.
    /// Default: 1830×1830 with 256 tiles → 8×8 tiles, last column/row
    /// extends past the image (8×256 = 2048 > 1830).
    #[test]
    fn cog_bytes_edge_tiles() {
        let config = CogConfig::default();
        let fp = test_footprint();

        let bytes = write_cog_bytes(&config, &fp, 0).unwrap();
        assert!(!bytes.is_empty());
    }

    /// Verify the IFD comes before tile data and the file contains
    /// the GDAL_STRUCTURAL_METADATA ghost header, making the output
    /// a strict Cloud-Optimized GeoTIFF.
    #[test]
    fn cog_strict_layout() {
        let config = CogConfig::default();
        let fp = test_footprint();

        let bytes = write_cog_bytes(&config, &fp, 0).unwrap();

        // Ghost header should be present after the 8-byte TIFF header
        let ghost_marker = b"GDAL_STRUCTURAL_METADATA_SIZE=";
        let header_region = &bytes[8..200.min(bytes.len())];
        assert!(
            header_region
                .windows(ghost_marker.len())
                .any(|w| w == ghost_marker),
            "missing GDAL_STRUCTURAL_METADATA ghost header"
        );

        // IFD offset should be past the ghost header but before tile data
        let ifd_offset = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;
        assert!(ifd_offset > 8, "IFD should be after ghost header");
        assert!(
            ifd_offset < 1024,
            "IFD should be near the start of the file"
        );
    }

    #[test]
    fn cog_bytes_all_dtypes() {
        let fp = test_footprint();
        for dtype in [
            RasterDtype::UInt8,
            RasterDtype::UInt16,
            RasterDtype::Float32,
        ] {
            let config = CogConfig {
                dtype,
                ..CogConfig::default()
            };
            let bytes = write_cog_bytes(&config, &fp, 0).unwrap();
            assert!(!bytes.is_empty(), "empty COG for {:?}", dtype);
        }
    }

    #[test]
    fn write_cog_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tif");
        let config = CogConfig::default();
        let fp = test_footprint();

        write_cog(&config, &fp, 0, &path).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert!(file_bytes.len() > 100);
    }

    /// Validate that GDAL can read the pure Rust COG and sees correct
    /// dimensions, CRS, geotransform, and pixel data.
    #[test]
    fn gdal_validates_cog() {
        let config = CogConfig::default();
        let fp = test_footprint();

        let bytes = write_cog_bytes(&config, &fp, 0).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tif");
        std::fs::write(&path, &bytes).unwrap();

        let ds = gdal::Dataset::open(&path).unwrap();
        let (w, h) = ds.raster_size();
        assert_eq!(w, 1830);
        assert_eq!(h, 1830);
        assert_eq!(ds.raster_count(), 1);

        // CRS
        let srs = ds.spatial_ref().unwrap();
        assert_eq!(srs.auth_code().unwrap(), fp.epsg as i32);

        // Geotransform
        let gt = ds.geo_transform().unwrap();
        assert!((gt[0] - fp.origin.0).abs() < 1e-6);
        assert!((gt[3] - fp.origin.1).abs() < 1e-6);
        assert!((gt[1] - 60.0).abs() < 1e-6);
        assert!((gt[5] - (-60.0)).abs() < 1e-6);

        // Pixel data has variation (not all zeros)
        let band = ds.rasterband(1).unwrap();
        let raster_buf = band
            .read_as::<u8>((0, 0), (w, h), (w as usize, h as usize), None)
            .unwrap();
        let pixels = raster_buf.data();
        let min = *pixels.iter().min().unwrap();
        let max = *pixels.iter().max().unwrap();
        assert!(max - min > 50, "expected variation, got [{min}, {max}]");
    }

    #[test]
    fn gdal_validates_u16_cog() {
        let config = CogConfig {
            dtype: RasterDtype::UInt16,
            ..CogConfig::default()
        };
        let fp = test_footprint();
        let bytes = write_cog_bytes(&config, &fp, 0).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test_u16.tif");
        std::fs::write(&path, &bytes).unwrap();

        let ds = gdal::Dataset::open(&path).unwrap();
        let band = ds.rasterband(1).unwrap();
        let raster_buf = band
            .read_as::<u16>((0, 0), ds.raster_size(), (1830, 1830), None)
            .unwrap();
        let pixels = raster_buf.data();
        let min = *pixels.iter().min().unwrap();
        let max = *pixels.iter().max().unwrap();
        assert!(
            max - min > 10000,
            "expected u16 variation, got [{min}, {max}]"
        );
    }

    #[test]
    fn gdal_validates_f32_cog() {
        let config = CogConfig {
            dtype: RasterDtype::Float32,
            ..CogConfig::default()
        };
        let fp = test_footprint();
        let bytes = write_cog_bytes(&config, &fp, 0).unwrap();

        let dir = tempdir().unwrap();
        let path = dir.path().join("test_f32.tif");
        std::fs::write(&path, &bytes).unwrap();

        let ds = gdal::Dataset::open(&path).unwrap();
        let band = ds.rasterband(1).unwrap();
        let raster_buf = band
            .read_as::<f32>((0, 0), ds.raster_size(), (1830, 1830), None)
            .unwrap();
        let pixels = raster_buf.data();
        assert!(pixels.iter().all(|&v| (0.0..=1.0).contains(&v)));
    }
}

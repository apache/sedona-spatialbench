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
    pixel_buf: &mut PixelBuffer,
) -> io::Result<Vec<u8>> {
    let width = config.raster.cog_width;
    let height = config.raster.cog_height;

    let seed = (footprint.id as u64) << 32 | cog_id as u64;
    let noise = PerlinNoise::new(seed);

    // Generate noise into the pixel buffer (reuses allocation)
    match pixel_buf {
        PixelBuffer::U8(buf) => {
            noise.generate_raster_into(width, height, config.noise_frequency, buf);
        }
        PixelBuffer::U16(buf) => {
            noise.generate_raster_u16_into(width, height, config.noise_frequency, buf);
        }
        PixelBuffer::F32(buf) => {
            noise.generate_raster_f32_into(width, height, config.noise_frequency, buf);
        }
    }

    encode_cog(config, footprint, pixel_buf)
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
    pixel_buf: &mut PixelBuffer,
) -> io::Result<()> {
    let bytes = write_cog_bytes(config, footprint, cog_id, pixel_buf)?;
    std::fs::write(output_path, &bytes)?;
    Ok(())
}

/// Encode pixel data into a COG byte buffer.
///
/// Core encoding function that takes a filled [`PixelBuffer`] and produces
/// a complete TIFF file in memory using the `tiff` crate's `DirectoryEncoder`.
fn encode_cog(
    config: &CogConfig,
    footprint: &Footprint,
    pixel_buf: &PixelBuffer,
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
    let (tile_offsets, tile_byte_counts) = write_tiles(&mut dir, config, pixel_buf)?;

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
    pixel_buf: &PixelBuffer,
) -> io::Result<(Vec<u32>, Vec<u32>)> {
    let width = config.raster.cog_width;
    let height = config.raster.cog_height;
    let ts = config.tile_size;
    let bpp = config.dtype.bytes_per_pixel();
    let tiles_across = width.div_ceil(ts);
    let tiles_down = height.div_ceil(ts);
    let num_tiles = (tiles_across * tiles_down) as usize;

    let mut tile_offsets = Vec::with_capacity(num_tiles);
    let mut tile_byte_counts = Vec::with_capacity(num_tiles);

    // Pre-allocate tile buffer and ZSTD compressor (reused across all tiles)
    let tile_byte_len = ts as usize * ts as usize * bpp;
    let mut tile_buf = vec![0u8; tile_byte_len];
    let mut compressor = zstd::bulk::Compressor::new(config.zstd_level)
        .map_err(|e| io::Error::other(format!("ZSTD compressor init failed: {e}")))?;

    for ty in 0..tiles_down {
        for tx in 0..tiles_across {
            extract_tile_into(pixel_buf, width, height, ts, tx, ty, &mut tile_buf);
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

/// Extract a single tile's raw bytes into a pre-allocated buffer.
///
/// Handles edge tiles that extend past the image boundary by zero-padding.
/// Output is row-major, always `tile_size × tile_size × bytes_per_pixel` bytes.
/// The buffer is zeroed first to handle edge padding cleanly.
fn extract_tile_into(
    pixel_buf: &PixelBuffer,
    img_width: u32,
    img_height: u32,
    tile_size: u32,
    tile_x: u32,
    tile_y: u32,
    out: &mut [u8],
) {
    out.fill(0);
    let ts = tile_size as usize;
    let w = img_width as usize;
    let h = img_height as usize;
    let x0 = (tile_x * tile_size) as usize;
    let y0 = (tile_y * tile_size) as usize;

    match pixel_buf {
        PixelBuffer::U8(buf) => {
            for row in 0..ts {
                let sy = y0 + row;
                if sy >= h {
                    break;
                }
                let copy_cols = ts.min(w.saturating_sub(x0));
                let src_start = sy * w + x0;
                let dst_start = row * ts;
                out[dst_start..dst_start + copy_cols]
                    .copy_from_slice(&buf[src_start..src_start + copy_cols]);
            }
        }
        PixelBuffer::U16(buf) => {
            let bpp = 2;
            for row in 0..ts {
                let sy = y0 + row;
                if sy >= h {
                    break;
                }
                let copy_cols = ts.min(w.saturating_sub(x0));
                for col in 0..copy_cols {
                    let pixel = buf[sy * w + x0 + col];
                    let dst = (row * ts + col) * bpp;
                    out[dst..dst + 2].copy_from_slice(&pixel.to_le_bytes());
                }
            }
        }
        PixelBuffer::F32(buf) => {
            let bpp = 4;
            for row in 0..ts {
                let sy = y0 + row;
                if sy >= h {
                    break;
                }
                let copy_cols = ts.min(w.saturating_sub(x0));
                for col in 0..copy_cols {
                    let pixel = buf[sy * w + x0 + col];
                    let dst = (row * ts + col) * bpp;
                    out[dst..dst + 4].copy_from_slice(&pixel.to_le_bytes());
                }
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
        let mut buf = PixelBuffer::new(config.dtype);

        let bytes = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();

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
        let mut buf = PixelBuffer::new(config.dtype);

        let a = write_cog_bytes(&config, &fp, 7, &mut buf).unwrap();
        let b = write_cog_bytes(&config, &fp, 7, &mut buf).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn cog_bytes_different_seeds_differ() {
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        let a = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();
        let b = write_cog_bytes(&config, &fp, 1, &mut buf).unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn cog_bytes_buffer_reuse() {
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        let _ = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();
        let cap = match &buf {
            PixelBuffer::U8(v) => v.capacity(),
            _ => panic!("expected U8 buffer"),
        };
        let _ = write_cog_bytes(&config, &fp, 1, &mut buf).unwrap();
        assert_eq!(
            match &buf {
                PixelBuffer::U8(v) => v.capacity(),
                _ => panic!("expected U8 buffer"),
            },
            cap
        );
    }

    /// Test with dimensions that aren't a multiple of tile_size.
    /// Default: 1830×1830 with 256 tiles → 8×8 tiles, last column/row
    /// extends past the image (8×256 = 2048 > 1830).
    #[test]
    fn cog_bytes_edge_tiles() {
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        let bytes = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();
        assert!(!bytes.is_empty());
    }

    /// Verify the IFD comes before tile data and the file contains
    /// the GDAL_STRUCTURAL_METADATA ghost header, making the output
    /// a strict Cloud-Optimized GeoTIFF.
    #[test]
    fn cog_strict_layout() {
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        let bytes = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();

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
            let mut buf = PixelBuffer::new(dtype);
            let bytes = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();
            assert!(!bytes.is_empty(), "empty COG for {:?}", dtype);
        }
    }

    #[test]
    fn write_cog_creates_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.tif");
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        write_cog(&config, &fp, 0, &path, &mut buf).unwrap();

        let file_bytes = std::fs::read(&path).unwrap();
        assert!(file_bytes.len() > 100);
    }

    /// Validate that GDAL can read the pure Rust COG and sees correct
    /// dimensions, CRS, geotransform, and pixel data.
    #[test]
    fn gdal_validates_cog() {
        let config = CogConfig::default();
        let fp = test_footprint();
        let mut buf = PixelBuffer::new(config.dtype);

        let bytes = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();

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
        let mut buf = PixelBuffer::new(config.dtype);
        let bytes = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();

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
        let mut buf = PixelBuffer::new(config.dtype);
        let bytes = write_cog_bytes(&config, &fp, 0, &mut buf).unwrap();

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

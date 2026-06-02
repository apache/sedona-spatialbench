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

//! 2D Perlin noise generator for deterministic raster pixel generation.
//!
//! Produces spatially correlated pixel values that mimic the spectral
//! characteristics of real satellite imagery, yielding realistic compression
//! ratios (~2-4x with DEFLATE/ZSTD).
//!
//! The implementation follows Ken Perlin's improved noise (2002) with a
//! permutation table seeded deterministically from a footprint/COG ID pair.
//!
//! Uses `f32` arithmetic for ~2x throughput on ARM NEON (Apple Silicon, AWS
//! Graviton). The 23-bit mantissa is more than sufficient for mapping to
//! pixel values.
//!
//! Supports direct generation into `u8`, `u16`, and `f32` buffers to avoid
//! intermediate allocations when the target dtype differs from UInt8.

/// A seeded 2D Perlin noise generator.
///
/// Each instance produces a unique but deterministic noise field based on its
/// seed. The permutation table is shuffled via a simple LCG seeded from the
/// input, ensuring reproducibility across runs.
pub struct PerlinNoise {
    perm: [u8; 512],
}

impl PerlinNoise {
    /// Create a new Perlin noise generator with a deterministic seed.
    ///
    /// Typical usage: `seed = (footprint_id as u64) << 32 | cog_id as u64`
    pub fn new(seed: u64) -> Self {
        let mut perm = [0u8; 512];
        // Initialize identity permutation
        for i in 0..256u16 {
            perm[i as usize] = i as u8;
        }
        // Fisher-Yates shuffle with LCG
        let mut rng = seed;
        for i in (1..256usize).rev() {
            rng = lcg_next(rng);
            let j = (rng >> 16) as usize % (i + 1);
            perm.swap(i, j);
        }
        // Duplicate for wrapping
        for i in 0..256 {
            perm[256 + i] = perm[i];
        }
        Self { perm }
    }

    /// Sample noise at (x, y), returning a value in [-1.0, 1.0].
    #[inline]
    pub fn sample(&self, x: f32, y: f32) -> f32 {
        let xi = x.floor() as i32;
        let yi = y.floor() as i32;
        let xf = x - x.floor();
        let yf = y - y.floor();

        let u = fade(xf);
        let v = fade(yf);

        // Hash corners
        let xi = (xi & 255) as usize;
        let yi = (yi & 255) as usize;
        let aa = self.perm[self.perm[xi] as usize + yi] as usize;
        let ab = self.perm[self.perm[xi] as usize + yi + 1] as usize;
        let ba = self.perm[self.perm[xi + 1] as usize + yi] as usize;
        let bb = self.perm[self.perm[xi + 1] as usize + yi + 1] as usize;

        // Gradient dot products and bilinear interpolation
        let x1 = lerp(grad(aa, xf, yf), grad(ba, xf - 1.0, yf), u);
        let x2 = lerp(grad(ab, xf, yf - 1.0), grad(bb, xf - 1.0, yf - 1.0), u);
        lerp(x1, x2, v)
    }

    /// Generate a full raster buffer of UInt8 pixel values.
    ///
    /// `width` and `height` are pixel dimensions. `frequency` controls the
    /// spatial scale of the noise (higher = more detail per tile).
    /// Writes into `buf`, resizing it to `width * height` bytes (row-major).
    /// Passing a pre-allocated buffer avoids repeated heap allocations when
    /// generating many COGs (see [`BufferRecycler`] pattern in spatialbench).
    ///
    /// Row-level y-axis values are hoisted out of the inner loop: the integer
    /// grid cell, fractional offset, fade curve, and permutation lookups for
    /// each row are computed once and reused across all columns.
    pub fn generate_raster_into(&self, width: u32, height: u32, frequency: f32, buf: &mut Vec<u8>) {
        let len = width as usize * height as usize;
        buf.clear();
        buf.reserve(len.saturating_sub(buf.capacity()));
        self.generate_noise(width, height, frequency, |val| {
            buf.push(noise_to_u8(val));
        });
    }

    /// Generate a full raster buffer of UInt8 pixel values.
    ///
    /// Convenience wrapper around [`Self::generate_raster_into`] that allocates
    /// a new buffer. Prefer `generate_raster_into` in hot loops.
    pub fn generate_raster(&self, width: u32, height: u32, frequency: f32) -> Vec<u8> {
        let mut buf = Vec::new();
        self.generate_raster_into(width, height, frequency, &mut buf);
        buf
    }

    /// Generate a full raster buffer of UInt16 pixel values directly.
    ///
    /// Maps Perlin noise [-1, 1] → [0, 65535] without an intermediate u8 buffer.
    /// The `* 257` scaling used in the u8→u16 path is equivalent to mapping
    /// `0..=255` → `0..=65535`, which this achieves directly.
    pub fn generate_raster_u16_into(
        &self,
        width: u32,
        height: u32,
        frequency: f32,
        buf: &mut Vec<u16>,
    ) {
        let len = width as usize * height as usize;
        buf.clear();
        buf.reserve(len.saturating_sub(buf.capacity()));
        self.generate_noise(width, height, frequency, |val| {
            buf.push(noise_to_u16(val));
        });
    }

    /// Generate a full raster buffer of Float32 pixel values directly.
    ///
    /// Maps Perlin noise [-1, 1] → [0.0, 1.0] without an intermediate u8 buffer.
    pub fn generate_raster_f32_into(
        &self,
        width: u32,
        height: u32,
        frequency: f32,
        buf: &mut Vec<f32>,
    ) {
        let len = width as usize * height as usize;
        buf.clear();
        buf.reserve(len.saturating_sub(buf.capacity()));
        self.generate_noise(width, height, frequency, |val| {
            buf.push(noise_to_f32(val));
        });
    }

    /// Core noise generation loop. Calls `emit` for each pixel with the raw
    /// noise value in [-1, 1]. Row-level y-axis values are hoisted out of the
    /// inner loop.
    #[inline]
    fn generate_noise(&self, width: u32, height: u32, frequency: f32, mut emit: impl FnMut(f32)) {
        let inv_w = frequency / width as f32;
        let inv_h = frequency / height as f32;

        for row in 0..height {
            let y = row as f32 * inv_h;
            let yi_raw = y.floor() as i32;
            let yf = y - y.floor();
            let v = fade(yf);
            let yi = (yi_raw & 255) as usize;

            for col in 0..width {
                let x = col as f32 * inv_w;
                let xi_raw = x.floor() as i32;
                let xf = x - x.floor();
                let u = fade(xf);
                let xi = (xi_raw & 255) as usize;

                let p_xi = self.perm[xi] as usize;
                let p_xi1 = self.perm[xi + 1] as usize;
                let aa = self.perm[p_xi + yi] as usize;
                let ab = self.perm[p_xi + yi + 1] as usize;
                let ba = self.perm[p_xi1 + yi] as usize;
                let bb = self.perm[p_xi1 + yi + 1] as usize;

                let x1 = lerp(grad(aa, xf, yf), grad(ba, xf - 1.0, yf), u);
                let x2 = lerp(grad(ab, xf, yf - 1.0), grad(bb, xf - 1.0, yf - 1.0), u);
                emit(lerp(x1, x2, v));
            }
        }
    }

    /// Invoke `emit(local_index, noise_value)` for every in-bounds pixel of the
    /// tile at `(tile_x, tile_y)`. `local_index` is the row-major index within
    /// the `tile_size × tile_size` tile (`ly * tile_size + lx`). Pixels past the
    /// image edge are not emitted — the caller zero-fills first.
    ///
    /// Uses the same global coordinates and arithmetic as [`Self::generate_noise`],
    /// so values are bit-identical to the whole-image path at the same pixel.
    /// Generating per tile avoids materializing the whole-image pixel buffer.
    // Image geometry + tile coordinates + output is an irreducible arg set for
    // a noise primitive; bundling would only obscure the call sites.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    fn generate_tile(
        &self,
        img_width: u32,
        img_height: u32,
        frequency: f32,
        tile_size: u32,
        tile_x: u32,
        tile_y: u32,
        mut emit: impl FnMut(usize, f32),
    ) {
        let inv_w = frequency / img_width as f32;
        let inv_h = frequency / img_height as f32;
        let ts = tile_size as usize;
        let w = img_width as usize;
        let h = img_height as usize;
        let x0 = tile_x as usize * ts;
        let y0 = tile_y as usize * ts;

        for ly in 0..ts {
            let gy = y0 + ly;
            if gy >= h {
                break;
            }
            let y = gy as f32 * inv_h;
            let yi_raw = y.floor() as i32;
            let yf = y - y.floor();
            let v = fade(yf);
            let yi = (yi_raw & 255) as usize;

            for lx in 0..ts {
                let gx = x0 + lx;
                if gx >= w {
                    break;
                }
                let x = gx as f32 * inv_w;
                let xi_raw = x.floor() as i32;
                let xf = x - x.floor();
                let u = fade(xf);
                let xi = (xi_raw & 255) as usize;

                let p_xi = self.perm[xi] as usize;
                let p_xi1 = self.perm[xi + 1] as usize;
                let aa = self.perm[p_xi + yi] as usize;
                let ab = self.perm[p_xi + yi + 1] as usize;
                let ba = self.perm[p_xi1 + yi] as usize;
                let bb = self.perm[p_xi1 + yi + 1] as usize;

                let x1 = lerp(grad(aa, xf, yf), grad(ba, xf - 1.0, yf), u);
                let x2 = lerp(grad(ab, xf, yf - 1.0), grad(bb, xf - 1.0, yf - 1.0), u);
                emit(ly * ts + lx, lerp(x1, x2, v));
            }
        }
    }

    /// Fill a UInt8 tile (`tile_size² × 1` bytes). Edge padding is zero.
    #[allow(clippy::too_many_arguments)]
    pub fn generate_tile_u8_into(
        &self,
        img_width: u32,
        img_height: u32,
        frequency: f32,
        tile_size: u32,
        tile_x: u32,
        tile_y: u32,
        out: &mut [u8],
    ) {
        out.fill(0);
        self.generate_tile(
            img_width,
            img_height,
            frequency,
            tile_size,
            tile_x,
            tile_y,
            |idx, val| out[idx] = noise_to_u8(val),
        );
    }

    /// Fill a UInt16 tile (`tile_size² × 2` bytes, little-endian). Edge padding is zero.
    #[allow(clippy::too_many_arguments)]
    pub fn generate_tile_u16_into(
        &self,
        img_width: u32,
        img_height: u32,
        frequency: f32,
        tile_size: u32,
        tile_x: u32,
        tile_y: u32,
        out: &mut [u8],
    ) {
        out.fill(0);
        self.generate_tile(
            img_width,
            img_height,
            frequency,
            tile_size,
            tile_x,
            tile_y,
            |idx, val| {
                let b = noise_to_u16(val).to_le_bytes();
                out[idx * 2..idx * 2 + 2].copy_from_slice(&b);
            },
        );
    }

    /// Fill a Float32 tile (`tile_size² × 4` bytes, little-endian). Edge padding is zero.
    #[allow(clippy::too_many_arguments)]
    pub fn generate_tile_f32_into(
        &self,
        img_width: u32,
        img_height: u32,
        frequency: f32,
        tile_size: u32,
        tile_x: u32,
        tile_y: u32,
        out: &mut [u8],
    ) {
        out.fill(0);
        self.generate_tile(
            img_width,
            img_height,
            frequency,
            tile_size,
            tile_x,
            tile_y,
            |idx, val| {
                let b = noise_to_f32(val).to_le_bytes();
                out[idx * 4..idx * 4 + 4].copy_from_slice(&b);
            },
        );
    }
}

/// Map raw Perlin noise in `[-1, 1]` to a UInt8 pixel.
///
/// Shared by the whole-image (`generate_raster_into`) and per-tile
/// (`generate_tile_u8_into`) paths so they produce identical bytes.
#[inline]
pub(crate) fn noise_to_u8(val: f32) -> u8 {
    (((val + 1.0) * 0.5).clamp(0.0, 1.0) * 255.0) as u8
}

/// Map raw Perlin noise in `[-1, 1]` to a UInt16 pixel.
#[inline]
pub(crate) fn noise_to_u16(val: f32) -> u16 {
    (((val + 1.0) * 0.5).clamp(0.0, 1.0) * 65535.0) as u16
}

/// Map raw Perlin noise in `[-1, 1]` to a Float32 pixel in `[0, 1]`.
#[inline]
pub(crate) fn noise_to_f32(val: f32) -> f32 {
    ((val + 1.0) * 0.5).clamp(0.0, 1.0)
}

/// Simple LCG for permutation table shuffling (Numerical Recipes constants).
#[inline]
fn lcg_next(state: u64) -> u64 {
    state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1)
}

/// Fade curve: 6t^5 - 15t^4 + 10t^3 (Perlin's improved noise, 2002).
#[inline]
fn fade(t: f32) -> f32 {
    t * t * t * (t * (t * 6.0 - 15.0) + 10.0)
}

/// Linear interpolation.
#[inline]
fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a + t * (b - a)
}

/// Gradient function using hash to select from 4 directions.
#[inline]
fn grad(hash: usize, x: f32, y: f32) -> f32 {
    match hash & 3 {
        0 => x + y,
        1 => -x + y,
        2 => x - y,
        _ => -x - y,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_output() {
        let n1 = PerlinNoise::new(42);
        let n2 = PerlinNoise::new(42);
        for i in 0..100 {
            let x = i as f32 * 0.1;
            let y = i as f32 * 0.07;
            assert_eq!(n1.sample(x, y).to_bits(), n2.sample(x, y).to_bits());
        }
    }

    #[test]
    fn different_seeds_differ() {
        let n1 = PerlinNoise::new(0);
        let n2 = PerlinNoise::new(1);
        let mut differ = false;
        for i in 0..100 {
            let x = i as f32 * 0.1;
            if n1.sample(x, 0.5) != n2.sample(x, 0.5) {
                differ = true;
                break;
            }
        }
        assert!(differ, "different seeds should produce different noise");
    }

    #[test]
    fn output_range() {
        let n = PerlinNoise::new(123);
        for i in 0..1000 {
            let x = i as f32 * 0.037;
            let y = i as f32 * 0.053;
            let val = n.sample(x, y);
            assert!(
                (-1.0..=1.0).contains(&val),
                "sample out of range: {val} at ({x}, {y})"
            );
        }
    }

    #[test]
    fn generate_raster_correct_size() {
        let n = PerlinNoise::new(7);
        let buf = n.generate_raster(64, 64, 4.0);
        assert_eq!(buf.len(), 64 * 64);
    }

    #[test]
    fn generate_raster_has_variation() {
        let n = PerlinNoise::new(99);
        let buf = n.generate_raster(128, 128, 8.0);
        let min = *buf.iter().min().unwrap();
        let max = *buf.iter().max().unwrap();
        // With Perlin noise at this frequency, we expect meaningful range
        assert!(
            max - min > 50,
            "expected pixel variation, got range [{min}, {max}]"
        );
    }

    #[test]
    fn generate_raster_u16_correct_size() {
        let n = PerlinNoise::new(7);
        let mut buf = Vec::new();
        n.generate_raster_u16_into(64, 64, 4.0, &mut buf);
        assert_eq!(buf.len(), 64 * 64);
    }

    #[test]
    fn generate_raster_u16_range() {
        let n = PerlinNoise::new(42);
        let mut buf = Vec::new();
        n.generate_raster_u16_into(128, 128, 8.0, &mut buf);
        let min = *buf.iter().min().unwrap();
        let max = *buf.iter().max().unwrap();
        assert!(
            max - min > 10000,
            "expected u16 variation, got [{min}, {max}]"
        );
    }

    #[test]
    fn generate_raster_f32_correct_size() {
        let n = PerlinNoise::new(7);
        let mut buf = Vec::new();
        n.generate_raster_f32_into(64, 64, 4.0, &mut buf);
        assert_eq!(buf.len(), 64 * 64);
    }

    #[test]
    fn generate_raster_f32_range() {
        let n = PerlinNoise::new(42);
        let mut buf = Vec::new();
        n.generate_raster_f32_into(128, 128, 8.0, &mut buf);
        for &v in &buf {
            assert!((0.0..=1.0).contains(&v), "f32 pixel out of range: {v}");
        }
    }

    #[test]
    fn generate_raster_u16_buffer_reuse() {
        let n = PerlinNoise::new(10);
        let mut buf = Vec::new();
        n.generate_raster_u16_into(32, 32, 4.0, &mut buf);
        assert_eq!(buf.len(), 32 * 32);
        let cap = buf.capacity();
        n.generate_raster_u16_into(32, 32, 4.0, &mut buf);
        assert_eq!(buf.capacity(), cap, "buffer should not reallocate");
    }

    /// Per-tile generation must produce bytes identical to extracting the same
    /// tile from the whole-image buffer — including edge tiles (100 is not a
    /// multiple of 32). This is the load-bearing bit-identity guard.
    #[test]
    fn tile_matches_whole_image() {
        let (w, h, ts, freq) = (100u32, 100u32, 32u32, 8.0f32);
        let n = PerlinNoise::new(0xABCD);
        let tiles_across = w.div_ceil(ts);
        let tiles_down = h.div_ceil(ts);

        // UInt8
        let mut whole_u8 = Vec::new();
        n.generate_raster_into(w, h, freq, &mut whole_u8);
        // UInt16
        let mut whole_u16 = Vec::new();
        n.generate_raster_u16_into(w, h, freq, &mut whole_u16);
        // Float32
        let mut whole_f32 = Vec::new();
        n.generate_raster_f32_into(w, h, freq, &mut whole_f32);

        for ty in 0..tiles_down {
            for tx in 0..tiles_across {
                // UInt8 (1 byte/px)
                let mut tile = vec![0u8; (ts * ts) as usize];
                n.generate_tile_u8_into(w, h, freq, ts, tx, ty, &mut tile);
                let expected = extract_ref(w, h, ts, tx, ty, 1, |px, out| out[0] = whole_u8[px]);
                assert_eq!(tile, expected, "u8 tile ({tx},{ty})");

                // UInt16 (2 bytes/px, LE)
                let mut tile = vec![0u8; (ts * ts * 2) as usize];
                n.generate_tile_u16_into(w, h, freq, ts, tx, ty, &mut tile);
                let expected = extract_ref(w, h, ts, tx, ty, 2, |px, out| {
                    out.copy_from_slice(&whole_u16[px].to_le_bytes());
                });
                assert_eq!(tile, expected, "u16 tile ({tx},{ty})");

                // Float32 (4 bytes/px, LE)
                let mut tile = vec![0u8; (ts * ts * 4) as usize];
                n.generate_tile_f32_into(w, h, freq, ts, tx, ty, &mut tile);
                let expected = extract_ref(w, h, ts, tx, ty, 4, |px, out| {
                    out.copy_from_slice(&whole_f32[px].to_le_bytes());
                });
                assert_eq!(tile, expected, "f32 tile ({tx},{ty})");
            }
        }
    }

    /// Reference tile extractor: zero-pad a tile and copy each in-bounds pixel
    /// from the whole-image buffer via `write_px(global_pixel_index, dst_slice)`.
    fn extract_ref(
        w: u32,
        h: u32,
        ts: u32,
        tx: u32,
        ty: u32,
        bpp: usize,
        mut write_px: impl FnMut(usize, &mut [u8]),
    ) -> Vec<u8> {
        let ts = ts as usize;
        let (w, h) = (w as usize, h as usize);
        let (x0, y0) = (tx as usize * ts, ty as usize * ts);
        let mut out = vec![0u8; ts * ts * bpp];
        for ly in 0..ts {
            let gy = y0 + ly;
            if gy >= h {
                break;
            }
            for lx in 0..ts {
                let gx = x0 + lx;
                if gx >= w {
                    break;
                }
                let dst = (ly * ts + lx) * bpp;
                write_px(gy * w + gx, &mut out[dst..dst + bpp]);
            }
        }
        out
    }

    /// Edge tile beyond the image bounds is zero-padded; in-bounds pixels match.
    #[test]
    fn tile_edge_is_zero_padded() {
        let (w, h, ts, freq) = (40u32, 40u32, 32u32, 8.0f32);
        let n = PerlinNoise::new(7);
        let mut whole = Vec::new();
        n.generate_raster_u16_into(w, h, freq, &mut whole);

        // Tile (1,1) covers global 32..64 in both axes; only 32..40 is valid.
        let mut tile = vec![0u8; (ts * ts * 2) as usize];
        n.generate_tile_u16_into(w, h, freq, ts, 1, 1, &mut tile);

        let ts = ts as usize;
        for ly in 0..ts {
            for lx in 0..ts {
                let (gx, gy) = (32 + lx, 32 + ly);
                let dst = (ly * ts + lx) * 2;
                let bytes = [tile[dst], tile[dst + 1]];
                if gx < w as usize && gy < h as usize {
                    let expected = whole[gy * w as usize + gx].to_le_bytes();
                    assert_eq!(bytes, expected, "in-bounds ({gx},{gy})");
                } else {
                    assert_eq!(bytes, [0, 0], "padding ({gx},{gy}) must be zero");
                }
            }
        }
    }
}

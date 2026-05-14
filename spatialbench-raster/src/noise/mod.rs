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
//! Produces spatially correlated UInt8 pixel values that mimic the spectral
//! characteristics of real satellite imagery, yielding realistic compression
//! ratios (~2-4x with DEFLATE/ZSTD).
//!
//! The implementation follows Ken Perlin's improved noise (2002) with a
//! permutation table seeded deterministically from a footprint/COG ID pair.

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
    pub fn sample(&self, x: f64, y: f64) -> f64 {
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
    /// Returns a row-major buffer of `width * height` bytes.
    pub fn generate_raster(&self, width: u32, height: u32, frequency: f64) -> Vec<u8> {
        let len = width as usize * height as usize;
        let mut buf = Vec::with_capacity(len);
        for row in 0..height {
            for col in 0..width {
                let x = col as f64 * frequency / width as f64;
                let y = row as f64 * frequency / height as f64;
                // Sample returns [-1, 1], map to [0, 255]
                let val = (self.sample(x, y) + 1.0) * 0.5;
                buf.push((val.clamp(0.0, 1.0) * 255.0) as u8);
            }
        }
        buf
    }
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
fn fade(t: f64) -> f64 {
    t * t * t * (t * (t * 6.0 - 15.0) + 10.0)
}

/// Linear interpolation.
#[inline]
fn lerp(a: f64, b: f64, t: f64) -> f64 {
    a + t * (b - a)
}

/// Gradient function using hash to select from 4 directions.
#[inline]
fn grad(hash: usize, x: f64, y: f64) -> f64 {
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
            let x = i as f64 * 0.1;
            let y = i as f64 * 0.07;
            assert_eq!(n1.sample(x, y).to_bits(), n2.sample(x, y).to_bits());
        }
    }

    #[test]
    fn different_seeds_differ() {
        let n1 = PerlinNoise::new(0);
        let n2 = PerlinNoise::new(1);
        let mut differ = false;
        for i in 0..100 {
            let x = i as f64 * 0.1;
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
            let x = i as f64 * 0.037;
            let y = i as f64 * 0.053;
            let val = n.sample(x, y);
            assert!(
                val >= -1.0 && val <= 1.0,
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
}

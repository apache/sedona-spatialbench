use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::f64::consts::PI;

#[inline]
pub fn seeded_rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

#[inline]
pub fn unit_clamp(v: f64) -> f64 {
    v.clamp(0.0, 1.0)
}

pub fn spider_seed_for_index(index: u64, global_seed: u64) -> u64 {
    let mut z = index
        .wrapping_add(global_seed)
        .wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

pub fn rand_normal(rng: &mut StdRng, mu: f64, sigma: f64) -> f64 {
    let u1: f64 = rng.gen();
    let u2: f64 = rng.gen();
    mu + sigma * (-2.0 * u1.ln()).sqrt() * (2.0 * PI * u2).cos()
}

#[inline]
pub fn hash_to_unit_u64(x: u64, salt: u64) -> f64 {
    let mut z = x.wrapping_add(salt).wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^= z >> 31;
    ((z >> 11) as f64) / ((1u64 << 53) as f64)
}

#[inline]
pub fn u01_from_seed(seed: u64) -> f64 {
    let z = spider_seed_for_index(seed, 0xA1CE_CAFE);
    ((z >> 11) as f64) / ((1u64 << 53) as f64)
}

#[inline]
pub fn gauss_around(rng: &mut StdRng, center: (f64, f64), sigma: f64) -> (f64, f64) {
    let (cx, cy) = center;
    let dx = rand_normal(rng, 0.0, sigma);
    let dy = rand_normal(rng, 0.0, sigma);
    (unit_clamp(cx + dx), unit_clamp(cy + dy))
}

pub fn halton_2d(i: u64, base_x: u32, base_y: u32) -> (f64, f64) {
    (radical_inverse(i, base_x), radical_inverse(i, base_y))
}

fn radical_inverse(mut n: u64, base: u32) -> f64 {
    let b = base as u64;
    let mut inv = 1.0 / b as f64;
    let mut val = 0.0;
    while n > 0 {
        let d = (n % b) as f64;
        val += d * inv;
        n /= b;
        inv /= b as f64;
    }
    val
}

pub fn pick_from_cdf(cdf: &[f64], u: f64) -> usize {
    let (mut lo, mut hi) = (0usize, cdf.len());
    while lo < hi {
        let mid = (lo + hi) / 2;
        if u <= cdf[mid] {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo.saturating_sub(0).min(cdf.len().saturating_sub(1))
}

pub fn build_cdf_from_weights(mut w: Vec<f64>) -> Vec<f64> {
    let sum = w.iter().copied().sum::<f64>().max(1e-12);
    let mut acc = 0.0;
    for wi in &mut w {
        acc += *wi / sum;
        *wi = acc;
    }
    w
}

pub fn pareto_draw(u: f64, alpha: f64, xm: f64) -> f64 {
    let a = alpha.max(1e-6);
    let s = xm.max(1e-12);
    s / (1.0 - u).powf(1.0 / a)
}

pub fn sample_normal_count(mu: f64, sd: f64, min_v: u32, max_v: u32, seed: u64) -> u32 {
    let mut rng = seeded_rng(seed);
    let draw = rand_normal(&mut rng, mu, sd).round();
    let mut k = draw.max(min_v as f64) as u32;
    if k > max_v {
        k = max_v;
    }
    if k < 1 {
        k = 1;
    }
    k
}

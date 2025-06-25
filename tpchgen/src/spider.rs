use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

#[derive(Debug, Clone, Copy)]
pub enum SpiderDistribution {
    Uniform,
    Normal { mu: f64, sigma: f64 },
    Diagonal { percentage: f64, buffer: f64 },
    Bit { probability: f64, digits: u32 },
    Sierpinski,
}

#[derive(Debug, Clone)]
pub struct SpiderConfig {
    pub dist: SpiderDistribution,
    pub global_seed: u64,
    pub affine: Option<[f64; 6]>,
}

#[derive(Clone, Debug)]
pub struct SpiderGenerator {
    pub config: SpiderConfig,
}

impl SpiderGenerator {
    pub fn new(config: SpiderConfig) -> Self {
        Self { config }
    }

    pub fn generate_point(&self, index: u64) -> (f64, f64) {
        let seed = spider_seed_for_index(index, self.config.global_seed);
        let mut rng = StdRng::seed_from_u64(seed);

        match self.config.dist {
            SpiderDistribution::Uniform => (rng.gen(), rng.gen()),

            SpiderDistribution::Normal { mu, sigma } => {
                let x = rand_normal(&mut rng, mu, sigma).clamp(0.0, 1.0);
                let y = rand_normal(&mut rng, mu, sigma).clamp(0.0, 1.0);
                (x, y)
            }

            SpiderDistribution::Diagonal { percentage, buffer } => {
                if rng.gen::<f64>() < percentage {
                    let v = rng.gen();
                    (v, v)
                } else {
                    let c: f64 = rng.gen();
                    let d: f64 = rand_normal(&mut rng, 0.0, buffer / 5.0);
                    let x: f64 = (c + d / f64::sqrt(2.0)).clamp(0.0, 1.0);
                    let y: f64 = (c - d / f64::sqrt(2.0)).clamp(0.0, 1.0);
                    (x, y)
                }
            }

            SpiderDistribution::Bit { probability, digits } => {
                let x = spider_bit(&mut rng, probability, digits);
                let y = spider_bit(&mut rng, probability, digits);
                (x, y)
            }

            SpiderDistribution::Sierpinski => {
                let (mut x, mut y) = (0.0, 0.0);
                let a = (0.0, 0.0);
                let b = (1.0, 0.0);
                let c = (0.5, (3.0f64).sqrt() / 2.0);
                for _ in 0..10 {
                    match rng.gen_range(0..3) {
                        0 => { x = (x + a.0) / 2.0; y = (y + a.1) / 2.0; }
                        1 => { x = (x + b.0) / 2.0; y = (y + b.1) / 2.0; }
                        _ => { x = (x + c.0) / 2.0; y = (y + c.1) / 2.0; }
                    }
                }
                (x, y)
            }
        }
    }

    pub fn generate_pickup_point(&self, trip_id: u64) -> (f64, f64) {
        let (x, y) = self.generate_point(trip_id);
        if let Some(aff) = &self.config.affine {
            apply_affine(x, y, aff)
        } else {
            (x, y)
        }
    }
}

// Affine transform
fn apply_affine(x: f64, y: f64, m: &[f64; 6]) -> (f64, f64) {
    let x_out = m[0] * x + m[1] * y + m[2];
    let y_out = m[3] * x + m[4] * y + m[5];
    (x_out, y_out)
}

// Deterministic hash (SplitMix64-like)
pub fn spider_seed_for_index(index: u64, global_seed: u64) -> u64 {
    let mut z = index.wrapping_add(global_seed).wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

// Box-Muller transform
fn rand_normal(rng: &mut StdRng, mu: f64, sigma: f64) -> f64 {
    let u1: f64 = rng.gen();
    let u2: f64 = rng.gen();
    mu + sigma * (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

fn spider_bit(rng: &mut StdRng, prob: f64, digits: u32) -> f64 {
    (1..=digits)
        .map(|i| if rng.gen::<f64>() < prob { 1.0 / 2f64.powi(i as i32) } else { 0.0 })
        .sum()
}

// In tpchgen/src/spider.rs

impl Default for SpiderGenerator {
    fn default() -> Self {
        let config = SpiderConfig {
            dist: SpiderDistribution::Uniform,
            global_seed: 42,
            affine: Some([
                58.368269, 0.0, -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0, 25.175375, 24.006328,    // scale Y to 25.18°, offset to 24.00°
            ]),
        };
        SpiderGenerator::new(config)
    }
}
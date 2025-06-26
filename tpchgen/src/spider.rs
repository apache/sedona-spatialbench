use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

#[derive(Debug, Clone, Copy)]
pub enum DistributionType {
    Uniform,
    Normal,
    Diagonal,
    Sierpinski,
    Bit,
    Parcel,
}

#[derive(Debug, Clone, Copy)]
pub enum GeomType {
    Polygon,
    Box,
    Point,
}

#[derive(Debug, Clone)]
pub struct BoxWithDepth {
    pub depth: i32,
    pub x: f64,
    pub y: f64,
    pub w: f64,
    pub h: f64,
}

#[derive(Debug, Clone)]
pub enum DistributionParams {
    None,
    Normal { mu: f64, sigma: f64 },
    Diagonal { percentage: f64, buffer: f64 },
    Bit { probability: f64, digits: u32 },
    Parcel { srange: f64, dither: f64 },
}

#[derive(Debug, Clone)]
pub struct SpiderConfig {
    pub dist_type: DistributionType,
    pub geom_type: GeomType,
    pub dim: i32,
    pub seed: u32,
    pub affine: Option<[f64; 6]>, // Affine transformation matrix

    // Box-specific fields
    pub width: f64,
    pub height: f64,

    // Polygon-specific fields
    pub maxseg: i32,
    pub polysize: f64,

    // Distribution-specific params
    pub params: DistributionParams,
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
        let seed = spider_seed_for_index(index, self.config.seed as u64);
        let mut rng = StdRng::seed_from_u64(seed);

        match self.config.dist_type {
            DistributionType::Uniform => self.generate_uniform(&mut rng),
            DistributionType::Normal => self.generate_normal(&mut rng),
            DistributionType::Diagonal => self.generate_diagonal(&mut rng),
            DistributionType::Bit => self.generate_bit(&mut rng),
            DistributionType::Sierpinski => self.generate_sierpinski(&mut rng),
            _ => (rng.gen(), rng.gen())
        }

    }

    fn generate_uniform(&self, rng: &mut StdRng) -> (f64, f64) {
        (rand_unit(rng), rand_unit(rng))
    }

    fn generate_normal(&self, rng: &mut StdRng) -> (f64, f64) {
        if let DistributionParams::Normal { mu, sigma } = self.config.params {
            let x = rand_normal(rng, mu, sigma).clamp(0.0, 1.0);
            let y = rand_normal(rng, mu, sigma).clamp(0.0, 1.0);
            (x, y)
        } else {
            // Default values or error handling
            (rng.gen(), rng.gen())
        }
    }

    fn generate_diagonal(&self, rng: &mut StdRng) -> (f64, f64) {
        if let DistributionParams::Diagonal { percentage, buffer } = self.config.params {
            if rng.gen::<f64>() < percentage {
                let v = rng.gen();
                (v, v)
            } else {
                let c: f64 = rng.gen();
                let d: f64 = rand_normal(rng, 0.0, buffer / 5.0);
                let x: f64 = (c + d / f64::sqrt(2.0)).clamp(0.0, 1.0);
                let y: f64 = (c - d / f64::sqrt(2.0)).clamp(0.0, 1.0);
                (x, y)
            }
        } else {
            // Default values or error handling
            (rng.gen(), rng.gen())
        }
    }

    fn generate_bit(&self, rng: &mut StdRng) -> (f64, f64) {
        if let DistributionParams::Bit { probability, digits } = self.config.params {
            let x = spider_bit(rng, probability, digits);
            let y = spider_bit(rng, probability, digits);
            (x, y)
        } else {
            // Default values or error handling
            (rng.gen(), rng.gen())
        }
    }

    fn generate_sierpinski(&self, rng: &mut StdRng) -> (f64, f64) {
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

    pub fn generate_parcel(&self, rng: &mut StdRng) -> String {
        if let DistributionParams::Parcel { srange, dither } = self.config.params {
            let mut box_stack = vec![BoxWithDepth {
                depth: 0,
                x: 0.0,
                y: 0.0,
                w: 1.0,
                h: 1.0,
            }];

            // Pick a depth based on dim (log2) or fixed depth
            let depth_limit = 6; // You can make this configurable if needed

            for _ in 0..depth_limit {
                let b = box_stack.pop().unwrap();
                let (b1, b2) = if b.w > b.h {
                    let split = b.w * (srange + rand_unit(rng) * (1.0 - 2.0 * srange));
                    (
                        BoxWithDepth { depth: b.depth + 1, x: b.x, y: b.y, w: split, h: b.h },
                        BoxWithDepth { depth: b.depth + 1, x: b.x + split, y: b.y, w: b.w - split, h: b.h },
                    )
                } else {
                    let split = b.h * (srange + rand_unit(rng) * (1.0 - 2.0 * srange));
                    (
                        BoxWithDepth { depth: b.depth + 1, x: b.x, y: b.y, w: b.w, h: split },
                        BoxWithDepth { depth: b.depth + 1, x: b.x, y: b.y + split, w: b.w, h: b.h - split },
                    )
                };

                // Randomly pick one of the two
                if rng.gen_bool(0.5) {
                    box_stack.push(b1);
                } else {
                    box_stack.push(b2);
                }
            }

            let mut b = box_stack.pop().unwrap();

            // Apply dither
            let dx = b.w * dither * (rand_unit(rng) - 0.5);
            let dy = b.h * dither * (rand_unit(rng) - 0.5);
            b.x += dx / 2.0;
            b.y += dy / 2.0;
            b.w -= dx;
            b.h -= dy;

            // Pick random point inside the box
            let _x = b.x + rand_unit(rng) * b.w;
            let _y = b.y + rand_unit(rng) * b.h;

            self.box_to_wkt(&b)
        } else {
            self.box_to_wkt(&BoxWithDepth {
                depth: 0,
                x: 0.0,
                y: 0.0,
                w: 1.0,
                h: 1.0,
            })
        }
    }

    fn box_to_wkt(&self, b: &BoxWithDepth) -> String {
        let corners = [
            (b.x, b.y),
            (b.x + b.w, b.y),
            (b.x + b.w, b.y + b.h),
            (b.x, b.y + b.h),
            (b.x, b.y),
        ];

        let affine = self.config.affine.unwrap_or([1.0, 0.0, 0.0, 0.0, 1.0, 0.0]);

        let coords: Vec<String> = corners
            .iter()
            .map(|&(x, y)| {
                let (tx, ty) = apply_affine(x, y, &affine);
                format!("{:.6} {:.6}", tx, ty)
            })
            .collect();

        format!("POLYGON (({}))", coords.join(", "))
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

pub fn rand_unit(rng: &mut StdRng) -> f64 {
    rng.gen::<f64>() // random number in [0.0, 1.0)
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

// impl Default for SpiderGenerator {
//     fn default() -> Self {
//         let config = SpiderConfig {
//             dist: SpiderDistribution::Uniform,
//             global_seed: 42,
//             affine: Some([
//                 58.368269, 0.0, -125.244606, // scale X to 58.37°, offset to -125.24°
//                 0.0, 25.175375, 24.006328,    // scale Y to 25.18°, offset to 24.00°
//             ]),
//         };
//         SpiderGenerator::new(config)
//     }
// }
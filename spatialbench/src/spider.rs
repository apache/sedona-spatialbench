use geo::{coord, Coord, CoordsIter, Geometry, LineString, Point, Polygon};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::f64::consts::PI;

const GEOMETRY_PRECISION: f64 = 100_000_000.0;

#[derive(Debug, Clone, Copy)]
pub enum DistributionType {
    Uniform,
    Normal,
    Diagonal,
    Sierpinski,
    Bit,
}

#[derive(Debug, Clone, Copy)]
pub enum GeomType {
    Polygon,
    Box,
    Point,
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
    pub continent_affines: Option<ContinentAffines>, // All 5 continent transformations

    // Box-specific fields
    pub width: f64,
    pub height: f64,

    // Polygon-specific fields
    pub maxseg: i32,
    pub polysize: f64,

    // Distribution-specific params
    pub params: DistributionParams,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ContinentAffines {
    pub eurasia: [f64; 6],
    pub north_america: [f64; 6],
    pub south_america: [f64; 6],
    pub oceania: [f64; 6],
    pub africa: [f64; 6],
}

// impl ContinentAffines {
//     pub fn get_by_name(&self, name: &str) -> Option<&[f64; 6]> {
//         match name {
//             "eurasia" => Some(&self.eurasia),
//             "north_america" => Some(&self.north_america),
//             "south_america" => Some(&self.south_america),
//             "oceania" => Some(&self.oceania),
//             "africa" => Some(&self.africa),
//             _ => None,
//         }
//     }
// }

#[derive(Clone, Debug)]
pub struct SpiderGenerator {
    pub config: SpiderConfig,
}

impl SpiderGenerator {
    pub fn new(config: SpiderConfig) -> Self {
        Self { config }
    }

    pub fn generate(&self, index: u64) -> Geometry {
        let seed = spider_seed_for_index(index, self.config.seed as u64);
        let mut rng = StdRng::seed_from_u64(seed);

        match self.config.dist_type {
            DistributionType::Uniform => self.generate_uniform(&mut rng),
            DistributionType::Normal => self.generate_normal(&mut rng),
            DistributionType::Diagonal => self.generate_diagonal(&mut rng),
            DistributionType::Bit => self.generate_bit(&mut rng),
            DistributionType::Sierpinski => self.generate_sierpinski(&mut rng),
        }
    }

    fn generate_uniform(&self, rng: &mut StdRng) -> Geometry {
        let x = rand_unit(rng);
        let y = rand_unit(rng);

        match self.config.geom_type {
            GeomType::Point => generate_point_geom((x, y), &self.config),
            GeomType::Box => generate_box_geom((x, y), &self.config, rng),
            GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng),
        }
    }

    fn generate_normal(&self, rng: &mut StdRng) -> Geometry {
        match self.config.params {
            DistributionParams::Normal { mu, sigma } => {
                let x = rand_normal(rng, mu, sigma).clamp(0.0, 1.0);
                let y = rand_normal(rng, mu, sigma).clamp(0.0, 1.0);

                match self.config.geom_type {
                    GeomType::Point => generate_point_geom((x, y), &self.config),
                    GeomType::Box => generate_box_geom((x, y), &self.config, rng),
                    GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng),
                }
            }
            _ => panic!(
                "Expected Normal distribution parameters but got {:?}",
                self.config.params
            ),
        }
    }

    fn generate_diagonal(&self, rng: &mut StdRng) -> Geometry {
        match self.config.params {
            DistributionParams::Diagonal { percentage, buffer } => {
                let (x, y) = if rng.gen::<f64>() < percentage {
                    let v = rng.gen();
                    (v, v)
                } else {
                    let c: f64 = rng.gen();
                    let d: f64 = rand_normal(rng, 0.0, buffer / 5.0);
                    let x: f64 = (c + d / f64::sqrt(2.0)).clamp(0.0, 1.0);
                    let y: f64 = (c - d / f64::sqrt(2.0)).clamp(0.0, 1.0);
                    (x, y)
                };

                match self.config.geom_type {
                    GeomType::Point => generate_point_geom((x, y), &self.config),
                    GeomType::Box => generate_box_geom((x, y), &self.config, rng),
                    GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng),
                }
            }
            _ => panic!(
                "Expected Diagonal distribution parameters but got {:?}",
                self.config.params
            ),
        }
    }

    fn generate_bit(&self, rng: &mut StdRng) -> Geometry {
        match self.config.params {
            DistributionParams::Bit {
                probability,
                digits,
            } => {
                let x = spider_bit(rng, probability, digits);
                let y = spider_bit(rng, probability, digits);

                match self.config.geom_type {
                    GeomType::Point => generate_point_geom((x, y), &self.config),
                    GeomType::Box => generate_box_geom((x, y), &self.config, rng),
                    GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng),
                }
            }
            _ => panic!(
                "Expected Bit distribution parameters but got {:?}",
                self.config.params
            ),
        }
    }

    fn generate_sierpinski(&self, rng: &mut StdRng) -> Geometry {
        let (mut x, mut y) = (0.0, 0.0);
        let a = (0.0, 0.0);
        let b = (1.0, 0.0);
        let c = (0.5, (3.0f64).sqrt() / 2.0);
        for _ in 0..27 {
            match rng.gen_range(0..3) {
                0 => {
                    x = (x + a.0) / 2.0;
                    y = (y + a.1) / 2.0;
                }
                1 => {
                    x = (x + b.0) / 2.0;
                    y = (y + b.1) / 2.0;
                }
                _ => {
                    x = (x + c.0) / 2.0;
                    y = (y + c.1) / 2.0;
                }
            }
        }

        match self.config.geom_type {
            GeomType::Point => generate_point_geom((x, y), &self.config),
            GeomType::Box => generate_box_geom((x, y), &self.config, rng),
            GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng),
        }
    }
}

pub fn rand_unit(rng: &mut StdRng) -> f64 {
    rng.gen::<f64>() // random number in [0.0, 1.0)
}

// Affine transform
pub(crate) fn apply_affine(x: f64, y: f64, m: &[f64; 6]) -> (f64, f64) {
    let x_out = m[0] * x + m[1] * y + m[2];
    let y_out = m[3] * x + m[4] * y + m[5];
    (x_out, y_out)
}

// Deterministic hash (SplitMix64-like)
pub fn spider_seed_for_index(index: u64, global_seed: u64) -> u64 {
    let mut z = index
        .wrapping_add(global_seed)
        .wrapping_add(0x9E3779B97F4A7C15);
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
        .map(|i| {
            if rng.gen::<f64>() < prob {
                1.0 / 2f64.powi(i as i32)
            } else {
                0.0
            }
        })
        .sum()
}

pub fn generate_point_geom(center: (f64, f64), config: &SpiderConfig) -> Geometry {
    let (x, y) = round_coordinates(center.0, center.1, GEOMETRY_PRECISION);
    Geometry::Point(Point::new(x, y))
}

pub fn generate_box_geom(center: (f64, f64), config: &SpiderConfig, rng: &mut StdRng) -> Geometry {
    let half_width = rand_unit(rng) * config.width / 2.0;
    let half_height = rand_unit(rng) * config.height / 2.0;

    let corners = [
        (center.0 - half_width, center.1 - half_height),
        (center.0 + half_width, center.1 - half_height),
        (center.0 + half_width, center.1 + half_height),
        (center.0 - half_width, center.1 + half_height),
        (center.0 - half_width, center.1 - half_height),
    ];

    let coords: Vec<_> = corners
        .iter()
        .map(|&(x, y)| round_coordinates(x, y, GEOMETRY_PRECISION))
        .map(|(x, y)| coord! { x: x, y: y })
        .collect();

    Geometry::Polygon(Polygon::new(LineString::from(coords), vec![]))
}

pub fn generate_polygon_geom(
    center: (f64, f64),
    config: &SpiderConfig,
    rng: &mut StdRng,
) -> Geometry {
    let min_segs = 3;
    let num_segments = if config.maxseg <= 3 {
        3
    } else {
        rng.gen_range(0..=(config.maxseg - min_segs)) + min_segs
    };

    // Sample angles and sort for a simple, non-self-intersecting polygon
    let mut angles: Vec<f64> = (0..num_segments)
        .map(|_| rand_unit(rng) * 2.0 * PI)
        .collect();
    angles.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut coords = angles
        .iter()
        .map(|&angle| {
            // 1) Propose vertex around center
            let x0 = center.0 + config.polysize * angle.cos();
            let y0 = center.1 + config.polysize * angle.sin();

            // 2) Clamp in unit square BEFORE affine to keep it in [0,1]^2
            let x1 = x0.clamp(0.0, 1.0);
            let y1 = y0.clamp(0.0, 1.0);

            // 3) Round coordinates before affine transformation
            let (x2, y2) = round_coordinates(x1, y1, GEOMETRY_PRECISION);

            coord! { x: x2, y: y2 }
        })
        .collect::<Vec<_>>();

    // Close ring
    if let Some(first) = coords.first().cloned() {
        coords.push(first);
    }

    Geometry::Polygon(Polygon::new(LineString::from(coords), vec![]))
}

#[inline]
fn round_coordinate(coord: f64, precision: f64) -> f64 {
    (coord * precision).round() / precision
}

#[inline]
fn round_coordinates(x: f64, y: f64, precision: f64) -> (f64, f64) {
    (
        round_coordinate(x, precision),
        round_coordinate(y, precision),
    )
}

/// Return a transformed copy of a Polygon<f64>
pub fn apply_affine_polygon(poly: &Polygon<f64>, m: &[f64; 6]) -> Polygon<f64> {
    // map a LineString by applying the affine to each coord
    let map_ls = |ls: &LineString<f64>| {
        let coords: Vec<Coord<f64>> = ls
            .coords_iter()
            .map(|c| {
                let (x, y) = apply_affine(c.x, c.y, m);
                Coord { x, y }
            })
            .collect();
        LineString::from(coords)
    };

    let exterior = map_ls(poly.exterior());
    let interiors = poly.interiors().iter().map(map_ls).collect::<Vec<_>>();
    Polygon::new(exterior, interiors)
}

/// In-place convenience (rebuilds and swaps)
pub fn apply_affine_polygon_in_place(poly: &mut Polygon<f64>, m: &[f64; 6]) {
    let transformed = apply_affine_polygon(poly, m);
    *poly = transformed;
}

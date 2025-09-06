use geo::{coord, Coord, CoordsIter, Geometry, LineString, Point, Polygon};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::f64::consts::PI;

const GEOMETRY_PRECISION: f64 = 10_000_000_000.0;

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
    pub africa: [f64; 6],
    pub europe: [f64; 6],
    pub south_asia: [f64; 6],
    pub north_asia: [f64; 6],
    pub oceania: [f64; 6],
    pub south_america: [f64; 6],
    pub south_north_america: [f64; 6],
    pub north_north_america: [f64; 6],
}

#[derive(Clone, Debug)]
pub struct SpiderGenerator {
    pub config: SpiderConfig,
}

impl SpiderGenerator {
    pub fn new(config: SpiderConfig) -> Self {
        Self { config }
    }

    pub fn generate(&self, index: u64, continent_affine: &[f64; 6]) -> Geometry {
        let seed = spider_seed_for_index(index, self.config.seed as u64);
        let mut rng = StdRng::seed_from_u64(seed);

        match self.config.dist_type {
            DistributionType::Uniform => self.generate_uniform(&mut rng, continent_affine),
            DistributionType::Normal => self.generate_normal(&mut rng, continent_affine),
            DistributionType::Diagonal => self.generate_diagonal(&mut rng, continent_affine),
            DistributionType::Bit => self.generate_bit(&mut rng, continent_affine),
            DistributionType::Sierpinski => self.generate_sierpinski(&mut rng, continent_affine),
        }
    }

    fn generate_uniform(&self, rng: &mut StdRng, continent_affine: &[f64; 6]) -> Geometry {
        let x = rand_unit(rng);
        let y = rand_unit(rng);

        match self.config.geom_type {
            GeomType::Point => generate_point_geom((x, y), continent_affine),
            GeomType::Box => generate_box_geom((x, y), &self.config, rng, continent_affine),
            GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng, continent_affine),
        }
    }

    fn generate_normal(&self, rng: &mut StdRng, continent_affine: &[f64; 6]) -> Geometry {
        match self.config.params {
            DistributionParams::Normal { mu, sigma } => {
                let x = rand_normal(rng, mu, sigma).clamp(0.0, 1.0);
                let y = rand_normal(rng, mu, sigma).clamp(0.0, 1.0);

                match self.config.geom_type {
                    GeomType::Point => generate_point_geom((x, y), continent_affine),
                    GeomType::Box => generate_box_geom((x, y), &self.config, rng, continent_affine),
                    GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng, continent_affine),
                }
            }
            _ => panic!(
                "Expected Normal distribution parameters but got {:?}",
                self.config.params
            ),
        }
    }

    fn generate_diagonal(&self, rng: &mut StdRng, continent_affine: &[f64; 6]) -> Geometry {
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
                    GeomType::Point => generate_point_geom((x, y), continent_affine),
                    GeomType::Box => generate_box_geom((x, y), &self.config, rng, continent_affine),
                    GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng, continent_affine),
                }
            }
            _ => panic!(
                "Expected Diagonal distribution parameters but got {:?}",
                self.config.params
            ),
        }
    }

    fn generate_bit(&self, rng: &mut StdRng, continent_affine: &[f64; 6]) -> Geometry {
        match self.config.params {
            DistributionParams::Bit {
                probability,
                digits,
            } => {
                let x = spider_bit(rng, probability, digits);
                let y = spider_bit(rng, probability, digits);

                match self.config.geom_type {
                    GeomType::Point => generate_point_geom((x, y), continent_affine),
                    GeomType::Box => generate_box_geom((x, y), &self.config, rng, continent_affine),
                    GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng, continent_affine),
                }
            }
            _ => panic!(
                "Expected Bit distribution parameters but got {:?}",
                self.config.params
            ),
        }
    }

    fn generate_sierpinski(&self, rng: &mut StdRng, continent_affine: &[f64; 6]) -> Geometry {
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
            GeomType::Point => generate_point_geom((x, y), continent_affine),
            GeomType::Box => generate_box_geom((x, y), &self.config, rng, continent_affine),
            GeomType::Polygon => generate_polygon_geom((x, y), &self.config, rng, continent_affine),
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

pub fn generate_point_geom(center: (f64, f64), continent_affine: &[f64; 6]) -> Geometry {
    let (x, y) = apply_affine(center.0, center.1, continent_affine);
    let (x, y) = round_coordinates(x, y, GEOMETRY_PRECISION);
    Geometry::Point(Point::new(x, y))
}

pub fn generate_box_geom(center: (f64, f64), config: &SpiderConfig, rng: &mut StdRng, continent_affine: &[f64; 6]) -> Geometry {
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
        .map(|&(x, y)| apply_affine(x, y, continent_affine))
        .map(|(x, y)| round_coordinates(x, y, GEOMETRY_PRECISION))
        .map(|(x, y)| coord! { x: x, y: y })
        .collect();

    Geometry::Polygon(Polygon::new(LineString::from(coords), vec![]))
}

pub fn generate_polygon_geom(
    center: (f64, f64),
    config: &SpiderConfig,
    rng: &mut StdRng,
    continent_affine: &[f64; 6],
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

            // 3) Apply affine transformation
            let (x2, y2) = apply_affine(x1, y1, continent_affine);

            // 4) Round coordinates before affine transformation
            let (xg, yg) = round_coordinates(x2, y2, GEOMETRY_PRECISION);

            coord! { x: xg, y: yg }
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

#[inline]
pub(crate) fn hash_to_unit_u64(x: u64, salt: u64) -> f64 {
    // SplitMix64-ish -> [0,1)
    let mut z = x.wrapping_add(salt).wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^= z >> 31;
    ((z >> 11) as f64) / ((1u64 << 53) as f64)
}

#[inline]
fn bbox_from_affine(m: &[f64; 6]) -> (f64, f64, f64, f64) {
    // X = a*x + c, Y = e*y + f   (b=d=0)
    let (a, c, e, f) = (m[0], m[2], m[4], m[5]);
    let (west, east) = if a >= 0.0 { (c, c + a) } else { (c + a, c) };
    let (south, north) = if e >= 0.0 { (f, f + e) } else { (f + e, f) };
    (west, east, south, north)
}

/// Spherical band area ~ width * (sin(phi_n) - sin(phi_s))
#[inline]
fn spherical_bbox_weight(west: f64, east: f64, south: f64, north: f64) -> f64 {
    let deg2rad = std::f64::consts::PI / 180.0;
    let width = (east - west).abs() * deg2rad;
    let (phi_s, phi_n) = (south * deg2rad, north * deg2rad);
    let band = (phi_n.sin() - phi_s.sin()).max(0.0);
    (width * band).max(0.0)
}

#[derive(Clone, Debug)]
pub(crate) struct WeightedTarget {
    pub(crate) m: [f64; 6],
    pub(crate) cdf: f64,
}

#[inline]
pub fn build_continent_cdf(aff: &ContinentAffines) -> Vec<(&str, [f64; 6], f64)> {
    let items = [
        ("africa", &aff.africa),
        ("europe", &aff.europe),
        ("south_asia", &aff.south_asia),
        ("north_asia", &aff.north_asia),
        ("oceania", &aff.oceania),
        ("south_america", &aff.south_america),
        ("south_north_america", &aff.south_north_america),
        ("north_north_america", &aff.north_north_america),
    ];

    let mut targets: Vec<(&str, [f64; 6], f64)> = items
        .iter()
        .map(|(name, m)| {
            let (w, e, s, n) = bbox_from_affine(m);
            let wt = spherical_bbox_weight(w, e, s, n);
            (*name, **m, wt) // Dereference both name and m
        })
        .collect();

    // Sort by weight descending for better target selection
    targets.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

    // Build CDF
    let total_weight: f64 = targets.iter().map(|(_, _, w)| w).sum();
    let mut cumulative = 0.0;

    for (_, _, weight) in &mut targets {
        cumulative += *weight;
        *weight = cumulative / total_weight;
    }

    targets
}
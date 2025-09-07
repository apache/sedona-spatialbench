use geo::{coord, Geometry, LineString, Point, Polygon};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::f64::consts::PI;
use std::sync::OnceLock;

const GEOMETRY_PRECISION: f64 = 1000_000_000.0;

#[derive(Debug, Clone, Copy)]
pub enum DistributionType {
    Uniform,
    Normal,
    Diagonal,
    Sierpinski,
    Bit,
    Thomas,
    HierThomas,
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
    Thomas {
        parents: u32,        // number of parent centers (K)
        mean_offspring: f64, // global density scale (kept for compatibility)
        sigma: f64,          // cluster stddev in unit coords
        pareto_alpha: f64,   // tail parameter (>0). Smaller => heavier tail (e.g., 1.0–1.5)
        pareto_xm: f64,      // scale (>0), typically 1.0
    },

    // hierarchical Thomas (cities -> subclusters)
    HierThomas {
        cities: u32,             // # top-level “city” centers

        // variable subclusters per city (normal, clamped)
        sub_mean: f64,
        sub_sd: f64,
        sub_min: u32,
        sub_max: u32,

        sigma_city: f64,         // spread of subcluster centers around their city
        sigma_sub: f64,          // spread of final points around the chosen subcluster

        // Pareto weights
        pareto_alpha_city: f64,  // city weights
        pareto_xm_city: f64,
        pareto_alpha_sub: f64,   // subcluster weights (within a city)
        pareto_xm_sub: f64,
    },
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
pub struct ThomasCache {
    cdf: Vec<f64>,   // normalized CDF in parent index order
    parents: usize,
    alpha: f64,
    xm: f64,
    seed: u64,
}

#[derive(Clone, Debug)]
pub struct HierThomasCache {
    city_cdf: Vec<f64>,          // global CDF over cities
    sub_cdfs: Vec<Vec<f64>>,     // per-city CDF over subclusters (variable length)
    subcounts: Vec<u32>,         // per-city subcluster count
    cities: usize,
    alpha_city: f64,
    xm_city: f64,
    alpha_sub: f64,
    xm_sub: f64,
    seed: u64,
}

#[derive(Clone, Debug)]
pub struct SpiderGenerator {
    pub config: SpiderConfig,
    pub thomas_cache: OnceLock<ThomasCache>,
    pub hier_cache: OnceLock<HierThomasCache>,
}

impl SpiderGenerator {
    pub fn new(config: SpiderConfig, thomas_cache: OnceLock<ThomasCache>, hier_cache: OnceLock<HierThomasCache>) -> Self {
        Self { config, thomas_cache, hier_cache,}
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
            DistributionType::Thomas => self.generate_thomas(index, continent_affine),
            DistributionType::HierThomas   => self.generate_hier_thomas(index, continent_affine),
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

    fn generate_thomas(&self, index: u64, m: &[f64; 6]) -> Geometry {
        let (parents, _mean_offspring, sigma, alpha, xm) = match self.config.params {
            DistributionParams::Thomas { parents, mean_offspring, sigma, pareto_alpha, pareto_xm } => {
                (parents.max(1), mean_offspring.max(1e-9), sigma.max(1e-6), pareto_alpha.max(1e-6), pareto_xm.max(1e-12))
            }
            _ => (24, 12.0, 0.03, 1.2, 1.0), // sensible defaults: heavy skew
        };
        let k = parents as usize;

        // draw U once for parent selection (deterministic from index & seed)
        let u = hash_to_unit_u64(index, (self.config.seed as u64) ^ 0xBADD_F00D);

        // Try to use (or build) cached CDF if params match; otherwise do a one-off O(k) pick
        let pid = match self.thomas_cache.get() {
            Some(cache) if cache.parents == k && (cache.alpha - alpha).abs() < 1e-15 && (cache.xm - xm).abs() < 1e-15 && cache.seed == self.config.seed as u64 => {
                // binary search on cached CDF
                let mut lo = 0usize;
                let mut hi = k;
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    if u <= cache.cdf[mid] { hi = mid; } else { lo = mid + 1; }
                }
                lo.min(k.saturating_sub(1))
            }
            _ => {
                // build once and cache (first thread wins)
                let _ = self.get_thomas_cdf(k, alpha, xm, self.config.seed as u64);
                // re-check and use cache; if racing, or params still don’t match, fallback O(k)
                if let Some(cache) = self.thomas_cache.get() {
                    if cache.parents == k && (cache.alpha - alpha).abs() < 1e-15 && (cache.xm - xm).abs() < 1e-15 && cache.seed == self.config.seed as u64 {
                        let mut lo = 0usize;
                        let mut hi = k;
                        while lo < hi {
                            let mid = (lo + hi) / 2;
                            if u <= cache.cdf[mid] { hi = mid; } else { lo = mid + 1; }
                        }
                        lo.min(k.saturating_sub(1))
                    } else {
                        // one-off parent pick without caching
                        self.pick_parent_pareto_once(u, k, alpha, xm, self.config.seed as u64)
                    }
                } else {
                    self.pick_parent_pareto_once(u, k, alpha, xm, self.config.seed as u64)
                }
            }
        };

        // Parent center (deterministic Halton)
        let (cx, cy) = halton_2d(pid as u64 + 1, 2, 3);

        // Gaussian offset around parent
        let seed = spider_seed_for_index(index, (self.config.seed as u64) ^ 0xC177001);
        let mut rng = StdRng::seed_from_u64(seed);
        let dx = rand_normal(&mut rng, 0.0, sigma);
        let dy = rand_normal(&mut rng, 0.0, sigma);
        let x = (cx + dx).clamp(0.0, 1.0);
        let y = (cy + dy).clamp(0.0, 1.0);

        match self.config.geom_type {
            GeomType::Point   => generate_point_geom((x, y), m),
            GeomType::Box     => generate_box_geom((x, y), &self.config, &mut rng, m),
            GeomType::Polygon => generate_polygon_geom((x, y), &self.config, &mut rng, m),
        }
    }

    fn get_thomas_cdf(&self, parents: usize, alpha: f64, xm: f64, seed: u64) -> &ThomasCache {
        self.thomas_cache.get_or_init(|| {
            // Deterministic Pareto weight per parent (depends only on seed & pid)
            let mut weights = Vec::with_capacity(parents);
            for pid in 0..parents {
                // independent U for each parent:
                let u = u01_from_seed(spider_seed_for_index(pid as u64, seed ^ 0x7EED));
                weights.push(pareto_draw(u, alpha, xm));
            }
            let sum_w = weights.iter().copied().sum::<f64>().max(1e-12);
            let mut cdf = Vec::with_capacity(parents);
            let mut acc = 0.0;
            for w in weights {
                acc += w / sum_w;
                cdf.push(acc);
            }
            ThomasCache { cdf, parents, alpha, xm, seed }
        })
    }

    #[inline]
    fn pick_parent_pareto_once(&self, u: f64, k: usize, alpha: f64, xm: f64, seed: u64) -> usize {
        // two-pass: sum weights, then walk until reaching u
        let mut sum_w = 0.0;
        let mut tmp = vec![0.0; k];
        for pid in 0..k {
            let uu = u01_from_seed(spider_seed_for_index(pid as u64, seed ^ 0x7EED));
            let w = pareto_draw(uu, alpha, xm);
            tmp[pid] = w;
            sum_w += w;
        }
        let mut acc = 0.0;
        for pid in 0..k {
            acc += tmp[pid] / sum_w;
            if u <= acc {
                return pid;
            }
        }
        k.saturating_sub(1)
    }

    fn generate_hier_thomas(&self, index: u64, m: &[f64; 6]) -> Geometry {
        let (nc, sub_mean, sub_sd, sub_min, sub_max,
            sigma_city, sigma_sub, a_c, xm_c, a_s, xm_s) = match self.config.params {
            DistributionParams::HierThomas {
                cities,
                sub_mean, sub_sd, sub_min, sub_max,
                sigma_city, sigma_sub,
                pareto_alpha_city, pareto_xm_city,
                pareto_alpha_sub,  pareto_xm_sub,
            } => (
                cities.max(1),
                sub_mean, sub_sd, sub_min, sub_max,
                sigma_city.max(1e-6), sigma_sub.max(1e-6),
                pareto_alpha_city.max(1e-6), pareto_xm_city.max(1e-12),
                pareto_alpha_sub.max(1e-6),  pareto_xm_sub.max(1e-12),
            ),
            _ => (16, 8.0, 3.0, 2, 24, 0.05, 0.01, 1.1, 1.0, 1.2, 1.0),
        };

        let cities = nc as usize;

        // Build/reuse cache with variable subcounts
        let cache = self.get_hier_cache(
            cities,
            sub_mean, sub_sd, sub_min, sub_max,
            a_c, xm_c, a_s, xm_s,
            self.config.seed as u64,
        );

        // Independent uniforms for the two picks
        let u_city = hash_to_unit_u64(index, (self.config.seed as u64) ^ 0xC17C1CF);
        let u_sub  = hash_to_unit_u64(index, (self.config.seed as u64) ^ 0x53BFACE);

        // city pick
        let city_id = {
            let mut lo = 0usize;
            let mut hi = cache.cities;
            while lo < hi {
                let mid = (lo + hi) / 2;
                if u_city <= cache.city_cdf[mid] { hi = mid; } else { lo = mid + 1; }
            }
            lo.min(cache.cities.saturating_sub(1))
        };

        // subcluster pick (variable length)
        let cdf = &cache.sub_cdfs[city_id];
        let mut lo = 0usize;
        let mut hi = cdf.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if u_sub <= cdf[mid] { hi = mid; } else { lo = mid + 1; }
        }
        let sub_id = lo.min(cdf.len().saturating_sub(1));

        // city center (deterministic)
        let (cx, cy) = halton_2d(city_id as u64 + 1, 2, 3);

        // subcenter (deterministic Gaussian around city)
        let sub_seed = spider_seed_for_index((city_id as u64) << 32 | (sub_id as u64),
                                             (self.config.seed as u64) ^ 0xC173_5FB);
        let mut rng_sub = StdRng::seed_from_u64(sub_seed);
        let sx = (cx + rand_normal(&mut rng_sub, 0.0, sigma_city)).clamp(0.0, 1.0);
        let sy = (cy + rand_normal(&mut rng_sub, 0.0, sigma_city)).clamp(0.0, 1.0);

        // final point (Gaussian around subcenter)
        let pt_seed = spider_seed_for_index(index, (self.config.seed as u64) ^ 0xF136D);
        let mut rng_pt = StdRng::seed_from_u64(pt_seed);
        let x = (sx + rand_normal(&mut rng_pt, 0.0, sigma_sub)).clamp(0.0, 1.0);
        let y = (sy + rand_normal(&mut rng_pt, 0.0, sigma_sub)).clamp(0.0, 1.0);

        match self.config.geom_type {
            GeomType::Point   => generate_point_geom((x, y), m),
            GeomType::Box     => generate_box_geom((x, y), &self.config, &mut rng_pt, m),
            GeomType::Polygon => generate_polygon_geom((x, y), &self.config, &mut rng_pt, m),
        }
    }

    fn get_hier_cache(
        &self,
        cities: usize,
        sub_mean: f64,
        sub_sd: f64,
        sub_min: u32,
        sub_max: u32,
        alpha_city: f64,
        xm_city: f64,
        alpha_sub: f64,
        xm_sub: f64,
        seed: u64,
    ) -> &HierThomasCache {
        self.hier_cache.get_or_init(|| {
            // City CDF (Pareto weights)
            let mut city_w = Vec::with_capacity(cities);
            for cid in 0..cities {
                let u = u01_from_seed(spider_seed_for_index(cid as u64, seed ^ 0xC17E));
                city_w.push(pareto_draw(u, alpha_city, xm_city));
            }
            let sum_city = city_w.iter().copied().sum::<f64>().max(1e-12);
            let mut city_cdf = Vec::with_capacity(cities);
            let mut acc = 0.0;
            for w in city_w {
                acc += w / sum_city;
                city_cdf.push(acc);
            }

            // Per-city subcluster counts (deterministic normal)
            let mut subcounts = Vec::with_capacity(cities);
            for cid in 0..cities {
                // seed depends on (seed, cid) so it’s stable
                let s = spider_seed_for_index(cid as u64, seed ^ 0x53_EBC132);
                let k = sample_normal_count(sub_mean, sub_sd.max(1e-9), sub_min.max(1), sub_max.max(1), s);
                subcounts.push(k);
            }

            // Per-city subcluster CDFs (Pareto weights), length = subcounts[cid]
            let mut sub_cdfs = Vec::with_capacity(cities);
            for cid in 0..cities {
                let n_sub = subcounts[cid] as usize;
                let mut w = Vec::with_capacity(n_sub);
                for sid in 0..n_sub {
                    let u = u01_from_seed(spider_seed_for_index(((cid as u64) << 32) | sid as u64, seed ^ 0x5EB5));
                    w.push(pareto_draw(u, alpha_sub, xm_sub));
                }
                let sum_w = w.iter().copied().sum::<f64>().max(1e-12);
                let mut cdf = Vec::with_capacity(n_sub);
                let mut sacc = 0.0;
                for wi in w {
                    sacc += wi / sum_w;
                    cdf.push(sacc);
                }
                sub_cdfs.push(cdf);
            }

            HierThomasCache {
                city_cdf,
                sub_cdfs,
                subcounts,
                cities,
                alpha_city,
                xm_city,
                alpha_sub,
                xm_sub,
                seed,
            }
        })
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

#[inline]
fn u01_from_seed(seed: u64) -> f64 {
    let z = spider_seed_for_index(seed, 0xA1CE_CAFE);
    ((z >> 11) as f64) / ((1u64 << 53) as f64) // [0,1)
}

#[inline]
fn pareto_draw(u: f64, alpha: f64, xm: f64) -> f64 {
    // Inverse CDF: X = xm / (1-u)^(1/alpha)
    let a = alpha.max(1e-6);
    let s = xm.max(1e-12);
    s / (1.0 - u).powf(1.0 / a)
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

#[inline]
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

#[inline]
fn halton_2d(i: u64, base_x: u32, base_y: u32) -> (f64, f64) {
    (radical_inverse(i, base_x), radical_inverse(i, base_y))
}

#[inline]
fn sample_normal_count(mu: f64, sd: f64, min_v: u32, max_v: u32, seed: u64) -> u32 {
    let mut rng = StdRng::seed_from_u64(seed);
    let draw = rand_normal(&mut rng, mu, sd).round();
    let mut k = draw.max(min_v as f64) as u32;
    if k > max_v { k = max_v; }
    if k < 1 { k = 1; }
    k
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
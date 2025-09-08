use crate::spatial::cache::{HierThomasCache, ThomasCache};
use crate::spatial::geometry::emit_geom;
use crate::spatial::utils::{
    build_cdf_from_weights, gauss_around, halton_2d, hash_to_unit_u64, pareto_draw, pick_from_cdf,
    rand_normal, sample_normal_count, seeded_rng, spider_seed_for_index, u01_from_seed, unit_clamp,
};
use crate::spatial::{DistributionParams, SpatialConfig};
use geo::Geometry;
use rand::rngs::StdRng;
use rand::Rng;
use std::sync::OnceLock;

pub fn generate_uniform(index: u64, config: &SpatialConfig, m: &[f64; 6]) -> Geometry {
    let seed = spider_seed_for_index(index, config.seed as u64);
    let mut rng = seeded_rng(seed);
    let center = (rng.gen::<f64>(), rng.gen::<f64>());
    emit_geom(center, config.geom_type, config, &mut rng, m)
}

pub fn generate_normal(index: u64, config: &SpatialConfig, m: &[f64; 6]) -> Geometry {
    let seed = spider_seed_for_index(index, config.seed as u64);
    let mut rng = seeded_rng(seed);
    let (mu, sigma) = match config.params {
        DistributionParams::Normal { mu, sigma } => (mu, sigma),
        _ => panic!("Expected Normal params, got {:?}", config.params),
    };
    let center = (
        unit_clamp(rand_normal(&mut rng, mu, sigma)),
        unit_clamp(rand_normal(&mut rng, mu, sigma)),
    );
    emit_geom(center, config.geom_type, config, &mut rng, m)
}

pub fn generate_diagonal(index: u64, config: &SpatialConfig, m: &[f64; 6]) -> Geometry {
    let seed = spider_seed_for_index(index, config.seed as u64);
    let mut rng = seeded_rng(seed);
    let (p, buffer) = match config.params {
        DistributionParams::Diagonal { percentage, buffer } => (percentage, buffer),
        _ => panic!("Expected Diagonal params, got {:?}", config.params),
    };

    let center = if rng.gen::<f64>() < p {
        let v: f64 = rng.gen();
        (v, v)
    } else {
        let c: f64 = rng.gen();
        let d: f64 = rand_normal(&mut rng, 0.0, buffer / 5.0);
        let x = unit_clamp(c + d / f64::sqrt(2.0));
        let y = unit_clamp(c - d / f64::sqrt(2.0));
        (x, y)
    };

    emit_geom(center, config.geom_type, config, &mut rng, m)
}

pub fn generate_bit(index: u64, config: &SpatialConfig, m: &[f64; 6]) -> Geometry {
    let seed = spider_seed_for_index(index, config.seed as u64);
    let mut rng = seeded_rng(seed);
    let (prob, digits) = match config.params {
        DistributionParams::Bit {
            probability,
            digits,
        } => (probability, digits),
        _ => panic!("Expected Bit params, got {:?}", config.params),
    };

    let center = (
        spider_bit(&mut rng, prob, digits),
        spider_bit(&mut rng, prob, digits),
    );
    emit_geom(center, config.geom_type, config, &mut rng, m)
}

pub fn generate_sierpinski(index: u64, config: &SpatialConfig, m: &[f64; 6]) -> Geometry {
    let seed = spider_seed_for_index(index, config.seed as u64);
    let mut rng = seeded_rng(seed);
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
    emit_geom((x, y), config.geom_type, config, &mut rng, m)
}

pub fn generate_thomas(
    index: u64,
    config: &SpatialConfig,
    thomas_cache: &OnceLock<ThomasCache>,
    m: &[f64; 6],
) -> Geometry {
    let (parents, _mean_offspring, sigma, alpha, xm) = match config.params {
        DistributionParams::Thomas {
            parents,
            mean_offspring,
            sigma,
            pareto_alpha,
            pareto_xm,
        } => (
            parents.max(1),
            mean_offspring.max(1e-9),
            sigma.max(1e-6),
            pareto_alpha.max(1e-6),
            pareto_xm.max(1e-12),
        ),
        _ => panic!("Expected Thomas params, got {:?}", config.params),
    };

    let k = parents as usize;
    let u = hash_to_unit_u64(index, (config.seed as u64) ^ 0xBADD_F00D);

    let pid = match thomas_cache.get() {
        Some(cache)
            if cache.parents == k
                && (cache.alpha - alpha).abs() < 1e-15
                && (cache.xm - xm).abs() < 1e-15
                && cache.seed == config.seed as u64 =>
        {
            pick_from_cdf(&cache.cdf, u)
        }
        _ => {
            get_or_create_thomas_cache(thomas_cache, k, alpha, xm, config.seed as u64);
            if let Some(cache) = thomas_cache.get() {
                pick_from_cdf(&cache.cdf, u)
            } else {
                pick_parent_pareto_once(u, k, alpha, xm, config.seed as u64)
            }
        }
    };

    let (cx, cy) = halton_2d(pid as u64 + 1, 2, 3);
    let mut rng = seeded_rng(spider_seed_for_index(
        index,
        (config.seed as u64) ^ 0xC177001,
    ));
    let center = gauss_around(&mut rng, (cx, cy), sigma);

    emit_geom(center, config.geom_type, config, &mut rng, m)
}

#[inline]
fn pick_parent_pareto_once(u: f64, k: usize, alpha: f64, xm: f64, seed: u64) -> usize {
    let mut weights = Vec::with_capacity(k);
    for pid in 0..k {
        let uu = u01_from_seed(spider_seed_for_index(pid as u64, seed ^ 0x7EED));
        weights.push(pareto_draw(uu, alpha, xm));
    }
    let cdf = build_cdf_from_weights(weights);
    pick_from_cdf(&cdf, u)
}

pub fn generate_hier_thomas(
    index: u64,
    config: &SpatialConfig,
    hier_cache: &OnceLock<HierThomasCache>,
    m: &[f64; 6],
) -> Geometry {
    let (nc, sub_mean, sub_sd, sub_min, sub_max, sigma_city, sigma_sub, a_c, xm_c, a_s, xm_s) =
        match config.params {
            DistributionParams::HierThomas {
                cities,
                sub_mean,
                sub_sd,
                sub_min,
                sub_max,
                sigma_city,
                sigma_sub,
                pareto_alpha_city,
                pareto_xm_city,
                pareto_alpha_sub,
                pareto_xm_sub,
            } => (
                cities.max(1),
                sub_mean,
                sub_sd,
                sub_min,
                sub_max,
                sigma_city.max(1e-6),
                sigma_sub.max(1e-6),
                pareto_alpha_city.max(1e-6),
                pareto_xm_city.max(1e-12),
                pareto_alpha_sub.max(1e-6),
                pareto_xm_sub.max(1e-12),
            ),
            _ => panic!("Expected HierThomas params, got {:?}", config.params),
        };

    let cache = get_or_create_hier_cache(
        hier_cache,
        nc as usize,
        sub_mean,
        sub_sd,
        sub_min,
        sub_max,
        a_c,
        xm_c,
        a_s,
        xm_s,
        config.seed as u64,
    );

    let u_city = hash_to_unit_u64(index, (config.seed as u64) ^ 0xC17C1CF);
    let u_sub = hash_to_unit_u64(index, (config.seed as u64) ^ 0x53BFACE);

    let city_id = pick_from_cdf(&cache.city_cdf, u_city);
    let sub_id = pick_from_cdf(&cache.sub_cdfs[city_id], u_sub);

    let (cx, cy) = halton_2d(city_id as u64 + 1, 2, 3);
    let mut rng_sub = seeded_rng(spider_seed_for_index(
        ((city_id as u64) << 32) | (sub_id as u64),
        (config.seed as u64) ^ 0x0C17_35FB,
    ));
    let (sx, sy) = gauss_around(&mut rng_sub, (cx, cy), sigma_city);

    let mut rng_pt = seeded_rng(spider_seed_for_index(index, (config.seed as u64) ^ 0xF136D));
    let center = gauss_around(&mut rng_pt, (sx, sy), sigma_sub);

    emit_geom(center, config.geom_type, config, &mut rng_pt, m)
}

fn get_or_create_thomas_cache(
    thomas_cache: &OnceLock<ThomasCache>,
    parents: usize,
    alpha: f64,
    xm: f64,
    seed: u64,
) {
    let _ = thomas_cache.get_or_init(|| {
        let weights: Vec<f64> = (0..parents)
            .map(|pid| {
                let u = u01_from_seed(spider_seed_for_index(pid as u64, seed ^ 0x7EED));
                pareto_draw(u, alpha, xm)
            })
            .collect();
        let cdf = build_cdf_from_weights(weights);
        ThomasCache {
            cdf,
            parents,
            alpha,
            xm,
            seed,
        }
    });
}

#[allow(clippy::too_many_arguments)]
fn get_or_create_hier_cache(
    hier_cache: &OnceLock<HierThomasCache>,
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
    hier_cache.get_or_init(|| {
        let city_weights: Vec<f64> = (0..cities)
            .map(|cid| {
                let u = u01_from_seed(spider_seed_for_index(cid as u64, seed ^ 0xC17E));
                pareto_draw(u, alpha_city, xm_city)
            })
            .collect();
        let city_cdf = build_cdf_from_weights(city_weights);

        let subcounts: Vec<u32> = (0..cities)
            .map(|cid| {
                let s = spider_seed_for_index(cid as u64, seed ^ 0x53_EBC132);
                sample_normal_count(
                    sub_mean,
                    sub_sd.max(1e-9),
                    sub_min.max(1),
                    sub_max.max(1),
                    s,
                )
            })
            .collect();

        let mut sub_cdfs = Vec::with_capacity(cities);
        for (cid, subcount) in subcounts.iter().enumerate().take(cities) {
            let n_sub = *subcount as usize;
            let weights: Vec<f64> = (0..n_sub)
                .map(|sid| {
                    let u = u01_from_seed(spider_seed_for_index(
                        ((cid as u64) << 32) | sid as u64,
                        seed ^ 0x5EB5,
                    ));
                    pareto_draw(u, alpha_sub, xm_sub)
                })
                .collect();
            sub_cdfs.push(build_cdf_from_weights(weights));
        }

        HierThomasCache { city_cdf, sub_cdfs }
    })
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

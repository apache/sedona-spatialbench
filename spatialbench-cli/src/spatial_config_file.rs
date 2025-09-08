use anyhow::Result;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use spatialbench::spatial::{
    DistributionParams, DistributionType, GeomType, SpatialConfig, SpatialGenerator,
};
use std::fmt;
use std::sync::OnceLock;

// Deserializer for DistributionType
fn deserialize_distribution_type<'de, D>(deserializer: D) -> Result<DistributionType, D::Error>
where
    D: Deserializer<'de>,
{
    struct DistributionTypeVisitor;

    impl Visitor<'_> for DistributionTypeVisitor {
        type Value = DistributionType;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string representing distribution type")
        }

        fn visit_str<E>(self, value: &str) -> Result<DistributionType, E>
        where
            E: de::Error,
        {
            match value.to_lowercase().as_str() {
                "uniform" => Ok(DistributionType::Uniform),
                "normal" => Ok(DistributionType::Normal),
                "diagonal" => Ok(DistributionType::Diagonal),
                "bit" => Ok(DistributionType::Bit),
                "sierpinski" => Ok(DistributionType::Sierpinski),
                "thomas" => Ok(DistributionType::Thomas),
                "hierthomas" => Ok(DistributionType::HierThomas),
                _ => Err(E::custom(format!("unknown distribution type: {}", value))),
            }
        }
    }

    deserializer.deserialize_str(DistributionTypeVisitor)
}

// Deserializer for GeomType
fn deserialize_geom_type<'de, D>(deserializer: D) -> Result<GeomType, D::Error>
where
    D: Deserializer<'de>,
{
    struct GeomTypeVisitor;

    impl Visitor<'_> for GeomTypeVisitor {
        type Value = GeomType;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a string representing geometry type")
        }

        fn visit_str<E>(self, value: &str) -> Result<GeomType, E>
        where
            E: de::Error,
        {
            match value.to_lowercase().as_str() {
                "point" => Ok(GeomType::Point),
                "box" => Ok(GeomType::Box),
                "polygon" => Ok(GeomType::Polygon),
                _ => Err(E::custom(format!("unknown geometry type: {}", value))),
            }
        }
    }

    deserializer.deserialize_str(GeomTypeVisitor)
}

#[derive(Deserialize)]
pub struct SpatialConfigFile {
    pub trip: Option<InlineSpatialConfig>,
    pub building: Option<InlineSpatialConfig>,
}

#[derive(Deserialize)]
pub struct InlineSpatialConfig {
    #[serde(deserialize_with = "deserialize_distribution_type")]
    pub dist_type: DistributionType,
    #[serde(deserialize_with = "deserialize_geom_type")]
    pub geom_type: GeomType,
    pub dim: u8,
    pub seed: u32,
    // geometry = box
    pub width: f64,
    pub height: f64,
    // geometry = polygon
    pub maxseg: i32,
    pub polysize: f64,
    pub params: InlineParams,
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum InlineParams {
    None,
    Normal {
        mu: f64,
        sigma: f64,
    },
    Diagonal {
        percentage: f64,
        buffer: f64,
    },
    Bit {
        probability: f64,
        digits: u32,
    },

    // Thomas (Gaussian Neyman–Scott): K parent clusters, Gaussian spread, optional lognormal skew
    Thomas {
        parents: u32,        // number of parent centers (K)
        mean_offspring: f64, // global density scale (kept for compatibility)
        sigma: f64,          // cluster stddev in unit coords
        // Pareto weights per parent (heavier tail => more skew)
        pareto_alpha: f64, // tail parameter (>0). Smaller => heavier tail (e.g., 1.0–1.5)
        pareto_xm: f64,    // scale (>0), typically 1.0
    },

    HierThomas {
        cities: u32, // top-level “city” centers
        sub_mean: f64,
        sub_sd: f64,
        sub_min: u32,
        sub_max: u32,
        sigma_city: f64,        // spread of subcluster centers around their city
        sigma_sub: f64,         // spread of final points around the chosen subcluster
        pareto_alpha_city: f64, // Pareto tail for city weights
        pareto_xm_city: f64,    // Pareto scale (xmin) for city weights
        pareto_alpha_sub: f64,  // Pareto tail for subcluster weights (within a city)
        pareto_xm_sub: f64,     // Pareto scale (xmin) for subcluster weights
    },
}

impl InlineSpatialConfig {
    pub fn to_generator(&self) -> SpatialGenerator {
        let params = match &self.params {
            InlineParams::None => DistributionParams::None,
            InlineParams::Normal { mu, sigma } => DistributionParams::Normal {
                mu: *mu,
                sigma: *sigma,
            },
            InlineParams::Diagonal { percentage, buffer } => DistributionParams::Diagonal {
                percentage: *percentage,
                buffer: *buffer,
            },
            InlineParams::Bit {
                probability,
                digits,
            } => DistributionParams::Bit {
                probability: *probability,
                digits: *digits,
            },
            InlineParams::Thomas {
                parents,
                mean_offspring,
                sigma,
                pareto_alpha,
                pareto_xm,
            } => DistributionParams::Thomas {
                parents: *parents,
                mean_offspring: *mean_offspring,
                sigma: *sigma,
                pareto_alpha: *pareto_alpha,
                pareto_xm: *pareto_xm,
            },
            InlineParams::HierThomas {
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
            } => DistributionParams::HierThomas {
                cities: *cities, // top-level “city” centers
                sub_mean: *sub_mean,
                sub_sd: *sub_sd,
                sub_min: *sub_min,
                sub_max: *sub_max,
                sigma_city: *sigma_city, // spread of subcluster centers around their city
                sigma_sub: *sigma_sub,   // spread of final points around the chosen subcluster
                pareto_alpha_city: *pareto_alpha_city, // Pareto tail for city weights
                pareto_xm_city: *pareto_xm_city, // Pareto scale (xmin) for city weights
                pareto_alpha_sub: *pareto_alpha_sub, // Pareto tail for subcluster weights (within a city)
                pareto_xm_sub: *pareto_xm_sub,       // Pareto scale (xmin) for subcluster weights
            },
        };

        let cfg = SpatialConfig {
            dist_type: self.dist_type,
            geom_type: self.geom_type,
            dim: self.dim as i32,
            seed: self.seed,
            width: self.width,
            height: self.height,
            maxseg: self.maxseg,
            polysize: self.polysize,
            params,
        };
        SpatialGenerator::new(cfg, OnceLock::new(), OnceLock::new())
    }
}

pub fn parse_yaml(text: &str) -> Result<SpatialConfigFile> {
    log::info!("Default spider config is being overridden by user-provided configuration");
    Ok(serde_yaml::from_str::<SpatialConfigFile>(text)?)
}

use anyhow::Result;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use spatialbench::spider::{
    DistributionParams, DistributionType, GeomType, SpiderConfig, SpiderGenerator,
};
use std::fmt;

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
pub struct SpiderConfigFile {
    pub trip: Option<InlineSpiderConfig>,
    pub building: Option<InlineSpiderConfig>,
}

#[derive(Deserialize)]
pub struct InlineSpiderConfig {
    #[serde(deserialize_with = "deserialize_distribution_type")]
    pub dist_type: DistributionType,
    #[serde(deserialize_with = "deserialize_geom_type")]
    pub geom_type: GeomType,
    pub dim: u8,
    pub seed: u32,
    pub affine: Option<[f64; 6]>,
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
    Normal { mu: f64, sigma: f64 },
    Diagonal { percentage: f64, buffer: f64 },
    Bit { probability: f64, digits: u32 },
    Parcel { srange: f64, dither: f64 },
}

impl InlineSpiderConfig {
    pub fn to_generator(&self) -> SpiderGenerator {
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
            InlineParams::Parcel { srange, dither } => DistributionParams::Parcel {
                srange: *srange,
                dither: *dither,
            },
        };

        let cfg = SpiderConfig {
            dist_type: self.dist_type,
            geom_type: self.geom_type,
            dim: self.dim as i32,
            seed: self.seed,
            affine: self.affine,
            width: self.width,
            height: self.height,
            maxseg: self.maxseg,
            polysize: self.polysize,
            params,
        };
        SpiderGenerator::new(cfg)
    }
}

pub fn parse_yaml(text: &str) -> Result<SpiderConfigFile> {
    log::info!("Default spider config is being overridden by user-provided configuration");
    Ok(serde_yaml::from_str::<SpiderConfigFile>(text)?)
}

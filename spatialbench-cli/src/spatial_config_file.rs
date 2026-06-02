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

use anyhow::Result;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};
use spatialbench::spatial::{
    ContinentAffines, DistributionParams, DistributionType, GeomType, SpatialConfig,
    SpatialGenerator,
};
use spatialbench_raster::cog::{CogConfig, RasterDtype};
use spatialbench_raster::footprint::FootprintConfig;
use std::fmt;
use std::io;
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
                "hierarchicalthomas" => Ok(DistributionType::HierarchicalThomas),
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
    pub raster: Option<RasterConfig>,
}

/// Configuration for raster generation from config file.
///
/// All fields are optional with defaults matching [`CogConfig::default()`]
/// and [`FootprintConfig::default()`].
#[derive(Deserialize, Debug)]
pub struct RasterConfig {
    /// Pixel resolution in meters.
    #[serde(default = "default_resolution")]
    pub resolution: u32,
    /// COG width in pixels.
    #[serde(default = "default_cog_width")]
    pub cog_width: u32,
    /// COG height in pixels.
    #[serde(default = "default_cog_height")]
    pub cog_height: u32,
    /// Internal COG tile size (pixels per side).
    #[serde(default = "default_tile_size")]
    pub tile_size: u32,
    /// Perlin noise frequency. Low-level knob for compression ratio: higher
    /// frequency = higher entropy = lower ratio. Dimension- and dtype-specific
    /// (e.g. ~112 yields ~2× only at 10980² UInt16). Ignored when
    /// `target_compression_ratio` is set.
    #[serde(default = "default_noise_frequency")]
    pub noise_frequency: f32,
    /// Pixel data type: "uint8", "uint16", "float32".
    #[serde(default = "default_dtype")]
    pub dtype: String,
    /// ZSTD compression level (1=fastest .. 22). See [`CogConfig::zstd_level`].
    #[serde(default = "default_zstd_level")]
    pub zstd_level: i32,
    /// Optional target compression ratio (raw / compressed, e.g. 2.0). When set,
    /// `noise_frequency` is calibrated at startup to hit this ratio for the
    /// configured dtype/dimensions/zstd_level, and the configured/default
    /// `noise_frequency` is ignored. Must be > 1.0. Prefer this over hand-tuning
    /// `noise_frequency`, which is dimension- and dtype-specific.
    #[serde(default)]
    pub target_compression_ratio: Option<f32>,
    /// Continent for spatial coverage. Valid values:
    /// "south_north_america" (default), "north_north_america", "europe",
    /// "africa", "south_asia", "north_asia", "oceania", "south_america".
    #[serde(default = "default_continent")]
    pub continent: String,
}

fn default_resolution() -> u32 {
    60
}
fn default_cog_width() -> u32 {
    1830
}
fn default_cog_height() -> u32 {
    1830
}
fn default_tile_size() -> u32 {
    256
}
fn default_noise_frequency() -> f32 {
    8.0
}
fn default_zstd_level() -> i32 {
    6
}
fn default_dtype() -> String {
    "uint8".to_string()
}
fn default_continent() -> String {
    "south_north_america".to_string()
}

/// Parse a dtype string into a [`RasterDtype`].
fn parse_dtype(s: &str) -> io::Result<RasterDtype> {
    match s {
        "uint8" => Ok(RasterDtype::UInt8),
        "uint16" => Ok(RasterDtype::UInt16),
        "float32" => Ok(RasterDtype::Float32),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported raster dtype: '{s}'. Valid values: uint8, uint16, float32"),
        )),
    }
}

/// Look up a continent's affine from [`ContinentAffines`] by name.
pub fn continent_affine(affines: &ContinentAffines, name: &str) -> io::Result<[f64; 6]> {
    match name {
        "africa" => Ok(affines.africa),
        "europe" => Ok(affines.europe),
        "south_asia" => Ok(affines.south_asia),
        "north_asia" => Ok(affines.north_asia),
        "oceania" => Ok(affines.oceania),
        "south_america" => Ok(affines.south_america),
        "south_north_america" => Ok(affines.south_north_america),
        "north_north_america" => Ok(affines.north_north_america),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "unknown continent: '{name}'. Valid values: africa, europe, south_asia, \
                 north_asia, oceania, south_america, south_north_america, north_north_america"
            ),
        )),
    }
}

impl Default for RasterConfig {
    fn default() -> Self {
        Self {
            resolution: default_resolution(),
            cog_width: default_cog_width(),
            cog_height: default_cog_height(),
            tile_size: default_tile_size(),
            noise_frequency: default_noise_frequency(),
            dtype: default_dtype(),
            zstd_level: default_zstd_level(),
            target_compression_ratio: None,
            continent: default_continent(),
        }
    }
}

impl RasterConfig {
    /// Convert to [`CogConfig`], parsing the dtype string.
    ///
    /// Note: `target_compression_ratio` is validated here but applied by the
    /// caller (it requires a calibration pass over sample tiles), so this
    /// conversion stays pure. `noise_frequency` is passed through as-is.
    pub fn to_cog_config(&self) -> io::Result<CogConfig> {
        let dtype = parse_dtype(&self.dtype)?;
        if let Some(r) = self.target_compression_ratio {
            if !(r.is_finite() && r > 1.0) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("target_compression_ratio must be a finite value > 1.0, got {r}"),
                ));
            }
        }
        Ok(CogConfig {
            raster: FootprintConfig {
                cog_width: self.cog_width,
                cog_height: self.cog_height,
                resolution: self.resolution,
            },
            tile_size: self.tile_size,
            noise_frequency: self.noise_frequency,
            dtype,
            zstd_level: self.zstd_level,
        })
    }
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

    HierarchicalThomas {
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
            InlineParams::HierarchicalThomas {
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
            } => DistributionParams::HierarchicalThomas {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_raster_config_defaults() {
        let yaml = "raster: {}";
        let cfg = parse_yaml(yaml).unwrap();
        let raster = cfg.raster.unwrap();
        assert_eq!(raster.resolution, 60);
        assert_eq!(raster.cog_width, 1830);
        assert_eq!(raster.tile_size, 256);
    }

    #[test]
    fn parse_raster_config_custom() {
        let yaml = r#"
raster:
  resolution: 10
  cog_width: 1024
  cog_height: 1024
  tile_size: 512
  noise_frequency: 4.0
"#;
        let cfg = parse_yaml(yaml).unwrap();
        let raster = cfg.raster.unwrap();
        assert_eq!(raster.resolution, 10);
        assert_eq!(raster.cog_width, 1024);
        let cog_config = raster.to_cog_config().unwrap();
        assert_eq!(cog_config.raster.resolution, 10);
        assert_eq!(cog_config.tile_size, 512);
    }

    #[test]
    fn parse_raster_config_absent() {
        let yaml = "trip:\n  dist_type: uniform\n  geom_type: point\n  dim: 2\n  seed: 42\n  width: 1.0\n  height: 1.0\n  maxseg: 5\n  polysize: 0.5\n  params:\n    type: none";
        let cfg = parse_yaml(yaml).unwrap();
        assert!(cfg.raster.is_none());
    }

    #[test]
    fn target_ratio_parses_when_present() {
        let raster = parse_yaml("raster:\n  target_compression_ratio: 2.5")
            .unwrap()
            .raster
            .unwrap();
        assert_eq!(raster.target_compression_ratio, Some(2.5));
    }

    #[test]
    fn target_ratio_absent_is_none() {
        let raster = parse_yaml("raster: {}").unwrap().raster.unwrap();
        assert_eq!(raster.target_compression_ratio, None);
    }

    #[test]
    fn target_ratio_invalid_rejected() {
        for bad in ["1.0", "0.5", ".nan"] {
            let raster = parse_yaml(&format!("raster:\n  target_compression_ratio: {bad}"))
                .unwrap()
                .raster
                .unwrap();
            let err = raster.to_cog_config().unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }
}

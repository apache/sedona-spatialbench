use crate::spatial::cache::{HierThomasCache, ThomasCache};
use crate::spatial::distributions::*;
use crate::spatial::{DistributionType, SpatialConfig};
use geo::Geometry;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct SpatialGenerator {
    pub config: SpatialConfig,
    pub thomas_cache: OnceLock<ThomasCache>,
    pub hier_cache: OnceLock<HierThomasCache>,
}

impl SpatialGenerator {
    pub fn new(
        config: SpatialConfig,
        thomas_cache: OnceLock<ThomasCache>,
        hier_cache: OnceLock<HierThomasCache>,
    ) -> Self {
        Self {
            config,
            thomas_cache,
            hier_cache,
        }
    }

    pub fn generate(&self, index: u64, continent_affine: &[f64; 6]) -> Geometry {
        match self.config.dist_type {
            DistributionType::Uniform => generate_uniform(index, &self.config, continent_affine),
            DistributionType::Normal => generate_normal(index, &self.config, continent_affine),
            DistributionType::Diagonal => generate_diagonal(index, &self.config, continent_affine),
            DistributionType::Bit => generate_bit(index, &self.config, continent_affine),
            DistributionType::Sierpinski => {
                generate_sierpinski(index, &self.config, continent_affine)
            }
            DistributionType::Thomas => {
                generate_thomas(index, &self.config, &self.thomas_cache, continent_affine)
            }
            DistributionType::HierThomas => {
                generate_hier_thomas(index, &self.config, &self.hier_cache, continent_affine)
            }
        }
    }
}

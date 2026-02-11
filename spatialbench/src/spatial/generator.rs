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
            DistributionType::HierarchicalThomas => generate_hierarchical_thomas(
                index,
                &self.config,
                &self.hier_cache,
                continent_affine,
            ),
        }
    }
}

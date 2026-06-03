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

//! Topology definitions and scene assignment logic.

use crate::scaling::ScalingTier;

/// The three topology lenses over a shared COG pile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Topology {
    /// T-heavy: many items × few channels. Stresses item enumeration.
    Temporal,
    /// Balanced: moderate items × moderate channels. Stresses asset pruning.
    Balanced,
    /// M-heavy: few items × wide channels. Stresses bulk per-item read.
    Wide,
}

impl Topology {
    /// All three topologies.
    pub const ALL: [Topology; 3] = [Topology::Temporal, Topology::Balanced, Topology::Wide];

    /// Topologies that share the single-band COG pile (Wide uses multi-band COGs).
    pub const SHARED_PILE: [Topology; 2] = [Topology::Temporal, Topology::Balanced];

    /// Return the (M, T) factoring for this topology from a scaling tier.
    pub fn factor(&self, tier: &ScalingTier) -> (u32, u32) {
        match self {
            Topology::Temporal => tier.temporal,
            Topology::Balanced => tier.balanced,
            Topology::Wide => tier.wide,
        }
    }

    /// Directory name for output paths.
    pub fn dir_name(&self) -> &'static str {
        match self {
            Topology::Temporal => "temporal",
            Topology::Balanced => "balanced",
            Topology::Wide => "wide",
        }
    }

    /// Item ID prefix for STAC catalogs.
    pub fn item_prefix(&self) -> &'static str {
        match self {
            Topology::Temporal => "TMP",
            Topology::Balanced => "BAL",
            Topology::Wide => "WDE",
        }
    }

    /// Semantic asset role labels for this topology.
    ///
    /// Returns a slice of role names. The slice length must be >= M for
    /// the topology at the given scale factor. Labels cycle if M exceeds
    /// the base label set.
    pub fn asset_labels(&self) -> &'static [&'static str] {
        match self {
            Topology::Temporal => &["tasmax", "tasmin"],
            Topology::Balanced => &[
                "red", "green", "blue", "nir", "swir1", "swir2", "coastal", "rededge1", "rededge2",
                "rededge3", "cirrus", "tir",
            ],
            Topology::Wide => &["dim"],
        }
    }

    /// Get the asset role label for a given mosaic index within this topology.
    ///
    /// For Temporal (M=2): cycles through `["tasmax", "tasmin"]`.
    /// For Balanced: cycles through spectral band names.
    /// For Wide: `"dim_NNN"` but Wide is not used with shared pile.
    pub fn asset_label_for(&self, mosaic_id: u32) -> String {
        let labels = self.asset_labels();
        if labels.len() == 1 {
            // Wide-style: dim_000, dim_001, ...
            format!("{}_{:03}", labels[0], mosaic_id)
        } else {
            labels[mosaic_id as usize % labels.len()].to_string()
        }
    }
}

/// A scene's assignment within a topology: which mosaic slot and which timeslice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SceneAssignment {
    /// Channel/mosaic index within an item (0..M).
    pub mosaic_id: u32,
    /// Timeslice index (0..T).
    pub timeslice_id: u32,
}

/// Assign a `cog_id` (0-based, < N) to a `(mosaic_id, timeslice_id)` pair
/// given the topology's M value.
///
/// The mapping is: `mosaic_id = cog_id % M`, `timeslice_id = cog_id / M`.
pub fn assign_scene(cog_id: u32, m: u32) -> SceneAssignment {
    SceneAssignment {
        mosaic_id: cog_id % m,
        timeslice_id: cog_id / m,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaling::SCALING_TABLE;

    #[test]
    fn factor_sf1() {
        let tier = &SCALING_TABLE[0];
        assert_eq!(Topology::Temporal.factor(tier), (2, 8));
        assert_eq!(Topology::Balanced.factor(tier), (4, 4));
        assert_eq!(Topology::Wide.factor(tier), (8, 2));
    }

    #[test]
    fn scene_assignment_temporal() {
        // M=2: cog 0 → (0,0), cog 1 → (1,0), cog 2 → (0,1), cog 3 → (1,1)
        assert_eq!(
            assign_scene(0, 2),
            SceneAssignment {
                mosaic_id: 0,
                timeslice_id: 0
            }
        );
        assert_eq!(
            assign_scene(1, 2),
            SceneAssignment {
                mosaic_id: 1,
                timeslice_id: 0
            }
        );
        assert_eq!(
            assign_scene(2, 2),
            SceneAssignment {
                mosaic_id: 0,
                timeslice_id: 1
            }
        );
        assert_eq!(
            assign_scene(3, 2),
            SceneAssignment {
                mosaic_id: 1,
                timeslice_id: 1
            }
        );
    }

    #[test]
    fn scene_assignment_wide() {
        // M=8: cog 0 → (0,0), cog 7 → (7,0), cog 8 → (0,1)
        assert_eq!(
            assign_scene(0, 8),
            SceneAssignment {
                mosaic_id: 0,
                timeslice_id: 0
            }
        );
        assert_eq!(
            assign_scene(7, 8),
            SceneAssignment {
                mosaic_id: 7,
                timeslice_id: 0
            }
        );
        assert_eq!(
            assign_scene(8, 8),
            SceneAssignment {
                mosaic_id: 0,
                timeslice_id: 1
            }
        );
    }

    #[test]
    fn all_cogs_assigned_exactly_once() {
        let tier = &SCALING_TABLE[0]; // SF=1, N=16
        for topo in Topology::ALL {
            let (m, t) = topo.factor(tier);
            // Every cog_id in 0..N maps to a unique (mosaic, timeslice)
            let mut seen = std::collections::HashSet::new();
            for cog_id in 0..tier.scenes_per_footprint {
                let a = assign_scene(cog_id, m);
                assert!(a.mosaic_id < m);
                assert!(a.timeslice_id < t);
                assert!(seen.insert((a.mosaic_id, a.timeslice_id)));
            }
            assert_eq!(seen.len(), (m * t) as usize);
        }
    }

    #[test]
    fn dir_names() {
        assert_eq!(Topology::Temporal.dir_name(), "temporal");
        assert_eq!(Topology::Balanced.dir_name(), "balanced");
        assert_eq!(Topology::Wide.dir_name(), "wide");
    }
}

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

//! Scaling table mapping SF to pile dimensions and topology factorings.

use std::io;

/// One row of the SF → footprint/scene scaling table.
#[derive(Debug, Clone, Copy)]
pub struct ScalingTier {
    /// Scale factor (1, 10, 100, 1000).
    pub sf: u32,
    /// Number of footprints (A). Derived from continent tiling.
    pub footprints: u32,
    /// Total COGs per footprint (N). Each topology factors this as M × T.
    pub scenes_per_footprint: u32,
    /// Narrow topology (M, T) factoring.
    pub narrow: (u32, u32),
    /// Balanced topology (M, T) factoring.
    pub balanced: (u32, u32),
    /// Wide topology (M, T) factoring.
    pub wide: (u32, u32),
}

/// The scaling ladder. Invariant: for every tier,
/// `narrow.0 * narrow.1 == balanced.0 * balanced.1 == wide.0 * wide.1 == scenes_per_footprint`.
pub const SCALING_TABLE: &[ScalingTier] = &[
    ScalingTier {
        sf: 1,
        footprints: 1_650,
        scenes_per_footprint: 16,
        narrow: (2, 8),
        balanced: (4, 4),
        wide: (8, 2),
    },
    ScalingTier {
        sf: 10,
        footprints: 4_234,
        scenes_per_footprint: 64,
        narrow: (2, 32),
        balanced: (8, 8),
        wide: (32, 2),
    },
    ScalingTier {
        sf: 100,
        footprints: 9_851,
        scenes_per_footprint: 384,
        narrow: (2, 192),
        balanced: (12, 32),
        wide: (96, 4),
    },
    ScalingTier {
        sf: 1000,
        footprints: 28_164,
        scenes_per_footprint: 1_152,
        narrow: (2, 576),
        balanced: (12, 96),
        wide: (192, 6),
    },
];

/// Look up the scaling tier for a given SF.
pub fn scaling_tier(sf: u32) -> io::Result<&'static ScalingTier> {
    SCALING_TABLE.iter().find(|t| t.sf == sf).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported scale factor: {sf}. Valid values: 1, 10, 100, 1000"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scaling_table_invariants() {
        for tier in SCALING_TABLE {
            assert_eq!(
                tier.narrow.0 * tier.narrow.1,
                tier.scenes_per_footprint,
                "Narrow M*T != N at SF={}",
                tier.sf
            );
            assert_eq!(
                tier.balanced.0 * tier.balanced.1,
                tier.scenes_per_footprint,
                "Balanced M*T != N at SF={}",
                tier.sf
            );
            assert_eq!(
                tier.wide.0 * tier.wide.1,
                tier.scenes_per_footprint,
                "Wide M*T != N at SF={}",
                tier.sf
            );
        }
    }

    #[test]
    fn scaling_tier_lookup() {
        let tier = scaling_tier(1).unwrap();
        assert_eq!(tier.footprints, 1_650);
        assert_eq!(tier.scenes_per_footprint, 16);
    }

    #[test]
    fn scaling_tier_invalid() {
        assert!(scaling_tier(5).is_err());
    }
}

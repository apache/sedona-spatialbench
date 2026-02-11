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

use log::debug;

const SUBTYPE_ROWS: &[(&str, i64)] = &[
    ("microhood", 74797),
    ("macrohood", 42619),
    ("neighborhood", 298615),
    ("county", 38679),
    ("localadmin", 19007),
    ("locality", 555834),
    ("region", 3905),
    ("dependency", 53),
    ("country", 219),
];

pub struct ZoneTableStats {
    scale_factor: f64,
    size_gb: f64,
    total_rows: i64,
}

impl ZoneTableStats {
    pub fn new(scale_factor: f64, parts: Option<i32>) -> Self {
        let (mut size_gb, mut total_rows) = Self::base_stats(scale_factor);

        if scale_factor <= 1.0 && parts > Option::from(1) {
            (size_gb, total_rows) = Self::base_stats(scale_factor / parts.unwrap() as f64);
        }

        debug!(
            "Stats: size_gb={}, total_rows={} for SF={}",
            size_gb, total_rows, scale_factor
        );

        Self {
            scale_factor,
            size_gb,
            total_rows,
        }
    }

    pub fn base_stats(sf: f64) -> (f64, i64) {
        if sf < 1.0 {
            (0.92 * sf, (156_095.0 * sf).ceil() as i64)
        } else if sf < 10.0 {
            (1.42, 156_095)
        } else if sf < 100.0 {
            (2.09, 454_710)
        } else if sf < 1000.0 {
            (5.68, 1_033_456)
        } else {
            (6.13, 1_033_675)
        }
    }

    pub fn subtypes(&self) -> Vec<&'static str> {
        let mut v = vec!["microhood", "macrohood", "county"];
        if self.scale_factor >= 10.0 {
            v.push("neighborhood");
        }
        if self.scale_factor >= 100.0 {
            v.extend_from_slice(&["localadmin", "locality", "region", "dependency"]);
        }
        if self.scale_factor >= 1000.0 {
            v.push("country");
        }
        v
    }

    pub fn estimated_total_rows(&self) -> i64 {
        let mut total = 0i64;
        for subtype in self.subtypes() {
            total += SUBTYPE_ROWS
                .iter()
                .find(|(name, _)| *name == subtype)
                .map(|(_, rows)| *rows)
                .unwrap_or(0);
        }

        if self.scale_factor < 1.0 {
            (total as f64 * self.scale_factor).ceil() as i64
        } else {
            total
        }
    }

    pub fn compute_rows_per_group(&self, target_bytes: i64, default_bytes: i64) -> usize {
        let total_bytes = self.size_gb * 1024.0 * 1024.0 * 1024.0;
        let bytes_per_row = total_bytes / self.total_rows as f64;

        let effective_target = if target_bytes <= 0 {
            default_bytes
        } else {
            target_bytes
        };

        debug!(
            "Stats: {:.2} GB, {} rows, {:.2} bytes/row, target: {} bytes",
            self.size_gb, self.total_rows, bytes_per_row, effective_target
        );

        let est = (effective_target as f64 / bytes_per_row).floor();
        est.clamp(1000.0, 32767.0) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subtypes_for_different_scale_factors() {
        let sf_01_stats = ZoneTableStats::new(0.1, Some(1));
        assert_eq!(
            sf_01_stats.subtypes(),
            vec!["microhood", "macrohood", "county"]
        );

        let sf_10_stats = ZoneTableStats::new(10.0, Some(1));
        assert_eq!(
            sf_10_stats.subtypes(),
            vec!["microhood", "macrohood", "county", "neighborhood"]
        );

        let sf_100_stats = ZoneTableStats::new(100.0, Some(1));
        assert!(sf_100_stats.subtypes().contains(&"localadmin"));
        assert!(sf_100_stats.subtypes().contains(&"locality"));

        let sf_1000_stats = ZoneTableStats::new(1000.0, Some(1));
        assert!(sf_1000_stats.subtypes().contains(&"country"));
    }

    #[test]
    fn test_rows_per_group_bounds() {
        let stats = ZoneTableStats::new(1.0, Some(1));

        let rows_per_group_tiny = stats.compute_rows_per_group(1_000_000, 128 * 1024 * 1024);
        assert!(rows_per_group_tiny >= 1000);

        let tiny_stats = ZoneTableStats {
            scale_factor: 0.001,
            size_gb: 1000.0,
            total_rows: 1000,
        };
        let rows_per_group_huge = tiny_stats.compute_rows_per_group(1, 128 * 1024 * 1024);
        assert!(rows_per_group_huge <= 32767);
    }

    #[test]
    fn test_estimated_rows_scaling_consistency() {
        let base_stats = ZoneTableStats::new(1.0, Some(1));
        let half_stats = ZoneTableStats::new(0.5, Some(1));
        let quarter_stats = ZoneTableStats::new(0.25, Some(1));

        let base_rows = base_stats.estimated_total_rows() as f64;
        let half_rows = half_stats.estimated_total_rows() as f64;
        let quarter_rows = quarter_stats.estimated_total_rows() as f64;

        assert!((half_rows - (base_rows * 0.5)).abs() < 1.0);
        assert!((quarter_rows - (base_rows * 0.25)).abs() < 1.0);
    }
}

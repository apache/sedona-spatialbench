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

use crate::zone::stats::ZoneTableStats;
use arrow_array::RecordBatch;
use datafusion::prelude::*;
use log::{debug, info};

pub struct PartitionStrategy {
    offset: i64,
    limit: i64,
}

impl PartitionStrategy {
    pub fn calculate(total_rows: i64, parts: Option<i32>, part: Option<i32>) -> Self {
        let parts = parts.unwrap_or(1);
        let part = part.unwrap_or(1);
        let i = part - 1;

        let base = total_rows / parts as i64;
        let rem = total_rows % parts as i64;

        let limit = base + if (i as i64) < rem { 1 } else { 0 };
        let offset = i as i64 * base + std::cmp::min(i as i64, rem);

        info!(
            "Partition: total={}, parts={}, part={}, offset={}, limit={}",
            total_rows, parts, part, offset, limit
        );

        Self { offset, limit }
    }

    /// Calculates the number of parts needed to approximate the output file size.
    pub(crate) fn calculate_parts_from_max_size(sf: f64, output_file_size_mb: f32) -> i32 {
        let (size_gb, _) = ZoneTableStats::base_stats(sf);

        let total_size_bytes = size_gb * 1024.0 * 1024.0 * 1024.0;
        let output_file_size_mb = output_file_size_mb * 1024.0 * 1024.0;

        let parts = ((total_size_bytes / output_file_size_mb as f64).round() as i32).max(1);

        debug!(
            "Calculated {parts} parts for table Zone (total size: {}MB, max size: {}MB)",
            total_size_bytes / (1024.0 * 1024.0),
            output_file_size_mb
        );

        parts
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn apply_to_dataframe(&self, df: DataFrame) -> datafusion::common::Result<DataFrame> {
        df.limit(self.offset as usize, Some(self.limit as usize))
    }

    /// Apply partition to already-collected batches
    pub fn apply_to_batches(&self, batches: &[RecordBatch]) -> anyhow::Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let mut current_offset = 0i64;
        let end_offset = self.offset + self.limit;

        for batch in batches {
            let batch_rows = batch.num_rows() as i64;
            let batch_end = current_offset + batch_rows;

            if batch_end <= self.offset || current_offset >= end_offset {
                current_offset = batch_end;
                continue;
            }

            let start_in_batch = (self.offset.saturating_sub(current_offset)).max(0) as usize;
            let end_in_batch = ((end_offset - current_offset).min(batch_rows)) as usize;
            let length = end_in_batch - start_in_batch;

            if length > 0 {
                let sliced = batch.slice(start_in_batch, length);
                result.push(sliced);
            }

            current_offset = batch_end;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_distribution() {
        let total_rows = 100i64;
        let parts = 3;

        let mut collected_rows = Vec::new();
        let mut collected_offsets = Vec::new();

        for part in 1..=parts {
            let strategy =
                PartitionStrategy::calculate(total_rows, Option::from(parts), Option::from(part));
            collected_rows.push(strategy.limit);
            collected_offsets.push(strategy.offset);
        }

        assert_eq!(collected_rows.iter().sum::<i64>(), total_rows);
        assert_eq!(collected_offsets[0], 0);

        for i in 1..parts as usize {
            let expected_offset = collected_offsets[i - 1] + collected_rows[i - 1];
            assert_eq!(collected_offsets[i], expected_offset);
        }
    }

    #[test]
    fn test_calculate_parts_from_max_size() {
        // Test with a scale factor that produces a known size
        let sf = 1.0;
        let (size_gb, _) = ZoneTableStats::base_stats(sf);
        let total_size_mb = size_gb * 1024.0;

        // Test case 1: file size larger than total - should return 1 part
        let output_file_size_mb = (total_size_mb * 2.0) as f32;
        let parts = PartitionStrategy::calculate_parts_from_max_size(sf, output_file_size_mb);
        assert_eq!(parts, 1);

        // Test case 2: file size exactly half of total - should return 2 parts
        let output_file_size_mb = (total_size_mb / 2.0) as f32;
        let parts = PartitionStrategy::calculate_parts_from_max_size(sf, output_file_size_mb);
        assert_eq!(parts, 2);

        // Test case 3: file size forces 3+ parts
        let output_file_size_mb = (total_size_mb / 3.5) as f32;
        let parts = PartitionStrategy::calculate_parts_from_max_size(sf, output_file_size_mb);
        assert_eq!(parts, 4); // ceil(3.5) = 4

        // Test case 5: very small file size
        let parts = PartitionStrategy::calculate_parts_from_max_size(sf, 1.0);
        assert!(parts > 1);
        assert_eq!(parts, (total_size_mb.round() as i32).max(1));
    }
}

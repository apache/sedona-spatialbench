use arrow_array::RecordBatch;
use datafusion::prelude::*;
use log::info;

pub struct PartitionStrategy {
    offset: i64,
    limit: i64,
}

impl PartitionStrategy {
    pub fn calculate(total_rows: i64, parts: i32, part: i32) -> Self {
        let parts = parts as i64;
        let i = (part as i64) - 1;

        let base = total_rows / parts;
        let rem = total_rows % parts;

        let limit = base + if i < rem { 1 } else { 0 };
        let offset = i * base + std::cmp::min(i, rem);

        info!(
            "Partition: total={}, parts={}, part={}, offset={}, limit={}",
            total_rows, parts, part, offset, limit
        );

        Self { offset, limit }
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
            let strategy = PartitionStrategy::calculate(total_rows, parts, part);
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
}

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
            total_rows,
            parts,
            part,
            offset,
            limit
        );

        Self {
            offset,
            limit,
        }
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn apply_to_dataframe(&self, df: DataFrame) -> datafusion::common::Result<DataFrame> {
        df.limit(self.offset as usize, Some(self.limit as usize))
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

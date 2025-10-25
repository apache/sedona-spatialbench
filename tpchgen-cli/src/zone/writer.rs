use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use log::{debug, info};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::{path::PathBuf, sync::Arc, time::Instant};

use super::config::ZoneDfArgs;
use super::stats::ZoneTableStats;

pub struct ParquetWriter {
    output_path: PathBuf,
    schema: SchemaRef,
    props: WriterProperties,
    args: ZoneDfArgs,
}

impl ParquetWriter {
    pub fn new(args: &ZoneDfArgs, stats: &ZoneTableStats, schema: SchemaRef) -> Self {
        let rows_per_group =
            stats.compute_rows_per_group(args.parquet_row_group_bytes, 128 * 1024 * 1024);

        let props = WriterProperties::builder()
            .set_compression(args.parquet_compression)
            .set_max_row_group_size(rows_per_group)
            .build();

        debug!("Using row group size: {} rows", rows_per_group);

        Self {
            output_path: args.output_filename(),
            schema,
            props,
            args: args.clone(),
        }
    }

    pub fn write(&self, batches: &[RecordBatch]) -> Result<()> {
        std::fs::create_dir_all(&self.args.output_dir)?;
        debug!("Created output directory: {:?}", self.args.output_dir);

        let t0 = Instant::now();
        let file = std::fs::File::create(&self.output_path)?;
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&self.schema), Some(self.props.clone()))?;

        for batch in batches {
            writer.write(batch)?;
        }

        writer.close()?;
        let duration = t0.elapsed();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        info!(
            "Zone -> {} (part {}/{}). write={:?}, total_rows={}",
            self.output_path.display(),
            self.args.part,
            self.args.parts,
            duration,
            total_rows
        );

        Ok(())
    }
}

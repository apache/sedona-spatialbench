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
        // Create parent directory of output file (handles both zone/ subdirectory and base dir)
        let parent_dir = self
            .output_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Invalid output path: {:?}", self.output_path))?;

        std::fs::create_dir_all(parent_dir)?;
        debug!("Created output directory: {:?}", parent_dir);

        // Check if file already exists
        if self.output_path.exists() {
            info!(
                "{} already exists, skipping generation",
                self.output_path.display()
            );
            return Ok(());
        }

        // Write to temp file first
        let temp_path = self.output_path.with_extension("inprogress");
        let t0 = Instant::now();
        let file = std::fs::File::create(&temp_path)?;
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(&self.schema), Some(self.props.clone()))?;

        for batch in batches {
            writer.write(batch)?;
        }

        writer.close()?;

        // Rename temp file to final output
        std::fs::rename(&temp_path, &self.output_path).map_err(|e| {
            anyhow::anyhow!(
                "Failed to rename {:?} to {:?}: {}",
                temp_path,
                self.output_path,
                e
            )
        })?;

        let duration = t0.elapsed();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        info!(
            "Zone -> {} (part {:?}/{:?}). write={:?}, total_rows={}",
            self.output_path.display(),
            self.args.part,
            self.args.parts,
            duration,
            total_rows
        );

        Ok(())
    }
}

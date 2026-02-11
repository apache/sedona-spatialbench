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

//! Zone table generation module using DataFusion and remote Parquet files

mod config;
mod datasource;
mod partition;
mod stats;
mod transform;
mod writer;

pub mod main;

use anyhow::Result;
use std::sync::Arc;

pub use config::ZoneDfArgs;
use datasource::ZoneDataSource;
use partition::PartitionStrategy;
use stats::ZoneTableStats;
use transform::ZoneTransformer;
use writer::ParquetWriter;

/// Generate a single part using LIMIT/OFFSET on the dataframe
pub async fn generate_zone_parquet_single(args: ZoneDfArgs) -> Result<()> {
    args.validate()?;

    let stats = ZoneTableStats::new(args.scale_factor, args.parts);
    let datasource = ZoneDataSource::new().await?;
    let ctx = datasource.create_context()?;

    let df = datasource.load_zone_data(&ctx, args.scale_factor).await?;

    let partition =
        PartitionStrategy::calculate(stats.estimated_total_rows(), args.parts, args.part);

    let df = partition.apply_to_dataframe(df)?;

    let transformer = ZoneTransformer::new(partition.offset());
    let df = transformer.transform(&ctx, df).await?;

    // Get schema before collecting (which moves df)
    let schema = Arc::new(transformer.arrow_schema(&df)?);
    let batches = df.collect().await?;

    let writer = ParquetWriter::new(&args, &stats, schema);
    writer.write(&batches)?;

    Ok(())
}

/// Generate all parts by collecting once and partitioning in memory
pub async fn generate_zone_parquet_multi(args: ZoneDfArgs) -> Result<()> {
    let stats = ZoneTableStats::new(args.scale_factor, args.parts);
    let datasource = ZoneDataSource::new().await?;
    let ctx = datasource.create_context()?;

    let df = datasource.load_zone_data(&ctx, args.scale_factor).await?;

    // Transform without offset (we'll adjust per-part later)
    let transformer = ZoneTransformer::new(0);
    let df = transformer.transform(&ctx, df).await?;

    // Collect once
    let schema = Arc::new(transformer.arrow_schema(&df)?);
    let batches = df.collect().await?;

    // Calculate total rows
    let total_rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();

    // Determine number of parts
    let mut parts = args.parts.unwrap_or(1);
    if let Some(max_size) = args.output_file_size_mb {
        parts = PartitionStrategy::calculate_parts_from_max_size(args.scale_factor, max_size);
    }

    // Write each part
    for part in 1..=parts {
        let partition =
            PartitionStrategy::calculate(total_rows, Option::from(parts), Option::from(part));
        let partitioned_batches = partition.apply_to_batches(&batches)?;

        let part_args = ZoneDfArgs::new(
            args.scale_factor,
            args.output_dir.clone(),
            Option::from(parts),
            Option::from(part),
            args.output_file_size_mb,
            args.parquet_row_group_bytes,
            args.parquet_compression,
        );

        let writer = ParquetWriter::new(&part_args, &stats, schema.clone());
        writer.write(&partitioned_batches)?;
    }

    Ok(())
}

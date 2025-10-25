//! Zone table generation module using DataFusion and remote Parquet files

mod config;
mod datasource;
mod partition;
mod stats;
mod transform;
mod writer;

pub mod main;

use std::sync::Arc;
use anyhow::Result;

pub use config::ZoneDfArgs;
use datasource::ZoneDataSource;
use partition::PartitionStrategy;
use stats::ZoneTableStats;
use transform::ZoneTransformer;
use writer::ParquetWriter;

pub async fn generate_zone_parquet(args: ZoneDfArgs) -> Result<()> {
    args.validate()?;

    let stats = ZoneTableStats::new(args.scale_factor, args.parts);
    let datasource = ZoneDataSource::new().await?;
    let ctx = datasource.create_context()?;

    let df = datasource
        .load_zone_data(&ctx, args.scale_factor)
        .await?;

    let partition = PartitionStrategy::calculate(
        stats.estimated_total_rows(),
        args.parts,
        args.part,
    );

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

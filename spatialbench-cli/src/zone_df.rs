use std::{path::PathBuf, sync::Arc, time::Instant};

use anyhow::{anyhow, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::{
    common::config::ConfigOptions, execution::runtime_env::RuntimeEnvBuilder, prelude::*,
    sql::TableReference,
};

use crate::plan::DEFAULT_PARQUET_ROW_GROUP_BYTES;
use datafusion::execution::runtime_env::RuntimeEnv;
use log::{debug, info};
use object_store::aws::AmazonS3Builder;
use object_store::ObjectStore;
use parquet::{
    arrow::ArrowWriter, basic::Compression as ParquetCompression,
    file::properties::WriterProperties,
};
use url::Url;

const OVERTURE_RELEASE_DATE: &str = "2025-08-20.1";
const OVERTURE_S3_BUCKET: &str = "overturemaps-us-west-2";
const OVERTURE_S3_PREFIX: &str = "release";

fn zones_parquet_url() -> String {
    format!(
        "s3://{}/{}/{}/theme=divisions/type=division_area/",
        OVERTURE_S3_BUCKET, OVERTURE_S3_PREFIX, OVERTURE_RELEASE_DATE
    )
}

fn subtypes_for_scale_factor(sf: f64) -> Vec<&'static str> {
    let mut v = vec!["microhood", "macrohood", "county"];
    if sf >= 10.0 {
        v.push("neighborhood");
    }
    if sf >= 100.0 {
        v.extend_from_slice(&["localadmin", "locality", "region", "dependency"]);
    }
    if sf >= 1000.0 {
        v.push("country");
    }
    v
}

fn estimated_total_rows_for_sf(sf: f64) -> i64 {
    let mut total = 0i64;
    for s in subtypes_for_scale_factor(sf) {
        total += match s {
            "microhood" => 74797,
            "macrohood" => 42619,
            "neighborhood" => 298615,
            "county" => 38679,
            "localadmin" => 19007,
            "locality" => 555834,
            "region" => 3905,
            "dependency" => 53,
            "country" => 219,
            _ => 0,
        };
    }
    if sf < 1.0 {
        (total as f64 * sf).floor() as i64
    } else {
        total
    }
}

fn get_zone_table_stats(sf: f64) -> (f64, i64) {
    // Returns (size_in_gb, total_rows) for the given scale factor
    if sf < 1.0 {
        (0.92 * sf, (156_095.0 * sf).floor() as i64)
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

fn compute_rows_per_group_from_stats(size_gb: f64, total_rows: i64, target_bytes: i64) -> usize {
    let total_bytes = size_gb * 1024.0 * 1024.0 * 1024.0; // Convert GB to bytes
    let bytes_per_row = total_bytes / total_rows as f64;

    // Use default if target_bytes is not specified or invalid
    let effective_target = if target_bytes <= 0 {
        DEFAULT_PARQUET_ROW_GROUP_BYTES
    } else {
        target_bytes
    };

    debug!(
        "Using hardcoded stats: {:.2} GB, {} rows, {:.2} bytes/row, target: {} bytes",
        size_gb, total_rows, bytes_per_row, effective_target
    );

    let est = (effective_target as f64 / bytes_per_row).floor();
    // Keep RG count <= 32k, but avoid too-tiny RGs
    est.max(10_000.0).min(10_000_000.0) as usize
}

fn writer_props_with_rowgroup(comp: ParquetCompression, rows_per_group: usize) -> WriterProperties {
    WriterProperties::builder()
        .set_compression(comp)
        .set_max_row_group_size(rows_per_group)
        .build()
}

fn write_parquet_with_rowgroup_bytes(
    out_path: &PathBuf,
    schema: SchemaRef,
    all_batches: Vec<RecordBatch>,
    target_rowgroup_bytes: i64,
    comp: ParquetCompression,
    scale_factor: f64,
) -> Result<()> {
    let (size_gb, total_rows) = get_zone_table_stats(scale_factor);
    debug!(
        "size_gb={}, total_rows={} for scale_factor={}",
        size_gb, total_rows, scale_factor
    );
    let rows_per_group =
        compute_rows_per_group_from_stats(size_gb, total_rows, target_rowgroup_bytes);
    let props = writer_props_with_rowgroup(comp, rows_per_group);

    debug!(
        "Using row group size: {} rows (based on hardcoded stats)",
        rows_per_group
    );

    let mut writer = ArrowWriter::try_new(std::fs::File::create(out_path)?, schema, Some(props))?;

    for batch in all_batches {
        writer.write(&batch)?;
    }
    writer.close()?;
    Ok(())
}

#[derive(Clone)]
pub struct ZoneDfArgs {
    pub scale_factor: f64,
    pub output_dir: PathBuf,
    pub parts: i32,
    pub part: i32,
    pub parquet_row_group_bytes: i64,
    pub parquet_compression: ParquetCompression,
}

impl ZoneDfArgs {
    fn output_filename(&self) -> PathBuf {
        // let fname = if self.parts > 1 {
        //     format!("zone.part-{:03}-of-{:03}.parquet", self.part, self.parts)
        // } else {
        //     "zone.parquet".to_string()
        // };
        let fname = "zone.parquet".to_string();
        self.output_dir.join(fname)
    }
}

pub async fn generate_zone_parquet(args: ZoneDfArgs) -> Result<()> {
    if args.part < 1 || args.part > args.parts {
        return Err(anyhow!(
            "Invalid --part={} for --parts={}",
            args.part,
            args.parts
        ));
    }

    info!(
        "Starting zone parquet generation with scale factor {}",
        args.scale_factor
    );
    debug!("Zone generation args: parts={}, part={}, output_dir={:?}, row_group_bytes={}, compression={:?}",
           args.parts, args.part, args.output_dir, args.parquet_row_group_bytes, args.parquet_compression);

    let subtypes = subtypes_for_scale_factor(args.scale_factor);
    info!(
        "Selected subtypes for SF {}: {:?}",
        args.scale_factor, subtypes
    );

    let estimated_rows = estimated_total_rows_for_sf(args.scale_factor);
    info!(
        "Estimated total rows for SF {}: {}",
        args.scale_factor, estimated_rows
    );

    let mut cfg = ConfigOptions::new();
    cfg.execution.target_partitions = 1;
    debug!("Created DataFusion config with target_partitions=1");

    let rt: Arc<RuntimeEnv> = Arc::new(RuntimeEnvBuilder::new().build()?);
    debug!("Built DataFusion runtime environment");

    // Register S3 store for Overture bucket (object_store 0.11)
    let bucket = OVERTURE_S3_BUCKET; // "overturemaps-us-west-2"
    info!("Registering S3 store for bucket: {}", bucket);
    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_skip_signature(true)
        .with_region("us-west-2")
        .build()?; // -> object_store 0.11 AmazonS3

    let s3_url = Url::parse(&format!("s3://{bucket}"))?;
    let s3_store: Arc<dyn ObjectStore> = Arc::new(s3);
    rt.register_object_store(&s3_url, s3_store);
    debug!("Successfully registered S3 object store");

    let ctx = SessionContext::new_with_config_rt(SessionConfig::from(cfg), rt);
    debug!("Created DataFusion session context");

    let url = zones_parquet_url();
    info!("Reading parquet data from: {}", url);
    let t_read_start = Instant::now();
    let mut df = ctx.read_parquet(url, ParquetReadOptions::default()).await?;
    let read_dur = t_read_start.elapsed();
    info!("Successfully read parquet data in {:?}", read_dur);

    // Build filter predicate
    debug!("Building filter predicate for subtypes: {:?}", subtypes);
    let mut pred = col("subtype").eq(lit("__never__"));
    for s in subtypes_for_scale_factor(args.scale_factor) {
        pred = pred.or(col("subtype").eq(lit(s)));
    }
    df = df.filter(pred.and(col("is_land").eq(lit(true))))?;
    info!("Applied subtype and is_land filters");

    // df = df.sort(vec![col("id").sort(true, true)])?;
    // debug!("Applied sorting by id");

    let total = estimated_total_rows_for_sf(args.scale_factor);
    let this = (args.part as i64) - 1;
    let rows_per_part = total / (args.parts as i64);
    let offset = this * rows_per_part;

    info!(
        "Partitioning data: total_rows={}, parts={}, rows_per_part={}, offset={}",
        total, args.parts, rows_per_part, offset
    );

    df = df.limit(offset as usize, Some(rows_per_part as usize))?;
    debug!(
        "Applied limit with offset={}, rows={}",
        offset, rows_per_part
    );

    ctx.register_table(TableReference::bare("zone_filtered"), df.into_view())?;
    debug!("Registered filtered data as 'zone_filtered' table");

    let sql = format!(
        r#"
        SELECT
          CAST(ROW_NUMBER() OVER (ORDER BY id) + {offset} AS BIGINT) AS z_zonekey,
          COALESCE(id, '')            AS z_gersid,
          COALESCE(country, '')       AS z_country,
          COALESCE(region,  '')       AS z_region,
          COALESCE(names.primary, '') AS z_name,
          COALESCE(subtype, '')       AS z_subtype,
          geometry                    AS z_boundary
        FROM zone_filtered
        "#
    );
    debug!("Executing SQL transformation with offset: {}", offset);
    let df2 = ctx.sql(&sql).await?;
    info!("SQL transformation completed successfully");

    let t0 = Instant::now();
    info!("Starting data collection...");
    let batches = df2.clone().collect().await?;
    let collect_dur = t0.elapsed();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    info!(
        "Collected {} record batches with {} total rows in {:?}",
        batches.len(),
        total_rows,
        collect_dur
    );

    std::fs::create_dir_all(&args.output_dir)?;
    debug!("Created output directory: {:?}", args.output_dir);

    let out = args.output_filename();
    info!("Writing output to: {}", out.display());

    debug!(
        "Created parquet writer properties with compression: {:?}",
        args.parquet_compression
    );

    // Convert DFSchema to Arrow Schema
    let schema = Arc::new(Schema::new(
        df2.schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));
    debug!(
        "Converted DataFusion schema to Arrow schema with {} fields",
        schema.fields().len()
    );

    let t1 = Instant::now();
    info!(
        "Starting parquet file write with row group size: {} bytes",
        args.parquet_row_group_bytes
    );
    write_parquet_with_rowgroup_bytes(
        &out,
        schema,
        batches,
        args.parquet_row_group_bytes,
        args.parquet_compression,
        args.scale_factor,
    )?;
    let write_dur = t1.elapsed();

    info!(
        "Zone -> {} (part {}/{}). collect={:?}, write={:?}, total_rows={}",
        out.display(),
        args.part,
        args.parts,
        collect_dur,
        write_dur,
        total_rows
    );

    Ok(())
}

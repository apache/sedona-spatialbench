// spatialbench-cli/src/zone_df.rs
use std::{path::PathBuf, sync::Arc, time::Instant};

use anyhow::{anyhow, Result};
use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion::{
    common::config::ConfigOptions, execution::runtime_env::RuntimeEnvBuilder, prelude::*,
    sql::TableReference,
};

use datafusion::execution::runtime_env::RuntimeEnv;
use log::info;
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
            "county" => 39680,
            "localadmin" => 19007,
            "locality" => 555834,
            "region" => 4714,
            "dependency" => 105,
            "country" => 378,
            _ => 0,
        };
    }
    if sf < 1.0 {
        (total as f64 * sf).ceil() as i64
    } else {
        total
    }
}

fn parquet_writer_props(comp: ParquetCompression) -> WriterProperties {
    WriterProperties::builder().set_compression(comp).build()
}

fn approx_bytes_per_row(batches: &[RecordBatch]) -> f64 {
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for b in batches {
        rows += b.num_rows();
        for col in b.columns() {
            bytes += col.get_array_memory_size();
        }
    }
    if rows == 0 {
        0.0
    } else {
        bytes as f64 / rows as f64
    }
}

fn write_parquet_with_rowgroup_bytes(
    out_path: &PathBuf,
    schema: SchemaRef,
    all_batches: Vec<RecordBatch>,
    target_rowgroup_bytes: i64,
    props: WriterProperties,
) -> Result<()> {
    let mut writer = ArrowWriter::try_new(std::fs::File::create(out_path)?, schema, Some(props))?;

    if all_batches.is_empty() {
        writer.close()?;
        return Ok(());
    }

    let bpr = approx_bytes_per_row(&all_batches);
    let rows_per_group: usize = if bpr > 0.0 {
        (target_rowgroup_bytes as f64 / bpr)
            .floor()
            .max(10_000.0)
            .min(1_000_000.0) as usize
    } else {
        128_000
    };

    for batch in all_batches {
        let mut start = 0usize;
        while start < batch.num_rows() {
            let end = (start + rows_per_group).min(batch.num_rows());
            writer.write(&batch.slice(start, end - start))?;
            start = end;
        }
    }
    writer.close()?;
    Ok(())
}

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
        let fname = if self.parts > 1 {
            format!("zone.part-{:03}-of-{:03}.parquet", self.part, self.parts)
        } else {
            "zone.parquet".to_string()
        };
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

    let mut cfg = ConfigOptions::new();
    cfg.execution.target_partitions = 1;

    let rt: Arc<RuntimeEnv> = Arc::new(RuntimeEnvBuilder::new().build()?);

    // Register S3 store for Overture bucket (object_store 0.11)
    let bucket = OVERTURE_S3_BUCKET; // "overturemaps-us-west-2"
    let s3 = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_skip_signature(true)
        .with_region("us-west-2")
        .build()?; // -> object_store 0.11 AmazonS3

    let s3_url = Url::parse(&format!("s3://{bucket}"))?;
    let s3_store: Arc<dyn ObjectStore> = Arc::new(s3);
    rt.register_object_store(&s3_url, s3_store);

    let ctx = SessionContext::new_with_config_rt(SessionConfig::from(cfg), rt);

    let url = zones_parquet_url();
    let mut df = ctx.read_parquet(url, ParquetReadOptions::default()).await?;

    let mut pred = col("subtype").eq(lit("__never__"));
    for s in subtypes_for_scale_factor(args.scale_factor) {
        pred = pred.or(col("subtype").eq(lit(s)));
    }
    df = df.filter(pred.and(col("is_land").eq(lit(true))))?;

    df = df.sort(vec![col("id").sort(true, true)])?;
    let total = estimated_total_rows_for_sf(args.scale_factor);
    let parts = args.parts as i64;
    let this = (args.part as i64) - 1;
    let rows_per_part = (total + parts - 1) / parts;
    let offset = this * rows_per_part;
    df = df.limit(offset as usize, Some(rows_per_part as usize))?;

    ctx.register_table(TableReference::bare("zone_filtered"), df.into_view())?;
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
    let df2 = ctx.sql(&sql).await?;

    let t0 = Instant::now();
    let batches = df2.clone().collect().await?;
    let collect_dur = t0.elapsed();

    std::fs::create_dir_all(&args.output_dir)?;
    let out = args.output_filename();
    let props = parquet_writer_props(args.parquet_compression);

    // Convert DFSchema to Arrow Schema
    let schema = Arc::new(Schema::new(
        df2.schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<_>>(),
    ));

    let t1 = Instant::now();
    write_parquet_with_rowgroup_bytes(&out, schema, batches, args.parquet_row_group_bytes, props)?;
    let write_dur = t1.elapsed();

    info!(
        "Zone -> {} (part {}/{}). collect={:?}, write={:?}",
        out.display(),
        args.part,
        args.parts,
        collect_dur,
        write_dur
    );

    Ok(())
}

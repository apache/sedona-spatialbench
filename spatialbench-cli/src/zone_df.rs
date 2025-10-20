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
use object_store::http::HttpBuilder;
use parquet::{
    arrow::ArrowWriter, basic::Compression as ParquetCompression,
    file::properties::WriterProperties,
};
use url::Url;

const OVERTURE_RELEASE_DATE: &str = "2025-08-20.1";
const HUGGINGFACE_URL: &str = "https://huggingface.co";
const COMMIT_HASH: &str = "67822daa2fbc0039681922f0d7fea4157f41d13f";

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
        (total as f64 * sf).ceil() as i64
    } else {
        total
    }
}

fn get_zone_table_stats(sf: f64) -> (f64, i64) {
    // Returns (size_in_gb, total_rows) for the given scale factor
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
    est.clamp(1000.0, 32767.0) as usize
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
    parts: i32,
) -> Result<()> {
    let (mut size_gb, mut total_rows) = get_zone_table_stats(scale_factor);

    // Use linear scaling stats for SF <= 1.0 with parts > 1
    if scale_factor <= 1.0 && parts > 1 {
        (size_gb, total_rows) = get_zone_table_stats(scale_factor / parts as f64);
    }

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
        let filename = "zone.parquet".to_string();
        self.output_dir.join(filename)
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

    // Register HTTPS object store for Hugging Face
    let hf_store = HttpBuilder::new().with_url(HUGGINGFACE_URL).build()?;
    let hf_url = Url::parse(HUGGINGFACE_URL)?;
    rt.register_object_store(&hf_url, Arc::new(hf_store));
    debug!("Registered HTTPS object store for huggingface.co");

    let ctx = SessionContext::new_with_config_rt(SessionConfig::from(cfg), rt);
    debug!("Created DataFusion session context");

    // Parquet parts from Hugging Face (programmatically generated)
    const PARQUET_PART_COUNT: usize = 4;
    const PARQUET_UUID: &str = "c998b093-fa14-440c-98f0-bbdb2126ed22";
    let parquet_urls: Vec<String> = (0..PARQUET_PART_COUNT)
        .map(|i| format!(
            "https://huggingface.co/datasets/apache-sedona/spatialbench/resolve/{}/omf-division-area-{}/part-{i:05}-{uuid}-c000.zstd.parquet",
            COMMIT_HASH,
            OVERTURE_RELEASE_DATE,
            i = i,
            uuid = PARQUET_UUID
        ))
        .collect();

    info!(
        "Reading {} Parquet parts from Hugging Face...",
        parquet_urls.len()
    );

    let t_read_start = Instant::now();
    let mut df = ctx
        .read_parquet(parquet_urls, ParquetReadOptions::default())
        .await?;
    let read_dur = t_read_start.elapsed();
    info!("Successfully read HF parquet data in {:?}", read_dur);

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
    let i = (args.part as i64) - 1; // 0-based part index
    let parts = args.parts as i64;

    let base = total / parts;
    let rem = total % parts;

    // first `rem` parts get one extra row
    let rows_this = base + if i < rem { 1 } else { 0 };
    let offset = i * base + std::cmp::min(i, rem);

    info!(
        "Partitioning data: total_rows={}, parts={}, base={}, rem={}, this_part_rows={}, offset={}",
        total, parts, base, rem, rows_this, offset
    );

    df = df.limit(offset as usize, Some(rows_this as usize))?;
    debug!("Applied limit with offset={}, rows={}", offset, rows_this);

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
        args.parts,
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

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::Compression;
    use tempfile::TempDir;

    fn create_test_args(scale_factor: f64, temp_dir: &TempDir) -> ZoneDfArgs {
        ZoneDfArgs {
            scale_factor,
            output_dir: temp_dir.path().to_path_buf(),
            parts: 1,
            part: 1,
            parquet_row_group_bytes: DEFAULT_PARQUET_ROW_GROUP_BYTES,
            parquet_compression: Compression::SNAPPY,
        }
    }

    #[tokio::test]
    async fn test_zone_generation_invalid_part() {
        let temp_dir = TempDir::new().unwrap();
        let mut args = create_test_args(1.0, &temp_dir);
        args.parts = 2;
        args.part = 3; // Invalid part number

        let result = generate_zone_parquet(args).await;
        assert!(result.is_err(), "Should fail with invalid part number");
    }

    #[tokio::test]
    async fn test_subtypes_for_different_scale_factors() {
        // Test scale factor categorization
        let sf_01_subtypes = subtypes_for_scale_factor(0.1);
        assert_eq!(sf_01_subtypes, vec!["microhood", "macrohood", "county"]);

        let sf_10_subtypes = subtypes_for_scale_factor(10.0);
        assert_eq!(
            sf_10_subtypes,
            vec!["microhood", "macrohood", "county", "neighborhood"]
        );

        let sf_100_subtypes = subtypes_for_scale_factor(100.0);
        assert!(sf_100_subtypes.contains(&"localadmin"));
        assert!(sf_100_subtypes.contains(&"locality"));

        let sf_1000_subtypes = subtypes_for_scale_factor(1000.0);
        assert!(sf_1000_subtypes.contains(&"country"));
    }

    #[test]
    fn test_partition_distribution_logic() {
        // Test the mathematical logic for distributing rows across partitions
        let total_rows = 100i64;
        let parts = 3i64;

        let mut collected_rows = Vec::new();
        let mut collected_offsets = Vec::new();

        // Simulate the partition calculation for each part
        for part_idx in 0..parts {
            let i = part_idx;
            let base = total_rows / parts;
            let rem = total_rows % parts;
            let rows_this = base + if i < rem { 1 } else { 0 };
            let offset = i * base + std::cmp::min(i, rem);

            collected_rows.push(rows_this);
            collected_offsets.push(offset);
        }

        // Verify partitioning logic
        assert_eq!(collected_rows.iter().sum::<i64>(), total_rows); // All rows accounted for
        assert_eq!(collected_offsets[0], 0); // First partition starts at 0

        // Verify no gaps or overlaps between partitions
        for i in 1..parts as usize {
            let expected_offset = collected_offsets[i - 1] + collected_rows[i - 1];
            assert_eq!(collected_offsets[i], expected_offset);
        }

        // Verify remainder distribution (first partitions get extra rows)
        let remainder = (total_rows % parts) as usize;
        for i in 0..remainder {
            assert_eq!(collected_rows[i], collected_rows[remainder] + 1);
        }
    }

    #[test]
    fn test_rows_per_group_bounds() {
        // Test that compute_rows_per_group_from_stats respects bounds

        // Test minimum bound (should be at least 1000)
        let rows_per_group_tiny = compute_rows_per_group_from_stats(0.001, 1000, 1_000_000);
        assert!(rows_per_group_tiny >= 1000);

        // Test maximum bound (should not exceed 32767)
        let rows_per_group_huge = compute_rows_per_group_from_stats(1000.0, 1000, 1);
        assert!(rows_per_group_huge <= 32767);

        // Test negative target bytes falls back to default
        let rows_per_group_negative = compute_rows_per_group_from_stats(1.0, 100000, -1);
        let rows_per_group_default =
            compute_rows_per_group_from_stats(1.0, 100000, DEFAULT_PARQUET_ROW_GROUP_BYTES);
        assert_eq!(rows_per_group_negative, rows_per_group_default);
    }

    #[test]
    fn test_subtype_selection_logic() {
        // Test the cumulative nature of subtype selection
        let base_subtypes = subtypes_for_scale_factor(1.0);
        let sf10_subtypes = subtypes_for_scale_factor(10.0);
        let sf100_subtypes = subtypes_for_scale_factor(100.0);
        let sf1000_subtypes = subtypes_for_scale_factor(1000.0);

        // Each higher scale factor should include all previous subtypes
        for subtype in &base_subtypes {
            assert!(sf10_subtypes.contains(subtype));
            assert!(sf100_subtypes.contains(subtype));
            assert!(sf1000_subtypes.contains(subtype));
        }

        for subtype in &sf10_subtypes {
            assert!(sf100_subtypes.contains(subtype));
            assert!(sf1000_subtypes.contains(subtype));
        }

        for subtype in &sf100_subtypes {
            assert!(sf1000_subtypes.contains(subtype));
        }

        // Verify progressive addition
        assert!(sf10_subtypes.len() > base_subtypes.len());
        assert!(sf100_subtypes.len() > sf10_subtypes.len());
        assert!(sf1000_subtypes.len() > sf100_subtypes.len());
    }

    #[test]
    fn test_estimated_rows_scaling_consistency() {
        // Test that estimated rows scale proportionally for SF < 1.0
        let base_rows = estimated_total_rows_for_sf(1.0);
        let half_rows = estimated_total_rows_for_sf(0.5);
        let quarter_rows = estimated_total_rows_for_sf(0.25);

        // Should scale proportionally (within rounding)
        assert!((half_rows as f64 - (base_rows as f64 * 0.5)).abs() < 1.0);
        assert!((quarter_rows as f64 - (base_rows as f64 * 0.25)).abs() < 1.0);

        // Test that SF >= 1.0 gives discrete jumps (not proportional scaling)
        let sf1_rows = estimated_total_rows_for_sf(1.0);
        let sf5_rows = estimated_total_rows_for_sf(5.0);
        let sf10_rows = estimated_total_rows_for_sf(10.0);

        // These should be equal (same category)
        assert_eq!(sf1_rows, sf5_rows);

        // This should be different (different category)
        assert_ne!(sf5_rows, sf10_rows);
    }
}

use log::info;
use parquet::basic::Compression as ParquetCompression;
use std::io;
use std::path::PathBuf;

use super::config::ZoneDfArgs;

/// Generates zone table in the requested format
#[allow(clippy::too_many_arguments)]
pub async fn generate_zone(
    format: OutputFormat,
    scale_factor: f64,
    output_dir: PathBuf,
    parts: Option<i32>,
    part: Option<i32>,
    max_file_size_mb: Option<f32>,
    parquet_row_group_bytes: i64,
    parquet_compression: ParquetCompression,
) -> io::Result<()> {
    match format {
        OutputFormat::Parquet => {
            let parts = parts.unwrap_or(1);

            if let Some(part_num) = part {
                // Single part mode - use LIMIT/OFFSET
                info!("Generating part {} of {} for zone table", part_num, parts);
                let args = ZoneDfArgs::new(
                    1.0f64.max(scale_factor),
                    output_dir,
                    Option::from(parts),
                    Option::from(part_num),
                    max_file_size_mb,
                    parquet_row_group_bytes,
                    parquet_compression,
                );
                super::generate_zone_parquet_single(args)
                    .await
                    .map_err(io::Error::other)
            } else {
                // Multi-part mode - collect once and partition in memory
                info!("Generating all {} part(s) for zone table", parts);
                let args = ZoneDfArgs::new(
                    1.0f64.max(scale_factor),
                    output_dir,
                    Option::from(parts),
                    None,
                    max_file_size_mb,
                    parquet_row_group_bytes,
                    parquet_compression,
                );
                super::generate_zone_parquet_multi(args)
                    .await
                    .map_err(io::Error::other)
            }
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Zone table is only supported in --format=parquet.",
        )),
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OutputFormat {
    Tbl,
    Csv,
    Parquet,
}

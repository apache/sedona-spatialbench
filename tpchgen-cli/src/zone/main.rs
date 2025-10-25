use std::io;
use std::path::PathBuf;
use parquet::basic::Compression as ParquetCompression;

use super::config::ZoneDfArgs;

/// Generates zone table in the requested format
pub async fn generate_zone(
    format: OutputFormat,
    scale_factor: f64,
    output_dir: PathBuf,
    parts: Option<i32>,
    part: Option<i32>,
    parquet_row_group_bytes: i64,
    parquet_compression: ParquetCompression,
) -> io::Result<()> {
    match format {
        OutputFormat::Parquet => {
            let args = ZoneDfArgs::new(
                1.0f64.max(scale_factor),
                output_dir,
                parts.unwrap_or(1),
                part.unwrap_or(1),
                parquet_row_group_bytes,
                parquet_compression,
            );
            super::generate_zone_parquet(args)
                .await
                .map_err(io::Error::other)
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

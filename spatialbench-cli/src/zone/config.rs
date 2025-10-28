use anyhow::{anyhow, Result};
use parquet::basic::Compression as ParquetCompression;
use std::path::PathBuf;

#[derive(Clone)]
pub struct ZoneDfArgs {
    pub scale_factor: f64,
    pub output_dir: PathBuf,
    pub parts: Option<i32>,
    pub part: Option<i32>,
    pub output_file_size_mb: Option<f32>,
    pub parquet_row_group_bytes: i64,
    pub parquet_compression: ParquetCompression,
}

impl ZoneDfArgs {
    pub fn new(
        scale_factor: f64,
        output_dir: PathBuf,
        parts: Option<i32>,
        part: Option<i32>,
        output_file_size_mb: Option<f32>,
        parquet_row_group_bytes: i64,
        parquet_compression: ParquetCompression,
    ) -> Self {
        Self {
            scale_factor,
            output_dir,
            parts,
            part,
            output_file_size_mb,
            parquet_row_group_bytes,
            parquet_compression,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if let (Some(part), Some(parts)) = (self.part, self.parts) {
            if part < 1 || part > parts {
                return Err(anyhow!("Invalid --part={} for --parts={}", part, parts));
            }
        }

        if self.output_file_size_mb.is_some() && (self.parts.is_some() || self.part.is_some()) {
            return Err(anyhow!(
                "Cannot specify --parts/--part with --max-file-size-mb"
            ));
        }

        Ok(())
    }

    pub fn output_filename(&self) -> PathBuf {
        if self.parts.unwrap_or(1) > 1 {
            // Create zone subdirectory and write parts within it
            self.output_dir
                .join("zone")
                .join(format!("zone.{}.parquet", self.part.unwrap_or(1)))
        } else {
            self.output_dir.join("zone.parquet")
        }
    }
}

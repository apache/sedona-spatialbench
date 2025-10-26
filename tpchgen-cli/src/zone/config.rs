use anyhow::{anyhow, Result};
use parquet::basic::Compression as ParquetCompression;
use std::path::PathBuf;

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
    pub fn new(
        scale_factor: f64,
        output_dir: PathBuf,
        parts: i32,
        part: i32,
        parquet_row_group_bytes: i64,
        parquet_compression: ParquetCompression,
    ) -> Self {
        Self {
            scale_factor,
            output_dir,
            parts,
            part,
            parquet_row_group_bytes,
            parquet_compression,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.part < 1 || self.part > self.parts {
            return Err(anyhow!(
                "Invalid --part={} for --parts={}",
                self.part,
                self.parts
            ));
        }
        Ok(())
    }

    pub fn output_filename(&self) -> PathBuf {
        if self.parts > 1 {
            // Create zone subdirectory and write parts within it
            self.output_dir
                .join("zone")
                .join(format!("zone.{}.parquet", self.part))
        } else {
            self.output_dir.join("zone.parquet")
        }
    }
}

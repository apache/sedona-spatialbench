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

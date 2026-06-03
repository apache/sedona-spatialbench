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

//! Spatial Bench data generation CLI with a dbgen compatible API.
//!
//! This crate provides a CLI for generating Spatial Bench data and tries to remain close
//! API wise to the original dbgen tool, as in we use the same command line flags
//! and arguments.
//!
//! See the documentation on [`Cli`] for more information on the command line
mod csv;
mod generate;
mod output_plan;
mod parquet;
mod plan;
mod raster_runner;
mod runner;
mod s3_writer;
mod spatial_config_file;
mod statistics;
mod tbl;
mod zone;

use crate::generate::Sink;
use crate::output_plan::OutputPlanGenerator;
use crate::parquet::*;
use crate::plan::{GenerationPlan, DEFAULT_PARQUET_ROW_GROUP_BYTES};
use crate::spatial_config_file::parse_yaml;
use crate::statistics::WriteStatistics;
use ::parquet::basic::Compression;
use clap::builder::TypedValueParser;
use clap::{Parser, ValueEnum};
use log::{debug, info, LevelFilter};
use object_store::ObjectStore;
use spatialbench::distribution::Distributions;
use spatialbench::spatial::overrides::{set_overrides, SpatialOverrides};
use spatialbench::text::TextPool;
use spatialbench_raster::cog::CogConfig;
use spatialbench_raster::footprint::FootprintGrid;
use spatialbench_raster::scaling::scaling_tier;
use spatialbench_raster::stac::write_stac_geoparquet;
use spatialbench_raster::topology::Topology;
use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, BufWriter, Stdout, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "spatialbench")]
#[command(version)]
#[command(about = "SpatialBench Data Generator", long_about = None)]
struct Cli {
    /// Scale factor to create
    #[arg(short, long, default_value_t = 1.)]
    scale_factor: f64,

    /// Output directory for generated files (default: current directory)
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// Which tables to generate (default: all)
    #[arg(short = 'T', long = "tables", value_delimiter = ',', value_parser = TableValueParser)]
    tables: Option<Vec<Table>>,

    /// YAML file path specifying configs for Trip and Building
    #[arg(long = "config")]
    config: Option<PathBuf>,

    /// Number of part(itions) to generate. If not specified creates a single file per table
    #[arg(short, long)]
    parts: Option<i32>,

    /// Which part(ition) to generate (1-based). If not specified, generates all parts
    #[arg(long)]
    part: Option<i32>,

    /// Output file size in MB. If specified, automatically determines the number of parts.
    /// Cannot be used with --parts or --part options.
    #[arg(long, conflicts_with_all = ["parts", "part"])]
    mb_per_file: Option<f32>,

    /// Output format: tbl, csv, parquet
    #[arg(short, long, default_value = "parquet")]
    format: OutputFormat,

    /// The number of threads for parallel generation, defaults to the number of CPUs
    #[arg(short, long, default_value_t = num_cpus::get())]
    num_threads: usize,

    /// Parquet block compression format.
    ///
    /// Supported values: UNCOMPRESSED, ZSTD(N), SNAPPY, GZIP, LZO, BROTLI, LZ4
    ///
    /// Note to use zstd you must supply the "compression" level (1-22)
    /// as a number in parentheses, e.g. `ZSTD(1)` for level 1 compression.
    ///
    /// Using `ZSTD` results in the best compression, but is about 2x slower than
    /// UNCOMPRESSED. For example, for the lineitem table at SF=10
    ///
    ///   ZSTD(1):      1.9G  (0.52 GB/sec)
    ///   SNAPPY:       2.4G  (0.75 GB/sec)
    ///   UNCOMPRESSED: 3.8G  (1.41 GB/sec)
    #[arg(short = 'c', long, default_value = "SNAPPY")]
    parquet_compression: Compression,

    /// Verbose output
    ///
    /// When specified, sets the log level to `info` and ignores the `RUST_LOG`
    /// environment variable. When not specified, uses `RUST_LOG`
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    /// Write the output to stdout instead of a file.
    #[arg(long, default_value_t = false)]
    stdout: bool,

    /// Target size in row group bytes in Parquet files
    ///
    /// Row groups are the typical unit of parallel processing and compression
    /// with many query engines. Therefore, smaller row groups enable better
    /// parallelism and lower peak memory use but may reduce compression
    /// efficiency.
    ///
    /// Note: Parquet files are limited to 32k row groups, so at high scale
    /// factors, the row group size may be increased to keep the number of row
    /// groups under this limit.
    ///
    /// Typical values range from 10MB to 100MB.
    #[arg(long, default_value_t = DEFAULT_PARQUET_ROW_GROUP_BYTES)]
    parquet_row_group_bytes: i64,

    /// Maximum number of raster footprints to generate (limits output size for fast iteration).
    #[arg(long)]
    max_footprints: Option<u32>,

    /// Skip raster COGs already present at the output destination (and skip
    /// re-uploading them). Assumes existing COGs were generated with the same
    /// configuration (frequency/dtype/dimensions) — changing config and
    /// resuming will keep stale COGs. Default: off (overwrite).
    #[arg(long, default_value_t = false)]
    resume: bool,

    /// Max concurrent raster output operations (S3 PUTs / file writes).
    /// Defaults to an auto-derived value from --raster-memory-budget-mb.
    #[arg(long)]
    raster_upload_concurrency: Option<usize>,

    /// Memory budget (MiB) for in-flight raster COG buffers; controls the
    /// auto-derived upload concurrency. Default: 8192.
    #[arg(long)]
    raster_memory_budget_mb: Option<u64>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Table {
    Vehicle,
    Driver,
    Customer,
    Trip,
    Building,
    Zone,
    Raster,
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Clone)]
struct TableValueParser;

impl TypedValueParser for TableValueParser {
    type Value = Table;

    /// Parse the value into a Table enum.
    fn parse_ref(
        &self,
        cmd: &clap::Command,
        _: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let value = value
            .to_str()
            .ok_or_else(|| clap::Error::new(clap::error::ErrorKind::InvalidValue).with_cmd(cmd))?;
        Table::from_str(value)
            .map_err(|_| clap::Error::new(clap::error::ErrorKind::InvalidValue).with_cmd(cmd))
    }

    fn possible_values(
        &self,
    ) -> Option<Box<dyn Iterator<Item = clap::builder::PossibleValue> + '_>> {
        Some(Box::new(
            [
                clap::builder::PossibleValue::new("driver").help("Driver table (alias: d)"),
                clap::builder::PossibleValue::new("customer").help("Customer table (alias: c)"),
                clap::builder::PossibleValue::new("vehicle").help("Vehicle table (alias: V)"),
                clap::builder::PossibleValue::new("trip").help("Trip table (alias: T)"),
                clap::builder::PossibleValue::new("building").help("Building table (alias: b)"),
                clap::builder::PossibleValue::new("zone").help("Zone table (alias: z)"),
                clap::builder::PossibleValue::new("raster")
                    .help("Raster COG pile + STAC catalogs (alias: r)"),
            ]
            .into_iter(),
        ))
    }
}

impl FromStr for Table {
    type Err = &'static str;

    /// Returns the table enum value from the given string full name or abbreviation
    ///
    /// The original dbgen tool allows some abbreviations to mean two different tables
    /// like 'p' which aliases to both 'part' and 'partsupp'. This implementation does
    /// not support this since it just adds unnecessary complexity and confusion so we
    /// only support the exclusive abbreviations.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "d" | "driver" => Ok(Table::Driver),
            "V" | "vehicle" => Ok(Table::Vehicle),
            "c" | "customer" => Ok(Table::Customer),
            "T" | "trip" => Ok(Table::Trip),
            "b" | "building" => Ok(Table::Building),
            "z" | "zone" => Ok(Table::Zone),
            "r" | "raster" => Ok(Table::Raster),
            _ => Err("Invalid table name {s}"),
        }
    }
}

impl Table {
    fn name(&self) -> &'static str {
        match self {
            Table::Vehicle => "vehicle",
            Table::Driver => "driver",
            Table::Customer => "customer",
            Table::Trip => "trip",
            Table::Building => "building",
            Table::Zone => "zone",
            Table::Raster => "raster",
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum OutputFormat {
    Tbl,
    Csv,
    Parquet,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Parse command line arguments
    let cli = Cli::parse();
    cli.main().await
}

impl Cli {
    /// Main function to run the generation
    async fn main(self) -> io::Result<()> {
        if self.verbose {
            // explicitly set logging to info / stdout
            env_logger::builder().filter_level(LevelFilter::Info).init();
            info!("Verbose output enabled (ignoring RUST_LOG environment variable)");
        } else {
            env_logger::init();
            debug!("Logging configured from environment variables");
        }

        // Create output directory if it doesn't exist and we are not writing to stdout
        // or to S3 (where local directories are meaningless).
        if !self.stdout && !self.output_dir.to_string_lossy().starts_with("s3://") {
            fs::create_dir_all(&self.output_dir)?;
        }

        // Load overrides if provided or if default config file exists
        let config_path = if let Some(path) = &self.config {
            // Use explicitly provided config path
            Some(path.clone())
        } else {
            // Look for default config file in current directory
            let default_config = PathBuf::from("spatialbench-config.yml");
            if default_config.exists() {
                Some(default_config)
            } else {
                None
            }
        };

        let parsed_config = if let Some(path) = config_path {
            let text = std::fs::read_to_string(&path).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("Failed reading {}: {e}", path.display()),
                )
            })?;

            match parse_yaml(&text) {
                Ok(file_cfg) => {
                    let trip = file_cfg.trip.as_ref().map(|c| c.to_generator());
                    let building = file_cfg.building.as_ref().map(|c| c.to_generator());
                    set_overrides(SpatialOverrides { trip, building });
                    info!("Loaded spider configuration from {}", path.display());
                    Some(file_cfg)
                }
                Err(e) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("Failed parsing spider-config YAML: {e}"),
                    ));
                }
            }
        } else {
            info!("Using default spider configuration from spider_defaults.rs");
            None
        };

        // Determine which tables to generate
        let tables: Vec<Table> = if let Some(tables) = self.tables.as_ref() {
            tables.clone()
        } else {
            vec![
                Table::Vehicle,
                Table::Driver,
                Table::Customer,
                Table::Trip,
                Table::Building,
                Table::Zone,
            ]
        };

        // Warn if parquet specific options are set but not generating parquet
        if self.format != OutputFormat::Parquet {
            if self.parquet_compression != Compression::SNAPPY {
                eprintln!(
                    "Warning: Parquet compression option set but not generating Parquet files"
                );
            }
            if self.parquet_row_group_bytes != DEFAULT_PARQUET_ROW_GROUP_BYTES {
                eprintln!(
                    "Warning: Parquet row group size option set but not generating Parquet files"
                );
            }
        }

        // Determine what files to generate
        let mut output_plan_generator = OutputPlanGenerator::new(
            self.format,
            self.scale_factor,
            self.parquet_compression,
            self.parquet_row_group_bytes,
            self.stdout,
            self.output_dir.clone(),
        );

        let mut generate_raster = false;
        for table in tables {
            if table == Table::Zone {
                self.generate_zone().await?
            } else if table == Table::Raster {
                generate_raster = true;
            } else {
                output_plan_generator.generate_plans(
                    table,
                    self.part,
                    self.parts,
                    self.mb_per_file,
                )?;
            }
        }
        let output_plans = output_plan_generator.build();

        // force the creation of the distributions and text pool to so it doesn't
        // get charged to the first table
        let start = Instant::now();
        debug!("Creating distributions and text pool");
        Distributions::static_default();
        TextPool::get_or_init_default();
        let elapsed = start.elapsed();
        info!("Created static distributions and text pools in {elapsed:?}");

        // Run
        let runner = runner::PlanRunner::new(output_plans, self.num_threads);
        runner.run().await?;

        // Raster generation
        if generate_raster {
            let sf = self.scale_factor as u32;
            let tier = scaling_tier(sf).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("raster scaling: {e}"))
            })?;

            let raster_config = parsed_config.as_ref().and_then(|c| c.raster.as_ref());

            let mut cog_config = if let Some(rc) = raster_config {
                rc.to_cog_config()?
            } else {
                CogConfig::default()
            };

            // If a target compression ratio is requested, calibrate the noise
            // frequency to hit it (dtype/dimension-aware), overriding the
            // configured/default noise_frequency.
            if let Some(target) = raster_config.and_then(|rc| rc.target_compression_ratio) {
                let cal = spatialbench_raster::cog::calibrate_frequency(&cog_config, target)?;
                if cal.clamped {
                    log::warn!(
                        "target_compression_ratio={target} unreachable for dtype={:?} at {}x{}; \
                         clamped noise_frequency={:.2} (achieved ~{:.2}x)",
                        cog_config.dtype,
                        cog_config.raster.cog_width,
                        cog_config.raster.cog_height,
                        cal.frequency,
                        cal.achieved_ratio,
                    );
                } else {
                    info!(
                        "calibrated noise_frequency={:.2} for target_compression_ratio={target} \
                         (achieved ~{:.2}x)",
                        cal.frequency, cal.achieved_ratio,
                    );
                }
                cog_config.noise_frequency = cal.frequency;
            }

            let continent_name = raster_config
                .map(|rc| rc.continent.as_str())
                .unwrap_or("south_north_america");

            let affines = spatialbench::spatial::ContinentAffines::default();
            let affine = crate::spatial_config_file::continent_affine(&affines, continent_name)?;

            let grid = FootprintGrid::new(affine, cog_config.raster, self.max_footprints);
            let footprints = grid.generate();

            info!(
                "continent={}, {} footprints, {} COGs/footprint, {} total COGs, dtype={:?}",
                continent_name,
                footprints.len(),
                tier.scenes_per_footprint,
                footprints.len() as u64 * tier.scenes_per_footprint as u64,
                cog_config.dtype,
            );

            let output_str = self.output_dir.to_string_lossy();
            let raster_dir = if output_str.starts_with("s3://") {
                format!("{}/raster", output_str.trim_end_matches('/'))
            } else {
                self.output_dir
                    .join("raster")
                    .to_string_lossy()
                    .into_owned()
            };
            let manifest = raster_runner::run_raster(raster_runner::RunRasterArgs {
                footprints: &footprints,
                tier,
                cog_config: &cog_config,
                output_dir: &raster_dir,
                num_threads: self.num_threads,
                resume: self.resume,
                upload_concurrency: self.raster_upload_concurrency,
                memory_budget_bytes: self.raster_memory_budget_mb.map(|mb| mb * 1024 * 1024),
            })
            .await?;

            info!("generated {} manifest entries", manifest.len());

            // Compute the pile base href for STAC asset hrefs.
            // S3: s3://bucket/output/raster/pile
            // Local: absolute path to pile directory
            let pile_base_href = if raster_dir.starts_with("s3://") {
                format!("{}/pile", raster_dir.trim_end_matches('/'))
            } else {
                // Canonicalize to absolute path for local output
                let pile_path = Path::new(&raster_dir).join("pile");
                pile_path
                    .canonicalize()
                    .unwrap_or(pile_path)
                    .to_string_lossy()
                    .into_owned()
            };

            // Write STAC geoparquet catalogs (Temporal + Balanced; Wide uses multi-band COGs)
            let is_s3 = raster_dir.starts_with("s3://");
            if is_s3 {
                let (bucket, prefix) = crate::s3_writer::parse_s3_uri(&raster_dir)?;
                let client = crate::s3_writer::build_s3_client(&bucket)?;
                for topo in Topology::SHARED_PILE {
                    let mut buf = Vec::new();
                    write_stac_geoparquet(&manifest, tier, topo, &mut buf, &pile_base_href)?;
                    let key = format!(
                        "{}/stac/{}.parquet",
                        prefix.trim_end_matches('/'),
                        topo.dir_name()
                    );
                    let path = object_store::path::Path::from(key.as_str());
                    client
                        .put(&path, buf.into())
                        .await
                        .map_err(|e| io::Error::other(format!("S3 upload failed: {e}")))?;
                    info!("wrote STAC catalog: s3://{}/{}", bucket, key);
                }
            } else {
                let stac_dir = Path::new(&raster_dir).join("stac");
                std::fs::create_dir_all(&stac_dir)?;
                for topo in Topology::SHARED_PILE {
                    let path = stac_dir.join(format!("{}.parquet", topo.dir_name()));
                    let file = std::fs::File::create(&path)?;
                    write_stac_geoparquet(&manifest, tier, topo, file, &pile_base_href)?;
                    info!("wrote STAC catalog: {}", path.display());
                }
            }
        }

        info!("Generation complete!");
        Ok(())
    }

    async fn generate_zone(&self) -> io::Result<()> {
        let format = match self.format {
            OutputFormat::Parquet => zone::main::OutputFormat::Parquet,
            OutputFormat::Csv => zone::main::OutputFormat::Csv,
            OutputFormat::Tbl => zone::main::OutputFormat::Tbl,
        };

        zone::main::generate_zone(
            format,
            self.scale_factor,
            self.output_dir.clone(),
            self.parts,
            self.part,
            self.mb_per_file,
            self.parquet_row_group_bytes,
            self.parquet_compression,
        )
        .await
    }
}

impl AsyncFinalize for BufWriter<Stdout> {
    async fn finalize(self) -> Result<usize, io::Error> {
        Ok(0)
    }
}

impl AsyncFinalize for BufWriter<File> {
    async fn finalize(self) -> Result<usize, io::Error> {
        let file = self.into_inner()?;
        let metadata = file.metadata()?;
        Ok(metadata.len() as usize)
    }
}

impl AsyncFinalize for s3_writer::S3Writer {
    async fn finalize(self) -> Result<usize, io::Error> {
        self.finish().await
    }
}

/// Wrapper around a buffer writer that counts the number of buffers and bytes written
struct WriterSink<W: Write> {
    statistics: WriteStatistics,
    inner: W,
}

impl<W: Write> WriterSink<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            statistics: WriteStatistics::new("buffers"),
        }
    }

    /// Consume the sink and return the inner writer for further finalization.
    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write + Send> Sink for WriterSink<W> {
    fn sink(&mut self, buffer: &[u8]) -> Result<(), io::Error> {
        self.statistics.increment_chunks(1);
        self.statistics.increment_bytes(buffer.len());
        self.inner.write_all(buffer)
    }

    fn flush(mut self) -> Result<Self, io::Error> {
        self.inner.flush()?;
        Ok(self)
    }
}

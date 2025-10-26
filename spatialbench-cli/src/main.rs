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
mod runner;
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
use spatialbench::distribution::Distributions;
use spatialbench::spatial::overrides::{set_overrides, SpatialOverrides};
use spatialbench::text::TextPool;
use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, BufWriter, Stdout, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "spatialbench")]
#[command(version)]
#[command(
    // -h output
    about = "TPC-H Data Generator",
    // --help output
    long_about = r#"
TPCH Data Generator (https://github.com/clflushopt/spatialbench-rs)

By default each table is written to a single file named <output_dir>/<table>.<format>

If `--part` option is specified, each table is written to a subdirectory in
multiple files named <output_dir>/<table>/<table>.<part>.<format>

Examples

# Generate all tables at scale factor 1 (1GB) in TBL format to /tmp/tpch directory:

spatialbench-cli -s 1 --output-dir=/tmp/tpch

# Generate the lineitem table at scale factor 100 in 10 Apache Parquet files to
# /tmp/tpch/lineitem

spatialbench-cli -s 100 --tables=lineitem --format=parquet --parts=10 --output-dir=/tmp/tpch

# Generate scale factor one in current directory, seeing debug output

RUST_LOG=debug spatialbench -s 1
"#
)]
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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Table {
    Vehicle,
    Driver,
    Customer,
    Trip,
    Building,
    Zone,
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

        // Create output directory if it doesn't exist and we are not writing to stdout.
        if !self.stdout {
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

        if let Some(path) = config_path {
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
        }

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

        for table in tables {
            if table == Table::Zone {
                self.generate_zone().await?
            } else {
                output_plan_generator.generate_plans(table, self.part, self.parts)?;
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
            self.parquet_row_group_bytes,
            self.parquet_compression,
        )
        .await
    }
}

impl IntoSize for BufWriter<Stdout> {
    fn into_size(self) -> Result<usize, io::Error> {
        // we can't get the size of stdout, so just return 0
        Ok(0)
    }
}

impl IntoSize for BufWriter<File> {
    fn into_size(self) -> Result<usize, io::Error> {
        let file = self.into_inner()?;
        let metadata = file.metadata()?;
        Ok(metadata.len() as usize)
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
}

impl<W: Write + Send> Sink for WriterSink<W> {
    fn sink(&mut self, buffer: &[u8]) -> Result<(), io::Error> {
        self.statistics.increment_chunks(1);
        self.statistics.increment_bytes(buffer.len());
        self.inner.write_all(buffer)
    }

    fn flush(mut self) -> Result<(), io::Error> {
        self.inner.flush()
    }
}

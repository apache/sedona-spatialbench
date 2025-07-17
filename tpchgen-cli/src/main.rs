//! TPCH data generation CLI with a dbgen compatible API.
//!
//! This crate provides a CLI for generating TPCH data and tries to remain close
//! API wise to the original dbgen tool, as in we use the same command line flags
//! and arguments.
//!
//! ```
//! USAGE:
//!     tpchgen-cli [OPTIONS]
//!
//! OPTIONS:
//!     -h, --help                    Prints help information
//!     -V, --version                 Prints version information
//!     -s, --scale-factor <FACTOR>  Scale factor for the data generation (default: 1)
//!     -T, --tables <TABLES>        Comma-separated list of tables to generate (default: all)
//!     -f, --format <FORMAT>        Output format: tbl, csv, or parquet (default: tbl)
//!     -o, --output-dir <DIR>       Output directory (default: current directory)
//!     -p, --parts <N>              Number of parts to split generation into (default: 1)
//!         --part <N>               Which part to generate (1-based, default: 1)
//!     -n, --num-threads <N>        Number of threads to use (default: number of CPUs)
//!     -c, --parquet-compression <C> Parquet compression codec, e.g., SNAPPY, ZSTD(1), UNCOMPRESSED (default: SNAPPY)
//!     -v, --verbose                Verbose output
//!         --stdout                 Write output to stdout instead of files
//!```
//!
//! # Logging:
//! Use the `-v` flag or `RUST_LOG` environment variable to control logging output.
//!
//! `-v` sets the log level to `info` and ignores the `RUST_LOG` environment variable.
//!
//! # Examples
//! ```
//! # see all info output
//! tpchgen-cli -s 1 -v
//!
//! # same thing using RUST_LOG
//! RUST_LOG=info tpchgen-cli -s 1
//!
//! # see all debug output
//! RUST_LOG=debug tpchgen -s 1
//! ```
mod csv;
mod generate;
mod parquet;
mod statistics;
mod tbl;

use crate::csv::*;
use crate::generate::{generate_in_chunks, Sink, Source};
use crate::parquet::*;
use crate::statistics::WriteStatistics;
use crate::tbl::*;
use ::parquet::basic::Compression;
use clap::builder::TypedValueParser;
use clap::{Parser, ValueEnum};
use log::{debug, info, LevelFilter};
use std::fmt::Display;
use std::fs::{self, File};
use std::io::{self, BufWriter, Stdout, Write};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use tpchgen::distribution::Distributions;
use tpchgen::generators::{
    BuildingGenerator, CustomerGenerator, DriverGenerator, TripGenerator, VehicleGenerator,
    ZoneGenerator,
};
use tpchgen::text::TextPool;
use tpchgen_arrow::{
    BuildingArrow, CustomerArrow, DriverArrow, RecordBatchIterator, TripArrow, VehicleArrow,
    ZoneArrow,
};

#[derive(Parser)]
#[command(name = "tpchgen")]
#[command(version)]
#[command(about = "TPC-H Data Generator", long_about = None)]
struct Cli {
    /// Scale factor to address (default: 1)
    #[arg(short, long, default_value_t = 1.)]
    scale_factor: f64,

    /// Output directory for generated files (default: current directory)
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// Which tables to generate (default: all)
    #[arg(short = 'T', long = "tables", value_delimiter = ',', value_parser = TableValueParser)]
    tables: Option<Vec<Table>>,

    /// Number of parts to generate (manual parallel generation)
    #[arg(short, long, default_value_t = 1)]
    parts: i32,

    /// Which part to generate (1-based, only relevant if parts > 1)
    #[arg(long, default_value_t = 1)]
    part: i32,

    /// Output format: tbl, csv, parquet (default: tbl)
    #[arg(short, long, default_value = "tbl")]
    format: OutputFormat,

    /// The number of threads for parallel generation, defaults to the number of CPUs
    #[arg(short, long, default_value_t = num_cpus::get())]
    num_threads: usize,

    /// Parquet block compression format. Default is SNAPPY
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

    /// Verbose output (default: false)
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    /// Write the output to stdout instead of a file.
    #[arg(long, default_value_t = false)]
    stdout: bool,
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

/// macro to create a Cli function for generating a table
///
/// Arguments:
/// $FUN_NAME: name of the function to create
/// $TABLE: The [`Table`] to generate
/// $GENERATOR: The generator type to use
/// $TBL_SOURCE: The [`Source`] type to use for TBL format
/// $CSV_SOURCE: The [`Source`] type to use for CSV format
/// $PARQUET_SOURCE: The [`RecordBatchIterator`] type to use for Parquet format
macro_rules! define_generate {
    ($FUN_NAME:ident,  $TABLE:expr, $GENERATOR:ident, $TBL_SOURCE:ty, $CSV_SOURCE:ty, $PARQUET_SOURCE:ty) => {
        async fn $FUN_NAME(&self) -> io::Result<()> {
            let filename = self.output_filename($TABLE);
            let (num_parts, parts) = self.parallel_target_part_count(&$TABLE);
            let scale_factor = self.scale_factor;
            info!("Writing table {} (SF={scale_factor}) to {filename}", $TABLE);
            debug!("Generating {num_parts} parts in total");
            let gens = parts
                .into_iter()
                .map(move |part| $GENERATOR::new(scale_factor, part, num_parts));
            match self.format {
                OutputFormat::Tbl => self.go(&filename, gens.map(<$TBL_SOURCE>::new)).await,
                OutputFormat::Csv => self.go(&filename, gens.map(<$CSV_SOURCE>::new)).await,
                OutputFormat::Parquet => {
                    self.go_parquet(&filename, gens.map(<$PARQUET_SOURCE>::new))
                        .await
                }
            }
        }
    };
}

impl Cli {
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

        // force the creation of the distributions and text pool to so it doesn't
        // get charged to the first table
        let start = Instant::now();
        debug!("Creating distributions and text pool");
        Distributions::static_default();
        TextPool::get_or_init_default();
        let elapsed = start.elapsed();
        info!("Created static distributions and text pools in {elapsed:?}");

        // Generate each table
        for table in tables {
            match table {
                Table::Vehicle => self.generate_vehicle().await?,
                Table::Driver => self.generate_driver().await?,
                Table::Customer => self.generate_customer().await?,
                Table::Trip => self.generate_trip().await?,
                Table::Building => self.generate_building().await?,
                Table::Zone => self.generate_zone().await?,
            }
        }

        info!("Generation complete!");
        Ok(())
    }
    define_generate!(
        generate_vehicle,
        Table::Vehicle,
        VehicleGenerator,
        VehicleTblSource,
        VehicleCsvSource,
        VehicleArrow
    );
    define_generate!(
        generate_driver,
        Table::Driver,
        DriverGenerator,
        DriverTblSource,
        DriverCsvSource,
        DriverArrow
    );
    define_generate!(
        generate_customer,
        Table::Customer,
        CustomerGenerator,
        CustomerTblSource,
        CustomerCsvSource,
        CustomerArrow
    );
    define_generate!(
        generate_trip,
        Table::Trip,
        TripGenerator,
        TripTblSource,
        TripCsvSource,
        TripArrow
    );
    define_generate!(
        generate_building,
        Table::Building,
        BuildingGenerator,
        BuildingTblSource,
        BuildingCsvSource,
        BuildingArrow
    );
    define_generate!(
        generate_zone,
        Table::Zone,
        ZoneGenerator,
        ZoneTblSource,
        ZoneCsvSource,
        ZoneArrow
    );

    /// return the output filename for the given table
    fn output_filename(&self, table: Table) -> String {
        let extension = match self.format {
            OutputFormat::Tbl => "tbl",
            OutputFormat::Csv => "csv",
            OutputFormat::Parquet => "parquet",
        };
        format!("{}.{extension}", table.name())
    }

    /// return a file for writing the given filename in the output directory
    fn new_output_file(&self, filename: &str) -> io::Result<File> {
        let path = self.output_dir.join(filename);
        File::create(path)
    }

    /// Returns a list of "parts" (data generator chunks, not TPCH parts) to create
    ///
    /// Tuple returned is `(num_parts, part_list)`:
    /// - num_parts is the total number of parts to generate
    /// - part_list is the list of parts to generate (1 based)
    fn parallel_target_part_count(&self, table: &Table) -> (i32, Vec<i32>) {
        // parallel generation disabled if user specifies a part explicitly
        if self.part != 1 || self.parts != 1 {
            return (self.parts, vec![self.part]);
        }

        // Note use part=1, part_count=1 to calculate the total row count
        // for the table
        //
        // Avg row size is an estimate of the average row size in bytes from the first 100 rows
        // of the table in tbl format
        let (avg_row_size_bytes, row_count) = match table {
            Table::Vehicle => (
                64,
                VehicleGenerator::calculate_row_count(self.scale_factor, 1, 1),
            ),
            Table::Driver => (
                80,
                DriverGenerator::calculate_row_count(self.scale_factor, 1, 1),
            ),
            Table::Customer => (
                84,
                CustomerGenerator::calculate_row_count(self.scale_factor, 1, 1),
            ),
            &Table::Trip => (
                144,
                TripGenerator::calculate_row_count(self.scale_factor, 1, 1),
            ),
            Table::Building => (
                212,
                BuildingGenerator::calculate_row_count(self.scale_factor, 1, 1),
            ),
            Table::Zone => {
                let generator = ZoneGenerator::new(self.scale_factor, 1, 1);
                (115, generator.calculate_row_count())
            }
        };
        // target chunks of about 16MB (use 15MB to ensure we don't exceed the target size)
        let target_chunk_size_bytes = 15 * 1024 * 1024;
        let mut num_parts = ((row_count * avg_row_size_bytes) / target_chunk_size_bytes) + 1;

        // parquet files can have at most 32767 row groups so cap the number of parts at that number
        if self.format == OutputFormat::Parquet {
            num_parts = num_parts.min(32767);
        }

        // convert to i32
        let num_parts = num_parts.try_into().unwrap();
        // generating all the parts
        (num_parts, (1..=num_parts).collect())
    }

    /// Generates the output file from the sources
    async fn go<I>(&self, filename: &str, sources: I) -> Result<(), io::Error>
    where
        I: Iterator<Item: Source> + 'static,
    {
        // Since generate_in_chunks already buffers, there is no need to buffer again
        if self.stdout {
            let sink = WriterSink::new(io::stdout());
            generate_in_chunks(sink, sources, self.num_threads).await
        } else {
            let sink = WriterSink::new(self.new_output_file(filename)?);
            generate_in_chunks(sink, sources, self.num_threads).await
        }
    }

    /// Generates an output parquet file from the sources
    async fn go_parquet<I>(&self, filename: &str, sources: I) -> Result<(), io::Error>
    where
        I: Iterator<Item: RecordBatchIterator> + 'static,
    {
        if self.stdout {
            // write to stdout
            let writer = BufWriter::with_capacity(32 * 1024 * 1024, io::stdout()); // 32MB buffer
            generate_parquet(writer, sources, self.num_threads, self.parquet_compression).await
        } else {
            // write to a file
            let file = self.new_output_file(filename)?;
            let writer = BufWriter::with_capacity(32 * 1024 * 1024, file); // 32MB buffer
            generate_parquet(writer, sources, self.num_threads, self.parquet_compression).await
        }
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

//! [`GenerationPlan`] that describes how to generate a TPC-H dataset.

use crate::{OutputFormat, Table};
use log::debug;
use spatialbench::generators::{
    BuildingGenerator, CustomerGenerator, DriverGenerator, TripGenerator, VehicleGenerator,
    ZoneGenerator,
};
use std::fmt::Display;
use std::ops::RangeInclusive;

/// A list of generator "parts" (data generator chunks, not Spatial Bench parts)
///
/// Controls the parallelization and layout of Parquet files in `spatialbench-cli`.
///
/// # Background
///
/// A "part" is a logical partition of a particular output table. Each data
/// generator can create parts individually.
///
/// For example, the parameters to [`TripGenerator::new`] `scale_factor,
/// `part_count` and `part_count` together define a partition of the `Trip`
/// table.
///
/// The entire output table results from generating each of the `part_count` parts. For
/// example, if `part_count` is 10, appending parts 1 to 10 results in a
/// complete `Trip` table.
///
/// Interesting properties of parts:
/// 1. They are independent of each other, so they can be generated in parallel.
/// 2. They scale. So for example, parts `0..10` with a `part_count` of 50
///    will generate the same data as parts `1` with a `part_count` of 5.
///
/// # Implication for spatialbench-cli
///
/// For `tbl` and `csv` files, spatialbench-cli generates `num-threads` parts in
/// parallel.
///
/// For Parquet files, the output file has one row group for each "part".
///
/// # Example
/// ```
/// let plan = GenerationPlan::new(
///   &Table::Trips,
///   OutputFormat::Parquet,
///   1.0, // scale factor
///   -1, // cli_part
///   -1, // cli_parts
///  );
/// let results = plan.into_iter().collect::<Vec<_>>();
/// /// assert_eq!(results.len(), 1);
/// ```
#[derive(Debug)]
pub struct GenerationPlan {
    /// Total number of parts to generate
    part_count: i32,
    /// List of parts (1..=part_count)
    part_list: RangeInclusive<i32>,
}

impl GenerationPlan {
    /// Returns a GenerationPlan number of parts to generate
    ///
    /// # Arguments
    /// * `cli_part`: optional part number to generate (1-based), `--part` CLI argument
    /// * `cli_part_count`: optional total number of parts, `--parts` CLI argument
    pub fn try_new(
        table: &Table,
        format: OutputFormat,
        scale_factor: f64,
        cli_part: Option<i32>,
        cli_part_count: Option<i32>,
    ) -> Result<Self, String> {
        // If a single part is specified, split it into chunks to enable parallel generation.
        match (cli_part, cli_part_count) {
            (Some(_part), None) => Err(String::from(
                "The --part option requires the --parts option to be set",
            )),
            (None, Some(_part_count)) => {
                // TODO automatically create multiple files if part_count > 1
                // and part is not specified
                Err(String::from(
                    "The --part_count option requires the --part option to be set",
                ))
            }
            (Some(part), Some(part_count)) => {
                Self::try_new_with_parts(table, format, scale_factor, part, part_count)
            }
            (None, None) => Self::try_new_without_parts(table, format, scale_factor),
        }
    }

    /// Returns a new `GenerationPlan` when partitioning
    ///
    /// See [`GenerationPlan::try_new`] for argument documentation.
    fn try_new_with_parts(
        table: &Table,
        format: OutputFormat,
        scale_factor: f64,
        cli_part: i32,
        cli_part_count: i32,
    ) -> Result<Self, String> {
        if cli_part < 1 {
            return Err(format!(
                "Invalid --part. Expected a number greater than zero, got {cli_part}"
            ));
        }
        if cli_part_count < 1 {
            return Err(format!(
                "Invalid --part_count. Expected a number greater than zero, got {cli_part_count}"
            ));
        }
        if cli_part > cli_part_count {
            return Err(format!(
                    "Invalid --part. Expected at most the value of --parts ({cli_part_count}), got {cli_part}"));
        }

        // These tables are so small they are not parameterized by part count,
        // so only a single part.
        if table == &Table::Vehicle || table == &Table::Driver {
            return Ok(Self {
                part_count: 1,
                part_list: 1..=1,
            });
        }

        // scale down the row count by the number of partitions being generated
        // so that the output is consistent with the original part count
        let num_chunks = OutputSize::new(table, scale_factor, format)
            .with_scaled_row_count(cli_part_count)
            .part_count();

        // The new total number of partitions is the original number of
        // partitions multiplied by the number of chunks.
        let new_total_parts = cli_part_count * num_chunks;

        // The new partitions to generate correspond to the chunks that make up
        // the original part.
        //
        // So for example, if the original partition count was 10 and the part was 2
        // and the number of chunks is 5, then:
        //
        // * new_total_parts = 10 * 5 = 50
        // * new_parts_to_generate = (2-1)*5+1 ..= 2*5 = 6..=10
        let start_part = (cli_part - 1) * num_chunks + 1;
        let end_part = cli_part * num_chunks;
        let new_parts_to_generate = start_part..=end_part;
        debug!(
            "User specified cli_parts={cli_part_count}, cli_part={cli_part}. \
            Generating {new_total_parts} partitions for table {table:?} \
            with scale factor {scale_factor}: {new_parts_to_generate:?}"
        );
        Ok(Self {
            part_count: new_total_parts,
            part_list: new_parts_to_generate,
        })
    }

    /// Returns a new `GenerationPlan` when no partitioning is specified on the command line
    fn try_new_without_parts(
        table: &Table,
        format: OutputFormat,
        scale_factor: f64,
    ) -> Result<Self, String> {
        let output_size = OutputSize::new(table, scale_factor, format);
        let num_parts = output_size.part_count();

        Ok(Self {
            part_count: num_parts,
            part_list: 1..=num_parts,
        })
    }
}

/// Converts the `GenerationPlan` into an iterator of (part_number, num_parts)
impl IntoIterator for GenerationPlan {
    type Item = (i32, i32);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.part_list
            .map(|part_number| (part_number, self.part_count))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

impl Display for GenerationPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GenerationPlan for {} parts", self.part_count)
    }
}

/// output size of a table
#[derive(Debug)]
struct OutputSize {
    /// Average row size in bytes
    avg_row_size_bytes: i64,
    /// Number of rows in the table
    row_count: i64,
    /// output target chunk size in bytes
    target_chunk_size_bytes: i64,
    /// maximum part count, if any
    max_part_count: Option<i64>,
}

impl OutputSize {
    pub fn new(table: &Table, scale_factor: f64, format: OutputFormat) -> Self {
        let row_count = Self::row_count_for_table(table, scale_factor);

        // The average row size in bytes for each table in the TPC-H schema
        // this was determined by sampling the data
        let avg_row_size_bytes = match format {
            OutputFormat::Tbl | OutputFormat::Csv => match table {
                Table::Vehicle => 64,
                Table::Driver => 80,
                Table::Customer => 84,
                Table::Trip => 144,
                Table::Building => 212,
                Table::Zone => 115,
            },
            // Average row size in bytes for each table at scale factor 1.0
            // computed using datafusion-cli:
            // ```shell
            // datafusion-cli -c "datafusion-cli -c "select row_group_id, count(*), min(row_group_bytes)::float/min(row_group_num_rows)::float as bytes_per_row from parquet_metadata('lineitem.parquet') GROUP BY 1 ORDER BY 1""
            // ```
            OutputFormat::Parquet => match table {
                Table::Vehicle => 54,
                Table::Driver => 84,
                Table::Customer => 87,
                Table::Trip => 69,
                Table::Building => 109,
                Table::Zone => {
                    // Scale based on zone subtype count for the scale factor
                    match scale_factor {
                        sf if sf < 10.0 => 1332,
                        sf if sf < 100.0 => 2000,
                        _ => 4258,
                    }
                }
            },
        };

        let target_chunk_size_bytes = match format {
            // for tbl/csv target chunks of about the same as the output buffer
            // size, 16MB. Use 15MB to ensure we don't exceed the buffer size.
            OutputFormat::Tbl | OutputFormat::Csv => 15 * 1024 * 1024,
            // target 7MB row groups
            // todo make this configurable
            // https://github.com/clflushopt/tpchgen-rs/issues/146
            // Use 128MB for parquet files, which is the default
            // row group size used by spark
            OutputFormat::Parquet => 128 * 1024 * 1024,
        };

        // parquet files can have at most 32767 row groups so cap the number of parts at that number
        let max_part_count = match format {
            OutputFormat::Tbl | OutputFormat::Csv => None,
            OutputFormat::Parquet => Some(32767),
        };

        debug!(
            "Output size for table {table:?} with scale factor {scale_factor}: \
                avg_row_size_bytes={avg_row_size_bytes}, row_count={row_count} \
                target_chunk_size_bytes={target_chunk_size_bytes}, max_part_count={max_part_count:?}",
        );

        OutputSize {
            avg_row_size_bytes,
            row_count,
            target_chunk_size_bytes,
            max_part_count,
        }
    }

    /// Return the number of parts to generate
    pub fn part_count(&self) -> i32 {
        let mut num_parts =
            ((self.row_count * self.avg_row_size_bytes) / self.target_chunk_size_bytes) + 1; // +1 to ensure we have at least one part

        if let Some(max_part_count) = self.max_part_count {
            // if the max part count is set, cap the number of parts at that number
            num_parts = num_parts.min(max_part_count)
        }

        // convert to i32
        num_parts.try_into().unwrap()
    }

    /// Scale the row count for the output by the number of partitions
    ///
    /// So for example if the row count is 1000 and the number of partitions is 10,
    /// the scaled row count will be 100.
    pub fn with_scaled_row_count(&self, cli_part_count: i32) -> OutputSize {
        // scale the row count by the number of partitions being generated
        let scaled_row_count = self.row_count / cli_part_count as i64;
        debug!(
            "Scaling row count from {} to {scaled_row_count}",
            self.row_count,
        );
        OutputSize {
            avg_row_size_bytes: self.avg_row_size_bytes,
            row_count: scaled_row_count,
            target_chunk_size_bytes: self.target_chunk_size_bytes,
            max_part_count: self.max_part_count,
        }
    }

    fn row_count_for_table(table: &Table, scale_factor: f64) -> i64 {
        //let (avg_row_size_bytes, row_count) = match table {
        match table {
            Table::Vehicle => VehicleGenerator::calculate_row_count(scale_factor, 1, 1),
            Table::Driver => DriverGenerator::calculate_row_count(scale_factor, 1, 1),
            Table::Customer => CustomerGenerator::calculate_row_count(scale_factor, 1, 1),
            Table::Trip => TripGenerator::calculate_row_count(scale_factor, 1, 1),
            Table::Building => BuildingGenerator::calculate_row_count(scale_factor, 1, 1),
            Table::Zone => {
                let generator = ZoneGenerator::new(scale_factor, 1, 1);
                generator.calculate_row_count()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Default layouts for generating TPC-H tables (tbl/csv format)
    // These tests explain the default layouts for each table (e.g. row groups in parquet)

    mod default_layouts {
        use super::*;
        #[test]
        fn tbl_sf1_default_vehicle() {
            Test::new()
                .with_table(Table::Vehicle)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        #[test]
        fn tbl_sf1_default_driver() {
            Test::new()
                .with_table(Table::Driver)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        #[test]
        fn tbl_sf1_default_customer() {
            Test::new()
                .with_table(Table::Customer)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        #[test]
        fn tbl_sf1_default_trip() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .assert(55, 1..=55)
        }

        #[test]
        fn tbl_sf1_default_buildings() {
            Test::new()
                .with_table(Table::Building)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        // #[test]
        // fn tbl_sf1_default_zone() {
        //     Test::new()
        //         .with_table(Table::Zone)
        //         .with_format(OutputFormat::Tbl)
        //         .with_scale_factor(0.001)
        //         .assert(1, 1..=1)
        // }

        #[test]
        fn parquet_sf1_default_vehicle() {
            Test::new()
                .with_table(Table::Vehicle)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        #[test]
        fn parquet_sf1_default_drivers() {
            Test::new()
                .with_table(Table::Driver)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        #[test]
        fn parquet_sf1_default_customer() {
            Test::new()
                .with_table(Table::Customer)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        #[test]
        fn parquet_sf1_default_trip() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                .assert(4, 1..=4)
        }

        #[test]
        fn parquet_sf1_default_building() {
            Test::new()
                .with_table(Table::Building)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                .assert(1, 1..=1)
        }

        // #[test]
        // fn parquet_sf1_default_zone() {
        //     Test::new()
        //         .with_table(Table::Zone)
        //         .with_format(OutputFormat::Parquet)
        //         .with_scale_factor(0.001)
        //         .assert(1, 1..=1)
        // }
    }

    // Test plans with CLI parts and partition counts
    mod partitions {
        use super::*;

        #[test]
        fn tbl_sf1_vehicle_cli_parts() {
            Test::new()
                .with_table(Table::Vehicle)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                // nation table is small, so it can not be made in parts
                .with_cli_part(1)
                .with_cli_part_count(10)
                // we expect there is still only one part
                .assert(1, 1..=1)
        }

        #[test]
        fn tbl_sf1_driver_cli_parts() {
            Test::new()
                .with_table(Table::Driver)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                // region table is small, so it can not be made in parts
                .with_cli_part(1)
                .with_cli_part_count(10)
                // we expect there is still only one part
                .assert(1, 1..=1)
        }

        #[test]
        fn tbl_sf1_customer_cli_parts_1() {
            Test::new()
                .with_table(Table::Customer)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                // Generate only part 1 of the trip table, but results in 10 partititions
                .with_cli_part(1)
                .with_cli_part_count(10)
                .assert(10, 1..=1)
        }

        #[test]
        fn tbl_sf1_trip_cli_parts_4() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .with_cli_part(4) // part 4 of 10
                .with_cli_part_count(10)
                .assert(60, 19..=24)
        }

        #[test]
        fn parquet_sf1_building_cli_parts() {
            Test::new()
                .with_table(Table::Building)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                // region table is small, so it can not be made in parts
                .with_cli_part(1)
                .with_cli_part_count(10)
                // we expect there is still only one part
                .assert(10, 1..=1)
        }

        // #[test]
        // fn parquet_sf1_zone_cli_parts_1() {
        //     Test::new()
        //         .with_table(Table::Zone)
        //         .with_format(OutputFormat::Parquet)
        //         .with_scale_factor(0.001)
        //         // Generate only part 1 of the trip table
        //         .with_cli_part(1)
        //         .with_cli_part_count(10)
        //         // we expect to generate the first 1 / 10 row groups (1/10)
        //         .assert(10, 1..=1)
        // }

        #[test]
        fn parquet_sf1_trip_cli_parts_4() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                .with_cli_part(4) // part 4 of 10
                .with_cli_part_count(10)
                // we expect to generate the 4th set of row groups
                .assert(10, 4..=4)
        }

        #[test]
        fn parquet_sf1_trip_cli_parts_10() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1.0)
                .with_cli_part(10) // part 10 of 10
                .with_cli_part_count(10)
                // expect the last 6 row groups
                .assert(10, 10..=10)
        }

        #[test]
        fn tbl_sf1_trip_cli_invalid_part() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .with_cli_part(0) // part 0 of 10 (invalid)
                .with_cli_part_count(10)
                .assert_err("Invalid --part. Expected a number greater than zero, got 0")
        }
    }

    //  Error cases for invalid CLI parts and partition
    mod errors {
        use super::*;

        #[test]
        fn sf1_trip_cli_invalid_part() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .with_cli_part(0) // part 0 of 10 (invalid)
                .with_cli_part_count(10)
                .assert_err("Invalid --part. Expected a number greater than zero, got 0")
        }

        #[test]
        fn tbl_sf1_trip_cli_parts_invalid_big() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .with_cli_part(11) // part 11 of 10 (invalid)
                .with_cli_part_count(10)
                .assert_err("Invalid --part. Expected at most the value of --parts (10), got 11");
        }

        #[test]
        fn tbl_sf1_trip_cli_invalid_part_count() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1.0)
                .with_cli_part(1) // part 0 of 0 (invalid)
                .with_cli_part_count(0)
                .assert_err("Invalid --part_count. Expected a number greater than zero, got 0");
        }
    }

    // test the row group limits for parquet
    mod limits {
        use super::*;
        #[test]
        fn parquet_sf10_trip_limit() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(10.0)
                .assert(31, 1..=31);
        }

        #[test]
        fn tbl_sf10_trip_limit() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(10.0)
                .assert(550, 1..=550);
        }
        #[test]
        fn tbl_sf1000_trip_limit() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Tbl)
                .with_scale_factor(1000.0)
                .assert(54932, 1..=54932);
        }

        #[test]
        fn parquet_sf1000_trip_limit() {
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1000.0)
                .assert(3085, 1..=3085);
        }

        // If we make a really large trip table, we can generate it in parts that will also go
        // in a large number of row groups, but still limited to 32k row groups in total.
        #[test]
        fn parquet_sf1000_trip_cli_parts_limit() {
            let expected_parts = 928..=1236;
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(1000.0)
                .with_cli_part(4) // part 4 of 10
                .with_cli_part_count(10)
                .assert(3090, expected_parts.clone());

            // can not have more than 32k actual row groups in a parquet file
            assert!(
                expected_parts.end() - expected_parts.start() <= 32767,
                "Expected parts {expected_parts:?} should not exceed 32k row groups",
            );
        }

        #[test]
        fn parquet_sf100000_trip_cli_parts_limit() {
            let expected_parts = 92539..=123384;
            Test::new()
                .with_table(Table::Trip)
                .with_format(OutputFormat::Parquet)
                .with_scale_factor(100000.0)
                .with_cli_part(4) // part 4 of 10
                .with_cli_part_count(10)
                .assert(308460, expected_parts.clone());

            // can not have more than 32k actual row groups in a parquet file
            assert!(
                expected_parts.end() - expected_parts.start() <= 32767,
                "Expected parts {expected_parts:?} should not exceed 32k row groups",
            );
        }
    }

    /// Test fixture for [`GenerationPlan`].
    #[derive(Debug)]
    struct Test {
        table: Table,
        format: OutputFormat,
        scale_factor: f64,
        cli_part: Option<i32>,
        cli_part_count: Option<i32>,
    }

    impl Test {
        fn new() -> Self {
            Default::default()
        }

        /// Create a [`GenerationPlan`] and assert it has the
        /// expected number of parts and part numbers.
        fn assert(self, expected_part_count: i32, expected_part_numbers: RangeInclusive<i32>) {
            let plan = GenerationPlan::try_new(
                &self.table,
                self.format,
                self.scale_factor,
                self.cli_part,
                self.cli_part_count,
            )
            .unwrap();
            assert_eq!(plan.part_count, expected_part_count);
            assert_eq!(plan.part_list, expected_part_numbers);
        }

        /// Assert that creating a [`GenerationPlan`] returns the specified error
        fn assert_err(self, expected_error: &str) {
            let actual_error = GenerationPlan::try_new(
                &self.table,
                self.format,
                self.scale_factor,
                self.cli_part,
                self.cli_part_count,
            )
            .unwrap_err();
            assert_eq!(actual_error, expected_error);
        }

        /// Set table
        fn with_table(mut self, table: Table) -> Self {
            self.table = table;
            self
        }

        /// Set output format
        fn with_format(mut self, format: OutputFormat) -> Self {
            self.format = format;
            self
        }

        /// Set scale factor
        fn with_scale_factor(mut self, scale_factor: f64) -> Self {
            self.scale_factor = scale_factor;
            self
        }

        /// Set CLI part
        fn with_cli_part(mut self, cli_part: i32) -> Self {
            self.cli_part = Some(cli_part);
            self
        }

        /// Set CLI partitition count
        fn with_cli_part_count(mut self, cli_part_count: i32) -> Self {
            self.cli_part_count = Some(cli_part_count);
            self
        }
    }

    impl Default for Test {
        fn default() -> Self {
            Self {
                table: Table::Trip,
                format: OutputFormat::Tbl,
                scale_factor: 1.0,
                cli_part: None,
                cli_part_count: None,
            }
        }
    }
}

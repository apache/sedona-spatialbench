use assert_cmd::Command;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::ParquetMetaDataReader;
use spatialbench::generators::TripGenerator;
use spatialbench_arrow::{RecordBatchIterator, TripArrow};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

/// Test TBL output for scale factor 0.51 and 0.001 using spatialbench-cli
/// A scale factor of 0.51 is used because a sf of 0.5 and below will yield 0 results in the Building table
#[test]
fn test_spatialbench_cli_tbl_scale_factor_v1() {
    // Create a temporary directory
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // Generate driver, vehicle, customer, building with scale factor 0.51
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.51")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--tables")
        .arg("driver,vehicle,customer,building")
        .assert()
        .success();

    // Generate trip with scale factor 0.01
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--tables")
        .arg("trip")
        .assert()
        .success();

    // List of expected files
    let expected_files = vec![
        "trip.tbl",
        "customer.tbl",
        "driver.tbl",
        "vehicle.tbl",
        "building.tbl",
    ];

    // Verify that all expected files are created
    for file in &expected_files {
        let generated_file = temp_dir.path().join(file);
        assert!(
            generated_file.exists(),
            "File {:?} does not exist",
            generated_file
        );
        let generated_contents = fs::read(generated_file).expect("Failed to read generated file");
        let generated_contents = String::from_utf8(generated_contents)
            .expect("Failed to convert generated contents to string");

        // load the reference file
        let reference_file = format!("../spatialbench/data/sf-v1/{}.gz", file);
        let reference_contents = match read_gzipped_file_to_string(&reference_file) {
            Ok(contents) => contents,
            Err(e) => {
                panic!("Failed to read reference file {reference_file}: {e}");
            }
        };

        assert_eq!(
            generated_contents, reference_contents,
            "Contents of {:?} do not match reference",
            file
        );
    }
}

/// Test generating the trip table using --parts and --part options
#[test]
fn test_spatialbench_cli_parts() {
    // Create a temporary directory
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // generate 4 parts of the trip table with scale factor 0.001
    // into directories /part1, /part2, /part3, /part4
    // use threads to run the command concurrently to minimize the time taken
    let num_parts = 4;
    let mut threads = vec![];
    for part in 1..=num_parts {
        let part_dir = temp_dir.path().join(format!("part{part}"));
        threads.push(std::thread::spawn(move || {
            fs::create_dir(&part_dir).expect("Failed to create part directory");

            // Run the spatialbench-cli command for each part
            Command::cargo_bin("spatialbench-cli")
                .expect("Binary not found")
                .arg("--scale-factor")
                .arg("0.001")
                .arg("--output-dir")
                .arg(&part_dir)
                .arg("--parts")
                .arg(num_parts.to_string())
                .arg("--part")
                .arg(part.to_string())
                .arg("--tables")
                .arg("trip")
                .assert()
                .success();
        }));
    }
    // Wait for all threads to finish
    for thread in threads {
        thread.join().expect("Thread panicked");
    }
    // Read the generated files into a single buffer and compare them
    // to the contents of the reference file
    let mut output_contents = Vec::new();
    for part in 1..=4 {
        let generated_file = temp_dir.path().join(format!("part{part}")).join("trip.tbl");
        assert!(
            generated_file.exists(),
            "File {:?} does not exist",
            generated_file
        );
        let generated_contents =
            fs::read_to_string(generated_file).expect("Failed to read generated file");
        output_contents.append(&mut generated_contents.into_bytes());
    }
    let output_contents =
        String::from_utf8(output_contents).expect("Failed to convert output contents to string");

    // load the reference file
    let reference_file = read_reference_file("trip", "v1");
    assert_eq!(output_contents, reference_file);
}

#[tokio::test]
async fn test_write_parquet_trips() {
    // Run the CLI command to generate parquet data
    let output_dir = tempdir().unwrap();
    let output_path = output_dir.path().join("trip.parquet");
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("parquet")
        .arg("--tables")
        .arg("trip")
        .arg("--scale-factor")
        .arg("0.1")
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    let batch_size = 4000;

    // Create the reference Arrow data using TripArrow
    let generator = TripGenerator::new(0.1, 1, 1);
    let mut arrow_generator = TripArrow::new(generator).with_batch_size(batch_size);

    // Read the generated parquet file
    let file = File::open(&output_path).expect("Failed to open parquet file");
    let options = ArrowReaderOptions::new().with_schema(Arc::clone(arrow_generator.schema()));

    let reader = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)
        .expect("Failed to create ParquetRecordBatchReaderBuilder")
        .with_batch_size(batch_size)
        .build()
        .expect("Failed to build ParquetRecordBatchReader");

    // Compare the record batches
    for batch in reader {
        let parquet_batch = batch.expect("Failed to read record batch from parquet");
        let arrow_batch = arrow_generator
            .next()
            .expect("Failed to generate record batch from TripArrow");
        assert_eq!(
            parquet_batch, arrow_batch,
            "Mismatch between parquet and arrow record batches"
        );
    }
}

#[tokio::test]
async fn test_write_parquet_row_group_size_default() {
    // Run the CLI command to generate parquet data with default settings
    let output_dir = tempdir().unwrap();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("parquet")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("trip,driver,vehicle,customer,building")
        .arg("--output-dir")
        .arg(output_dir.path())
        .assert()
        .success();

    expect_row_group_sizes(
        output_dir.path(),
        vec![
            RowGroups {
                table: "customer",
                row_group_bytes: vec![2600113],
            },
            RowGroups {
                table: "trip",
                row_group_bytes: vec![123519959, 123486809, 123476361, 123492237],
            },
            RowGroups {
                table: "driver",
                row_group_bytes: vec![41594],
            },
            RowGroups {
                table: "vehicle",
                row_group_bytes: vec![5393],
            },
            RowGroups {
                table: "building",
                row_group_bytes: vec![2492865],
            },
        ],
    );
}

#[tokio::test]
async fn test_write_parquet_row_group_size_20mb() {
    // Run the CLI command to generate parquet data with larger row group size
    let output_dir = tempdir().unwrap();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("parquet")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("trip,driver,vehicle,customer,building")
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--parquet-row-group-bytes")
        .arg("20000000") // 20 MB
        .assert()
        .success();

    expect_row_group_sizes(
        output_dir.path(),
        vec![
            RowGroups {
                table: "customer",
                row_group_bytes: vec![2600113],
            },
            RowGroups {
                table: "trip",
                row_group_bytes: vec![
                    24361422, 24361685, 24350928, 24348682, 24353605, 24335813, 24358941, 24343011,
                    24345967, 24361312, 24337627, 24345972, 24348724, 24361400, 24361528, 24346264,
                    24351137, 24338412, 24348304, 24361680, 24351433,
                ],
            },
            RowGroups {
                table: "driver",
                row_group_bytes: vec![41594],
            },
            RowGroups {
                table: "vehicle",
                row_group_bytes: vec![5393],
            },
            RowGroups {
                table: "building",
                row_group_bytes: vec![2492865],
            },
        ],
    );
}

#[test]
fn test_spatialbench_cli_part_no_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // CLI Error test --part but not --parts
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("42")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "The --part option requires the --parts option to be set",
        ));
}

#[test]
fn test_spatialbench_cli_parts_no_part() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // CLI Error test --parts and but not --part
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--parts")
        .arg("42")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "The --part_count option requires the --part option to be set",
        ));
}

#[test]
fn test_spatialbench_cli_too_many_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // This should fail because --part is 42 which is more than the --parts 10
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("42")
        .arg("--parts")
        .arg("10")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Invalid --part. Expected at most the value of --parts (10), got 42",
        ));
}

#[test]
fn test_spatialbench_cli_zero_part() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("0")
        .arg("--parts")
        .arg("10")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Invalid --part. Expected a number greater than zero, got 0",
        ));
}
#[test]
fn test_spatialbench_cli_zero_part_zero_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .arg("--part")
        .arg("0")
        .arg("--parts")
        .arg("0")
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Invalid --part. Expected a number greater than zero, got 0",
        ));
}

/// Test specifying parquet options even when writing tbl output
#[tokio::test]
async fn test_incompatible_options_warnings() {
    let output_dir = tempdir().unwrap();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("csv")
        .arg("--tables")
        .arg("trip")
        .arg("--scale-factor")
        .arg("0.0001")
        .arg("--output-dir")
        .arg(output_dir.path())
        // pass in parquet options that are incompatible with csv
        .arg("--parquet-compression")
        .arg("zstd(1)")
        .arg("--parquet-row-group-bytes")
        .arg("8192")
        .assert()
        // still success, but should see warnings
        .success()
        .stderr(predicates::str::contains(
            "Warning: Parquet compression option set but not generating Parquet files",
        ))
        .stderr(predicates::str::contains(
            "Warning: Parquet row group size option set but not generating Parquet files",
        ));
}

fn read_gzipped_file_to_string<P: AsRef<Path>>(path: P) -> Result<String, std::io::Error> {
    let file = File::open(path)?;
    let mut decoder = flate2::read::GzDecoder::new(file);
    let mut contents = Vec::new();
    decoder.read_to_end(&mut contents)?;
    let contents = String::from_utf8(contents)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(contents)
}

/// Reads the reference file for the specified table and scale factor.
///
/// example usage: `read_reference_file("trip", "0.1")`
fn read_reference_file(table_name: &str, scale_factor: &str) -> String {
    let reference_file = format!("../spatialbench/data/sf-{scale_factor}/{table_name}.tbl.gz");
    match read_gzipped_file_to_string(&reference_file) {
        Ok(contents) => contents,
        Err(e) => {
            panic!("Failed to read reference file {reference_file}: {e}");
        }
    }
}

#[derive(Debug, PartialEq)]
struct RowGroups {
    table: &'static str,
    /// total bytes in each row group
    row_group_bytes: Vec<i64>,
}

/// For each table in tables, check that the parquet file in output_dir has
/// a file with the expected row group sizes.
fn expect_row_group_sizes(output_dir: &Path, expected_row_groups: Vec<RowGroups>) {
    let mut actual_row_groups = vec![];
    for table in &expected_row_groups {
        let output_path = output_dir.join(format!("{}.parquet", table.table));
        assert!(
            output_path.exists(),
            "Expected parquet file {:?} to exist",
            output_path
        );
        // read the metadata to get the row group size
        let file = File::open(&output_path).expect("Failed to open parquet file");
        let mut metadata_reader = ParquetMetaDataReader::new();
        metadata_reader.try_parse(&file).unwrap();
        let metadata = metadata_reader.finish().unwrap();
        let row_groups = metadata.row_groups();
        let actual_row_group_bytes: Vec<_> =
            row_groups.iter().map(|rg| rg.total_byte_size()).collect();
        actual_row_groups.push(RowGroups {
            table: table.table,
            row_group_bytes: actual_row_group_bytes,
        })
    }
    // compare the expected and actual row groups debug print actual on failure
    // for better output / easier comparison
    let expected_row_groups = format!("{expected_row_groups:#?}");
    let actual_row_groups = format!("{actual_row_groups:#?}");
    assert_eq!(actual_row_groups, expected_row_groups);
}

use arrow_array::RecordBatch;
use assert_cmd::Command;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::file::metadata::ParquetMetaDataReader;
use spatialbench::generators::TripGenerator;
use spatialbench_arrow::{RecordBatchIterator, TripArrow};
use std::fs;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
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
        .arg("--format")
        .arg("tbl")
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
        .arg("--format")
        .arg("tbl")
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

/// Test that when creating output, if the file already exists it is not overwritten
#[test]
fn test_spatialbench_cli_tbl_no_overwrite() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let expected_file = temp_dir.path().join("trip.tbl");

    let run_command = || {
        Command::cargo_bin("spatialbench-cli")
            .expect("Binary not found")
            .arg("--scale-factor")
            .arg("0.001")
            .arg("--format")
            .arg("tbl")
            .arg("--tables")
            .arg("trip")
            .arg("--output-dir")
            .arg(temp_dir.path())
            .assert()
            .success()
    };

    run_command();
    let original_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), 826311);

    // Run the spatialbench-cli command again with the same parameters and expect the
    // file to not be overwritten
    run_command();
    let new_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), new_metadata.len());
    assert_eq!(
        original_metadata
            .modified()
            .expect("Failed to get modified time"),
        new_metadata
            .modified()
            .expect("Failed to get modified time")
    );
}

#[tokio::test]
async fn test_zone_parquet_no_overwrite() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let expected_file = temp_dir.path().join("zone/zone.1.parquet");

    let run_command = || {
        Command::cargo_bin("spatialbench-cli")
            .expect("Binary not found")
            .arg("--scale-factor")
            .arg("1")
            .arg("--tables")
            .arg("zone")
            .arg("--parts")
            .arg("100")
            .arg("--part")
            .arg("1")
            .arg("--output-dir")
            .arg(temp_dir.path())
            .assert()
            .success()
    };

    run_command();
    let original_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), 25400203);

    // Run the spatialbench-cli command again with the same parameters and expect the
    // file to not be overwritten
    run_command();

    let new_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), new_metadata.len());
    assert_eq!(
        original_metadata
            .modified()
            .expect("Failed to get modified time"),
        new_metadata
            .modified()
            .expect("Failed to get modified time")
    );
}

// Test that when creating output, if the file already exists it is not for parquet
#[test]
fn test_spatialbench_cli_parquet_no_overwrite() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");
    let expected_file = temp_dir.path().join("building.parquet");

    let run_command = || {
        Command::cargo_bin("spatialbench-cli")
            .expect("Binary not found")
            .arg("--scale-factor")
            .arg("0.001")
            .arg("--tables")
            .arg("building")
            .arg("--output-dir")
            .arg(temp_dir.path())
            .assert()
            .success()
    };

    run_command();
    let original_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), 412);

    // Run the spatialbench-cli command again with the same parameters and expect the
    // file to not be overwritten
    run_command();

    let new_metadata =
        fs::metadata(&expected_file).expect("Failed to get metadata of generated file");
    assert_eq!(original_metadata.len(), new_metadata.len());
    assert_eq!(
        original_metadata
            .modified()
            .expect("Failed to get modified time"),
        new_metadata
            .modified()
            .expect("Failed to get modified time")
    );
}

/// Test zone parquet output determinism - same data should be generated every time
#[tokio::test]
async fn test_zone_deterministic_parts_generation() {
    let temp_dir1 = tempdir().expect("Failed to create temporary directory 1");

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("parquet")
        .arg("--scale-factor")
        .arg("1.0")
        .arg("--output-dir")
        .arg(temp_dir1.path())
        .arg("--tables")
        .arg("zone")
        .arg("--parts")
        .arg("100")
        .arg("--part")
        .arg("1")
        .assert()
        .success();

    let zone_file1 = temp_dir1.path().join("zone/zone.1.parquet");

    // Reference file is a sf=0.01 zone table with z_boundary column removed
    let reference_file = PathBuf::from("../spatialbench/data/sf-v1/zone.parquet");

    assert!(
        zone_file1.exists(),
        "First zone.parquet file was not created"
    );
    assert!(
        reference_file.exists(),
        "Reference zone.parquet file does not exist"
    );

    let file1 = File::open(&zone_file1).expect("Failed to open generated zone.parquet file");
    let file2 = File::open(&reference_file).expect("Failed to open reference zone.parquet file");

    let reader1 = ParquetRecordBatchReaderBuilder::try_new(file1)
        .expect("Failed to create reader for generated file")
        .build()
        .expect("Failed to build reader for generated file");

    let reader2 = ParquetRecordBatchReaderBuilder::try_new(file2)
        .expect("Failed to create reader for reference file")
        .build()
        .expect("Failed to build reader for reference file");

    let batches1: Result<Vec<RecordBatch>, _> = reader1.collect();
    let batches2: Result<Vec<RecordBatch>, _> = reader2.collect();

    let batches1 = batches1.expect("Failed to read batches from generated file");
    let batches2 = batches2.expect("Failed to read batches from reference file");

    // Check that files are non-empty
    assert!(
        !batches1.is_empty(),
        "Generated zone parquet file has no data"
    );
    assert!(
        !batches2.is_empty(),
        "Reference zone parquet file has no data"
    );

    // Check that both files have the same number of batches
    assert_eq!(
        batches1.len(),
        batches2.len(),
        "Different number of record batches"
    );

    // Compare each batch, excluding z_boundary column
    for (i, (batch1, batch2)) in batches1.iter().zip(batches2.iter()).enumerate() {
        assert_eq!(
            batch1.num_rows(),
            batch2.num_rows(),
            "Batch {} has different number of rows",
            i
        );

        let schema1 = batch1.schema();

        // Compare all columns except z_boundary
        for field in schema1.fields() {
            let column_name = field.name();
            if column_name == "z_boundary" {
                continue;
            }

            let col1 = batch1
                .column_by_name(column_name)
                .unwrap_or_else(|| panic!("Column {} not found in generated file", column_name));
            let col2 = batch2
                .column_by_name(column_name)
                .unwrap_or_else(|| panic!("Column {} not found in reference file", column_name));

            assert_eq!(
                col1, col2,
                "Column {} differs between generated and reference files in batch {}",
                column_name, i
            );
        }
    }
}

/// Test generating the trip table using 4 parts implicitly
#[test]
fn test_spatialbench_cli_parts() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // generate 4 parts of the trip table with scale factor 0.001 and let
    // spatialbench-cli generate the multiple files

    let num_parts = 4;
    let output_dir = temp_dir.path().to_path_buf();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--format")
        .arg("tbl")
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--parts")
        .arg(num_parts.to_string())
        .arg("--tables")
        .arg("trip")
        .assert()
        .success();

    verify_table(temp_dir.path(), "trip", num_parts, "v1");
}

/// Test generating the order table with multiple invocations using --parts and
/// --part options
#[test]
fn test_spatialbench_cli_parts_explicit() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    // generate 4 parts of the orders table with scale factor 0.001
    // use threads to run the command concurrently to minimize the time taken
    let num_parts = 4;
    let mut threads = vec![];
    for part in 1..=num_parts {
        let output_dir = temp_dir.path().to_path_buf();
        threads.push(std::thread::spawn(move || {
            // Run the spatialbench-cli command for each part
            // output goes into `output_dir/orders/orders.{part}.tbl`
            Command::cargo_bin("spatialbench-cli")
                .expect("Binary not found")
                .arg("--scale-factor")
                .arg("0.001")
                .arg("--format")
                .arg("tbl")
                .arg("--output-dir")
                .arg(&output_dir)
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
    verify_table(temp_dir.path(), "trip", num_parts, "v1");
}

/// Create all tables using --parts option and verify the output layouts
#[test]
fn test_spatialbench_cli_parts_all_tables() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    let num_parts = 8;
    let output_dir = temp_dir.path().to_path_buf();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.51")
        .arg("--format")
        .arg("tbl")
        .arg("--tables")
        .arg("building,driver,vehicle,customer")
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--parts")
        .arg(num_parts.to_string())
        .assert()
        .success();

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--scale-factor")
        .arg("0.001")
        .arg("--format")
        .arg("tbl")
        .arg("--tables")
        .arg("trip")
        .arg("--output-dir")
        .arg(&output_dir)
        .arg("--parts")
        .arg(num_parts.to_string())
        .assert()
        .success();

    verify_table(temp_dir.path(), "trip", num_parts, "v1");
    verify_table(temp_dir.path(), "customer", num_parts, "v1");
    // Note, building, vehicle and driver have only a single part regardless of --parts
    verify_table(temp_dir.path(), "building", 1, "v1");
    verify_table(temp_dir.path(), "vehicle", 1, "v1");
    verify_table(temp_dir.path(), "driver", 1, "v1");
}

/// Read the N files from `output_dir/table_name/table_name.part.tbl` into a
/// single buffer and compare them to the contents of the reference file
fn verify_table(output_dir: &Path, table_name: &str, parts: usize, scale_factor: &str) {
    let mut output_contents = Vec::new();
    for part in 1..=parts {
        let generated_file = output_dir
            .join(table_name)
            .join(format!("{table_name}.{part}.tbl"));
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
    let reference_file = read_reference_file(table_name, scale_factor);
    assert_eq!(output_contents, reference_file);
}

#[tokio::test]
async fn test_write_parquet_trips() {
    // Run the CLI command to generate parquet data
    let output_dir = tempdir().unwrap();
    let output_path = output_dir.path().join("trip.parquet");
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
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
                row_group_bytes: vec![2599669],
            },
            RowGroups {
                table: "trip",
                row_group_bytes: vec![123493205, 123460055, 123449607, 123465483],
            },
            RowGroups {
                table: "driver",
                row_group_bytes: vec![41361],
            },
            RowGroups {
                table: "vehicle",
                row_group_bytes: vec![5214],
            },
            RowGroups {
                table: "building",
                row_group_bytes: vec![2492359],
            },
        ],
    );
}

#[tokio::test]
async fn test_zone_write_parquet_row_group_size_default() {
    // Run the CLI command to generate parquet data with default settings
    let output_dir = tempdir().unwrap();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("parquet")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("zone")
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--parts")
        .arg("10")
        .arg("--part")
        .arg("1")
        .assert()
        .success();

    expect_row_group_sizes(
        output_dir.path(),
        vec![RowGroups {
            table: "zone/zone.1",
            row_group_bytes: vec![86288517],
        }],
    );
}

#[tokio::test]
async fn test_write_parquet_row_group_size_20mb() {
    // Run the CLI command to generate parquet data with larger row group size
    let output_dir = tempdir().unwrap();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
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
                row_group_bytes: vec![2599669],
            },
            RowGroups {
                table: "trip",
                row_group_bytes: vec![
                    24356144, 24356407, 24345650, 24343404, 24348327, 24330535, 24353663, 24337733,
                    24340689, 24356034, 24332349, 24340694, 24343446, 24356122, 24356250, 24340986,
                    24345859, 24333134, 24343026, 24356402, 24346155,
                ],
            },
            RowGroups {
                table: "driver",
                row_group_bytes: vec![41361],
            },
            RowGroups {
                table: "vehicle",
                row_group_bytes: vec![5214],
            },
            RowGroups {
                table: "building",
                row_group_bytes: vec![2492359],
            },
        ],
    );
}

#[tokio::test]
async fn test_zone_write_parquet_row_group_size_20mb() {
    // Run the CLI command to generate parquet data with larger row group size
    let output_dir = tempdir().unwrap();
    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("parquet")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("zone")
        .arg("--output-dir")
        .arg(output_dir.path())
        .arg("--parquet-row-group-bytes")
        .arg("20000000") // 20 MB
        .arg("--parts")
        .arg("10")
        .arg("--part")
        .arg("1")
        .assert()
        .success();

    expect_row_group_sizes(
        output_dir.path(),
        vec![RowGroups {
            table: "zone/zone.1",
            row_group_bytes: vec![15428592, 17250042, 19338201, 17046885, 17251978],
        }],
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

#[test]
fn test_zone_generation_tbl_fails() {
    let temp_dir = tempdir().expect("Failed to create temporary directory");

    Command::cargo_bin("spatialbench-cli")
        .expect("Binary not found")
        .arg("--format")
        .arg("tbl")
        .arg("--scale-factor")
        .arg("1")
        .arg("--tables")
        .arg("zone")
        .arg("--output-dir")
        .arg(temp_dir.path())
        .assert()
        .failure()
        .stderr(predicates::str::contains(
            "Zone table is only supported in --format=parquet",
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

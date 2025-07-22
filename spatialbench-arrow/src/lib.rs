//! Generate TPCH data as Arrow RecordBatches
//!
//! This crate provides generators for TPCH tables that directly produces
//! Arrow [`RecordBatch`]es. This is significantly faster than generating TBL or CSV
//! files and then parsing them into Arrow.
//!
//! # Example
//! ```
//! # use spatialbench::generators::TripGenerator;
//! # use spatialbench_arrow::TripArrow;
//! # use arrow::util::pretty::pretty_format_batches;
//! // Create a SF=1 generator for the LineItem table
//! let generator = TripGenerator::new(1.0, 1, 1);
//! let mut arrow_generator = TripArrow::new(generator)
//!   .with_batch_size(10);
//! // The generator is a Rust iterator, producing RecordBatch
//! let batch = arrow_generator.next().unwrap();
//! // compare the output by pretty printing it
//! let formatted_batches = pretty_format_batches(&[batch]).unwrap().to_string();
//! assert_eq!(formatted_batches.lines().collect::<Vec<_>>(), vec![
//!   "+-----------+-----------+-------------+--------------+--------------+---------------+---------+---------+---------------+------------+--------------------------------------------+--------------------------------------------+",
//!   "| t_tripkey | t_custkey | t_driverkey | t_vehiclekey | t_pickuptime | t_dropofftime | t_fare  | t_tip   | t_totalamount | t_distance | t_pickuploc                                | t_dropoffloc                               |",
//!   "+-----------+-----------+-------------+--------------+--------------+---------------+---------+---------+---------------+------------+--------------------------------------------+--------------------------------------------+",
//!   "| 1         | 21425     | 47          | 46           | 1997-07-24   | 1997-07-24    | 0.00034 | 0.00002 | 0.00037       | 0.00014    | 010100000000000000009f65c000000000008056c0 | 01010000003c13323f719f65c0c62bcff1f28856c0 |",
//!   "| 2         | 17012     | 66          | 65           | 1997-12-24   | 1997-12-24    | 0.00003 | 0.00000 | 0.00004       | 0.00001    | 010100000000000000800165c000000000001835c0 | 0101000000ed03047c0f0165c00ee6b7aa721735c0 |",
//!   "| 3         | 4454      | 68          | 67           | 1993-06-27   | 1993-06-27    | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000000000000007265c000000000809953c0 | 0101000000336b00b00e7265c02f695d509e9953c0 |",
//!   "| 4         | 3875      | 82          | 81           | 1996-08-02   | 1996-08-02    | 0.00005 | 0.00000 | 0.00005       | 0.00002    | 010100000000000000800f56c00000000000c63bc0 | 01010000004a916d7e111056c0621ccaa6fdcb3bc0 |",
//!   "| 5         | 23027     | 9           | 8            | 1996-08-23   | 1996-08-23    | 0.00002 | 0.00000 | 0.00003       | 0.00001    | 010100000000000000406460c00000000000da4640 | 0101000000acb0a6a1ae6460c0e1a5886c17db4640 |",
//!   "| 6         | 4573      | 41          | 40           | 1994-11-16   | 1994-11-16    | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 010100000000000000002666c000000000806f40c0 | 01010000006100a6303e2666c09f84c465c06d40c0 |",
//!   "| 7         | 28319     | 60          | 59           | 1996-01-20   | 1996-01-20    | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000000000000002963c00000000000e040c0 | 010100000000000000002963c00000000000e040c0 |",
//!   "| 8         | 23288     | 32          | 31           | 1995-01-09   | 1995-01-10    | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 010100000000000000008056c000000000c03955c0 | 01010000007fcc20a00d8156c0daf2ab4bd83955c0 |",
//!   "| 9         | 17744     | 100         | 99           | 1993-10-13   | 1993-10-13    | 0.00005 | 0.00001 | 0.00007       | 0.00003    | 010100000000000000005366c00000000000e050c0 | 010100000065523404e75266c045ff5c8406e250c0 |",
//!   "| 10        | 11800     | 98          | 97           | 1994-11-08   | 1994-11-08    | 0.00001 | 0.00000 | 0.00001       | 0.00000    | 010100000000000000008066c000000000c07456c0 | 01010000001ded0e6fe27f66c001744f41837456c0 |",
//!   "+-----------+-----------+-------------+--------------+--------------+---------------+---------+---------+---------------+------------+--------------------------------------------+--------------------------------------------+"
//! ]);
//! ```

mod building;
pub mod conversions;
mod customer;
mod driver;
mod trip;
mod vehicle;
mod zone;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
pub use building::BuildingArrow;
pub use customer::CustomerArrow;
pub use driver::DriverArrow;
pub use trip::TripArrow;
pub use vehicle::VehicleArrow;
pub use zone::ZoneArrow;

/// Iterator of Arrow [`RecordBatch`] that also knows its schema
pub trait RecordBatchIterator: Iterator<Item = RecordBatch> + Send {
    fn schema(&self) -> &SchemaRef;
}

/// The default number of rows in each Batch
pub const DEFAULT_BATCH_SIZE: usize = 8 * 1000;

//! Generate Spatial Bench data as Arrow RecordBatches
//!
//! This crate provides generators for Spatial Bench tables that directly produces
//! Arrow [`RecordBatch`]es. This is significantly faster than generating TBL or CSV
//! files and then parsing them into Arrow.
//!
//! # Example
//! ```
//! # use spatialbench::generators::TripGenerator;
//! # use spatialbench_arrow::TripArrow;
//! # use arrow::util::pretty::pretty_format_batches;
//! // Create a SF=0.01 generator for the LineItem table
//! let generator = TripGenerator::new(0.01, 1, 1);
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
//!   "| 1         | 215       | 1           | 1            | 1997-07-24   | 1997-07-24    | 0.00034 | 0.00002 | 0.00037       | 0.00014    | 010100000000000000009f65c000000000008056c0 | 0101000000ea6f323f719f65c0a190cff1f28856c0 |",
//!   "| 2         | 172       | 1           | 1            | 1997-12-24   | 1997-12-24    | 0.00003 | 0.00000 | 0.00004       | 0.00001    | 010100000000000000800165c000000000001835c0 | 01010000007707047c0f0165c0e360c2aa721735c0 |",
//!   "| 3         | 46        | 1           | 1            | 1993-06-27   | 1993-06-27    | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000000000000007265c000000000809953c0 | 0101000000123a01b00e7265c0fc9862509e9953c0 |",
//!   "| 4         | 40        | 1           | 1            | 1996-08-02   | 1996-08-02    | 0.00005 | 0.00000 | 0.00005       | 0.00002    | 010100000000000000800f56c00000000000c63bc0 | 01010000005c186d7e111056c0435fb4a6fdcb3bc0 |",
//!   "| 5         | 232       | 1           | 1            | 1996-08-23   | 1996-08-23    | 0.00002 | 0.00000 | 0.00003       | 0.00001    | 010100000000000000406460c00000000000da4640 | 01010000003da9a3a1ae6460c00036836c17db4640 |",
//!   "| 6         | 46        | 1           | 1            | 1994-11-16   | 1994-11-16    | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 010100000000000000002666c000000000806f40c0 | 01010000009fbda7303e2666c0cdb6cb65c06d40c0 |",
//!   "| 7         | 284       | 1           | 1            | 1996-01-20   | 1996-01-20    | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000000000000002963c00000000000e040c0 | 010100000000000000002963c00000000000e040c0 |",
//!   "| 8         | 233       | 1           | 1            | 1995-01-09   | 1995-01-10    | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 010100000000000000008056c000000000c03955c0 | 0101000000c0e91ba00d8156c06e03b14bd83955c0 |",
//!   "| 9         | 178       | 1           | 1            | 1993-10-13   | 1993-10-13    | 0.00005 | 0.00001 | 0.00007       | 0.00003    | 010100000000000000005366c00000000000e050c0 | 0101000000a6ef3504e75266c0448c538406e250c0 |",
//!   "| 10        | 118       | 1           | 1            | 1994-11-08   | 1994-11-08    | 0.00001 | 0.00000 | 0.00001       | 0.00000    | 010100000000000000008066c000000000c07456c0 | 01010000001459106fe27f66c08d065341837456c0 |",
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

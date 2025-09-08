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
//!   "+-----------+-----------+-------------+--------------+---------------------+---------------------+---------+---------+---------------+------------+--------------------------------------------+--------------------------------------------+",
//!   "| t_tripkey | t_custkey | t_driverkey | t_vehiclekey | t_pickuptime        | t_dropofftime       | t_fare  | t_tip   | t_totalamount | t_distance | t_pickuploc                                | t_dropoffloc                               |",
//!   "+-----------+-----------+-------------+--------------+---------------------+---------------------+---------+---------+---------------+------------+--------------------------------------------+--------------------------------------------+",
//!   "| 1         | 215       | 1           | 1            | 1997-07-24T06:58:22 | 1997-07-24T13:59:54 | 0.00034 | 0.00002 | 0.00037       | 0.00014    | 010100000018fc05d4fe7933c0e693f96da66b2a40 | 0101000000677b99cd887d33c0cfe06bdf0e242a40 |",
//!   "| 2         | 172       | 1           | 1            | 1997-12-24T08:47:14 | 1997-12-24T09:28:57 | 0.00003 | 0.00000 | 0.00004       | 0.00001    | 01010000005a2f6fd6ecb35b40bfbd2e6d3f384540 | 010100000073b76fdecdb45b409895dc1786384540 |",
//!   "| 3         | 46        | 1           | 1            | 1993-06-27T13:27:07 | 1993-06-27T13:34:51 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000039c3814e70f2534094e0003f52441540 | 0101000000b2f079ee52f2534016653e396d421540 |",
//!   "| 4         | 40        | 1           | 1            | 1996-08-02T04:14:27 | 1996-08-02T05:29:32 | 0.00005 | 0.00000 | 0.00005       | 0.00002    | 01010000008aa8333e66db54c0fffa582953ec3ac0 | 0101000000a24f9abcf7db54c0541f27d050f23ac0 |",
//!   "| 5         | 232       | 1           | 1            | 1996-08-23T12:48:20 | 1996-08-23T13:36:15 | 0.00002 | 0.00000 | 0.00003       | 0.00001    | 01010000008415c0f7eb1a54c0b75354ac03a8d8bf | 0101000000fe67073bc91b54c0449293684d1cd8bf |",
//!   "| 6         | 46        | 1           | 1            | 1994-11-16T16:39:14 | 1994-11-16T17:26:07 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 0101000000ce6b7abd2cf54040aa3efca13e7a05c0 | 0101000000924fd9fa33f4404006df0bfe445e05c0 |",
//!   "| 7         | 284       | 1           | 1            | 1996-01-20T06:18:56 | 1996-01-20T06:18:56 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000063714ed8bd1c524047a536433ace4940 | 0101000000e6bc52d8bd1c5240c45932433ace4940 |",
//!   "| 8         | 233       | 1           | 1            | 1995-01-09T23:26:54 | 1995-01-10T00:16:28 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 01010000003bb9cfbfd7bc50407b16af721b8f4940 | 01010000007bcfb31fcabb5040a6a655dbea8e4940 |",
//!   "| 9         | 178       | 1           | 1            | 1993-10-13T11:07:04 | 1993-10-13T12:42:27 | 0.00005 | 0.00001 | 0.00007       | 0.00003    | 01010000009b898e89b99952c0a488490e51cc46c0 | 0101000000e768fa91879952c07112f7165ed046c0 |",
//!   "| 10        | 118       | 1           | 1            | 1994-11-08T21:05:58 | 1994-11-08T21:21:29 | 0.00001 | 0.00000 | 0.00001       | 0.00000    | 0101000000e2c7173be1a45340311fd18349d14940 | 01010000009b28f85c1ca553405b833101c3d14940 |",
//!   "+-----------+-----------+-------------+--------------+---------------------+---------------------+---------+---------+---------------+------------+--------------------------------------------+--------------------------------------------+"
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

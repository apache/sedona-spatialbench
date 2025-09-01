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
//!   "| 1         | 215       | 1           | 1            | 1997-07-24T06:58:22 | 1997-07-24T13:59:54 | 0.00034 | 0.00002 | 0.00037       | 0.00014    | 01010000006c5ace4aff9e65c0e479ba04f19b4d40 | 010100000055ca008a709f65c0a1581b210b8a4d40 |",
//!   "| 2         | 172       | 1           | 1            | 1997-12-24T08:47:14 | 1997-12-24T09:28:57 | 0.00003 | 0.00000 | 0.00004       | 0.00001    | 0101000000e711ed7431fd64c02fab0bd352644140 | 01010000005d19f1f0c0fc64c0bd7aaa7d99644140 |",
//!   "| 3         | 46        | 1           | 1            | 1993-06-27T13:27:07 | 1993-06-27T13:34:51 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 0101000000a620e461467165c033cd2a2279fd4340 | 0101000000b85ae511557165c03a9b65813cfd4340 |",
//!   "| 4         | 40        | 1           | 1            | 1996-08-02T04:14:27 | 1996-08-02T05:29:32 | 0.00005 | 0.00000 | 0.00005       | 0.00002    | 010100000060a28b97b80756c095bfd60000fb4d40 | 0101000000bcbaf8154a0856c0f38f7c2d01f84d40 |",
//!   "| 5         | 232       | 1           | 1            | 1996-08-23T12:48:20 | 1996-08-23T13:36:15 | 0.00002 | 0.00000 | 0.00003       | 0.00001    | 010100000096c4fe57c25b60c00080d1c19f8664bf | 0101000000d46da2f9305c60c0031ad78540aa783f |",
//!   "| 6         | 46        | 1           | 1            | 1994-11-16T16:39:14 | 1994-11-16T17:26:07 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 0101000000c356bf886c2266c000fa5635520004c0 | 0101000000611467b9aa2266c0b566129258e403c0 |",
//!   "| 7         | 284       | 1           | 1            | 1996-01-20T06:18:56 | 1996-01-20T06:18:56 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000097a0d0fc7b2563c074fb9b06fbf54340 | 010100000097a0d0fc7b2563c074fb9b06fbf54340 |",
//!   "| 8         | 233       | 1           | 1            | 1995-01-09T23:26:54 | 1995-01-10T00:16:28 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 01010000002c7986ba597f56c0a27a6b60ab544340 | 0101000000ec62a25a678056c0c77309c97a544340 |",
//!   "| 9         | 178       | 1           | 1            | 1993-10-13T11:07:04 | 1993-10-13T12:42:27 | 0.00005 | 0.00001 | 0.00007       | 0.00003    | 0101000000b3295778975166c09078680effff4840 | 010100000059198d7c7e5166c00760c105f2fb4840 |",
//!   "| 10        | 118       | 1           | 1            | 1994-11-08T21:05:58 | 1994-11-08T21:21:29 | 0.00001 | 0.00000 | 0.00001       | 0.00000    | 01010000004900edfdfc7f66c0c58ec6a17eef5240 | 01010000005d59fd6cdf7f66c038887360bbef5240 |",
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

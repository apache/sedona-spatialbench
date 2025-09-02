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
//!   "| 1         | 215       | 1           | 1            | 1997-07-24T06:58:22 | 1997-07-24T13:59:54 | 0.00034 | 0.00002 | 0.00037       | 0.00014    | 01010000000c63c1b3f39e65c0e4086355ce984840 | 0101000000f6d2f3f2649f65c0a1e7c371e8864840 |",
//!   "| 2         | 172       | 1           | 1            | 1997-12-24T08:47:14 | 1997-12-24T09:28:57 | 0.00003 | 0.00000 | 0.00004       | 0.00001    | 01010000007ea1ecd22bfd64c0e885e05dd3282c40 | 0101000000f5a8f04ebbfc64c021c45b08ee292c40 |",
//!   "| 3         | 46        | 1           | 1            | 1993-06-27T13:27:07 | 1993-06-27T13:34:51 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 01010000007bbe66b96c865fc0b898b047f2e63d40 | 01010000009f3269198a865fc0c834260679e63d40 |",
//!   "| 4         | 40        | 1           | 1            | 1996-08-02T04:14:27 | 1996-08-02T05:29:32 | 0.00005 | 0.00000 | 0.00005       | 0.00002    | 01010000008e90813cbb0456c0987384679dec4d40 | 0101000000eba8eeba4c0556c0f6432a949ee94d40 |",
//!   "| 5         | 232       | 1           | 1            | 1996-08-23T12:48:20 | 1996-08-23T13:36:15 | 0.00002 | 0.00000 | 0.00003       | 0.00001    | 01010000005da8fc6b79e75dc0c8c5bd9e540049c0 | 0101000000d7fa43af56e85dc0c98f3a323dff48c0 |",
//!   "| 6         | 46        | 1           | 1            | 1994-11-16T16:39:14 | 1994-11-16T17:26:07 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 0101000000406716574b700740c8dbb694984c2ac0 | 01010000009eff262dbf600740ffb6e52b9a452ac0 |",
//!   "| 7         | 284       | 1           | 1            | 1996-01-20T06:18:56 | 1996-01-20T06:18:56 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 01010000002028b7ed7bbd61c090cde90d52eb3d40 | 01010000002028b7ed7bbd61c08fcde90d52eb3d40 |",
//!   "| 8         | 233       | 1           | 1            | 1995-01-09T23:26:54 | 1995-01-10T00:16:28 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 010100000095eeaeb321ab53c0a8da13c9fca83740 | 010100000056d8ca532fac53c0f2cc4f9a9ba83740 |",
//!   "| 9         | 178       | 1           | 1            | 1993-10-13T11:07:04 | 1993-10-13T12:42:27 | 0.00005 | 0.00001 | 0.00007       | 0.00003    | 0101000000d4be1479ed1756c000b14d2a1a6beb3f | 0101000000209e8081bb1756c0568f8700d867ea3f |",
//!   "| 10        | 118       | 1           | 1            | 1994-11-08T21:05:58 | 1994-11-08T21:21:29 | 0.00001 | 0.00000 | 0.00001       | 0.00000    | 0101000000b0251de5609e35c07455eaa39d544440 | 010100000047ee9f5d749d35c05948442117554440 |",
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

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
//!   "| 1         | 215       | 1           | 1            | 1997-07-24T06:58:22 | 1997-07-24T13:59:54 | 0.00034 | 0.00002 | 0.00037       | 0.00014    | 01010000009f3c318dd43735405930592bc6062040 | 0101000000d408a2934a34354083fa96395d7e1f40 |",
//!   "| 2         | 172       | 1           | 1            | 1997-12-24T08:47:14 | 1997-12-24T09:28:57 | 0.00003 | 0.00000 | 0.00004       | 0.00001    | 010100000066ea0ba7209b5740dc070cd122e33d40 | 01010000007f720caf019c57407cf24d26b0e33d40 |",
//!   "| 3         | 46        | 1           | 1            | 1993-06-27T13:27:07 | 1993-06-27T13:34:51 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000003cc2607066e5b40f26f32d2d4d0ff3f | 01010000009be61da7e86d5b407ac002b940c9ff3f |",
//!   "| 4         | 40        | 1           | 1            | 1996-08-02T04:14:27 | 1996-08-02T05:29:32 | 0.00005 | 0.00000 | 0.00005       | 0.00002    | 0101000000897921e03a0e4cc08816c7ebfbc745c0 | 0101000000c4f5ffdc5d0f4cc0eb6b23bffaca45c0 |",
//!   "| 5         | 232       | 1           | 1            | 1996-08-23T12:48:20 | 1996-08-23T13:36:15 | 0.00002 | 0.00000 | 0.00003       | 0.00001    | 0101000000ff3ea1a62fcb4dc014fc64fc630b2ac0 | 0101000000f3e32f2deacc4dc026e9714a06072ac0 |",
//!   "| 6         | 46        | 1           | 1            | 1994-11-16T16:39:14 | 1994-11-16T17:26:07 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 0101000000855c114bb6562440b2810ccef493d83f | 0101000000ac47af40d3522440915fa2eec173d93f |",
//!   "| 7         | 284       | 1           | 1            | 1996-01-20T06:18:56 | 1996-01-20T06:18:56 | 0.00000 | 0.00000 | 0.00000       | 0.00000    | 010100000088904b0024855a40ea87d1a6fc8b4340 | 01010000000bdc4f0024855a40f01edaa6fc8b4340 |",
//!   "| 8         | 233       | 1           | 1            | 1995-01-09T23:26:54 | 1995-01-10T00:16:28 | 0.00003 | 0.00000 | 0.00003       | 0.00001    | 010100000002f5d829e5845640f2770bfe6053f8bf | 01010000003b74b489d78356402a9b07ea7359f8bf |",
//!   "| 9         | 178       | 1           | 1            | 1993-10-13T11:07:04 | 1993-10-13T12:42:27 | 0.00005 | 0.00001 | 0.00007       | 0.00003    | 0101000000dd8b0712968e49c061d63d122c131640 | 0101000000ec67d222328e49c0d7fd9dccc3f21540 |",
//!   "| 10        | 118       | 1           | 1            | 1994-11-08T21:05:58 | 1994-11-08T21:21:29 | 0.00001 | 0.00000 | 0.00001       | 0.00000    | 0101000000df66d30c07e75940ff4f81705eca3c40 | 01010000003469ae2e42e75940c49e2c6b51cb3c40 |",
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

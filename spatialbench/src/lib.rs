//! Rust Spatial Bench Data Generator
//!
//! This crate provides a native Rust implementation of functions and utilities
//! necessary for generating the TPC-H benchmark dataset in several popular
//! formats.
//!
//! # Example: TBL output format
//! ```
//! # use spatialbench::generators::TripGenerator;
//! // Create Generator for the TRIP table at Scale Factor 0.01 (SF 0.01)
//! let scale_factor = 0.01;
//! let part = 1;
//! let num_parts = 1;
//! let generator = TripGenerator::new(scale_factor, part, num_parts);
//!
//! // Output the first 3 rows in classic Spatial Bench TBL format
//! // (the generators are normal rust iterators and combine well with the Rust ecosystem)
//! let trips: Vec<_> = generator.iter()
//!    .take(3)
//!    .map(|trips| trips.to_string()) // use Display impl to get TBL format
//!    .collect::<Vec<_>>();
//!  assert_eq!(
//!   trips.join("\n"),"\
//!     1|215|1|1|1997-07-24 06:58:22|1997-07-24 13:59:54|0.34|0.02|0.37|0.14|POINT(-172.9686636 59.2182928)|POINT(-172.98248768 59.07846464)|\n\
//!     2|172|1|1|1997-12-24 08:47:14|1997-12-24 09:28:57|0.03|0.00|0.04|0.01|POINT(-167.9122872 34.7837776)|POINT(-167.89855239 34.78593417)|\n\
//!     3|46|1|1|1993-06-27 13:27:07|1993-06-27 13:34:51|0.00|0.00|0.00|0.00|POINT(-171.5398416 39.9802592)|POINT(-171.54163451 39.97840898)|"
//!   );
//! ```
//!
//! The TPC-H dataset is composed of several tables with foreign key relations
//! between them. For each table we implement and expose a generator that uses
//! the iterator API to produce structs e.g [`Trip`] that represent a single
//! row.
//!
//! For each struct type we expose several facilities that allow fast conversion
//! to Tbl and Csv formats but can also be extended to support other output formats.
//!
//! This crate currently supports the following output formats:
//!
//! - TBL: The `Display` impl of the row structs produces the Spatial Bench TBL format.
//! - CSV: the [`csv`] module has formatters for CSV output (e.g. [`TripCsv`]).
//!
//! [`Trip`]: generators::Trip
//! [`TripCsv`]: csv::TripCsv
//!
//!
//! The library was designed to be easily integrated in existing Rust projects as
//! such it avoids exposing a malleable API and purposely does not have any dependencies
//! on other Rust crates. It is focused entire on the core
//! generation logic.
//!
//! If you want an easy way to generate the TPC-H dataset for usage with external
//! systems you can use CLI tool instead.
pub mod csv;
pub mod dates;
pub mod decimal;
pub mod distribution;
pub mod generators;
pub mod kde;
pub mod q_and_a;
pub mod random;
pub mod spider;
pub mod spider_defaults;
pub mod spider_overrides;
pub mod text;

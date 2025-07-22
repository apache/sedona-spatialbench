//! Rust TPCH Data Generator
//!
//! This crate provides a native Rust implementation of functions and utilities
//! necessary for generating the TPC-H benchmark dataset in several popular
//! formats.
//!
//! # Example: TBL output format
//! ```
//! # use spatialbench::generators::TripGenerator;
//! // Create Generator for the TRIP table at Scale Factor 1 (SF 1)
//! let scale_factor = 1.0;
//! let part = 1;
//! let num_parts = 1;
//! let generator = TripGenerator::new(scale_factor, part, num_parts);
//!
//! // Output the first 3 rows in classic TPCH TBL format
//! // (the generators are normal rust iterators and combine well with the Rust ecosystem)
//! let trips: Vec<_> = generator.iter()
//!    .take(3)
//!    .map(|trips| trips.to_string()) // use Display impl to get TBL format
//!    .collect::<Vec<_>>();
//!  assert_eq!(
//!   trips.join("\n"),"\
//!     1|21425|47|46|1997-07-24 06:58:22|1997-07-24 13:59:54|0.34|0.02|0.37|0.14|POINT(-172.96875 -90.0)|POINT(-172.98257407932567 -90.13982815963308)|\n\
//!     2|17012|66|65|1997-12-24 08:47:14|1997-12-24 09:28:57|0.03|0.00|0.04|0.01|POINT(-168.046875 -21.09375)|POINT(-168.03314018997426 -21.091593427559978)|\n\
//!     3|4454|68|67|1993-06-27 13:27:07|1993-06-27 13:34:51|0.00|0.00|0.00|0.00|POINT(-171.5625 -78.3984375)|POINT(-171.56429290849482 -78.40028771516948)|"
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
//! - TBL: The `Display` impl of the row structs produces the TPCH TBL format.
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
pub mod queries;
pub mod random;
pub mod spider;
pub mod spider_presets;
pub mod text;

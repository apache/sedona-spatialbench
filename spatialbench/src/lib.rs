// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Rust Spatial Bench Data Generator
//!
//! This crate provides a native Rust implementation of functions and utilities
//! necessary for generating the SpatialBench benchmark dataset in several popular
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
//!     1|215|1|1|1997-07-24 06:58:22|1997-07-24 13:59:54|0.34|0.02|0.37|0.14|POINT(21.218087029 8.013230662)|POINT(21.20426295 7.8734025)|\n\
//!     2|172|1|1|1997-12-24 08:47:14|1997-12-24 09:28:57|0.03|0.00|0.04|0.01|POINT(94.423867952 29.887250009)|POINT(94.43760277 29.88940658)|\n\
//!     3|46|1|1|1993-06-27 13:27:07|1993-06-27 13:34:51|0.00|0.00|0.00|0.00|POINT(109.719117916 1.988484212)|POINT(109.717325 1.98663399)|"
//!  );
//! ```
//!
//! The SpatialBench dataset is composed of several tables with foreign key relations
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
//! If you want an easy way to generate the SpatialBench dataset for usage with external
//! systems you can use CLI tool instead.
pub mod csv;
pub mod dates;
pub mod decimal;
pub mod distribution;
pub mod generators;
pub mod kde;
pub mod q_and_a;
pub mod random;
pub mod spatial;
pub mod text;

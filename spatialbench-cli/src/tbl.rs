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

//! Implementations of [`Source`] for generating data in TBL format

use super::generate::Source;
use spatialbench::generators::{
    BuildingGenerator, CustomerGenerator, DriverGenerator, TripGenerator, VehicleGenerator,
};
use std::io::Write;

/// Define a Source that writes the table in TBL format
macro_rules! define_tbl_source {
    ($SOURCE_NAME:ident, $GENERATOR_TYPE:ty) => {
        pub struct $SOURCE_NAME {
            inner: $GENERATOR_TYPE,
        }

        impl $SOURCE_NAME {
            pub fn new(inner: $GENERATOR_TYPE) -> Self {
                Self { inner }
            }
        }

        impl Source for $SOURCE_NAME {
            fn header(&self, buffer: Vec<u8>) -> Vec<u8> {
                // TBL source does not have a header
                buffer
            }

            fn create(self, mut buffer: Vec<u8>) -> Vec<u8> {
                for item in self.inner.iter() {
                    // The default Display impl writes TBL format
                    writeln!(&mut buffer, "{item}").expect("writing to memory is infallible");
                }
                buffer
            }
        }
    };
}

// Define .tbl sources for all tables
define_tbl_source!(VehicleTblSource, VehicleGenerator<'static>);
define_tbl_source!(DriverTblSource, DriverGenerator<'static>);
define_tbl_source!(CustomerTblSource, CustomerGenerator<'static>);
define_tbl_source!(TripTblSource, TripGenerator);
define_tbl_source!(BuildingTblSource, BuildingGenerator<'static>);

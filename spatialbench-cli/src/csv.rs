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
use spatialbench::csv::{BuildingCsv, CustomerCsv, DriverCsv, TripCsv, VehicleCsv};
use spatialbench::generators::{
    BuildingGenerator, CustomerGenerator, DriverGenerator, TripGenerator, VehicleGenerator,
};
use std::io::Write;

/// Define a Source that writes the table in CSV format
macro_rules! define_csv_source {
    ($SOURCE_NAME:ident, $GENERATOR_TYPE:ty, $FORMATTER:ty) => {
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
                let mut buffer = buffer;
                writeln!(&mut buffer, "{}", <$FORMATTER>::header())
                    .expect("writing to memory is infallible");
                buffer
            }

            fn create(self, mut buffer: Vec<u8>) -> Vec<u8> {
                for item in self.inner.into_iter() {
                    let formatter = <$FORMATTER>::new(item);
                    writeln!(&mut buffer, "{formatter}").expect("writing to memory is infallible");
                }
                buffer
            }
        }
    };
}

// Define .csv sources for all tables
define_csv_source!(VehicleCsvSource, VehicleGenerator<'static>, VehicleCsv);
define_csv_source!(DriverCsvSource, DriverGenerator<'static>, DriverCsv);
define_csv_source!(CustomerCsvSource, CustomerGenerator<'static>, CustomerCsv);
define_csv_source!(TripCsvSource, TripGenerator, TripCsv);
define_csv_source!(BuildingCsvSource, BuildingGenerator<'static>, BuildingCsv);

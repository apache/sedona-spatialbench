//! Implementations of [`Source`] for generating data in TBL format
use super::generate::Source;
use std::io::Write;
use tpchgen::csv::{
    BuildingCsv, CustomerCsv, DriverCsv, LineItemCsv, NationCsv, OrderCsv, RegionCsv, TripCsv,
    VehicleCsv,
};
use tpchgen::generators::{
    BuildingGenerator, CustomerGenerator, DriverGenerator, LineItemGenerator, NationGenerator,
    OrderGenerator, RegionGenerator, TripGenerator, VehicleGenerator,
};

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
                for item in self.inner.iter() {
                    let formatter = <$FORMATTER>::new(item);
                    writeln!(&mut buffer, "{formatter}").expect("writing to memory is infallible");
                }
                buffer
            }
        }
    };
}

// Define .csv sources for all tables
define_csv_source!(NationCsvSource, NationGenerator<'static>, NationCsv);
define_csv_source!(RegionCsvSource, RegionGenerator<'static>, RegionCsv);
define_csv_source!(VehicleCsvSource, VehicleGenerator<'static>, VehicleCsv);
define_csv_source!(DriverCsvSource, DriverGenerator<'static>, DriverCsv);
define_csv_source!(CustomerCsvSource, CustomerGenerator<'static>, CustomerCsv);
define_csv_source!(OrderCsvSource, OrderGenerator<'static>, OrderCsv);
define_csv_source!(LineItemCsvSource, LineItemGenerator<'static>, LineItemCsv);
define_csv_source!(TripCsvSource, TripGenerator, TripCsv);
define_csv_source!(BuildingCsvSource, BuildingGenerator<'static>, BuildingCsv);

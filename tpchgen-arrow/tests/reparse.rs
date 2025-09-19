//! Verifies the correctness of the Arrow Spatial Bench generator by parsing the canonical TBL format
//! and comparing with the generated Arrow RecordBatches

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use std::io::Write;
use std::sync::Arc;
use tpchgen::csv::{BuildingCsv, CustomerCsv, DriverCsv, TripCsv, VehicleCsv, ZoneCsv};
use tpchgen::generators::{
    Building, BuildingGenerator, Customer, CustomerGenerator, Driver, DriverGenerator, Trip,
    TripGenerator, Vehicle, VehicleGenerator, Zone, ZoneGenerator,
};
use tpchgen_arrow::{
    BuildingArrow, CustomerArrow, DriverArrow, RecordBatchIterator, TripArrow, VehicleArrow,
    ZoneArrow,
};

use arrow::array::Array;
use arrow::array::BinaryArray;
use geo::Geometry;
use geozero::ToGeo;
use geozero::wkb::Wkb;

/// Macro that defines tests for tbl for a given type
macro_rules! test_row_type {
    ($FUNCNAME:ident, $GENERATOR:ty, $ARROWITER:ty, $FORMATTYPE:expr) => {
        #[test]
        fn $FUNCNAME() {
            let scale_factor = 0.1;
            let batch_size = 1000;
            let part = 1;
            let part_count = 1;
            let generator = <$GENERATOR>::new(scale_factor, part, part_count);
            $FORMATTYPE.test(
                generator.clone().iter(),
                <$ARROWITER>::new(generator).with_batch_size(batch_size),
            );
        }
    };
}

test_row_type!(customer_tbl, CustomerGenerator, CustomerArrow, Test::tbl());
test_row_type!(customer_csv, CustomerGenerator, CustomerArrow, Test::csv());
test_row_type!(vehicle_tbl, VehicleGenerator, VehicleArrow, Test::tbl());
test_row_type!(vehicle_csv, VehicleGenerator, VehicleArrow, Test::csv());
test_row_type!(driver_tbl, DriverGenerator, DriverArrow, Test::tbl());
test_row_type!(driver_csv, DriverGenerator, DriverArrow, Test::csv());
test_row_type!(trip_tbl, TripGenerator, TripArrow, Test::tbl());
test_row_type!(trip_csv, TripGenerator, TripArrow, Test::csv());
test_row_type!(building_tbl, BuildingGenerator, BuildingArrow, Test::tbl());
test_row_type!(building_csv, BuildingGenerator, BuildingArrow, Test::csv());
test_row_type!(zone_tbl, ZoneGenerator, ZoneArrow, Test::tbl());
test_row_type!(zone_csv, ZoneGenerator, ZoneArrow, Test::csv());

/// Common trait for writing rows in TBL and CSV format
trait RowType {
    /// write a row in TBL format, WITHOUT newline
    fn write_tbl_row(self, text_data: &mut Vec<u8>);
    /// write the header in CSV format
    fn write_csv_header(text_data: &mut Vec<u8>);
    /// write a row in CSV format, WITH newline
    fn write_csv_row(self, text_data: &mut Vec<u8>);
}

/// Macro that implements the RowType trait for a given type
macro_rules! impl_row_type {
    ($type:ty, $csv_type:ty) => {
        impl RowType for $type {
            fn write_tbl_row(self, text_data: &mut Vec<u8>) {
                write!(text_data, "{}", self).unwrap();
            }
            fn write_csv_header(text_data: &mut Vec<u8>) {
                writeln!(text_data, "{}", <$csv_type>::header()).unwrap();
            }
            fn write_csv_row(self, text_data: &mut Vec<u8>) {
                writeln!(text_data, "{}", <$csv_type>::new(self)).unwrap();
            }
        }
    };
}

impl_row_type!(Customer<'_>, CustomerCsv);
impl_row_type!(Vehicle<'_>, VehicleCsv);
impl_row_type!(Driver, DriverCsv);
impl_row_type!(Trip, TripCsv);
impl_row_type!(Building<'_>, BuildingCsv);
impl_row_type!(Zone, ZoneCsv);

#[derive(Debug, Clone, Copy)]
#[allow(clippy::upper_case_acronyms)]
enum Test {
    /// Generate and parse data as TBL format ('|' delimited)
    TBL,
    /// Generate and parse data as CSV format
    CSV,
}

impl Test {
    fn tbl() -> Self {
        Self::TBL
    }

    fn csv() -> Self {
        Self::CSV
    }

    fn test<R, RI, RBI>(&self, mut row_iter: RI, mut arrow_iter: RBI)
    where
        R: RowType,
        RI: Iterator<Item = R>,
        RBI: RecordBatchIterator,
    {
        let schema = Arc::clone(arrow_iter.schema());

        // Check for unsupported types for reparsing
        let contains_binary = schema
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), arrow::datatypes::DataType::Binary));

        while let Some(arrow_batch) = arrow_iter.next() {
            let batch_size = arrow_batch.num_rows();

            for (i, field) in arrow_batch.schema().fields().iter().enumerate() {
                if let arrow::datatypes::DataType::Binary = field.data_type() {
                    let col = arrow_batch.column(i);
                    let bin_array = col
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .expect("Expected BinaryArray");

                    let expected_geoms = match field.name().as_str() {
                        "t_pickuploc" | "t_dropoffloc" => &["Point"][..],
                        "b_boundary" => &["Polygon"][..],
                        "z_boundary" => &["Polygon", "MultiPolygon"][..],
                        _ => &["Unknown"][..],
                    };

                    validate_wkb_column(bin_array, expected_geoms);
                }
            }

            // Skip reparsing for Binary-containing schemas
            if contains_binary {
                continue;
            }

            let mut text_data = Vec::new();
            self.write_header::<R>(&mut text_data);
            row_iter.by_ref().take(batch_size).for_each(|item| {
                self.write_row(item, &mut text_data);
            });

            let reparsed_batch = self.parse(&text_data, &schema, batch_size);
            assert_eq!(reparsed_batch, arrow_batch);
        }
    }

    fn write_header<R: RowType>(&self, text_data: &mut Vec<u8>) {
        if let Test::CSV = self {
            R::write_csv_header(text_data);
        }
    }

    fn write_row<R: RowType>(&self, row: R, text_data: &mut Vec<u8>) {
        match self {
            Test::TBL => {
                row.write_tbl_row(text_data);
                let end_offset = text_data.len() - 1;
                text_data[end_offset] = b'\n';
            }
            Test::CSV => {
                row.write_csv_row(text_data);
            }
        }
    }

    fn parse(&self, data: &[u8], schema: &SchemaRef, batch_size: usize) -> RecordBatch {
        let builder =
            arrow_csv::reader::ReaderBuilder::new(Arc::clone(schema)).with_batch_size(batch_size);
        let builder = match self {
            Test::TBL => builder.with_header(false).with_delimiter(b'|'),
            Test::CSV => builder.with_header(true),
        };

        let mut parser = builder.build(data).unwrap();
        let batch = parser
            .next()
            .expect("should have a batch")
            .expect("should have no errors parsing");
        assert!(parser.next().is_none(), "should have only one batch");
        batch
    }
}

fn validate_wkb_column(array: &BinaryArray, expected_types: &[&str]) {
    for i in 0..array.len() {
        if array.is_null(i) {
            panic!("Unexpected null geometry at row {i}");
        }

        let bytes = array.value(i);
        let geom = Wkb(bytes.to_vec()).to_geo().unwrap_or_else(|err| {
            panic!("Row {i}: Failed to parse WKB: {err}");
        });

        let type_name = match &geom {
            Geometry::Point(_) => "Point",
            Geometry::Line(_) => "Line",
            Geometry::LineString(_) => "LineString",
            Geometry::Polygon(_) => "Polygon",
            Geometry::MultiPoint(_) => "MultiPoint",
            Geometry::MultiLineString(_) => "MultiLineString",
            Geometry::MultiPolygon(_) => "MultiPolygon",
            Geometry::GeometryCollection(_) => "GeometryCollection",
            _ => "Unknown", // Catch Rect, Triangle
        };

        assert!(
            expected_types.contains(&type_name),
            "Row {i}: Unexpected geometry type: got {}, expected one of {:?}",
            type_name,
            expected_types
        );
    }
}

use crate::conversions::string_view_array_from_display_iter;
use crate::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use arrow::array::{Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{VehicleGenerator, VehicleGeneratorIterator};

/// Generate [`Vehicle`]s in [`RecordBatch`] format
///
/// [`Vehicle`]: tpchgen::generators::Vehicle
///
/// # Example
/// ```
/// # use tpchgen::generators::{VehicleGenerator};
/// # use tpchgen_arrow::VehicleArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = VehicleGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = VehicleArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// let lines = formatted_batches.lines().collect::<Vec<_>>();
/// assert_eq!(lines, vec![
///  "+-----------+------------------------------------------+----------------+----------+-------------------------+--------+-------------+---------------+----------------------+",
///   "| v_vehiclekey | v_name                                   | v_mfgr         | v_brand  | v_type                  | v_size | v_container | v_retailprice | v_comment            |",
///   "+-----------+------------------------------------------+----------------+----------+-------------------------+--------+-------------+---------------+----------------------+",
///   "| 1         | goldenrod lavender spring chocolate lace | Manufacturer#1 | Brand#13 | PROMO BURNISHED COPPER  | 7      | JUMBO PKG   | 901.00        | ly. slyly ironi      |",
///   "| 2         | blush thistle blue yellow saddle         | Manufacturer#1 | Brand#13 | LARGE BRUSHED BRASS     | 1      | LG CASE     | 902.00        | lar accounts amo     |",
///   "| 3         | spring green yellow purple cornsilk      | Manufacturer#4 | Brand#42 | STANDARD POLISHED BRASS | 21     | WRAP CASE   | 903.00        | egular deposits hag  |",
///   "| 4         | cornflower chocolate smoke green pink    | Manufacturer#3 | Brand#34 | SMALL PLATED BRASS      | 14     | MED DRUM    | 904.00        | p furiously r        |",
///   "| 5         | forest brown coral puff cream            | Manufacturer#3 | Brand#32 | STANDARD POLISHED TIN   | 15     | SM PKG      | 905.00        |  wake carefully      |",
///   "| 6         | bisque cornflower lawn forest magenta    | Manufacturer#2 | Brand#24 | PROMO PLATED STEEL      | 4      | MED BAG     | 906.00        | sual a               |",
///   "| 7         | moccasin green thistle khaki floral      | Manufacturer#1 | Brand#11 | SMALL PLATED COPPER     | 45     | SM BAG      | 907.00        | lyly. ex             |",
///   "| 8         | misty lace thistle snow royal            | Manufacturer#4 | Brand#44 | PROMO BURNISHED TIN     | 41     | LG DRUM     | 908.00        | eposi                |",
///   "| 9         | thistle dim navajo dark gainsboro        | Manufacturer#4 | Brand#43 | SMALL BURNISHED STEEL   | 12     | WRAP CASE   | 909.00        | ironic foxe          |",
///   "| 10        | linen pink saddle puff powder            | Manufacturer#5 | Brand#54 | LARGE BURNISHED STEEL   | 44     | LG CAN      | 910.01        | ithely final deposit |",
///   "+-----------+------------------------------------------+----------------+----------+-------------------------+--------+-------------+---------------+----------------------+"
/// ]);
/// ```
pub struct VehicleArrow {
    inner: VehicleGeneratorIterator<'static>,
    batch_size: usize,
}

impl VehicleArrow {
    pub fn new(generator: VehicleGenerator<'static>) -> Self {
        Self {
            inner: generator.iter(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for VehicleArrow {
    fn schema(&self) -> &SchemaRef {
        &VEHICLE_SCHEMA
    }
}

impl Iterator for VehicleArrow {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let v_vehiclekey = Int64Array::from_iter_values(rows.iter().map(|r| r.v_vehiclekey));
        let v_mfgr = string_view_array_from_display_iter(rows.iter().map(|r| r.v_mfgr));
        let v_brand = string_view_array_from_display_iter(rows.iter().map(|r| r.v_brand));
        let v_type = StringViewArray::from_iter_values(rows.iter().map(|r| r.v_type));
        let v_license = StringViewArray::from_iter_values(rows.iter().map(|r| r.v_license));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(v_vehiclekey),
                Arc::new(v_mfgr),
                Arc::new(v_brand),
                Arc::new(v_type),
                Arc::new(v_license),
            ],
        )
        .unwrap();
        Some(batch)
    }
}

/// Schema for the Vehicle
static VEHICLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_vehicle_schema);
fn make_vehicle_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("v_vehiclekey", DataType::Int64, false),
        Field::new("v_mfgr", DataType::Utf8View, false),
        Field::new("v_brand", DataType::Utf8View, false),
        Field::new("v_type", DataType::Utf8View, false),
        Field::new("v_comment", DataType::Utf8View, false),
    ]))
}

use crate::conversions::string_view_array_from_display_iter;
use crate::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{ZoneGenerator, ZoneGeneratorIterator};

/// Generate [`Zone`]s in [`RecordBatch`] format
///
/// [`Zone`]: tpchgen::generators::Zone
///
/// # Example
/// ```
/// # use tpchgen::generators::{ZoneGenerator};
/// # use tpchgen_arrow::ZoneArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = ZoneGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = ZoneArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// ```
pub struct ZoneArrow {
    inner: ZoneGeneratorIterator,
    batch_size: usize,
}

impl ZoneArrow {
    pub fn new(generator: ZoneGenerator) -> Self {
        let inner = generator.clone().into_iter();
        Self {
            inner,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for ZoneArrow {
    fn schema(&self) -> &SchemaRef {
        &ZONE_SCHEMA
    }
}

impl Iterator for ZoneArrow {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let z_zonekey = Int64Array::from_iter_values(rows.iter().map(|r| r.z_zonekey));
        let z_gersid = string_view_array_from_display_iter(rows.iter().map(|r| &r.z_gersid));
        let z_name = string_view_array_from_display_iter(rows.iter().map(|r| &r.z_name));
        let z_subtype = string_view_array_from_display_iter(rows.iter().map(|r| &r.z_subtype));
        let z_boundary = string_view_array_from_display_iter(rows.iter().map(|r| &r.z_boundary));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(z_zonekey),
                Arc::new(z_gersid),
                Arc::new(z_name),
                Arc::new(z_subtype),
                Arc::new(z_boundary),
            ],
        )
        .unwrap();
        Some(batch)
    }
}

/// Schema for the Zone
static ZONE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_zone_schema);
fn make_zone_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("z_zonekey", DataType::Int64, false),
        Field::new("z_gersid", DataType::Utf8View, false),
        Field::new("z_name", DataType::Utf8View, false),
        Field::new("z_subtype", DataType::Utf8View, false),
        Field::new("z_boundary", DataType::Utf8View, false),
    ]))
}

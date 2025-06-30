use crate::conversions::string_view_array_from_display_iter;
use crate::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use arrow::array::{Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{DriverGenerator, DriverGeneratorIterator};

/// Generate [`Driver`]s in [`RecordBatch`] format
///
/// [`Driver`]: tpchgen::generators::Driver
///
/// # Example:
/// ```
/// # use tpchgen::generators::{DriverGenerator};
/// # use tpchgen_arrow::DriverArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = DriverGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = DriverArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// let lines = formatted_batches.lines().collect::<Vec<_>>();
/// assert_eq!(lines, vec![
///   "+-----------+--------------------+-------------------------------------+-------------+-----------------+-----------+-----------------------------------------------------------------------------------------------------+", "| s_suppkey | s_name             | s_address                           | s_nationkey | s_phone         | s_acctbal | s_comment                                                                                           |", "+-----------+--------------------+-------------------------------------+-------------+-----------------+-----------+-----------------------------------------------------------------------------------------------------+", "| 1         | Driver#000000001 |  N kD4on9OM Ipw3,gf0JBoQDd7tgrzrddZ | 17          | 27-918-335-1736 | 5755.94   | each slyly above the careful                                                                        |", "| 2         | Driver#000000002 | 89eJ5ksX3ImxJQBvxObC,               | 5           | 15-679-861-2259 | 4032.68   |  slyly bold instructions. idle dependen                                                             |", "| 3         | Driver#000000003 | q1,G3Pj6OjIuUYfUoH18BFTKP5aU9bEV3   | 1           | 11-383-516-1199 | 4192.40   | blithely silent requests after the express dependencies are sl                                      |", "| 4         | Driver#000000004 | Bk7ah4CK8SYQTepEmvMkkgMwg           | 15          | 25-843-787-7479 | 4641.08   | riously even requests above the exp                                                                 |", "| 5         | Driver#000000005 | Gcdm2rJRzl5qlTVzc                   | 11          | 21-151-690-3663 | -283.84   | . slyly regular pinto bea                                                                           |", "| 6         | Driver#000000006 | tQxuVm7s7CnK                        | 14          | 24-696-997-4969 | 1365.79   | final accounts. regular dolphins use against the furiously ironic decoys.                           |", "| 7         | Driver#000000007 | s,4TicNGB4uO6PaSqNBUq               | 23          | 33-990-965-2201 | 6820.35   | s unwind silently furiously regular courts. final requests are deposits. requests wake quietly blit |", "| 8         | Driver#000000008 | 9Sq4bBH2FQEmaFOocY45sRTxo6yuoG      | 17          | 27-498-742-3860 | 7627.85   | al pinto beans. asymptotes haggl                                                                    |", "| 9         | Driver#000000009 | 1KhUgZegwM3ua7dsYmekYBsK            | 10          | 20-403-398-8662 | 5302.37   | s. unusual, even requests along the furiously regular pac                                           |", "| 10        | Driver#000000010 | Saygah3gYWMp72i PY                  | 24          | 34-852-489-8585 | 3891.91   | ing waters. regular requests ar                                                                     |", "+-----------+--------------------+-------------------------------------+-------------+-----------------+-----------+-----------------------------------------------------------------------------------------------------+"
/// ]);
/// ```
pub struct DriverArrow {
    inner: DriverGeneratorIterator<'static>,
    batch_size: usize,
}

impl DriverArrow {
    pub fn new(generator: DriverGenerator<'static>) -> Self {
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

impl RecordBatchIterator for DriverArrow {
    fn schema(&self) -> &SchemaRef {
        &DRIVER_SCHEMA
    }
}

impl Iterator for DriverArrow {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let d_driverkey = Int64Array::from_iter_values(rows.iter().map(|r| r.d_driverkey));
        let d_name = string_view_array_from_display_iter(rows.iter().map(|r| r.d_name));
        let d_address = string_view_array_from_display_iter(rows.iter().map(|r| &r.d_address));
        let d_region = StringViewArray::from_iter_values(rows.iter().map(|r| &r.d_region));
        let d_nation = StringViewArray::from_iter_values(rows.iter().map(|r| &r.d_nation));
        let d_phone = string_view_array_from_display_iter(rows.iter().map(|r| &r.d_phone));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(d_driverkey),
                Arc::new(d_name),
                Arc::new(d_address),
                Arc::new(d_region),
                Arc::new(d_nation),
                Arc::new(d_phone),
            ],
        )
        .unwrap();
        Some(batch)
    }
}

/// Schema for the PartSupp
static DRIVER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_driver_schema);
fn make_driver_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("d_driverkey", DataType::Int64, false),
        Field::new("d_name", DataType::Utf8View, false),
        Field::new("d_address", DataType::Utf8View, false),
        Field::new("d_region", DataType::Utf8View, false),
        Field::new("d_nation", DataType::Utf8View, false),
        Field::new("d_phone", DataType::Utf8View, false),
    ]))
}

use crate::conversions::string_view_array_from_display_iter;
use crate::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use arrow::array::{Int64Array, RecordBatch, StringViewArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use tpchgen::generators::{CustomerGenerator, CustomerGeneratorIterator};
use std::sync::{Arc, LazyLock};

/// Generate [`Customer`]s in [`RecordBatch`] format
///
/// [`Customer`]: tpchgen::generators::Customer
///
/// # Example
/// ```
/// # use tpchgen::generators::{CustomerGenerator};
/// # use tpchgen_arrow::CustomerArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = CustomerGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = CustomerArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first 10 batches
/// let batch = arrow_generator.next().unwrap();
/// // compare the output by pretty printing it
/// let formatted_batches = arrow::util::pretty::pretty_format_batches(&[batch])
///   .unwrap()
///   .to_string();
/// let lines = formatted_batches.lines().collect::<Vec<_>>();
/// assert_eq!(lines, vec![
///   "+-----------+--------------------+---------------------------------------+-------------+--------------+-----------------+",
///   "| c_custkey | c_name             | c_address                             | c_region    | c_nation     | c_phone         |",
///   "+-----------+--------------------+---------------------------------------+-------------+--------------+-----------------+",
///   "| 1         | Customer#000000001 | IVhzIApeRb ot,c,E                     | AFRICA      | MOROCCO      | 25-989-741-2988 |",
///   "| 2         | Customer#000000002 | XSTf4,NCwDVaWNe6tEgvwfmRchLXak        | MIDDLE EAST | JORDAN       | 23-768-687-3665 |",
///   "| 3         | Customer#000000003 | MG9kdTD2WBHm                          | AMERICA     | ARGENTINA    | 11-719-748-3364 |",
///   "| 4         | Customer#000000004 | XxVSJsLAGtn                           | MIDDLE EAST | EGYPT        | 14-128-190-5944 |",
///   "| 5         | Customer#000000005 | KvpyuHCplrB84WgAiGV6sYpZq7Tj          | AMERICA     | CANADA       | 13-750-942-6364 |",
///   "| 6         | Customer#000000006 | sKZz0CsnMD7mp4Xd0YrBvx,LREYKUWAh yVn  | MIDDLE EAST | SAUDI ARABIA | 30-114-968-4951 |",
///   "| 7         | Customer#000000007 | TcGe5gaZNgVePxU5kRrvXBfkasDTea        | ASIA        | CHINA        | 28-190-982-9759 |",
///   "| 8         | Customer#000000008 | I0B10bB0AymmC, 0PrRYBCP1yGJ8xcBPmWhl5 | AMERICA     | PERU         | 27-147-574-9335 |",
///   "| 9         | Customer#000000009 | xKiAFTjUsCuxfeleNqefumTrjS            | ASIA        | INDIA        | 18-338-906-3675 |",
///   "| 10        | Customer#000000010 | 6LrEaV6KR6PLVcgl2ArL Q3rqzLzcT1 v2    | AFRICA      | ETHIOPIA     | 15-741-346-9870 |",
///   "+-----------+--------------------+---------------------------------------+-------------+--------------+-----------------+"
///   ]);
/// ```
pub struct CustomerArrow {
    inner: CustomerGeneratorIterator<'static>,
    batch_size: usize,
}

impl CustomerArrow {
    pub fn new(generator: CustomerGenerator<'static>) -> Self {
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

impl RecordBatchIterator for CustomerArrow {
    fn schema(&self) -> &SchemaRef {
        &CUSTOMER_SCHEMA
    }
}

impl Iterator for CustomerArrow {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let c_custkey = Int64Array::from_iter_values(rows.iter().map(|r| r.c_custkey));
        let c_name = string_view_array_from_display_iter(rows.iter().map(|r| r.c_name));
        let c_address = string_view_array_from_display_iter(rows.iter().map(|r| &r.c_address));
        let c_region = StringViewArray::from_iter_values(rows.iter().map(|r| r.c_region));
        let c_nation = StringViewArray::from_iter_values(rows.iter().map(|r| r.c_nation));
        let c_phone = string_view_array_from_display_iter(rows.iter().map(|r| &r.c_phone));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![
                Arc::new(c_custkey),
                Arc::new(c_name),
                Arc::new(c_address),
                Arc::new(c_region),
                Arc::new(c_nation),
                Arc::new(c_phone),
            ],
        )
        .unwrap();
        Some(batch)
    }
}

/// Schema for the Customer
static CUSTOMER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_customer_schema);
fn make_customer_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("c_custkey", DataType::Int64, false),
        Field::new("c_name", DataType::Utf8View, false),
        Field::new("c_address", DataType::Utf8View, false),
        Field::new("c_region", DataType::Utf8View, false),
        Field::new("c_nation", DataType::Utf8View, false),
        Field::new("c_phone", DataType::Utf8View, false),
    ]))
}

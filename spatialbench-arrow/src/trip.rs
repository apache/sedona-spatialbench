use crate::conversions::{decimal128_array_from_iter, to_arrow_timestamp_millis};
use crate::{DEFAULT_BATCH_SIZE, RecordBatchIterator};
use arrow::array::{BinaryArray, Int64Array, RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use spatialbench::generators::{Trip, TripGenerator, TripGeneratorIterator};
use std::sync::{Arc, LazyLock, Mutex};

// Thread-safe wrapper for TripGeneratorIterator
struct ThreadSafeTripGenerator {
    generator: Mutex<TripGeneratorIterator>,
}

impl ThreadSafeTripGenerator {
    fn new(generator: TripGenerator) -> Self {
        Self {
            generator: Mutex::new(generator.iter()),
        }
    }

    fn next_batch(&self, batch_size: usize) -> Vec<Trip> {
        let mut generator = self.generator.lock().unwrap();
        generator.by_ref().take(batch_size).collect()
    }
}

// This is safe because we're using Mutex for synchronization
unsafe impl Send for ThreadSafeTripGenerator {}
unsafe impl Sync for ThreadSafeTripGenerator {}

pub struct TripArrow {
    generator: ThreadSafeTripGenerator,
    batch_size: usize,
    schema: SchemaRef,
}

impl TripArrow {
    pub fn new(generator: TripGenerator) -> Self {
        Self {
            generator: ThreadSafeTripGenerator::new(generator),
            batch_size: DEFAULT_BATCH_SIZE,
            schema: TRIP_SCHEMA.clone(),
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RecordBatchIterator for TripArrow {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

impl Iterator for TripArrow {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows = self.generator.next_batch(self.batch_size);
        if rows.is_empty() {
            return None;
        }

        // Convert column by column
        let t_tripkey = Int64Array::from_iter_values(rows.iter().map(|row| row.t_tripkey));
        let t_custkey = Int64Array::from_iter_values(rows.iter().map(|row| row.t_custkey));
        let t_driverkey = Int64Array::from_iter_values(rows.iter().map(|row| row.t_driverkey));
        let t_vehiclekey = Int64Array::from_iter_values(rows.iter().map(|row| row.t_vehiclekey));
        let t_pickuptime = TimestampMillisecondArray::from_iter_values(
            rows.iter()
                .map(|row| to_arrow_timestamp_millis(row.t_pickuptime)),
        );
        let t_dropofftime = TimestampMillisecondArray::from_iter_values(
            rows.iter()
                .map(|row| to_arrow_timestamp_millis(row.t_dropofftime)),
        );
        let t_fare = decimal128_array_from_iter(rows.iter().map(|row| row.t_fare));
        let t_tip = decimal128_array_from_iter(rows.iter().map(|row| row.t_tip));
        let t_totalamount = decimal128_array_from_iter(rows.iter().map(|row| row.t_totalamount));
        let t_distance = decimal128_array_from_iter(rows.iter().map(|row| row.t_distance));
        let t_pickuploc = BinaryArray::from_iter_values(rows.iter().map(|row| {
            Geometry::Point(row.t_pickuploc)
                .to_wkb(CoordDimensions::xy())
                .expect("Failed to convert pickup location to WKB")
        }));
        let t_dropoffloc = BinaryArray::from_iter_values(rows.iter().map(|row| {
            Geometry::Point(row.t_dropoffloc)
                .to_wkb(CoordDimensions::xy())
                .expect("Failed to convert dropoff location to WKB")
        }));

        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(t_tripkey),
                Arc::new(t_custkey),
                Arc::new(t_driverkey),
                Arc::new(t_vehiclekey),
                Arc::new(t_pickuptime),
                Arc::new(t_dropofftime),
                Arc::new(t_fare),
                Arc::new(t_tip),
                Arc::new(t_totalamount),
                Arc::new(t_distance),
                Arc::new(t_pickuploc),
                Arc::new(t_dropoffloc),
            ],
        )
        .unwrap();

        Some(batch)
    }
}

/// Schema for the Trip table
static TRIP_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_trip_schema);

fn make_trip_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("t_tripkey", DataType::Int64, false),
        Field::new("t_custkey", DataType::Int64, false),
        Field::new("t_driverkey", DataType::Int64, false),
        Field::new("t_vehiclekey", DataType::Int64, false),
        Field::new(
            "t_pickuptime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new(
            "t_dropofftime",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("t_fare", DataType::Decimal128(15, 5), false),
        Field::new("t_tip", DataType::Decimal128(15, 5), false),
        Field::new("t_totalamount", DataType::Decimal128(15, 5), false),
        Field::new("t_distance", DataType::Decimal128(15, 5), false),
        Field::new("t_pickuploc", DataType::Binary, false),
        Field::new("t_dropoffloc", DataType::Binary, false),
    ]))
}

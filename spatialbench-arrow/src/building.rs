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

use crate::conversions::string_view_array_from_display_iter;
use crate::{RecordBatchIterator, DEFAULT_BATCH_SIZE};
use arrow::array::{BinaryArray, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use geo::Geometry;
use geozero::{CoordDimensions, ToWkb};
use spatialbench::generators::{BuildingGenerator, BuildingGeneratorIterator};
use std::sync::{Arc, LazyLock};

/// Generate [`Building`]s in [`RecordBatch`] format
///
/// [`Building`]: spatialbench::generators::Building
///
/// # Example
/// ```
/// # use spatialbench::generators::{BuildingGenerator};
/// # use spatialbench_arrow::BuildingArrow;
///
/// // Create a SF=1.0 generator and wrap it in an Arrow generator
/// let generator = BuildingGenerator::new(1.0, 1, 1);
/// let mut arrow_generator = BuildingArrow::new(generator)
///   .with_batch_size(10);
/// // Read the first batch
/// let batch = arrow_generator.next().unwrap();
/// ```
pub struct BuildingArrow {
    inner: BuildingGeneratorIterator<'static>,
    batch_size: usize,
}

impl BuildingArrow {
    pub fn new(generator: BuildingGenerator<'static>) -> Self {
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

impl RecordBatchIterator for BuildingArrow {
    fn schema(&self) -> &SchemaRef {
        &BUILDING_SCHEMA
    }
}

impl Iterator for BuildingArrow {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        // Get next rows to convert
        let rows: Vec<_> = self.inner.by_ref().take(self.batch_size).collect();
        if rows.is_empty() {
            return None;
        }

        let buildingkey = Int64Array::from_iter_values(rows.iter().map(|r| r.b_buildingkey));
        let name = string_view_array_from_display_iter(rows.iter().map(|r| &r.b_name));

        // Convert geo::Polygon to WKB binary format
        let wkb_array = BinaryArray::from_iter_values(rows.iter().map(|r| {
            Geometry::Polygon(r.b_boundary.clone())
                .to_wkb(CoordDimensions::xy())
                .unwrap()
        }));

        let batch = RecordBatch::try_new(
            Arc::clone(self.schema()),
            vec![Arc::new(buildingkey), Arc::new(name), Arc::new(wkb_array)],
        )
        .unwrap();
        Some(batch)
    }
}

/// Schema for the Building
static BUILDING_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(make_building_schema);
fn make_building_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("b_buildingkey", DataType::Int64, false),
        Field::new("b_name", DataType::Utf8View, false),
        Field::new("b_boundary", DataType::Binary, false),
    ]))
}

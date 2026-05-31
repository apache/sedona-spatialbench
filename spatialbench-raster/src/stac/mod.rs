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

//! STAC geoparquet writer for raster benchmark catalogs.
//!
//! Generates a STAC-compliant geoparquet file from a manifest of COG entries
//! and a topology. Uses column-at-a-time Arrow builders for performance
//! and hand-encoded WKB for bbox geometry (no geometry library dependency).

use crate::scaling::ScalingTier;
use crate::topology::{assign_scene, Topology};
use crate::ManifestEntry;

use arrow::array::{
    BinaryBuilder, Float64Builder, StringBuilder, TimestampMillisecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use std::io;
use std::path::Path;
use std::sync::Arc;

/// Size of a WKB Polygon with a single 5-point ring.
const WKB_BBOX_SIZE: usize = 93;

/// Encode a bounding box as a WKB Polygon (5-point ring: SW, SE, NE, NW, SW).
///
/// Layout: byte_order(1) + type(4) + num_rings(4) + num_points(4) + 5×(x:f64 + y:f64) = 93 bytes.
/// Little-endian throughout. Stack-allocated, no heap allocation.
#[inline]
fn encode_bbox_wkb(bbox: &[f64; 4]) -> [u8; WKB_BBOX_SIZE] {
    let mut buf = [0u8; WKB_BBOX_SIZE];
    let [west, south, east, north] = *bbox;

    // Byte order: little-endian
    buf[0] = 1;
    // Type: Polygon (3)
    buf[1..5].copy_from_slice(&3u32.to_le_bytes());
    // Num rings: 1
    buf[5..9].copy_from_slice(&1u32.to_le_bytes());
    // Num points: 5
    buf[9..13].copy_from_slice(&5u32.to_le_bytes());

    // Points: SW, SE, NE, NW, SW (closed ring)
    let points: [(f64, f64); 5] = [
        (west, south),
        (east, south),
        (east, north),
        (west, north),
        (west, south),
    ];

    let mut offset = 13;
    for (x, y) in &points {
        buf[offset..offset + 8].copy_from_slice(&x.to_le_bytes());
        offset += 8;
        buf[offset..offset + 8].copy_from_slice(&y.to_le_bytes());
        offset += 8;
    }
    debug_assert_eq!(offset, WKB_BBOX_SIZE);

    buf
}

/// Build the Arrow schema for STAC geoparquet output.
fn stac_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("geometry", DataType::Binary, false),
        Field::new(
            "bbox",
            DataType::Struct(
                vec![
                    Field::new("xmin", DataType::Float64, false),
                    Field::new("ymin", DataType::Float64, false),
                    Field::new("xmax", DataType::Float64, false),
                    Field::new("ymax", DataType::Float64, false),
                ]
                .into(),
            ),
            false,
        ),
        Field::new(
            "datetime",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("mosaic_id", DataType::UInt32, false),
        Field::new("timeslice_id", DataType::UInt32, false),
        Field::new("footprint_id", DataType::UInt32, false),
        Field::new("cog_id", DataType::UInt32, false),
        Field::new("epsg", DataType::UInt32, false),
        Field::new("asset_href", DataType::Utf8, false),
        Field::new("proj:epsg", DataType::UInt32, false),
        Field::new("eo:bands", DataType::Utf8, false),
    ])
}

/// Synthetic epoch: 2024-01-01T00:00:00Z as milliseconds since Unix epoch.
const EPOCH_2024_MS: i64 = 1_704_067_200_000;

/// Interval between timeslices: 5 days in milliseconds.
const TIMESLICE_INTERVAL_MS: i64 = 5 * 24 * 60 * 60 * 1000;

/// Constant eo:bands JSON string for single-band NIR COGs.
const EO_BANDS_JSON: &str = r#"[{"name":"band1","common_name":"nir"}]"#;

/// Maximum rows per Parquet row group. At SF=1000 (~32M COGs × 3 topologies),
/// this limits memory while writing.
const MAX_ROW_GROUP_SIZE: usize = 1_000_000;

/// Write a STAC geoparquet catalog for one topology.
///
/// Each call produces one Parquet file with one row per COG. The topology
/// determines how `cog_id` maps to `(mosaic_id, timeslice_id)` via
/// [`assign_scene`].
pub fn write_stac_geoparquet(
    manifest: &[ManifestEntry],
    tier: &ScalingTier,
    topology: Topology,
    output_path: &Path,
) -> io::Result<()> {
    let (m, _t) = topology.factor(tier);
    let schema = Arc::new(stac_schema());

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).unwrap(),
        ))
        .set_max_row_group_size(MAX_ROW_GROUP_SIZE)
        .build();

    let file = std::fs::File::create(output_path)?;
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(props))
        .map_err(|e| io::Error::other(format!("failed to create ArrowWriter: {e}")))?;

    // Process in chunks for large manifests
    for chunk in manifest.chunks(MAX_ROW_GROUP_SIZE) {
        let batch = build_record_batch(chunk, m, &schema)?;
        writer
            .write(&batch)
            .map_err(|e| io::Error::other(format!("failed to write batch: {e}")))?;
    }

    writer
        .close()
        .map_err(|e| io::Error::other(format!("failed to close writer: {e}")))?;

    Ok(())
}

/// Build a single [`RecordBatch`] from a slice of manifest entries.
fn build_record_batch(
    entries: &[ManifestEntry],
    m: u32,
    schema: &Arc<Schema>,
) -> io::Result<RecordBatch> {
    let n = entries.len();

    // Pre-allocate all builders with known capacity
    let mut id_builder = StringBuilder::with_capacity(n, n * 12);
    let mut geom_builder = BinaryBuilder::with_capacity(n, n * WKB_BBOX_SIZE);
    let mut bbox_xmin = Float64Builder::with_capacity(n);
    let mut bbox_ymin = Float64Builder::with_capacity(n);
    let mut bbox_xmax = Float64Builder::with_capacity(n);
    let mut bbox_ymax = Float64Builder::with_capacity(n);
    let mut datetime_builder = TimestampMillisecondBuilder::with_capacity(n);
    let mut mosaic_builder = UInt32Builder::with_capacity(n);
    let mut timeslice_builder = UInt32Builder::with_capacity(n);
    let mut fp_builder = UInt32Builder::with_capacity(n);
    let mut cog_builder = UInt32Builder::with_capacity(n);
    let mut epsg_builder = UInt32Builder::with_capacity(n);
    let mut href_builder = StringBuilder::with_capacity(n, n * 24);
    let mut proj_epsg_builder = UInt32Builder::with_capacity(n);
    let mut bands_builder = StringBuilder::with_capacity(n, n * EO_BANDS_JSON.len());

    // Stack buffer for id formatting (avoids String allocation per row)
    let mut id_buf = String::with_capacity(12);

    for entry in entries {
        use std::fmt::Write;

        // id: "{fp:05}_{cog:04}"
        id_buf.clear();
        write!(id_buf, "{:05}_{:04}", entry.footprint_id, entry.cog_id).unwrap();
        id_builder.append_value(&id_buf);

        // geometry: WKB
        let wkb = encode_bbox_wkb(&entry.bbox_4326);
        geom_builder.append_value(wkb);

        // bbox struct
        bbox_xmin.append_value(entry.bbox_4326[0]);
        bbox_ymin.append_value(entry.bbox_4326[1]);
        bbox_xmax.append_value(entry.bbox_4326[2]);
        bbox_ymax.append_value(entry.bbox_4326[3]);

        // scene assignment
        let scene = assign_scene(entry.cog_id, m);

        // datetime: epoch + timeslice_id × 5 days
        let ts = EPOCH_2024_MS + scene.timeslice_id as i64 * TIMESLICE_INTERVAL_MS;
        datetime_builder.append_value(ts);

        mosaic_builder.append_value(scene.mosaic_id);
        timeslice_builder.append_value(scene.timeslice_id);
        fp_builder.append_value(entry.footprint_id);
        cog_builder.append_value(entry.cog_id);
        epsg_builder.append_value(entry.epsg);

        // asset_href: "pile/{fp:05}/{cog:04}.tif"
        id_buf.clear();
        write!(
            id_buf,
            "pile/{:05}/{:04}.tif",
            entry.footprint_id, entry.cog_id
        )
        .unwrap();
        href_builder.append_value(&id_buf);

        proj_epsg_builder.append_value(entry.epsg);
        bands_builder.append_value(EO_BANDS_JSON);
    }

    // Build bbox struct array
    let bbox_struct = arrow::array::StructArray::from(vec![
        (
            Arc::new(Field::new("xmin", DataType::Float64, false)),
            Arc::new(bbox_xmin.finish()) as arrow::array::ArrayRef,
        ),
        (
            Arc::new(Field::new("ymin", DataType::Float64, false)),
            Arc::new(bbox_ymin.finish()) as arrow::array::ArrayRef,
        ),
        (
            Arc::new(Field::new("xmax", DataType::Float64, false)),
            Arc::new(bbox_xmax.finish()) as arrow::array::ArrayRef,
        ),
        (
            Arc::new(Field::new("ymax", DataType::Float64, false)),
            Arc::new(bbox_ymax.finish()) as arrow::array::ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(geom_builder.finish()),
            Arc::new(bbox_struct),
            Arc::new(datetime_builder.finish().with_timezone("UTC")),
            Arc::new(mosaic_builder.finish()),
            Arc::new(timeslice_builder.finish()),
            Arc::new(fp_builder.finish()),
            Arc::new(cog_builder.finish()),
            Arc::new(epsg_builder.finish()),
            Arc::new(href_builder.finish()),
            Arc::new(proj_epsg_builder.finish()),
            Arc::new(bands_builder.finish()),
        ],
    )
    .map_err(|e| io::Error::other(format!("failed to build RecordBatch: {e}")))?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaling::scaling_tier;

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn sample_manifest() -> Vec<ManifestEntry> {
        (0..10)
            .map(|i| ManifestEntry {
                footprint_id: i / 4,
                cog_id: i % 4,
                bbox_4326: [-100.0, 30.0, -99.0, 31.0],
                epsg: 32614,
            })
            .collect()
    }

    #[test]
    fn wkb_correct_size() {
        let wkb = encode_bbox_wkb(&[-100.0, 30.0, -99.0, 31.0]);
        assert_eq!(wkb.len(), 93);
        // Byte order: little-endian
        assert_eq!(wkb[0], 1);
        // Type: Polygon (3)
        assert_eq!(u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]]), 3);
        // Num rings: 1
        assert_eq!(u32::from_le_bytes([wkb[5], wkb[6], wkb[7], wkb[8]]), 1);
        // Num points: 5
        assert_eq!(u32::from_le_bytes([wkb[9], wkb[10], wkb[11], wkb[12]]), 5);
    }

    #[test]
    fn wkb_first_point_is_sw() {
        let bbox = [-100.0, 30.0, -99.0, 31.0];
        let wkb = encode_bbox_wkb(&bbox);
        let x = f64::from_le_bytes(wkb[13..21].try_into().unwrap());
        let y = f64::from_le_bytes(wkb[21..29].try_into().unwrap());
        assert_eq!(x, -100.0); // west
        assert_eq!(y, 30.0); // south
    }

    #[test]
    fn write_and_read_stac_geoparquet() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("narrow.parquet");

        write_stac_geoparquet(&manifest, tier, Topology::Narrow, &path).unwrap();

        // Read back and verify
        let file = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 10);

        // Verify column count
        assert_eq!(batches[0].num_columns(), 12);
    }

    #[test]
    fn all_topologies_produce_output() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();
        let dir = tempfile::tempdir().unwrap();

        for topo in Topology::ALL {
            let path = dir.path().join(format!("{}.parquet", topo.dir_name()));
            write_stac_geoparquet(&manifest, tier, topo, &path).unwrap();
            assert!(path.exists());
            assert!(std::fs::metadata(&path).unwrap().len() > 0);
        }
    }
}

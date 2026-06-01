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
//! Generates a STAC-compliant geoparquet file with one row per **item**
//! (not per COG). Each item contains a nested `assets` map with M entries,
//! where M is determined by the topology's factoring. The topologies differ
//! in row count: Narrow produces `T × A` rows with few assets each, while
//! Balanced produces fewer rows with more assets each.

use crate::scaling::ScalingTier;
use crate::topology::{assign_scene, Topology};
use crate::ManifestEntry;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Builder, MapBuilder, StringBuilder,
    TimestampMillisecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use std::collections::BTreeMap;
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

/// Asset entry within a STAC item's assets map.
struct AssetEntry {
    /// Role label (e.g., "tasmax", "red", "nir").
    role: String,
    /// Relative path to the COG file.
    href: String,
}

/// An assembled STAC item, ready to write as one row.
struct StacItem {
    /// Item ID (e.g., "NRW_00000_t0000").
    id: String,
    /// Footprint ID.
    footprint_id: u32,
    /// Timeslice index.
    timeslice_id: u32,
    /// Bounding box in EPSG:4326.
    bbox_4326: [f64; 4],
    /// EPSG code.
    epsg: u32,
    /// Assets in this item (one per mosaic slot).
    assets: Vec<AssetEntry>,
}

/// Build the Arrow schema for STAC geoparquet output.
///
/// The `assets` column is a `Map<String, String>` mapping role labels to
/// relative COG paths (e.g., `"nir" -> "pile/00000/0003.tif"`).
fn stac_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("collection", DataType::Utf8, false),
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
        Field::new("timeslice_id", DataType::UInt32, false),
        Field::new("footprint_id", DataType::UInt32, false),
        Field::new("epsg", DataType::UInt32, false),
        Field::new("proj:epsg", DataType::UInt32, false),
        Field::new("workload:asset_count", DataType::UInt32, false),
        Field::new(
            "assets",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            false,
        ),
    ])
}

/// Synthetic epoch: 2024-01-01T00:00:00Z as milliseconds since Unix epoch.
const EPOCH_2024_MS: i64 = 1_704_067_200_000;

/// Interval between timeslices: 5 days in milliseconds.
const TIMESLICE_INTERVAL_MS: i64 = 5 * 24 * 60 * 60 * 1000;

/// Maximum rows per Parquet row group.
const MAX_ROW_GROUP_SIZE: usize = 1_000_000;

/// Write a STAC geoparquet catalog for one topology.
///
/// Each call produces one Parquet file with one row per **item**.
/// Row count = `T × A_actual` where `A_actual` is the number of distinct
/// footprints in the manifest. Each item has M assets in a nested map.
pub fn write_stac_geoparquet(
    manifest: &[ManifestEntry],
    tier: &ScalingTier,
    topology: Topology,
    output_path: &Path,
) -> io::Result<()> {
    let (m, _t) = topology.factor(tier);

    // Group manifest entries into items: key = (footprint_id, timeslice_id)
    let items = assemble_items(manifest, m, topology);

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

    for chunk in items.chunks(MAX_ROW_GROUP_SIZE) {
        let batch = build_record_batch(chunk, topology, &schema)?;
        writer
            .write(&batch)
            .map_err(|e| io::Error::other(format!("failed to write batch: {e}")))?;
    }

    writer
        .close()
        .map_err(|e| io::Error::other(format!("failed to close writer: {e}")))?;

    Ok(())
}

/// Group manifest entries into STAC items.
///
/// Each item corresponds to a unique `(footprint_id, timeslice_id)` pair.
/// Assets within an item are the M COGs assigned to that timeslice.
fn assemble_items(manifest: &[ManifestEntry], m: u32, topology: Topology) -> Vec<StacItem> {
    // Group by (footprint_id, timeslice_id)
    let mut groups: BTreeMap<(u32, u32), Vec<&ManifestEntry>> = BTreeMap::new();
    for entry in manifest {
        let scene = assign_scene(entry.cog_id, m);
        groups
            .entry((entry.footprint_id, scene.timeslice_id))
            .or_default()
            .push(entry);
    }

    let prefix = topology.item_prefix();
    let mut items = Vec::with_capacity(groups.len());

    for ((footprint_id, timeslice_id), entries) in &groups {
        let mut assets = Vec::with_capacity(entries.len());
        for entry in entries {
            let scene = assign_scene(entry.cog_id, m);
            let role = topology.asset_label_for(scene.mosaic_id);
            let href = format!("pile/{:05}/{:04}.tif", entry.footprint_id, entry.cog_id);
            assets.push(AssetEntry { role, href });
        }

        // Sort assets by role label for deterministic output
        assets.sort_by(|a, b| a.role.cmp(&b.role));

        let id = format!("{prefix}_F{footprint_id:05}_t{timeslice_id:04}");

        // Use first entry's bbox/epsg (all entries in same footprint share these)
        let first = entries[0];

        items.push(StacItem {
            id,
            footprint_id: *footprint_id,
            timeslice_id: *timeslice_id,
            bbox_4326: first.bbox_4326,
            epsg: first.epsg,
            assets,
        });
    }

    items
}

/// Build a single [`RecordBatch`] from a slice of assembled STAC items.
fn build_record_batch(
    items: &[StacItem],
    topology: Topology,
    schema: &Arc<Schema>,
) -> io::Result<RecordBatch> {
    let n = items.len();
    let collection_name = topology.dir_name();

    let mut id_builder = StringBuilder::with_capacity(n, n * 20);
    let mut collection_builder = StringBuilder::with_capacity(n, n * 10);
    let mut geom_builder = BinaryBuilder::with_capacity(n, n * WKB_BBOX_SIZE);
    let mut bbox_xmin = Float64Builder::with_capacity(n);
    let mut bbox_ymin = Float64Builder::with_capacity(n);
    let mut bbox_xmax = Float64Builder::with_capacity(n);
    let mut bbox_ymax = Float64Builder::with_capacity(n);
    let mut datetime_builder = TimestampMillisecondBuilder::with_capacity(n);
    let mut timeslice_builder = UInt32Builder::with_capacity(n);
    let mut fp_builder = UInt32Builder::with_capacity(n);
    let mut epsg_builder = UInt32Builder::with_capacity(n);
    let mut proj_epsg_builder = UInt32Builder::with_capacity(n);
    let mut asset_count_builder = UInt32Builder::with_capacity(n);

    // Map builder: Map<String, Struct{href: String}>
    let mut assets_builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

    for item in items {
        id_builder.append_value(&item.id);
        collection_builder.append_value(collection_name);

        let wkb = encode_bbox_wkb(&item.bbox_4326);
        geom_builder.append_value(wkb);

        bbox_xmin.append_value(item.bbox_4326[0]);
        bbox_ymin.append_value(item.bbox_4326[1]);
        bbox_xmax.append_value(item.bbox_4326[2]);
        bbox_ymax.append_value(item.bbox_4326[3]);

        let ts = EPOCH_2024_MS + item.timeslice_id as i64 * TIMESLICE_INTERVAL_MS;
        datetime_builder.append_value(ts);

        timeslice_builder.append_value(item.timeslice_id);
        fp_builder.append_value(item.footprint_id);
        epsg_builder.append_value(item.epsg);
        proj_epsg_builder.append_value(item.epsg);
        asset_count_builder.append_value(item.assets.len() as u32);

        // Append assets map entries
        for asset in &item.assets {
            assets_builder.keys().append_value(&asset.role);
            assets_builder.values().append_value(&asset.href);
        }
        assets_builder.append(true).unwrap();
    }

    // Build bbox struct
    let bbox_struct = arrow::array::StructArray::from(vec![
        (
            Arc::new(Field::new("xmin", DataType::Float64, false)),
            Arc::new(bbox_xmin.finish()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("ymin", DataType::Float64, false)),
            Arc::new(bbox_ymin.finish()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("xmax", DataType::Float64, false)),
            Arc::new(bbox_xmax.finish()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("ymax", DataType::Float64, false)),
            Arc::new(bbox_ymax.finish()) as ArrayRef,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(collection_builder.finish()),
            Arc::new(geom_builder.finish()),
            Arc::new(bbox_struct),
            Arc::new(datetime_builder.finish().with_timezone("UTC")),
            Arc::new(timeslice_builder.finish()),
            Arc::new(fp_builder.finish()),
            Arc::new(epsg_builder.finish()),
            Arc::new(proj_epsg_builder.finish()),
            Arc::new(asset_count_builder.finish()),
            Arc::new(assets_builder.finish()),
        ],
    )
    .map_err(|e| io::Error::other(format!("failed to build RecordBatch: {e}")))?;

    Ok(batch)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaling::scaling_tier;

    use arrow::array::{AsArray, MapArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// Build a manifest for testing: 2 footprints × 16 COGs each = 32 entries.
    fn sample_manifest() -> Vec<ManifestEntry> {
        let mut entries = Vec::with_capacity(32);
        for fp in 0..2u32 {
            for cog in 0..16u32 {
                entries.push(ManifestEntry {
                    footprint_id: fp,
                    cog_id: cog,
                    bbox_4326: [-100.0 + fp as f64, 30.0, -99.0 + fp as f64, 31.0],
                    epsg: 32614,
                });
            }
        }
        entries
    }

    #[test]
    fn wkb_correct_size() {
        let wkb = encode_bbox_wkb(&[-100.0, 30.0, -99.0, 31.0]);
        assert_eq!(wkb.len(), 93);
        assert_eq!(wkb[0], 1);
        assert_eq!(u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]]), 3);
        assert_eq!(u32::from_le_bytes([wkb[5], wkb[6], wkb[7], wkb[8]]), 1);
        assert_eq!(u32::from_le_bytes([wkb[9], wkb[10], wkb[11], wkb[12]]), 5);
    }

    #[test]
    fn wkb_first_point_is_sw() {
        let bbox = [-100.0, 30.0, -99.0, 31.0];
        let wkb = encode_bbox_wkb(&bbox);
        let x = f64::from_le_bytes(wkb[13..21].try_into().unwrap());
        let y = f64::from_le_bytes(wkb[21..29].try_into().unwrap());
        assert_eq!(x, -100.0);
        assert_eq!(y, 30.0);
    }

    #[test]
    fn narrow_has_more_items_than_balanced() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();

        // Narrow: M=2, T=8 → 8 items per footprint × 2 footprints = 16 items
        let narrow_items = assemble_items(&manifest, 2, Topology::Narrow);
        // Balanced: M=4, T=4 → 4 items per footprint × 2 footprints = 8 items
        let balanced_items = assemble_items(&manifest, 4, Topology::Balanced);

        assert_eq!(narrow_items.len(), 16); // T=8 × 2 footprints
        assert_eq!(balanced_items.len(), 8); // T=4 × 2 footprints
        assert!(narrow_items.len() > balanced_items.len());

        // Narrow items have 2 assets each, balanced have 4
        assert!(narrow_items.iter().all(|i| i.assets.len() == 2));
        assert!(balanced_items.iter().all(|i| i.assets.len() == 4));
    }

    #[test]
    fn narrow_asset_labels_are_climate_vars() {
        let manifest = sample_manifest();
        let items = assemble_items(&manifest, 2, Topology::Narrow);
        let labels: Vec<&str> = items[0].assets.iter().map(|a| a.role.as_str()).collect();
        assert_eq!(labels, &["tasmax", "tasmin"]);
    }

    #[test]
    fn balanced_asset_labels_are_spectral_bands() {
        let manifest = sample_manifest();
        let items = assemble_items(&manifest, 4, Topology::Balanced);
        let labels: Vec<&str> = items[0].assets.iter().map(|a| a.role.as_str()).collect();
        // M=4 at SF=1: first 4 from the balanced label list
        assert_eq!(labels, &["blue", "green", "nir", "red"]);
    }

    #[test]
    fn item_id_format() {
        let manifest = sample_manifest();
        let items = assemble_items(&manifest, 2, Topology::Narrow);
        assert_eq!(items[0].id, "NRW_F00000_t0000");
        assert_eq!(items[1].id, "NRW_F00000_t0001");
    }

    #[test]
    fn all_cogs_appear_exactly_once_per_topology() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();

        for topo in Topology::SHARED_PILE {
            let (m, _t) = topo.factor(tier);
            let items = assemble_items(&manifest, m, topo);

            // Count total assets across all items
            let total_assets: usize = items.iter().map(|i| i.assets.len()).sum();
            assert_eq!(total_assets, manifest.len());

            // Every COG href is unique
            let mut hrefs: Vec<&str> = items
                .iter()
                .flat_map(|i| i.assets.iter().map(|a| a.href.as_str()))
                .collect();
            hrefs.sort();
            hrefs.dedup();
            assert_eq!(hrefs.len(), manifest.len());
        }
    }

    #[test]
    fn write_and_read_narrow() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("narrow.parquet");

        write_stac_geoparquet(&manifest, tier, Topology::Narrow, &path).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 2 footprints × T=8 = 16 items
        assert_eq!(total_rows, 16);
    }

    #[test]
    fn write_and_read_balanced() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("balanced.parquet");

        write_stac_geoparquet(&manifest, tier, Topology::Balanced, &path).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        let batches: Vec<_> = reader.collect::<Result<_, _>>().unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        // 2 footprints × T=4 = 8 items
        assert_eq!(total_rows, 8);
    }

    #[test]
    fn topologies_have_different_row_counts() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();
        let dir = tempfile::tempdir().unwrap();

        let mut row_counts = Vec::new();
        for topo in Topology::SHARED_PILE {
            let path = dir.path().join(format!("{}.parquet", topo.dir_name()));
            write_stac_geoparquet(&manifest, tier, topo, &path).unwrap();

            let file = std::fs::File::open(&path).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let reader = builder.build().unwrap();
            let count: usize = reader
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .iter()
                .map(|b| b.num_rows())
                .sum();
            row_counts.push(count);
        }

        // Narrow (16) != Balanced (8)
        assert_ne!(row_counts[0], row_counts[1]);
        assert!(row_counts[0] > row_counts[1]); // Narrow has more items
    }

    #[test]
    fn assets_map_has_correct_entries() {
        let manifest = sample_manifest();
        let tier = scaling_tier(1).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("narrow.parquet");

        write_stac_geoparquet(&manifest, tier, Topology::Narrow, &path).unwrap();

        let file = std::fs::File::open(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        let batch = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let batch = &batch[0];

        // Check assets column is a MapArray
        let assets_col = batch.column(batch.schema().index_of("assets").unwrap());
        let map_array = assets_col.as_any().downcast_ref::<MapArray>().unwrap();

        // First item should have 2 assets (M=2 for Narrow)
        let first_item_len = map_array.value_length(0);
        assert_eq!(first_item_len, 2);
    }
}

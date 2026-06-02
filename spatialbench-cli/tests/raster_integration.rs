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

//! Integration test for raster COG + STAC generation pipeline.

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::tempdir;

#[test]
fn raster_end_to_end_sf1_max2() {
    let dir = tempdir().unwrap();

    Command::cargo_bin("spatialbench-cli")
        .unwrap()
        .args([
            "--tables",
            "raster",
            "--scale-factor",
            "1",
            "--max-footprints",
            "2",
            "--output-dir",
            dir.path().to_str().unwrap(),
            "--verbose",
        ])
        .timeout(std::time::Duration::from_secs(300))
        .assert()
        .success()
        .stderr(predicate::str::contains("raster complete:"));

    // Verify pile structure
    let pile_dir = dir.path().join("raster/pile");
    assert!(pile_dir.exists(), "pile directory missing");

    // 2 footprints
    let fp0 = pile_dir.join("00000");
    let fp1 = pile_dir.join("00001");
    assert!(fp0.exists(), "footprint 00000 missing");
    assert!(fp1.exists(), "footprint 00001 missing");

    // Each footprint has 16 COGs (SF=1 => scenes_per_footprint=16)
    let cog_count = std::fs::read_dir(&fp0)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "tif"))
        .count();
    assert_eq!(cog_count, 16, "expected 16 COGs per footprint");

    // Verify STAC catalogs (Narrow + Balanced; Wide is deferred)
    let stac_dir = dir.path().join("raster/stac");
    assert!(stac_dir.exists(), "stac directory missing");
    for name in ["narrow", "balanced"] {
        let path = stac_dir.join(format!("{name}.parquet"));
        assert!(path.exists(), "STAC {name}.parquet missing");
        let meta = std::fs::metadata(&path).unwrap();
        assert!(meta.len() > 0, "STAC {name}.parquet is empty");
    }
}

#[test]
fn raster_without_vector() {
    // When only --tables raster is set, only raster output is produced
    let dir = tempdir().unwrap();

    Command::cargo_bin("spatialbench-cli")
        .unwrap()
        .args([
            "--tables",
            "raster",
            "--scale-factor",
            "1",
            "--max-footprints",
            "1",
            "--output-dir",
            dir.path().to_str().unwrap(),
            "--verbose",
        ])
        .timeout(std::time::Duration::from_secs(300))
        .assert()
        .success();

    // Raster output exists
    assert!(dir.path().join("raster/pile/00000/0000.tif").exists());
    // STAC exists
    assert!(dir.path().join("raster/stac/narrow.parquet").exists());
}

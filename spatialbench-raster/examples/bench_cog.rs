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

//! Benchmark: Perlin noise generation + COG writing.
//!
//! Usage: `cargo run --release -p spatialbench-raster --features cog-writer --example bench_cog`

use spatialbench_raster::cog::{write_cog, CogConfig};
use spatialbench_raster::footprint::{lon_to_utm_zone, lonlat_to_utm, utm_to_lonlat, Footprint};
use spatialbench_raster::noise::PerlinNoise;

use std::hint::black_box;
use std::time::Instant;

fn test_footprint() -> Footprint {
    let lon = -100.0_f64;
    let lat = 35.0_f64;
    let zone = lon_to_utm_zone(lon);
    let (e, n) = lonlat_to_utm(lon, lat, zone);
    let step = 109_800.0;
    let nw = utm_to_lonlat(e, n, zone, true);
    let se = utm_to_lonlat(e + step, n - step, zone, true);
    Footprint {
        id: 0,
        epsg: 32600 + zone,
        origin: (e, n),
        bbox_4326: [
            nw.0.min(se.0),
            nw.1.min(se.1),
            nw.0.max(se.0),
            nw.1.max(se.1),
        ],
    }
}

fn bench_noise_only() {
    let iterations = 10u32;
    let noise = PerlinNoise::new(42);
    let mut buf = Vec::new();

    // Warmup
    noise.generate_raster_into(1830, 1830, 8.0, &mut buf);

    let start = Instant::now();
    for i in 0..iterations {
        let n = PerlinNoise::new(i as u64);
        n.generate_raster_into(1830, 1830, 8.0, &mut buf);
        black_box(&buf);
    }
    let elapsed = start.elapsed();

    let per_call = elapsed / iterations;
    let pixels = 1830u64 * 1830;
    let mpix_per_sec = (pixels as f64 / per_call.as_secs_f64()) / 1_000_000.0;

    println!("=== Perlin noise generation (1830x1830, f32) ===");
    println!("  {iterations} iterations in {elapsed:.2?}");
    println!("  {per_call:.2?} per raster");
    println!("  {mpix_per_sec:.1} Mpix/sec");
    println!("  Buffer reuse: {:.1} MB", buf.capacity() as f64 / 1e6);
}

fn bench_cog_write() {
    let iterations = 10u32;
    let config = CogConfig::default();
    let fp = test_footprint();
    let dir = tempfile::tempdir().unwrap();

    // Warmup
    write_cog(&config, &fp, 999, &dir.path().join("warmup.tif")).unwrap();

    let start = Instant::now();
    for i in 0..iterations {
        let path = dir.path().join(format!("{i:04}.tif"));
        write_cog(&config, &fp, i, &path).unwrap();
    }
    let elapsed = start.elapsed();

    let per_call = elapsed / iterations;
    let file_size = std::fs::metadata(dir.path().join("0000.tif"))
        .unwrap()
        .len();

    println!("\n=== Full COG write (noise + ZSTD + disk) ===");
    println!("  {iterations} iterations in {elapsed:.2?}");
    println!("  {per_call:.2?} per COG");
    println!(
        "  {:.1} COGs/sec",
        iterations as f64 / elapsed.as_secs_f64()
    );
    println!("  File size: {:.1} KB", file_size as f64 / 1024.0);

    // Breakdown: noise vs GDAL
    let mut buf = Vec::new();
    let noise_start = Instant::now();
    for i in 0..iterations {
        let n = PerlinNoise::new(i as u64);
        n.generate_raster_into(1830, 1830, 8.0, &mut buf);
        black_box(&buf);
    }
    let noise_elapsed = noise_start.elapsed();
    let encode_elapsed = elapsed - noise_elapsed;

    println!("\n=== Breakdown ===");
    println!(
        "  Noise:  {:.2?} ({:.0}%)",
        noise_elapsed,
        100.0 * noise_elapsed.as_secs_f64() / elapsed.as_secs_f64()
    );
    println!(
        "  Encode: {:.2?} ({:.0}%)",
        encode_elapsed,
        100.0 * encode_elapsed.as_secs_f64() / elapsed.as_secs_f64()
    );

    // Extrapolate SF=1
    let sf1_cogs: u64 = 1320 * 16;
    let sf1_time = per_call.as_secs_f64() * sf1_cogs as f64;
    println!("\n=== SF=1 projection (single-threaded) ===");
    println!(
        "  {sf1_cogs} COGs × {per_call:.2?} = {sf1_time:.0}s ({:.1} min)",
        sf1_time / 60.0
    );
}

fn main() {
    bench_noise_only();
    bench_cog_write();
}

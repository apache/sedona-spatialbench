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

//! Footprint grid generation using UTM-based tessellation.
//!
//! Each footprint is a fixed-metric-size tile (default 109,800m × 109,800m)
//! in the appropriate UTM zone, matching how real raster archives (Sentinel-2,
//! Landsat) tile data in projected coordinates.
//!
//! The UTM projection implementation follows the series expansion from
//! Snyder, J.P. (1987) "Map Projections — A Working Manual", USGS Professional
//! Paper 1395, equations 8-1 through 8-18 (forward) and 8-20 through 8-23 (inverse).

// WGS84 ellipsoid constants (Snyder 1987, Table 1).
const WGS84_A: f64 = 6_378_137.0; // Semi-major axis (meters)
const WGS84_F: f64 = 1.0 / 298.257223563; // Flattening
const WGS84_E2: f64 = 2.0 * WGS84_F - WGS84_F * WGS84_F; // Eccentricity squared
const WGS84_E_PRIME2: f64 = WGS84_E2 / (1.0 - WGS84_E2); // Second eccentricity squared
const UTM_K0: f64 = 0.9996; // UTM scale factor at central meridian
const UTM_FALSE_EASTING: f64 = 500_000.0;
const UTM_FALSE_NORTHING_SOUTH: f64 = 10_000_000.0;

/// A footprint: a fixed-metric-size tile in a UTM projection.
#[derive(Debug, Clone, Copy)]
pub struct Footprint {
    /// Unique footprint ID (0-based).
    pub id: u32,
    /// EPSG code for this footprint's CRS (e.g., 32617 for UTM zone 17N).
    pub epsg: u32,
    /// Origin (easting, northing) of the NW corner in the UTM CRS (meters).
    pub origin: (f64, f64),
    /// Bounding box in EPSG:4326: [west, south, east, north] degrees.
    /// Used for STAC catalog bbox field.
    pub bbox_4326: [f64; 4],
}

/// Configuration for footprint generation.
#[derive(Debug, Clone, Copy)]
pub struct FootprintConfig {
    /// COG width in pixels.
    pub cog_width: u32,
    /// COG height in pixels.
    pub cog_height: u32,
    /// Pixel resolution in meters.
    pub resolution: u32,
}

impl Default for FootprintConfig {
    fn default() -> Self {
        Self {
            cog_width: 1830,
            cog_height: 1830,
            resolution: 60,
        }
    }
}

impl FootprintConfig {
    /// Footprint step size in meters (east-west).
    pub fn step_x(&self) -> f64 {
        self.cog_width as f64 * self.resolution as f64
    }

    /// Footprint step size in meters (north-south).
    pub fn step_y(&self) -> f64 {
        self.cog_height as f64 * self.resolution as f64
    }
}

/// Generates footprints by tessellating a continent affine into a UTM-based grid.
///
/// The continent affine is selected via config (default: S. North America).
/// Footprint count is derived from tessellation — not from the scaling tier.
/// The scaling tier controls only temporal depth (scenes per footprint).
#[derive(Debug, Clone)]
pub struct FootprintGrid {
    affine: [f64; 6],
    config: FootprintConfig,
    max_footprints: Option<u32>,
}

impl FootprintGrid {
    /// Create a new footprint grid for a specific continent affine.
    ///
    /// `max_footprints` caps the number of footprints generated (dev flag).
    pub fn new(affine: [f64; 6], config: FootprintConfig, max_footprints: Option<u32>) -> Self {
        Self {
            affine,
            config,
            max_footprints,
        }
    }

    /// Generate footprints by tessellating the continent extent.
    ///
    /// Footprint count is determined by the continent's geographic extent
    /// and the configured resolution/tile dimensions. The `--max-footprints`
    /// CLI flag caps the result for fast iteration.
    pub fn generate(&self) -> Vec<Footprint> {
        let affine = self.affine;
        // Affine layout: [width_deg, shx, west, shy, -height_deg, north]
        let west = affine[2];
        let east = west + affine[0];
        let north = affine[5];
        let south = north + affine[4]; // affine[4] is negative

        let step_x = self.config.step_x();
        let step_y = self.config.step_y();

        let limit = self.max_footprints.unwrap_or(u32::MAX);

        let mut footprints = Vec::with_capacity(limit.min(4096) as usize);

        // Iterate over UTM zones that intersect the extent
        let zone_start = lon_to_utm_zone(west);
        let zone_end = lon_to_utm_zone(east);

        for zone in zone_start..=zone_end {
            if footprints.len() as u32 >= limit {
                break;
            }

            // Zone's longitude bounds
            let zone_west_lon = (zone as f64 - 1.0) * 6.0 - 180.0;
            let zone_east_lon = zone_west_lon + 6.0;

            // Clip to continent extent
            let clip_west = west.max(zone_west_lon);
            let clip_east = east.min(zone_east_lon);
            let clip_south = south;
            let clip_north = north;

            // Convert clipped corners to UTM meters
            let (min_e, min_n) = lonlat_to_utm(clip_west, clip_south, zone);
            let (max_e, max_n) = lonlat_to_utm(clip_east, clip_north, zone);

            // Align to grid
            let start_e = (min_e / step_x).floor() * step_x;
            let start_n = (min_n / step_y).floor() * step_y;

            let mut northing = max_n;
            while northing - step_y >= start_n {
                let mut easting = start_e;
                while easting + step_x <= max_e {
                    if footprints.len() as u32 >= limit {
                        break;
                    }

                    // NW corner of this tile
                    let origin = (easting, northing);

                    // Determine hemisphere from tile center latitude
                    let center_lat =
                        utm_to_lonlat(easting + step_x / 2.0, northing - step_y / 2.0, zone, true)
                            .1;
                    let is_north = center_lat >= 0.0;
                    let epsg = if is_north { 32600 + zone } else { 32700 + zone };

                    // Compute 4326 bbox from the NW and SE corners
                    let nw = utm_to_lonlat(easting, northing, zone, is_north);
                    let se = utm_to_lonlat(easting + step_x, northing - step_y, zone, is_north);

                    let bbox_4326 = [
                        nw.0.min(se.0), // west
                        nw.1.min(se.1), // south
                        nw.0.max(se.0), // east
                        nw.1.max(se.1), // north
                    ];

                    footprints.push(Footprint {
                        id: footprints.len() as u32,
                        epsg,
                        origin,
                        bbox_4326,
                    });

                    easting += step_x;
                }
                northing -= step_y;
            }
        }

        footprints.truncate(limit as usize);
        footprints
    }
}

/// Determine UTM zone number (1-60) from longitude.
pub fn lon_to_utm_zone(lon: f64) -> u32 {
    (((lon + 180.0) / 6.0).floor() as u32 + 1).clamp(1, 60)
}

/// Convert (longitude, latitude) to UTM (easting, northing) in a given zone.
///
/// Snyder 1987, equations 8-1 through 8-6 (forward Transverse Mercator).
pub fn lonlat_to_utm(lon: f64, lat: f64, zone: u32) -> (f64, f64) {
    let lon0 = ((zone as f64 - 1.0) * 6.0 - 180.0 + 3.0).to_radians();
    let lat_rad = lat.to_radians();
    let lon_rad = lon.to_radians();

    let n = WGS84_A / (1.0 - WGS84_E2 * lat_rad.sin().powi(2)).sqrt();
    let t = lat_rad.tan().powi(2);
    let c = WGS84_E_PRIME2 * lat_rad.cos().powi(2);
    let aa = (lon_rad - lon0) * lat_rad.cos();

    let m = meridian_arc(lat_rad);

    let easting = UTM_K0
        * n
        * (aa
            + (1.0 - t + c) * aa.powi(3) / 6.0
            + (5.0 - 18.0 * t + t * t + 72.0 * c - 58.0 * WGS84_E_PRIME2) * aa.powi(5) / 120.0)
        + UTM_FALSE_EASTING;

    let northing = UTM_K0
        * (m + n
            * lat_rad.tan()
            * (aa * aa / 2.0
                + (5.0 - t + 9.0 * c + 4.0 * c * c) * aa.powi(4) / 24.0
                + (61.0 - 58.0 * t + t * t + 600.0 * c - 330.0 * WGS84_E_PRIME2) * aa.powi(6)
                    / 720.0));

    let northing = if lat < 0.0 {
        northing + UTM_FALSE_NORTHING_SOUTH
    } else {
        northing
    };

    (easting, northing)
}

/// Convert UTM (easting, northing) to (longitude, latitude) in a given zone.
///
/// Snyder 1987, equations 8-20 through 8-23 (inverse Transverse Mercator).
pub fn utm_to_lonlat(easting: f64, northing: f64, zone: u32, is_north: bool) -> (f64, f64) {
    let e1 = (1.0 - (1.0 - WGS84_E2).sqrt()) / (1.0 + (1.0 - WGS84_E2).sqrt());

    let lon0 = ((zone as f64 - 1.0) * 6.0 - 180.0 + 3.0).to_radians();

    let x = easting - UTM_FALSE_EASTING;
    let y = if is_north {
        northing
    } else {
        northing - UTM_FALSE_NORTHING_SOUTH
    };

    let m = y / UTM_K0;
    let mu = m
        / (WGS84_A
            * (1.0
                - WGS84_E2 / 4.0
                - 3.0 * WGS84_E2 * WGS84_E2 / 64.0
                - 5.0 * WGS84_E2.powi(3) / 256.0));

    let phi1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1.powi(3) / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1 * e1 / 16.0 - 55.0 * e1.powi(4) / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1.powi(3) / 96.0) * (6.0 * mu).sin();

    let n1 = WGS84_A / (1.0 - WGS84_E2 * phi1.sin().powi(2)).sqrt();
    let t1 = phi1.tan().powi(2);
    let c1 = WGS84_E_PRIME2 * phi1.cos().powi(2);
    let r1 = WGS84_A * (1.0 - WGS84_E2) / (1.0 - WGS84_E2 * phi1.sin().powi(2)).powf(1.5);
    let d = x / (n1 * UTM_K0);

    let lat = phi1
        - (n1 * phi1.tan() / r1)
            * (d * d / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1 * c1 - 9.0 * WGS84_E_PRIME2) * d.powi(4)
                    / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1 * t1
                    - 252.0 * WGS84_E_PRIME2
                    - 3.0 * c1 * c1)
                    * d.powi(6)
                    / 720.0);

    let lon = lon0
        + (d - (1.0 + 2.0 * t1 + c1) * d.powi(3) / 6.0
            + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1 * c1 + 8.0 * WGS84_E_PRIME2 + 24.0 * t1 * t1)
                * d.powi(5)
                / 120.0)
            / phi1.cos();

    (lon.to_degrees(), lat.to_degrees())
}

/// Compute meridian arc length from equator to latitude `phi` (Snyder 1987, eq. 3-21).
fn meridian_arc(phi: f64) -> f64 {
    let e4 = WGS84_E2 * WGS84_E2;
    let e6 = e4 * WGS84_E2;
    WGS84_A
        * ((1.0 - WGS84_E2 / 4.0 - 3.0 * e4 / 64.0 - 5.0 * e6 / 256.0) * phi
            - (3.0 * WGS84_E2 / 8.0 + 3.0 * e4 / 32.0 + 45.0 * e6 / 1024.0) * (2.0 * phi).sin()
            + (15.0 * e4 / 256.0 + 45.0 * e6 / 1024.0) * (4.0 * phi).sin()
            - (35.0 * e6 / 3072.0) * (6.0 * phi).sin())
}

#[cfg(test)]
mod tests {
    use super::*;
    use spatialbench::spatial::ContinentAffines;

    /// Default S. North America affine for tests.
    fn test_affine() -> [f64; 6] {
        ContinentAffines::default().south_north_america
    }

    #[test]
    fn utm_zone_assignment() {
        assert_eq!(lon_to_utm_zone(-124.0), 10);
        assert_eq!(lon_to_utm_zone(-70.0), 19);
        assert_eq!(lon_to_utm_zone(-93.0), 15);
        assert_eq!(lon_to_utm_zone(0.0), 31);
    }

    #[test]
    fn utm_roundtrip() {
        let lon = -100.0;
        let lat = 35.0;
        let zone = lon_to_utm_zone(lon);
        let (e, n) = lonlat_to_utm(lon, lat, zone);
        let (lon2, lat2) = utm_to_lonlat(e, n, zone, true);
        assert!((lon - lon2).abs() < 1e-6, "lon: {lon} vs {lon2}");
        assert!((lat - lat2).abs() < 1e-6, "lat: {lat} vs {lat2}");
    }

    #[test]
    fn utm_roundtrip_edge_cases() {
        for &lon in &[-124.0, -69.0, -90.0] {
            for &lat in &[12.0, 42.0, 30.0] {
                let zone = lon_to_utm_zone(lon);
                let (e, n) = lonlat_to_utm(lon, lat, zone);
                let (lon2, lat2) = utm_to_lonlat(e, n, zone, lat >= 0.0);
                assert!(
                    (lon - lon2).abs() < 1e-5,
                    "roundtrip failed for ({lon}, {lat}): got ({lon2}, {lat2})"
                );
                assert!(
                    (lat - lat2).abs() < 1e-5,
                    "roundtrip failed for ({lon}, {lat}): got ({lon2}, {lat2})"
                );
            }
        }
    }

    #[test]
    fn generates_footprints() {
        let grid = FootprintGrid::new(test_affine(), FootprintConfig::default(), None);
        let footprints = grid.generate();
        assert!(
            footprints.len() > 1000,
            "expected >1000 footprints, got {}",
            footprints.len()
        );
    }

    #[test]
    fn max_footprints_caps() {
        let grid = FootprintGrid::new(test_affine(), FootprintConfig::default(), Some(5));
        let footprints = grid.generate();
        assert_eq!(footprints.len(), 5);
    }

    #[test]
    fn footprint_has_valid_bbox() {
        let grid = FootprintGrid::new(test_affine(), FootprintConfig::default(), Some(10));
        let footprints = grid.generate();
        for fp in &footprints {
            assert!(
                fp.bbox_4326[0] < fp.bbox_4326[2],
                "fp {}: invalid bbox",
                fp.id
            );
            assert!(
                fp.bbox_4326[1] < fp.bbox_4326[3],
                "fp {}: invalid bbox",
                fp.id
            );
            let width = fp.bbox_4326[2] - fp.bbox_4326[0];
            let height = fp.bbox_4326[3] - fp.bbox_4326[1];
            assert!(
                width > 0.5 && width < 3.0,
                "fp {}: width {width}° unexpected",
                fp.id
            );
            assert!(
                height > 0.5 && height < 3.0,
                "fp {}: height {height}° unexpected",
                fp.id
            );
        }
    }

    #[test]
    fn footprint_epsg_is_valid_utm() {
        let grid = FootprintGrid::new(test_affine(), FootprintConfig::default(), Some(10));
        let footprints = grid.generate();
        for fp in &footprints {
            assert!(
                fp.epsg >= 32601 && fp.epsg <= 32660,
                "fp {}: invalid EPSG {}",
                fp.id,
                fp.epsg
            );
        }
    }

    #[test]
    fn footprint_ids_sequential() {
        let grid = FootprintGrid::new(test_affine(), FootprintConfig::default(), Some(20));
        let footprints = grid.generate();
        for (i, fp) in footprints.iter().enumerate() {
            assert_eq!(fp.id, i as u32);
        }
    }
}

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

//! Geometry validation tests for generated spatial data.
//!
//! This test suite validates that all generated geometries meet quality and correctness standards:
//! - **Coordinate validity**: Longitude [-180°, 180°], latitude [-90°, 90°]
//! - **OGC compliance**: Valid topology, counter-clockwise winding for exterior rings
//! - **Precision constraints**: Coordinates limited to 9 decimal places
//! - **Antimeridian handling**: No dateline crossings in generated polygons
//!
//! These tests ensure spatial data quality and compatibility with spatial databases and GIS tools.

#[cfg(test)]
mod geometry_tests {
    use geo::{Validation, Winding};
    use spatialbench::generators::{BuildingGenerator, TripGenerator};
    use spatialbench::spatial::geometry::GEOMETRY_PRECISION;
    use spatialbench::spatial::utils::crosses_dateline;

    #[test]
    fn test_trip_coordinates_are_valid() {
        let generator = TripGenerator::new(0.1, 1, 1);
        let trips: Vec<_> = generator.iter().collect();

        for trip in trips {
            // Check pickup coordinates are valid
            assert!(
                trip.t_pickuploc.x() >= -180.0 && trip.t_pickuploc.x() <= 180.0,
                "Pickup longitude out of range: {}",
                trip.t_pickuploc.x()
            );
            assert!(
                trip.t_pickuploc.y() >= -90.0 && trip.t_pickuploc.y() <= 90.0,
                "Pickup latitude out of range: {}",
                trip.t_pickuploc.y()
            );

            // Check dropoff coordinates are valid
            assert!(
                trip.t_dropoffloc.x() >= -180.0 && trip.t_dropoffloc.x() <= 180.0,
                "Dropoff longitude out of range: {}",
                trip.t_dropoffloc.x()
            );
            assert!(
                trip.t_dropoffloc.y() >= -90.0 && trip.t_dropoffloc.y() <= 90.0,
                "Dropoff latitude out of range: {}",
                trip.t_dropoffloc.y()
            );

            // Check coordinates have proper precision (9 decimal places)
            let pickup_x_precision =
                (trip.t_pickuploc.x() * GEOMETRY_PRECISION).round() / GEOMETRY_PRECISION;
            assert_eq!(trip.t_pickuploc.x(), pickup_x_precision);

            let dropoff_x_precision =
                (trip.t_dropoffloc.x() * GEOMETRY_PRECISION).round() / GEOMETRY_PRECISION;
            assert_eq!(trip.t_dropoffloc.x(), dropoff_x_precision);
        }
    }

    #[test]
    fn test_building_polygons_are_valid() {
        let generator = BuildingGenerator::new(10.0, 1, 1);
        let buildings: Vec<_> = generator.iter().collect();

        for building in buildings {
            let polygon = &building.b_boundary;

            assert!(
                polygon.is_valid(),
                "Building {} has invalid polygon",
                building.b_buildingkey
            );
        }
    }

    #[test]
    fn test_building_polygon_winding() {
        // Create a generator with a small scale factor
        let generator = BuildingGenerator::new(1.0, 1, 1);
        let buildings: Vec<_> = generator.iter().collect();

        // Check that all building polygons have counter-clockwise winding
        for building in buildings.iter() {
            let exterior = building.b_boundary.exterior();
            assert!(
                exterior.is_ccw(),
                "Building {} polygon should have counter-clockwise winding",
                building.b_buildingkey
            );
        }
    }

    #[test]
    fn test_coordinate_precision() {
        let generator = TripGenerator::new(0.01, 1, 1);
        let trips: Vec<_> = generator.iter().collect();

        for trip in trips {
            // Check 9 decimal place precision
            let pickup_x_str = format!("{:.9}", trip.t_pickuploc.x());
            let pickup_y_str = format!("{:.9}", trip.t_pickuploc.y());
            let dropoff_x_str = format!("{:.9}", trip.t_dropoffloc.x());
            let dropoff_y_str = format!("{:.9}", trip.t_dropoffloc.y());

            // Verify no extra precision beyond 9 decimals
            let pickup_x_parsed: f64 = pickup_x_str.parse().unwrap();
            let pickup_y_parsed: f64 = pickup_y_str.parse().unwrap();
            let dropoff_x_parsed: f64 = dropoff_x_str.parse().unwrap();
            let dropoff_y_parsed: f64 = dropoff_y_str.parse().unwrap();

            assert_eq!(trip.t_pickuploc.x(), pickup_x_parsed);
            assert_eq!(trip.t_pickuploc.y(), pickup_y_parsed);
            assert_eq!(trip.t_dropoffloc.x(), dropoff_x_parsed);
            assert_eq!(trip.t_dropoffloc.y(), dropoff_y_parsed);
        }
    }

    #[test]
    fn test_building_dateline_crossing() {
        use spatialbench::generators::BuildingGenerator;

        let generator = BuildingGenerator::new(1000.0, 1, 1);
        let buildings: Vec<_> = generator.iter().collect();

        for building in buildings {
            let polygon = &building.b_boundary;

            assert_eq!(
                crosses_dateline(polygon),
                false,
                "Building {} polygon crosses dateline: {:?}",
                building.b_buildingkey,
                building.b_boundary
            );
        }
    }
}

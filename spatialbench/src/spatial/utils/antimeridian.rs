use geo::{Centroid, LineString, Polygon};

/// Wraps a longitude value to ensure it stays within the valid range of [-180, 180] degrees.
///
/// Longitude is a circular coordinate:
/// - If longitude exceeds 180°, it wraps around from the eastern hemisphere back to the western hemisphere.
/// - If longitude is below -180°, it wraps around from the western hemisphere back to the eastern hemisphere.
pub fn wrap_around_longitude(mut lon: f64) -> f64 {
    while lon > 180.0 {
        lon -= 360.0;
    }
    while lon < -180.0 {
        lon += 360.0;
    }
    lon
}

/// Checks if a polygon crosses the dateline (antimeridian at ±180°)
pub fn crosses_dateline(polygon: &Polygon) -> bool {
    let coords = polygon.exterior().coords();
    let mut has_east = false;
    let mut has_west = false;

    for coord in coords {
        if (coord.x > 90.0 && coord.x <= 180.0) || coord.x < -180.0 {
            has_east = true;
        }
        if coord.x > 180.0 || (coord.x >= -180.0 && coord.x < -90.0) {
            has_west = true;
        }
        if has_east && has_west {
            return true;
        }
    }
    false
}

/// Clamps a polygon's longitude coordinates to one side of the antimeridian (±180°).
///
/// When a polygon crosses the dateline, this function constrains its coordinates to remain
/// on either the eastern (0° to 180°) or western (-180° to 0°) hemisphere based on where
/// the polygon's centroid is located.
///
/// # Behavior
/// - If the centroid is in the eastern hemisphere (≥ 0°), coordinates are clamped to [0°, 180°]
///   or kept at their original values if already within [-180°, 0°]
/// - If the centroid is in the western hemisphere (< 0°), coordinates are clamped to [-180°, 0°]
///   or kept at their original values if already within [0°, 180°]
/// - Latitude values (y-coordinates) remain unchanged
///
pub fn clamp_polygon_to_dateline(polygon: &Polygon) -> Polygon {
    let centroid = polygon.centroid().expect("Polygon should have centroid");
    let east_bound = centroid.x() >= 0.0;
    let keep_east = (centroid.x() >= 0.0 && centroid.x() <= 180.0) || (centroid.x() < -180.0);

    let exterior_coords: Vec<_> = polygon
        .exterior()
        .coords()
        .map(|coord| {
            let clamped_x = if keep_east {
                if east_bound {
                    coord.x.clamp(0.0, 180.0)
                } else {
                    coord.x.max(-180.0)
                }
            } else if east_bound {
                coord.x.min(180.0)
            } else {
                coord.x.clamp(-180.0, 0.0)
            };
            geo::Coord {
                x: clamped_x,
                y: coord.y,
            }
        })
        .collect();

    if exterior_coords.len() >= 4 {
        Polygon::new(LineString::from(exterior_coords), vec![])
    } else {
        polygon.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::polygon;

    #[test]
    fn test_wrap_around_longitude_positive_overflow() {
        assert_eq!(wrap_around_longitude(190.0), -170.0);
        assert_eq!(wrap_around_longitude(240.0), -120.0);
    }

    #[test]
    fn test_wrap_around_longitude_negative_overflow() {
        assert_eq!(wrap_around_longitude(-190.0), 170.0);
        assert_eq!(wrap_around_longitude(-240.0), 120.0);
    }

    #[test]
    fn test_wrap_around_longitude_within_range() {
        assert_eq!(wrap_around_longitude(0.0), 0.0);
        assert_eq!(wrap_around_longitude(90.0), 90.0);
        assert_eq!(wrap_around_longitude(-90.0), -90.0);
        assert_eq!(wrap_around_longitude(180.0), 180.0);
        assert_eq!(wrap_around_longitude(-180.0), -180.0);
    }

    #[test]
    fn test_crosses_dateline_no_crossing() {
        let poly = polygon![
            (x: 10.0, y: 10.0),
            (x: 20.0, y: 10.0),
            (x: 20.0, y: 20.0),
            (x: 10.0, y: 20.0),
            (x: 10.0, y: 10.0),
        ];
        assert!(!crosses_dateline(&poly));
    }

    #[test]
    fn test_crosses_dateline_crossing() {
        let mut poly = polygon![
            (x: 170.0, y: 10.0),
            (x: -170.0, y: 10.0),
            (x: -170.0, y: 20.0),
            (x: 170.0, y: 20.0),
            (x: 170.0, y: 10.0),
        ];
        assert!(crosses_dateline(&poly));

        poly = polygon![
            (x: -160.0, y: 10.0),
            (x: -170.0, y: 10.0),
            (x: -170.0, y: 20.0),
            (x: -160.0, y: 20.0),
            (x: -160.0, y: 10.0),
        ];
        assert!(!crosses_dateline(&poly));
    }

    #[test]
    fn test_clamp_polygon_to_dateline_positive_side() {
        let mut poly = polygon![
            (x: 170.0, y: 10.0),
            (x: 180.0, y: 10.0),
            (x: 180.0, y: 20.0),
            (x: 170.0, y: 20.0),
            (x: 170.0, y: 10.0),
        ];
        let mut clamped = clamp_polygon_to_dateline(&poly);

        // Polygon should be preserved appropriately
        assert_eq!(clamped, poly);

        poly = polygon![
            (x: 170.0, y: 10.0),
            (x: 185.0, y: 10.0),
            (x: 185.0, y: 20.0),
            (x: 170.0, y: 20.0),
            (x: 170.0, y: 10.0),
        ];
        clamped = clamp_polygon_to_dateline(&poly);

        let expected = polygon![
            (x: 170.0, y: 10.0),
            (x: 180.0, y: 10.0),
            (x: 180.0, y: 20.0),
            (x: 170.0, y: 20.0),
            (x: 170.0, y: 10.0),
        ];

        // Polygon should be clamped appropriately
        assert_eq!(clamped, expected);
    }

    #[test]
    fn test_clamp_polygon_to_dateline_with_centroid_on_dateline() {
        // East bound polygon
        let mut poly = polygon![
            (x: 170.0, y: 10.0),
            (x: 190.0, y: 10.0),
            (x: 190.0, y: 20.0),
            (x: 170.0, y: 20.0),
            (x: 170.0, y: 10.0),
        ];
        let mut clamped = clamp_polygon_to_dateline(&poly);

        let mut expected = polygon![
            (x: 170.0, y: 10.0),
            (x: 180.0, y: 10.0),
            (x: 180.0, y: 20.0),
            (x: 170.0, y: 20.0),
            (x: 170.0, y: 10.0),
        ];

        // Polygon should be preserved appropriately
        assert_eq!(clamped, expected);

        // West bound polygon
        poly = polygon![
            (x: -170.0, y: 10.0),
            (x: -190.0, y: 10.0),
            (x: -190.0, y: 20.0),
            (x: -170.0, y: 20.0),
            (x: -170.0, y: 10.0),
        ];
        clamped = clamp_polygon_to_dateline(&poly);

        expected = polygon![
            (x: -170.0, y: 10.0),
            (x: -180.0, y: 10.0),
            (x: -180.0, y: 20.0),
            (x: -170.0, y: 20.0),
            (x: -170.0, y: 10.0),
        ];

        // Polygon should be clamped appropriately
        assert_eq!(clamped, expected);
    }

    #[test]
    fn test_clamp_polygon_to_dateline_negative_side() {
        let mut poly = polygon![
            (x: -170.0, y: 10.0),
            (x: -180.0, y: 10.0),
            (x: -180.0, y: 20.0),
            (x: -170.0, y: 20.0),
            (x: -170.0, y: 10.0),
        ];
        let mut clamped = clamp_polygon_to_dateline(&poly);

        // Polygon should be preserved appropriately
        assert_eq!(clamped, poly);

        poly = polygon![
            (x: -170.0, y: 10.0),
            (x: -185.0, y: 10.0),
            (x: -185.0, y: 20.0),
            (x: -170.0, y: 20.0),
            (x: -170.0, y: 10.0),
        ];
        clamped = clamp_polygon_to_dateline(&poly);

        let expected = polygon![
            (x: -170.0, y: 10.0),
            (x: -180.0, y: 10.0),
            (x: -180.0, y: 20.0),
            (x: -170.0, y: 20.0),
            (x: -170.0, y: 10.0),
        ];

        // Polygon should be clamped appropriately
        assert_eq!(clamped, expected);
    }
}

#[inline]
pub fn apply_affine(x: f64, y: f64, m: &[f64; 6]) -> (f64, f64) {
    (m[0] * x + m[1] * y + m[2], m[3] * x + m[4] * y + m[5])
}

#[inline]
pub fn round_coordinate(coord: f64, precision: f64) -> f64 {
    (coord * precision).round() / precision
}

#[inline]
pub fn round_coordinates(x: f64, y: f64, precision: f64) -> (f64, f64) {
    (
        round_coordinate(x, precision),
        round_coordinate(y, precision),
    )
}

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

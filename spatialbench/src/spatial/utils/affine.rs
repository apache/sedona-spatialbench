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
    (round_coordinate(x, precision), round_coordinate(y, precision))
}
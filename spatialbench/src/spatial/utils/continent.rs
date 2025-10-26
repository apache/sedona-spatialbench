use crate::spatial::ContinentAffines;
use std::f64::consts::PI;

#[derive(Clone, Debug)]
pub struct WeightedTarget {
    pub m: [f64; 6],
    pub cdf: f64,
}

#[inline]
fn bbox_from_affine(m: &[f64; 6]) -> (f64, f64, f64, f64) {
    let (a, c, e, f) = (m[0], m[2], m[4], m[5]);
    let (west, east) = if a >= 0.0 { (c, c + a) } else { (c + a, c) };
    let (south, north) = if e >= 0.0 { (f, f + e) } else { (f + e, f) };
    (west, east, south, north)
}

#[inline]
fn spherical_bbox_weight(west: f64, east: f64, south: f64, north: f64) -> f64 {
    let deg2rad = PI / 180.0;
    let width = (east - west).abs() * deg2rad;
    let (phi_s, phi_n) = (south * deg2rad, north * deg2rad);
    let band = (phi_n.sin() - phi_s.sin()).max(0.0);
    (width * band).max(0.0)
}

pub fn build_continent_cdf(aff: &ContinentAffines) -> Vec<(&str, [f64; 6], f64)> {
    let items = [
        ("africa", &aff.africa),
        ("europe", &aff.europe),
        ("south_asia", &aff.south_asia),
        ("north_asia", &aff.north_asia),
        ("oceania", &aff.oceania),
        ("south_america", &aff.south_america),
        ("south_north_america", &aff.south_north_america),
        ("north_north_america", &aff.north_north_america),
    ];

    let mut targets: Vec<(&str, [f64; 6], f64)> = items
        .iter()
        .map(|(name, m)| {
            let (w, e, s, n) = bbox_from_affine(m);
            let wt = spherical_bbox_weight(w, e, s, n);
            (*name, **m, wt)
        })
        .collect();

    targets.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

    let total: f64 = targets.iter().map(|(_, _, w)| w).sum();
    let mut acc = 0.0;
    for (_, _, w) in &mut targets {
        acc += *w;
        *w = acc / total.max(1e-12);
    }
    targets
}

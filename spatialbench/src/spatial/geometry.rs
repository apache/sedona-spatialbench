use crate::spatial::{SpatialConfig, GeomType};
use crate::spatial::utils::{apply_affine, round_coordinates};
use geo::{coord, Geometry, LineString, Point, Polygon};
use rand::rngs::StdRng;
use rand::Rng;
use std::f64::consts::PI;

const GEOMETRY_PRECISION: f64 = 1_000_000_000.0;

pub fn emit_geom(
    center01: (f64, f64),
    geom_type: GeomType,
    config: &SpatialConfig,
    rng: &mut StdRng,
    m: &[f64; 6],
) -> Geometry {
    match geom_type {
        GeomType::Point => generate_point_geom(center01, m),
        GeomType::Box => generate_box_geom(center01, config, rng, m),
        GeomType::Polygon => generate_polygon_geom(center01, config, rng, m),
    }
}

pub fn generate_point_geom(center: (f64, f64), m: &[f64; 6]) -> Geometry {
    let (x, y) = apply_affine(center.0, center.1, m);
    let (x, y) = round_coordinates(x, y, GEOMETRY_PRECISION);
    Geometry::Point(Point::new(x, y))
}

pub fn generate_box_geom(center: (f64, f64), config: &SpatialConfig, rng: &mut StdRng, m: &[f64; 6]) -> Geometry {
    let half_width = rng.gen::<f64>() * config.width / 2.0;
    let half_height = rng.gen::<f64>() * config.height / 2.0;

    let corners = [
        (center.0 - half_width, center.1 - half_height),
        (center.0 + half_width, center.1 - half_height),
        (center.0 + half_width, center.1 + half_height),
        (center.0 - half_width, center.1 + half_height),
        (center.0 - half_width, center.1 - half_height),
    ];

    let coords: Vec<_> = corners
        .iter()
        .map(|&(x, y)| apply_affine(x, y, m))
        .map(|(x, y)| round_coordinates(x, y, GEOMETRY_PRECISION))
        .map(|(x, y)| coord! { x: x, y: y })
        .collect();

    Geometry::Polygon(Polygon::new(LineString::from(coords), vec![]))
}

pub fn generate_polygon_geom(center: (f64, f64), config: &SpatialConfig, rng: &mut StdRng, m: &[f64; 6]) -> Geometry {
    let min_segs = 3;
    let num_segments = if config.maxseg <= 3 {
        3
    } else {
        rng.gen_range(0..=(config.maxseg - min_segs)) + min_segs
    };

    let mut angles: Vec<f64> = (0..num_segments).map(|_| rng.gen::<f64>() * 2.0 * PI).collect();
    angles.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut ring = angles
        .iter()
        .map(|&ang| {
            let (x0, y0) = (center.0 + config.polysize * ang.cos(), center.1 + config.polysize * ang.sin());
            let (x1, y1) = (x0.clamp(0.0, 1.0), y0.clamp(0.0, 1.0));
            let (x2, y2) = apply_affine(x1, y1, m);
            let (xr, yr) = round_coordinates(x2, y2, GEOMETRY_PRECISION);
            coord! { x: xr, y: yr }
        })
        .collect::<Vec<_>>();

    if let Some(first) = ring.first().copied() {
        ring.push(first);
    }

    Geometry::Polygon(Polygon::new(LineString::from(ring), vec![]))
}
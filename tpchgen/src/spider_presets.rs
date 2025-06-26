use crate::spider::{SpiderGenerator, SpiderConfig, DistributionType, DistributionParams, GeomType};

pub struct SpiderPresets;

impl SpiderPresets {
    pub fn for_trip_pickups() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Uniform,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 42,
            affine: Some([
                58.368269, 0.0, -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0, 25.175375, 24.006328,   // scale Y to 25.18°, offset to 24.00°
            ]),

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::None,
        };
        SpiderGenerator::new(config)
    }

    pub fn for_building_polygons() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Parcel,
            geom_type: GeomType::Box,
            dim: 2,
            seed: 12345,
            affine: Some([
                58.368269, 0.0, -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0, 25.175375, 24.006328,   // scale Y to 25.18°, offset to 24.00°
            ]),

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::Parcel { srange: 0.1, dither: 2.0 },
        };
        SpiderGenerator::new(config)
    }
}
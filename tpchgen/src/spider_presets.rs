use crate::spider::{
    DistributionParams, DistributionType, GeomType, SpiderConfig, SpiderGenerator,
};

pub struct SpiderPresets;

impl SpiderPresets {
    pub fn for_trip_pickups() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Uniform,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 42,
            affine: Some([
                58.368269,
                0.0,
                -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0,
                25.175375,
                24.006328, // scale Y to 25.18°, offset to 24.00°
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

    pub fn for_trip_pickups2() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Diagonal,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 42,
            affine: Some([
                58.368269,
                0.0,
                -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0,
                25.175375,
                24.006328, // scale Y to 25.18°, offset to 24.00°
            ]),

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::Diagonal {
                percentage: 0.5,
                buffer: 0.5,
            },
        };
        SpiderGenerator::new(config)
    }

    pub fn for_trip_pickups3() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Sierpinski,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 42,
            affine: Some([
                58.368269,
                0.0,
                -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0,
                25.175375,
                24.006328, // scale Y to 25.18°, offset to 24.00°
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

    pub fn for_trip_pickups4() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Bit,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 42,
            affine: Some([
                58.368269,
                0.0,
                -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0,
                25.175375,
                24.006328, // scale Y to 25.18°, offset to 24.00°
            ]),

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::Bit {
                probability: 0.2,
                digits: 10,
            },
        };
        SpiderGenerator::new(config)
    }

    pub fn for_trip_pickups5() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Normal,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 42,
            affine: Some([
                58.368269,
                0.0,
                -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0,
                25.175375,
                24.006328, // scale Y to 25.18°, offset to 24.00°
            ]),

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::Normal {
                mu: 0.5,
                sigma: 0.1,
            },
        };
        SpiderGenerator::new(config)
    }

    pub fn for_building_polygons() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Bit,
            geom_type: GeomType::Box,
            dim: 2,
            seed: 12345,
            affine: Some([
                58.368269,
                0.0,
                -125.244606, // scale X to 58.37°, offset to -125.24°
                0.0,
                25.175375,
                24.006328, // scale Y to 25.18°, offset to 24.00°
            ]),

            // geometry = box
            width: 0.00005,
            height: 0.0001,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::Bit {
                probability: 0.5,
                digits: 20,
            },
        };
        SpiderGenerator::new(config)
    }
}

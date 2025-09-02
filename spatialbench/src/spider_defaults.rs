use crate::spider::{ContinentAffines, DistributionParams, DistributionType, GeomType, SpiderConfig, SpiderGenerator};

pub struct SpiderDefaults;

impl Default for ContinentAffines {
    fn default() -> Self {
        Self {
            eurasia:       [190.0, 0.0, -10.0, 0.0, -70.0, 70.0],
            north_america: [116.0, 0.0, -168.0, 0.0, -55.0, 60.0],
            south_america: [48.0, 0.0, -82.0, 0.0, -69.0, 13.0],
            oceania:       [70.0, 0.0, 110.0, 0.0, -50.0, 0.0],
            africa:        [70.0, 0.0, -18.0, 0.0, -73.0, 38.0],
        }
    }
}

impl SpiderDefaults {
    const FULL_WORLD_AFFINE: [f64; 6] = [
        360.0, // Scale X to cover full longitude range (-180° to 180°)
        0.0, -180.0, // Offset X to start at -180° (west edge of map)
        0.0, -160.0, // Scale Y: maps unit square [0,1] to latitude range [80°, -80°]
        80.0,   // Offset Y to start at 80° (north edge of map)
    ];

    pub fn trip_default() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Bit,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 42,
            continent_affines: Some(ContinentAffines::default()),

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::Bit {
                probability: 0.2,
                digits: 30,
            },
        };
        SpiderGenerator::new(config)
    }

    pub fn building_default() -> SpiderGenerator {
        let config = SpiderConfig {
            dist_type: DistributionType::Sierpinski,
            geom_type: GeomType::Polygon,
            dim: 2,
            seed: 12345,
            continent_affines: Some(ContinentAffines::default()),

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 5,
            polysize: 0.000039,

            params: DistributionParams::None,
        };
        SpiderGenerator::new(config)
    }
}

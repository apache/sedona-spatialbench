use crate::spatial::{
    ContinentAffines, DistributionParams, DistributionType, GeomType, SpatialConfig,
    SpatialGenerator,
};
use std::sync::OnceLock;

pub struct SpatialDefaults;

impl Default for ContinentAffines {
    fn default() -> Self {
        Self {
            africa: [84.194319, 0.0, -20.062752, 0.0, -77.623846, 37.579421],
            europe: [76.108853, 0.0, -11.964479, 0.0, 33.901968, 37.926872],
            south_asia: [80.942556, 0.0, 64.583540, 0.0, -61.381606, 51.672557],
            north_asia: [114.339049, 0.0, 64.495655, 0.0, 25.952988, 51.944267],
            oceania: [68.287041, 0.0, 112.481901, 0.0, -38.751779, -10.228433],
            south_america: [49.92948, 0.0, -83.833822, 0.0, -68.381204, 12.211188],
            south_north_america: [55.379532, 0.0, -124.890724, 0.0, -30.170149, 42.55308],
            north_north_america: [114.424763, 0.0, -166.478008, 0.0, -29.9779543, 72.659041],
        }
    }
}

impl SpatialDefaults {
    pub fn trip_default() -> SpatialGenerator {
        let config = SpatialConfig {
            dist_type: DistributionType::Bit,
            geom_type: GeomType::Point,
            dim: 2,
            seed: 56789,

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 0,
            polysize: 0.0,

            params: DistributionParams::Bit {
                probability: 0.35,
                digits: 30,
            },
        };
        SpatialGenerator::new(config, OnceLock::new(), OnceLock::new())
    }

    pub fn building_default() -> SpatialGenerator {
        let config = SpatialConfig {
            dist_type: DistributionType::Sierpinski,
            geom_type: GeomType::Polygon,
            dim: 2,
            seed: 12345,

            // geometry = box
            width: 0.0,
            height: 0.0,

            // geometry = polygon
            maxseg: 5,
            polysize: 0.000039,

            params: DistributionParams::None,
        };
        SpatialGenerator::new(config, OnceLock::new(), OnceLock::new())
    }
}

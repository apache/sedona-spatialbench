use crate::spider::{ContinentAffines, DistributionParams, DistributionType, GeomType, SpiderConfig, SpiderGenerator};

pub struct SpiderDefaults;

impl ContinentAffines {
    pub fn default() -> Self {
        Self {
            africa: [84.194319, 0.0, -20.062752, 0.0, -77.623846, 37.579421],
            europe: [76.108853, 0.0, -11.964479, 0.0,  33.901968, 37.926872],
            south_asia: [80.942556, 0.0,  64.583540, 0.0, -61.381606, 51.672557],
            north_asia: [114.339049, 0.0,  64.495655, 0.0,  25.952988, 51.944267],
            oceania: [68.287041, 0.0, 112.481901, 0.0, -38.751779, -10.228433],
            south_america: [66.177789, 0.0, -99.246451, 0.0, -70.347818, 13.068781],
            south_north_america: [77.075593, 0.0, -129.127525, 0.0, -35.337948, 48.748946],
            north_north_america: [115.130019, 0.0, -167.181951, 0.0,  22.683444, 48.980218],
        }
    }
}

impl SpiderDefaults {

    pub fn trip_default() -> SpiderGenerator {
        let config = SpiderConfig {
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
                digits: 50,
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

#[derive(Debug, Clone, Copy)]
pub enum DistributionType {
    Uniform,
    Normal,
    Diagonal,
    Sierpinski,
    Bit,
    Thomas,
    HierThomas,
}

#[derive(Debug, Clone, Copy)]
pub enum GeomType {
    Polygon,
    Box,
    Point,
}

#[derive(Debug, Clone)]
pub enum DistributionParams {
    None,
    Normal {
        mu: f64,
        sigma: f64,
    },
    Diagonal {
        percentage: f64,
        buffer: f64,
    },
    Bit {
        probability: f64,
        digits: u32,
    },
    Parcel {
        srange: f64,
        dither: f64,
    },
    Thomas {
        parents: u32,
        mean_offspring: f64,
        sigma: f64,
        pareto_alpha: f64,
        pareto_xm: f64,
    },
    HierThomas {
        cities: u32,
        // variable subclusters per city (normal, clamped)
        sub_mean: f64,
        sub_sd: f64,
        sub_min: u32,
        sub_max: u32,
        sigma_city: f64,
        sigma_sub: f64,
        // Pareto weights
        pareto_alpha_city: f64,
        pareto_xm_city: f64,
        pareto_alpha_sub: f64,
        pareto_xm_sub: f64,
    },
}

#[derive(Debug, Clone)]
pub struct SpatialConfig {
    pub dist_type: DistributionType,
    pub geom_type: GeomType,
    pub dim: i32,
    pub seed: u32,

    // Box-specific
    pub width: f64,
    pub height: f64,

    // Polygon-specific
    pub maxseg: i32,
    pub polysize: f64,

    // Distribution-specific
    pub params: DistributionParams,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ContinentAffines {
    pub africa: [f64; 6],
    pub europe: [f64; 6],
    pub south_asia: [f64; 6],
    pub north_asia: [f64; 6],
    pub oceania: [f64; 6],
    pub south_america: [f64; 6],
    pub south_north_america: [f64; 6],
    pub north_north_america: [f64; 6],
}

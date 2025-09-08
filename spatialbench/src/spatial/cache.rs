#[derive(Clone, Debug)]
pub struct ThomasCache {
    pub cdf: Vec<f64>,
    pub parents: usize,
    pub alpha: f64,
    pub xm: f64,
    pub seed: u64,
}

#[derive(Clone, Debug)]
pub struct HierThomasCache {
    pub city_cdf: Vec<f64>,
    pub sub_cdfs: Vec<Vec<f64>>,
}
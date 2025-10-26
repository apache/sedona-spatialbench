use crate::spatial::SpatialGenerator;
use once_cell::sync::OnceCell;

#[derive(Clone, Default)]
pub struct SpatialOverrides {
    pub trip: Option<SpatialGenerator>,
    pub building: Option<SpatialGenerator>,
}

static OVERRIDES: OnceCell<SpatialOverrides> = OnceCell::new();

pub fn set_overrides(o: SpatialOverrides) {
    let _ = OVERRIDES.set(o);
}

pub fn trip_or_default<F: FnOnce() -> SpatialGenerator>(fallback: F) -> SpatialGenerator {
    OVERRIDES
        .get()
        .and_then(|o| o.trip.clone())
        .unwrap_or_else(fallback)
}

pub fn building_or_default<F: FnOnce() -> SpatialGenerator>(fallback: F) -> SpatialGenerator {
    OVERRIDES
        .get()
        .and_then(|o| o.building.clone())
        .unwrap_or_else(fallback)
}

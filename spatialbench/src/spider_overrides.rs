use crate::spider::SpiderGenerator;
use once_cell::sync::OnceCell;

#[derive(Clone, Default)]
pub struct SpiderOverrides {
    pub trip: Option<SpiderGenerator>,
    pub building: Option<SpiderGenerator>,
}

static OVERRIDES: OnceCell<SpiderOverrides> = OnceCell::new();

pub fn set_overrides(o: SpiderOverrides) {
    let _ = OVERRIDES.set(o);
}

pub fn trip_or_default<F: FnOnce() -> SpiderGenerator>(fallback: F) -> SpiderGenerator {
    OVERRIDES
        .get()
        .and_then(|o| o.trip.clone())
        .unwrap_or_else(fallback)
}

pub fn building_or_default<F: FnOnce() -> SpiderGenerator>(fallback: F) -> SpiderGenerator {
    OVERRIDES
        .get()
        .and_then(|o| o.building.clone())
        .unwrap_or_else(fallback)
}

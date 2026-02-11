// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

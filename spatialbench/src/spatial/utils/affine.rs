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

#[inline]
pub fn apply_affine(x: f64, y: f64, m: &[f64; 6]) -> (f64, f64) {
    (m[0] * x + m[1] * y + m[2], m[3] * x + m[4] * y + m[5])
}

#[inline]
pub fn round_coordinate(coord: f64, precision: f64) -> f64 {
    (coord * precision).round() / precision
}

#[inline]
pub fn round_coordinates(x: f64, y: f64, precision: f64) -> (f64, f64) {
    (
        round_coordinate(x, precision),
        round_coordinate(y, precision),
    )
}

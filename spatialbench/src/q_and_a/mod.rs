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

//! TPC-H Queries and Answers.
//!
//! This module exposes a bundled query and answer tuple that makes it
//! easier to work with them in benchmark contexts.
pub mod answers_sf1;
pub mod queries;

/// QueryAndAnswer is a struct that contains a TPC-H query and its expected answer.
pub struct QueryAndAnswer(
    &'static str, // The TPC-H query as a string
    &'static str, // The expected answer as a string
);

impl QueryAndAnswer {
    /// Creates a new QueryAndAnswer instance.
    pub fn new(num: i32, scale_factor: f64) -> Result<Self, String> {
        match (num, scale_factor) {
            (1..=22, 1.) => Ok(QueryAndAnswer(
                queries::query(num).unwrap(),
                answers_sf1::answer(num).unwrap(),
            )),
            _ => Err(format!("Invalid TPC-H query number: {} the answers are only available for queries (1 to 22) and a scale factor of 1.0", num)),
        }
    }

    /// Returns the query string.
    pub fn query(&self) -> &str {
        self.0
    }

    /// Returns the expected answer string.
    pub fn answer(&self) -> &str {
        self.1
    }
}

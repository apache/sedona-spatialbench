<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

# Release Notes

## SpatialBench 0.2.0

### Highlights

* DuckDB and SedonaDB nightly benchmark support
* Spatial Polars integration in the automated benchmark framework
* S3 write support for benchmark output
* Pre-commit hooks and CI improvements

### New Features

* feature: Add DuckDB and SedonaDB nightly support (#76)
* feature: Add Spatial Polars to the automated benchmark framework (#73)
* feature: Add a benchmark GitHub Action to compare spatial libraries (#72)
* feat: Add S3 write support (#71)

### Bug Fixes

* spatial polars Q11 query fix (#75)
* Fix the failed Python build (#69)

### Improvements

* Unify navigation bar and theme with main Apache Sedona site (#88)
* Add basic EditorConfig file (#80)
* [CI] Add pre-commit hook to trim trailing whitespace (#79)
* [CI] Add pre-commit with basic checks (#77)
* [CI] Add Dependabot config for github-actions ecosystem (#84)
* docs: Add uncompressed data sizes of tables (#68)
* docs: Update docs and readme (#74)
* misc: Fix spelling and typos (#78, #81)

## SpatialBench 0.1.0

This is the initial release of SpatialBench, a spatial data processing benchmark suite for evaluating and comparing spatial libraries.

### Highlights

* Benchmark framework for comparing spatial data processing libraries
* Support for Spatial Polars benchmark queries
* Configurable output file sizes and partitioning
* Overture Divisions data loading from Hugging Face over HTTPS
* Geometry validity enforcement with CCW orientation and antimeridian handling
* PyPI and Cargo release support
* Quickstart guide and contributors guide documentation

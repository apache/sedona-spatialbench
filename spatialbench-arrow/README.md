<!---
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

# Spatial Bench Data Generator in Arrow format

This crate generates Spatial Bench data directly into [Apache Arrow] format using the [arrow] crate

[Apache Arrow]: https://arrow.apache.org/
[arrow]: https://crates.io/crates/arrow

# Example usage:

See [docs.rs page](https://docs.rs/tpchgen-arrow/latest/tpchgen_arrow/)

# Testing:
This crate ensures correct results using two methods.

1. Basic functional tests are in Rust doc tests in the source code (`cargo test --doc`)
2. The `reparse` integration test ensures that the Arrow generators
   produce the same results as parsing the original `tbl` format (`cargo test --test reparse`)

# Contributing:

Please see [CONTRIBUTING.md] for more information on how to contribute to this project.

[CONTRIBUTING.md]: https://github.com/apache/sedona-spatialbench/blob/main/CONTRIBUTING.md

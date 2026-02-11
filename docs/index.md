---
title: SpatialBench
---

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

SpatialBench is a benchmark for assessing geospatial SQL analytics query performance across database systems, making it easy to run tests on a realistic dataset with any query engine.

The methodology is unbiased, allowing you to run the benchmarks in any environment to compare the relative performance between runtimes.

## Why SpatialBench

SpatialBench was created because standard database benchmarks don't adequately test the unique demands of geospatial queries. SpatialBench provides an open-source, standardized, and scalable framework designed specifically for geospatial analytics.

Inspired by the Star Schema Benchmark (SSB) and NYC taxi data, SpatialBench combines realistic urban mobility scenarios
with a star schema extended with spatial attributes like pickup/dropoff points, zones, and building footprints.

This design enables evaluation of the following geospatial operations:

* Spatial joins
* Distance queries
* Aggregations
* Point-in-polygon analysis

Let's dive into the advantages of SpatialBench.

## Key Features

To ensure fair and comprehensive testing, SpatialBench provides the following advantages:

* Features realistic spatial datasets with native geometry columns.
* Includes a suite of queries that test various operations such as spatial predicates and joins.
* Provides a built-in synthetic data generator for creating consistent test data.
* Offers a configurable scale factor to benchmark performance across various
  environments, from a single local machine to a large-scale cloud cluster.
* Ensures consistent and reproducible benchmark results across all environments.
* Utilizes a fully documented and unbiased methodology to facilitate fair comparisons.
* Open-source and community-driven to foster transparency and continuous improvement.

## Generate synthetic data

Here's how you can install the synthetic data generator:

```
cargo install --path ./spatialbench-cli
```

Here's how you can generate the synthetic dataset:

```
spatialbench-cli -s 1 --format=parquet
```

See the project repository [README](https://github.com/apache/sedona-spatialbench) for the complete set of straightforward data generation instructions.

## Example query

Here's an example query that counts the number of trips that start within 500 meters of each building:

```sql
SELECT
    b.b_buildingkey,
    b.b_name,
    COUNT(*) AS nearby_pickup_count
FROM trip t
JOIN building b
ON ST_DWithin(t.t_pickup_loc, b.b_boundary, 500)
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC;
```

This query performs a distance join, followed by an aggregation. It's a great example of a query that's useful for performance benchmarking a spatial engine that can process vector geometries.

## Automated Testing

SpatialBench includes an automated benchmark that runs on GitHub Actions to verify that all queries are fully runnable across supported engines (DuckDB, GeoPandas, SedonaDB, and Spatial Polars).

**[View the latest test results →](https://github.com/apache/sedona-spatialbench/actions/workflows/benchmark.yml)**

Click on any successful workflow run and scroll to the **Summary** section to see the results.

!!! note
    The GitHub Actions benchmark is designed to validate correctness and runnability, not for serious performance comparisons. For meaningful performance benchmarks, see the [Single Node Benchmarks](single-node-benchmarks.md) page.

## Join the community

Feel free to start a [GitHub Discussion](https://github.com/apache/sedona/discussions) or join the [Discord community](https://discord.gg/9A3k5dEBsY) to ask the developers any questions you may have.

We look forward to collaborating with you on these benchmarks!

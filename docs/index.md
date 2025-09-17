---
hide:
  - navigation

title: Sedona SpatialBench
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

Sedona SpatialBench makes it easy to run spatial benchmarks on a realistic dataset with any query engine.

The methodology is unbiased and the benchmarks in any environment to compare relative performance between runtimes.

## Why SpatialBench

SpatialBench is a geospatial benchmark for testing and optimizing spatial analytical query performance in database systems. Inspired by the SSB and NYC taxi data, it combines realistic urban mobility scenarios with a star schema extended with spatial attributes like pickup/dropoff points, zones, and building footprints.

This design enables evaluation of the following geospatial operations:

* spatial joins
* distance queries
* aggregations
* point-in-polygon analysis

Let’s dive into the advantages of SpatialBench.

## Key advantages

* Uses spatial datasets with geometry columns.
* Includes queries with different spatial predicates.
* Easily reproducible results.
* Includes a dataset generator to so results are reproducible.
* The scale factors of the datasets can be changed so that you can run the queries locally, in a data warehouse, or on a large cluster in the cloud.
* All the specifications used to run the benchmarks are documented, and the methodology is unbiased.
* The code is open source, allowing the community to provide feedback and keep the benchmarks up-to-date and reliable over time.

## Generate synthetic data

Here’s how you can install the synthetic data generator:

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

This query performs a distance join, followed by an aggregation. It’s a great example of a query that’s useful for performance benchmarking a spatial engine that can process vector geometries.

## Join the community

Feel free to start a [GitHub Discussion](https://github.com/apache/sedona/discussions) or join the [Discord community](https://discord.gg/9A3k5dEBsY) to ask the developers any questions you may have.

We look forward to collaborating with you on these benchmarks!

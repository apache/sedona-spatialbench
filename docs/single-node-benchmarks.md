---
title: SpatialBench Single Node Benchmarks
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

This page presents the SpatialBench single-node benchmark results for SedonaDB, DuckDB, and GeoPandas. The benchmark was conducted on September 22, 2025, using SpatialBench v0.1.0 pre-release (commit `9094be8` on the main branch).

Here are the results from SpatialBench v0.1 for Queries 1–12 at scale factor 1 (SF1) and scale factor 10 (SF10).

![Scale Factor 1 benchmark results](image/sf1.png){ width="400" }
![Scale Factor 10 benchmark results](image/sf10.png){ width="400" }
{: .grid }

## Hardware

This benchmark was run on an AWS EC2 `m7i.2xlarge` instance, which has 8 CPUs and 32 GB of RAM. We encourage you to try running SpatialBench on different hardware configurations and share your results with the community.

## Test Data

The datasets are generated using SpatialBench’s dbgen and stored in an AWS S3 bucket in pure Parquet format. Geometry columns are encoded in Well-Known Binary (WKB) using the Parquet BINARY type. All systems read Parquet files directly from the S3 bucket. No local pre-loading is involved. Each Parquet row group is 128 MB. To better reflect real-world scenarios, large Parquet files are split into smaller ones, each around 200–300 MB in size.

We provide public datasets for Scale Factor 1 and 10 in the `us-west-2` region. You can access them here:

=== "Scale Factor = 1"

    ```txt
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf1/building/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf1/customer/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf1/driver/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf1/trip/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf1/vehicle/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf1/zone/
    ```

=== "Scale Factor = 10"

    ```txt
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf10/building/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf10/customer/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf10/driver/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf10/trip/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf10/vehicle/
    s3://wherobots-examples/data/spatialbench/SpatialBench_sf10/zone/
    ```

## Software

The following software versions were used in this benchmark:

* GeoPandas: 1.1.1
* Shapely: 2.1.1
* NumPy: 2.3.3
* DuckDB: 1.4.0
* SedonaDB: 0.1.0

This benchmark report specifies software versions to make it easier to track performance improvements over time. We use the default settings of all software unless otherwise noted. For DuckDB, we explicitly set `enable_external_file_cache` to `false` to focus on the cold start queries runtime, consistent with the other engines.

The reported runtimes include the entire query execution for each engine, including data loading. We used `COUNT` on every query result to trigger full execution, but did not write outputs to external files in order to avoid introducing additional overhead from data writing. A query timeout of `1200` seconds was applied.

Since GeoPandas executes in a single thread and lacks a query optimizer, any parallelization or optimization must be implemented manually. For this benchmark, we did a straightforward Python implementation that mirrors the SQL queries run on other engines. If you are a GeoPandas expert, we would be happy to collaborate on a more optimized or parallelized version.

## Result analysis

### Spatial filters and basic operations (Q1–Q6)

DuckDB and SedonaDB achieve similar low-latency performance at both SF 1 and SF 10, while GeoPandas struggles to keep up at larger scales. The main reasons are the lack of a query optimizer to choose efficient execution strategies and the absence of multi-core parallelism. By contrast, DuckDB and SedonaDB leverage columnar data layouts, vectorized execution, multi-core parallelism, and query optimization to achieve strong performance. However, SedonaDB faces challenges with spatial aggregation (Q5), where DuckDB performs significantly better. This is a known issue in SedonaDB and is planned for improvement.

### Geometric computations (Q7–Q9)

SedonaDB is especially effective on intersection/IoU (Q9), showing substantial efficiency improvements. These queries focus on geometric operations like area calculations, distance computations, and spatial intersections.

### Complex spatial joins and aggregations (Q10–Q11)

SedonaDB consistently delivers strong results on heavier joins, particularly Q10 and Q11, aided by its adaptive spatial join strategy that picks the best algorithm per partition based on spatial statistics. DuckDB handles some join queries well but encounters scaling issues in certain cases, while GeoPandas completes SF 1 but not SF 10.

### Nearest-neighbor joins (Q12)

SedonaDB completes KNN joins at both SF 1 and SF 10, thanks to its native operator and optimized algorithm. In contrast, DuckDB and GeoPandas currently lack built-in KNN join support. For these engines, we had to implement additional code manually, which proved less efficient. Adding native KNN capabilities in the future would likely help both engines close this gap.

### Overall

SedonaDB demonstrates balanced performance across all query types and scales effectively to SF 10. DuckDB excels at spatial filters and some geometric operations but faces challenges with complex joins and KNN queries. GeoPandas, while popular in the Python ecosystem, requires manual optimization and parallelization to handle larger datasets effectively.

## Benchmark code

You can access and run the benchmark code in the [sedona-spatialbench GitHub](https://github.com/apache/sedona-spatialbench) repository.

You can generate datasets and run benchmarks both locally and in cloud environments.

The repository has an issue tracker where you can file bug reports or suggest code improvements.

## Raw Benchmark Performance Numbers

The following tables present the benchmark results in detail. All times are in seconds. ERROR indicates that the software failed to complete the query due to an exception (e.g., out-of-memory). TIMEOUT indicates that the query exceeded 1200 seconds without returning results.

=== "Scale Factor = 1"

    | Query | SedonaDB | DuckDB | GeoPandas |
    |-------|----------|--------|-----------|
    | q1    | 0.66     | 0.96   | 12.78     |
    | q2    | 8.07     | 9.95   | 20.74     |
    | q3    | 0.80     | 1.17   | 13.59     |
    | q4    | 8.41     | 9.83   | 25.24     |
    | q5    | 5.10     | 1.80   | 47.08     |
    | q6    | 8.59     | 9.36   | 24.43     |
    | q7    | 1.66     | 1.82   | 137.00    |
    | q8    | 1.10     | 1.08   | 16.08     |
    | q9    | 0.23     | 50.15  | 0.28      |
    | q10   | 18.79    | 207.84 | 46.13     |
    | q11   | 32.98    | TIMEOUT| 51.01     |
    | q12   | 14.55    | ERROR  | TIMEOUT   |


=== "Scale Factor = 10"

    | Query | SedonaDB | DuckDB | GeoPandas |
    |-------|----------|--------|-----------|
    | q1    | 3.04     | 4.58   | ERROR     |
    | q2    | 8.89     | 8.26   | ERROR     |
    | q3    | 4.09     | 5.17   | TIMEOUT   |
    | q4    | 7.52     | 8.51   | ERROR     |
    | q5    | 50.81    | 14.40  | ERROR     |
    | q6    | 9.11     | 10.67  | ERROR     |
    | q7    | 14.44    | 14.03  | ERROR     |
    | q8    | 7.24     | 7.57   | TIMEOUT   |
    | q9    | 0.38     | 942.98 | 0.49      |
    | q10   | 42.02    | ERROR  | ERROR     |
    | q11   | 97.52    | ERROR  | ERROR     |
    | q12   | 145.66   | ERROR  | TIMEOUT   |

## Future work

We plan to include additional engines and databases in future work, such as:

* `dask-geopandas` for single-node parallelism across cores
* An R geospatial engine

If you're an expert in any of these technologies, we welcome you to take on this project or reach out to us about collaborating.

For clarity and simplicity, compute engines designed for multi-node environments are intentionally excluded from these single-node results. Likewise, transactional databases such as PostGIS execute queries in fundamentally different ways from pure Python engines (e.g., GeoPandas) or analytical engines (e.g., SedonaDB, DuckDB). Since SpatialBench is primarily focused on analytical workloads, these systems are not included in the current study.

The overarching goal of the SpatialBench initiative is to provide the spatial community with a reliable set of benchmarks and to help accelerate the development of better tooling for users.

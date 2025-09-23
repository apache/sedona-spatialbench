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

This page presents the SpatialBench single-node benchmark results for SedonaDB, DuckDB, and GeoPandas. The benchmark was conducted on September 22, 2025, using SpatialBench v0.1.0 pre-release (commit 9094be8 on the main branch).

Here are the results for v0.1 of the SpatialBench queries for scale factor 1 (SF1):

![Scale Factor 1 benchmark results](image/sf1.png)

Here are the results for SF10:

![Scale Factor 10 benchmark results](image/sf10.png)

The remainder of this document summarizes the hardware and software versions, query methodologies for specific engines, and provides information on where to find the benchmark code.

## Hardware

This benchmark was run on an AWS EC2 m7i.2xlarge instance, which has 8 CPUs and 32 GB of RAM. We encourage you to try running SpatialBench on different hardware configurations and share your results with the community.


## Benchmark settings

Here are the software versions used in this benchmark:

* GeoPandas: 1.1.1
* Shapely: 2.1.1
* NumPy: 2.3.3
* DuckDB: 1.4.0
* SedonaDB: 0.1

This benchmark report lists software versions, so it’s easy to track how engine performance improves over time.  We use the default settings of all software unless otherwise noted. For DuckDB, we explicitly set enable_external_file_cache to false to focus on the cold start queries runtime, consistent with the other engines.

The code execution runtime includes the entire query runtime for all engines. The query timeout is set to 1200 seconds.

## GeoPandas query methodology

The GeoPandas queries are written in Python, since GeoPandas does not support SQL. GeoPandas executes queries by loading data fully into memory and then processing it directly.

Since GeoPandas runs in a single thread and lacks a query optimizer, any parallelization or optimization must be implemented manually. This benchmark implemented a straightforward implementation that mirrors the SQL queries used for other engines. If you’re a GeoPandas expert, we’d be glad to collaborate on a more optimized and/or parallelized version.

## Result analysis

### Spatial filters (Q1–Q3, Q6)

DuckDB and SedonaDB achieve similar low-latency performance at both SF 1 and SF 10, while GeoPandas struggles to keep up at larger scales. The main reasons are the lack of a query optimizer to choose efficient execution strategies and the absence of multi-core parallelism. By contrast, DuckDB and SedonaDB leverage columnar data layouts, vectorized execution, multi-core parallelism, and query optimization to achieve strong performance.

### Aggregation with spatial joins (Q4, Q10, Q11)

SedonaDB consistently delivers strong results on heavier joins, particularly Q10 and Q11, aided by its adaptive spatial join strategy that picks the best algorithm per partition based on spatial statistics. DuckDB handles some join queries well but encounters scaling issues in certain cases, while GeoPandas completes SF 1 but not SF 10.

### Geometric computations (Q5, Q7, Q9)

SedonaDB is especially effective on intersection/IoU (Q9), showing substantial efficiency improvements, while Q5 (convex hull aggregation) highlights areas where DuckDB currently performs faster. SedonaDB’s overhead in geometry copying in spatial aggregation is a known bottleneck and is planned for improvement.

### Nearest-neighbor joins (Q12)

SedonaDB completes KNN joins at both SF 1 and SF 10, thanks to its native operator and optimized algorithm. In contrast, DuckDB and GeoPandas currently lack built-in KNN join support. For these engines, we had to implement additional code manually, which proved less efficient. Adding native KNN capabilities in the future would likely help both engines close this gap.

### Overall

SedonaDB demonstrates balanced strengths across all categories and successfully scales to SF 10 on an AWS m7i.2xlarge instance. DuckDB delivers solid performance on simpler filters and certain geometric computations, but has room to improve on complex joins and KNN queries. GeoPandas, while not scaling as effectively in this benchmark, remains a widely used tool in the Python ecosystem; however, it currently requires manual optimization and parallelization to be deployed at scale.

## Benchmark code

You can access and run the benchmark code in the [sedona-spatialbench GitHub](https://github.com/apache/sedona-spatialbench) repository.

It’s easy to generate the datasets locally or in the cloud.  You can also run the benchmarks locally or in the cloud.

The repository has an issue tracker where you can file bug reports or suggest code improvements.

## Future work

It would be great to include other engines and databases in the future:

* dask-geopandas for single-node parallelism across cores
* PostGIS (Postgres SQL extension)
* An R geospatial engine

If you’re an expert in any of these technologies, we welcome you to take on this project or reach out to us about collaborating.

Note that compute engines designed for multi-node environments are intentionally excluded from these single-node results for clarity and simplicity.

Similarly, transactional databases such as PostGIS execute queries in fundamentally different ways than pure Python engines, like GeoPandas, or analytical engines, like SedonaDB and DuckDB. Since SpatialBench is primarily focused on analytical workloads, these systems are not yet included in this discussion.

The overarching goal of the SpatialBench initiative is to provide the spatial community with a reliable set of benchmarks and to help accelerate the development of better tooling for users.

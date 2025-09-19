---
title: SpatialBench Overview and Methodology
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

# SpatialBench Overview and Methodology

SpatialBench is an open benchmark suite of representative spatial queries designed to evaluate the performance of different engines at multiple scale factors.

The SpatialBench queries are a great way to compare the relative performance between engines for analytical spatial workloads.  You can use a small scale factor for single-machine queries, and a large scale factor to benchmark an engine that distributes computations in the cloud.

Let’s take a deeper look at why SpatialBench is so essential.

## Why SpatialBench?

Spatial workflows encompass queries such as spatial joins, spatial filtering, and spatial-specific operations, including KNN joins.

General-purpose analytics query benchmarks don’t cover spatial queries.  They focus on analytical queries, such as joins and aggregations, on tabular data. Here are some popular analytical benchmarks:

* [TPC-H](https://www.tpc.org/tpch/)
* [TPC-DS](https://www.tpc.org/tpcds/)
* [ClickBench](https://benchmark.clickhouse.com/)
* [YCSB](https://github.com/brianfrankcooper/YCSB)
* [db-benchmark](https://duckdblabs.github.io/db-benchmark/)

The analytical benchmarks help analyze analytical performance, but that doesn’t necessarily translate to spatial queries.  An engine can be blazing fast for a large tabular aggregation and terrible for spatial joins.

SpatialBench is tailored for spatial queries.  It’s the best modern option to assess the spatial performance of an engine.  Let’s take a look at some of the older spatial benchmarks.

## Hardware and software

SpatialBench runs benchmarks on commodity hardware, with software versions fully disclosed for each release.

When comparing different runtimes, developers should make a good-faith effort to use similar hardware and software versions.  It’s not helpful to compare one runtime with another runtime that has a lot less computational power.

SpatialBench benchmarks should always be presented with associated hardware/software specifications so readers can assess the reliability of the comparison.

## Accurately comparing different engines

It is challenging to compare fundamentally different engines, such as PostGIS (an OLTP database), DuckDB (an OLAP database), and GeoPandas (a Python engine).

For example, let’s compare how two engines execute a query differently:

* PostGIS: create tables, load data into the tables, build an index (can be expensive), run the query
* GeoPandas: read data into memory and run a query

PostGIS and GeoPandas execute queries differently, so you need to present the query runtime with caution.  For example, you can’t just ignore the time it takes to build the PostGIS index because that can be the slowest part of the query.  That’s a critical detail for users running ad hoc queries.

The SpatialBench results strive to present runtimes for all relevant portions of the query so users are best informed about how to interpret the results.

## Engine tuning in benchmarks

Engines can be tuned by configuring settings or optimizing code.  For example, you can optimize Spark code by tuning the JVM.  You can optimize GeoPandas code by adding indexes.  Benchmarks that tune one engine and don’t tune any of the other engines aren’t reliable.

All performance tuning is fully disclosed in the SpatialBench results.  Some results are presented both naively and fully tuned to give a better picture of out-of-the-box performance and what’s possible for expert users.

## Open source benchmarks vs. vendor benchmarks

The SpatialBench benchmarks report results for some open source spatial engines/databases.

The SpatialBench repository does not report results for any proprietary engines or vendor runtimes.  Vendors are free to use the SpatialBench data generators and run the benchmarks on their own.  We ask vendors to credit SpatialBench when they run the benchmarks and fully disclose the results so that other practitioners can reproduce the results.

## How to contribute

There are a variety of ways to contribute to the SpatialBench project:

* Submit [pull requests](https://github.com/apache/sedona-spatialbench/pulls) to add features
* Create [issues](https://github.com/apache/sedona-spatialbench/issues) for bug reports
* Reproduce results or help add new spatial engines
* Publish vendor benchmarks

Here is how you can communicate with the team:

* Chat with us on the [Apache Sedona Discord](https://discord.gg/9A3k5dEBsY)
* Create [GitHub Discussions](https://github.com/apache/sedona/discussions)

## Future work

In the next release, we will add raster datasets and raster queries.  These will stress test an engine’s ability to analyze raster data.  They will also show performance when joining vector and raster datasets.

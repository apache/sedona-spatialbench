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

SpatialBench 是一个用于评估各类数据库系统中地理空间 SQL 分析查询性能的基准测试套件，能够让你在任意查询引擎上轻松地基于真实场景的数据集运行测试。

该方法论保持中立公正，你可以在任意环境中运行基准测试，以比较不同运行时之间的相对性能。

## 为什么选择 SpatialBench

SpatialBench 的诞生，源于现有标准数据库基准测试无法充分覆盖地理空间查询的独特需求。SpatialBench 提供了一个开源、标准化、可扩展的框架，专门为地理空间分析而设计。

SpatialBench 借鉴了星型模式基准测试（Star Schema Benchmark，SSB）和纽约市出租车数据的思路，将真实的城市出行场景与扩展了空间属性的星型模式（如上车/下车点、区域以及建筑物轮廓等）相结合。

这种设计能够评估以下空间操作：

* 空间连接（Spatial joins）
* 距离查询（Distance queries）
* 聚合（Aggregations）
* 点在多边形内分析（Point-in-polygon analysis）

下面让我们深入了解 SpatialBench 的优势。

## 主要特性

为了确保测试的公平性和全面性，SpatialBench 提供了以下优势：

* 提供包含原生几何列的真实空间数据集。
* 包含一套查询，可测试空间谓词、空间连接等多种操作。
* 内置合成数据生成器，用于生成一致的测试数据。
* 提供可配置的规模因子（Scale Factor），便于在从单机到大规模云集群的各种环境中进行性能测试。
* 在所有环境中均能给出一致且可复现的基准测试结果。
* 采用完整记录、立场中立的方法论，便于公平比较。
* 开源且由社区驱动，倡导透明性和持续改进。

## 生成合成数据

下面是安装合成数据生成器的方法：

```
cargo install --path ./spatialbench-cli
```

下面是生成合成数据集的方法：

```
spatialbench-cli -s 1 --format=parquet
```

完整的数据生成说明请参见项目仓库的 [README](https://github.com/apache/sedona-spatialbench)。

## 示例查询

下面是一个示例查询，统计每栋建筑物 500 米范围内的行程数量：

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

该查询先执行一个基于距离的连接，再进行聚合。它非常适合用于评测可处理矢量几何的空间引擎的性能。

## 自动化测试

SpatialBench 包含一个在 GitHub Actions 上运行的自动化基准测试，用于验证所有查询能够在受支持的引擎（DuckDB、GeoPandas、SedonaDB 和 Spatial Polars）上完整运行。

**[查看最新的测试结果 →](https://github.com/apache/sedona-spatialbench/actions/workflows/benchmark.yml)**

点击任一成功的工作流运行记录，并向下滚动到 **Summary** 部分即可查看结果。

!!! note
    GitHub Actions 上的基准测试主要用于验证查询的正确性和可运行性，并不适用于严肃的性能对比。如需进行有意义的性能基准测试，请参阅 [单节点基准测试](single-node-benchmarks.md) 页面。

## 加入社区

欢迎在 [GitHub Discussions](https://github.com/apache/sedona/discussions) 发起讨论，或加入 [Discord 社区](https://discord.gg/9A3k5dEBsY)，向开发者提出任何问题。

我们期待与你一同推进这些基准测试的工作！

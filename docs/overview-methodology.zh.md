---
title: SpatialBench 方法论
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

SpatialBench 是一套具有代表性的空间查询开放基准测试套件，旨在评估不同引擎在多种规模因子下的性能。

SpatialBench 查询是比较不同引擎在分析型空间工作负载下相对性能的有效手段。你可以使用较小的规模因子进行单机查询，也可以使用较大的规模因子来评测在云端分布式计算的引擎。

下面我们更深入地探讨 SpatialBench 为什么如此重要。

## 为什么需要 SpatialBench？

空间工作流通常包含空间连接、空间过滤、以及 KNN 连接等空间专用操作。

通用的分析查询基准测试无法覆盖空间查询。它们关注的是表格数据上的分析查询，如连接和聚合。下面是一些常见的分析型基准测试：

* [TPC-H](https://www.tpc.org/tpch/)
* [TPC-DS](https://www.tpc.org/tpcds/)
* [ClickBench](https://benchmark.clickhouse.com/)
* [YCSB](https://github.com/brianfrankcooper/YCSB)
* [db-benchmark](https://duckdblabs.github.io/db-benchmark/)

这些分析型基准测试有助于评估分析查询性能，但其结果并不一定能反映空间查询的性能。一个引擎在大型表格聚合上可能表现出色，但在空间连接上却可能非常糟糕。

SpatialBench 专为空间查询而设计，是评估引擎空间性能的最佳现代方案。下面给出一些使用建议，帮助你获得最准确、最公平的结果。

## 硬件和软件

SpatialBench 基准测试在通用硬件上运行，每次发布都会完整披露所使用的软件版本。

在对比不同运行时时，开发者应当尽可能使用相近的硬件和软件版本。如果一个运行时的计算能力远低于另一个，这种对比是没有意义的。

SpatialBench 基准测试结果应始终连同对应的硬件/软件规格一起呈现，以便读者评估比较的可靠性。

## 准确对比不同的引擎

要对本质上不同的引擎（如 PostGIS（OLTP 数据库）、DuckDB（OLAP 数据库）以及 GeoPandas（Python 引擎））进行公平比较是颇具挑战的。

例如，让我们看看两个引擎执行同一查询的方式有何不同：

* PostGIS：创建表、将数据加载到表中、构建索引（可能较为耗时）、执行查询
* GeoPandas：将数据读取至内存并执行查询

由于 PostGIS 和 GeoPandas 在执行查询的方式上存在差异，因此在呈现查询运行时间时需要格外谨慎。例如，你不能忽略 PostGIS 构建索引所花费的时间，因为它可能是查询中最耗时的部分。对于运行临时查询的用户来说，这是非常关键的细节。

SpatialBench 在呈现结果时，会尽量给出查询各相关阶段的运行时间，以帮助用户更好地解读结果。

## 基准测试中的引擎调优

引擎可以通过配置参数或优化代码来进行调优。例如，你可以通过调优 JVM 来优化 Spark 代码，也可以通过添加索引来优化 GeoPandas 代码。如果一个基准测试仅对某一个引擎进行了调优，却没有对其他引擎做同样的调优，那么这样的结果是不可靠的。

SpatialBench 完整披露所有性能调优信息。部分结果会同时呈现“开箱即用”和“充分调优”两种情形，以便更全面地反映默认性能以及专家用户所能达到的性能。

## 开源基准测试与厂商基准测试

SpatialBench 基准测试报告了部分开源空间引擎/数据库的结果。

SpatialBench 仓库本身不会报告任何专有引擎或厂商运行时的结果。厂商可以自由地使用 SpatialBench 的数据生成器并自行运行基准测试。我们希望厂商在使用本基准测试时能注明出处，并完整披露结果，以便其他从业者能够复现这些结果。

## 如何贡献

为 SpatialBench 项目做贡献的方式多种多样：

* 提交 [Pull Request](https://github.com/apache/sedona-spatialbench/pulls) 来添加新特性
* 创建 [Issue](https://github.com/apache/sedona-spatialbench/issues) 来报告 bug
* 复现结果，或协助接入新的空间引擎
* 发布厂商基准测试

下面是与团队沟通的渠道：

* 在 [Apache Sedona Discord](https://discord.gg/9A3k5dEBsY) 上交流
* 创建 [GitHub Discussions](https://github.com/apache/sedona/discussions)

## 未来工作

在下一个发布版本中，我们会加入栅格数据集和栅格查询。它们将对引擎处理栅格数据的能力进行压力测试，同时也会展示在矢量数据与栅格数据连接场景下的性能表现。

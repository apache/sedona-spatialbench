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

# 发布说明

## SpatialBench 0.2.0

### 主要亮点

* 支持 DuckDB 与 SedonaDB 的每日（nightly）基准测试
* 在自动化基准测试框架中集成 Spatial Polars
* 基准测试输出支持写入 S3
* 引入 pre-commit hook，并改进 CI

### 新特性

* feature: Add DuckDB and SedonaDB nightly support (#76)
* feature: Add Spatial Polars to the automated benchmark framework (#73)
* feature: Add a benchmark GitHub Action to compare spatial libraries (#72)
* feat: Add S3 write support (#71)

### Bug 修复

* spatial polars Q11 query fix (#75)
* Fix the failed Python build (#69)

### 改进

* Unify navigation bar and theme with main Apache Sedona site (#88)
* Add basic EditorConfig file (#80)
* [CI] Add pre-commit hook to trim trailing whitespace (#79)
* [CI] Add pre-commit with basic checks (#77)
* [CI] Add Dependabot config for github-actions ecosystem (#84)
* docs: Add uncompressed data sizes of tables (#68)
* docs: Update docs and readme (#74)
* misc: Fix spelling and typos (#78, #81)

## SpatialBench 0.1.0

这是 SpatialBench 的首个发布版本。SpatialBench 是一个空间数据处理基准测试套件，用于评估和比较各类空间数据处理库。

### 主要亮点

* 用于比较各类空间数据处理库的基准测试框架
* 支持 Spatial Polars 基准测试查询
* 可配置的输出文件大小和分区方式
* 通过 HTTPS 从 Hugging Face 加载 Overture Divisions 数据
* 强制几何有效性，并对 CCW 朝向和反子午线进行处理
* 支持 PyPI 与 Cargo 的发布
* 提供快速开始指南和贡献者指南文档

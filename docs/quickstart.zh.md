---
title: 快速开始
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

## 安装

从源代码安装：

```shell
git clone https://github.com/apache/sedona-spatialbench.git
cd sedona-spatialbench
cargo install --path spatialbench-cli
```

安装完成后，你应当能够运行：

```shell
spatialbench-cli --help
```

## 生成 SF1 数据

以 Parquet 格式生成规模因子为 1 的完整数据集：

```shell
spatialbench-cli --scale-factor 1
```

该命令会生成六张表：

* trip
* customer
* driver
* vehicle
* zone
* building

默认情况下，输出会写入当前目录。

## 自定义输出文件

下面介绍几个常用的输出文件自定义选项。要查看全部可用选项，请运行 `spatialbench-cli --help`。

### 仅生成部分表

```shell
spatialbench-cli --scale-factor 1 --tables trip,building
```

### 将表输出分区为多个文件

手动指定分区数量：

```shell
spatialbench-cli --scale-factor 10 --tables trip --parts 4
```

或者让 CLI 根据目标文件大小来自动决定文件数量：

```shell
spatialbench-cli --scale-factor 10 --mb-per-file 512
```

### 设置输出目录

```shell
spatialbench-cli --scale-factor 1 --output-dir data/sf1
```

### 直接生成到 S3

你可以通过将输出目录设置为 S3 URI，将数据直接生成到 Amazon S3 或兼容 S3 的存储中：

```shell
# 设置 AWS 凭据
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-west-2"  # 必须与你的桶所在区域一致

# 生成数据到 S3
spatialbench-cli --scale-factor 10 --mb-per-file 256 --output-dir s3://my-bucket/spatialbench/sf10

# 对于兼容 S3 的服务（如 MinIO 等）
export AWS_ENDPOINT="http://localhost:9000"
spatialbench-cli --scale-factor 1 --output-dir s3://my-bucket/data
```

S3 写入器采用流式分段上传（multipart upload），在上传分段前以 32 MB 的块对数据进行缓冲。所有标准的 AWS 环境变量均受支持，包括用于临时凭据的 `AWS_SESSION_TOKEN`。

## 配置空间数据分布

SpatialBench 使用一个空间数据生成器，按照真实的空间分布来生成合成的点和多边形数据。

要详细了解 SpatialBench 提供的各种空间分布，请参见[此处](https://sedona.apache.org/spatialbench/spatialbench-distributions/)。

有关空间分布的调优、完整的 YAML schema 与示例的更多细节，请参阅 [CONFIGURATION.md](https://github.com/apache/sedona-spatialbench/blob/main/spatialbench-cli/CONFIGURATION.md)。

你可以通过 `--config` 参数传入 YAML 文件，从而在运行时覆盖默认值：

```shell
spatialbench-cli --scale-factor 1 --config spatialbench-config.yml
```

如果未提供 `--config`，SpatialBench 会检查 `./spatialbench-config.yml`。如果该文件也不存在，则回退到内置默认配置。

---
title: Quickstart
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

## Installation

Install from source:

```shell
git clone https://github.com/apache/sedona-spatialbench.git
cd sedona-spatialbench
cargo install --path spatialbench-cli
```

After installation, you should be able to run:

```shell
spatialbench-cli --help
```

## Generate SF1 Data

To generate the full dataset at scale factor 1 in Parquet format:

```shell
spatialbench-cli --scale-factor 1
```

This creates six tables:

* trip
* customer
* driver
* vehicle
* zone
* building

Output is written to the current directory by default.

## Customizing Output Files

We'll go over a few common options to customize the output files. To see all available options, run `spatialbench-cli --help`.

### Generate a Subset of Tables

```shell
spatialbench-cli --scale-factor 1 --tables trip,building
```

### Partition Table Output into Multiple Files

Specify the number of partitions manually:

```shell
spatialbench-cli --scale-factor 10 --tables trip --parts 4
```

Or let the CLI determine the number of files using target size:

```shell
spatialbench-cli --scale-factor 10 --mb-per-file 512
```

### Set Output Directory

```shell
spatialbench-cli --scale-factor 1 --output-dir data/sf1
```

### Generate Data Directly to S3

You can generate data directly to Amazon S3 or S3-compatible storage by providing an S3 URI as the output directory:

```shell
# Set AWS credentials
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-west-2"  # Must match your bucket's region

# Generate to S3
spatialbench-cli --scale-factor 10 --mb-per-file 256 --output-dir s3://my-bucket/spatialbench/sf10

# For S3-compatible services (MinIO, etc.)
export AWS_ENDPOINT="http://localhost:9000"
spatialbench-cli --scale-factor 1 --output-dir s3://my-bucket/data
```

The S3 writer uses streaming multipart upload, buffering data in 32 MB chunks before uploading parts. All standard AWS environment variables are supported, including `AWS_SESSION_TOKEN` for temporary credentials.

## Configuring Spatial Distributions

SpatialBench uses a spatial data generator to generate synthetic points and polygons using realistic spatial distributions.

To read more about the different spatial distributions offered by SpatialBench see [here](https://sedona.apache.org/spatialbench/spatialbench-distributions/).

For more details about tuning the spatial distributions and the full YAML schema and examples, see [CONFIGURATION.md](https://github.com/apache/sedona-spatialbench/blob/main/spatialbench-cli/CONFIGURATION.md).

You can override these defaults at runtime by passing a YAML file via the `--config` flag:

```shell
spatialbench-cli --scale-factor 1 --config spatialbench-config.yml
```

If `--config` is not provided, SpatialBench checks for `./spatialbench-config.yml`. If absent, it falls back to built-in defaults.

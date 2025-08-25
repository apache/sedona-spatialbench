# SpatialBench

SpatialBench is a high-performance geospatial benchmark for generating synthetic spatial data at scale. Inspired by the Star Schema Benchmark (SSB) and real-world mobility data like the NYC TLC dataset, SpatialBench is designed to evaluate spatial query performance in modern data platforms.

Built in Rust and powered by Apache Arrow, SpatialBench brings fast, scalable, and streaming-friendly data generation for spatial workloads—minimal dependencies, blazing speed.

SpatialBench provides a reproducible and scalable way to evaluate the performance of spatial data engines using realistic synthetic workloads.

Goals:

- Establish a fair and extensible benchmark suite for spatial data processing.
- Help users compare engines and frameworks across different data scales.
- Support open standards and foster collaboration in the spatial computing community.

## Data Model

SpatialBench defines a spatial star schema with the following tables:

| Table      | Type         | Abbr. | Description                                 | Spatial Attributes        | Cardinality per SF   |
|------------|--------------|-------|---------------------------------------------|----------------------------|----------------------|
| Trip       | Fact Table   | `t_`  | Individual trip records                     | pickup & dropoff points    | 6M × SF              |
| Customer   | Dimension    | `c_`  | Trip customer info                          | None                       | 30K × SF             |
| Driver     | Dimension    | `s_`  | Trip driver info                            | None                       | 500 × SF             |
| Vehicle    | Dimension    | `v_`  | Trip vehicle info                           | None                       | 100 × SF             |
| Zone       | Dimension    | `z_`  | Administrative zones                        | Polygon                    | ~867k (fixed)        |
| Building   | Dimension    | `b_`  | Building footprints                         | Polygon                    | 20K × (1 + log₂(SF)) |

Unlike other tables in the benchmark, the Zone table does not scale with the scale factor. It is a fixed-size reference table representing administrative boundaries and is derived from the Overture Maps Divisions theme, release version 2025-06-25.0. This ensures consistency and realism for spatial join workloads such as point-in-polygon or zone-based aggregations.

![image.png](images/data_model.png)

## Performance

SpatialBench inherits its speed and efficiency from the tpchgen-rs project, which is one of the fastest open-source data generators available.

Key performance benefits:
- **Zero-copy, streaming architecture**: Generates data in constant memory, suitable for very large datasets.
- **Multithreaded from the ground up**: Leverages all CPU cores for high-throughput generation.
- **Arrow-native output**: Supports fast serialization to Parquet and other formats without bottlenecks.
- **Fast geometry generation**: The Spider module generates millions of spatial geometries per second, with deterministic output and affine transforms.

## How is SpatialBench dbgen built?

SpatialBench is a Rust-based fork of the tpchgen-rs project. It preserves the original’s high-performance, multi-threaded, streaming architecture, while extending it with a spatial star schema and geometry generation logic.

You can build the SpatialBench data generator using Cargo:

```bash
cargo build --release
```

Alternatively, install it directly using:

```bash
cargo install --path ./spatialbench-cli
```

### Notes

- The core generator logic lives in the spatialbench crate.
- Geometry-aware logic is in spatialbench-arrow and integrated via Arrow-based schemas.
- The spatial extension modules like the Spider geometry generator reside in [spider.rs](https://github.com/wherobots/sedona-spatialbench/blob/main/spatialbench/src/spider.rs).
- The generator supports output formats like .tbl and Apache Parquet via the Arrow writer.

For contribution or debugging, refer to the [ARCHITECTURE.md](https://github.com/wherobots/sedona-spatialbench/blob/main/ARCHITECTURE.md) guide.

## Usage

#### Generate All Tables (Scale Factor 1)

```bash
spatialbench-cli -s 1 --format=parquet
```

#### Generate Individual Tables

```bash
spatialbench-cli -s 1 --format=parquet --tables trip,building --output-dir sf1-parquet
```

#### Partitioned Output Example

```bash
for PART in $(seq 1 4); do
  mkdir part-$PART
  spatialbench-cli -s 10 --tables trip,building --output-dir part-$PART --parts 4 --part $PART
done
```

#### Custom Spider Configuration

You can override these defaults at runtime by passing a YAML file via the `--config` flag:

```bash
spatialbench-cli -s 1 --format=parquet --tables trip,building --config spatialbench-config.yml
```

If --config is not provided, SpatialBench checks for ./spatialbench-config.yml. If absent, it falls back to built-in defaults.

For reference, see the provided [spatialbench-config.yml](spatialbench-config.yml).

See [SPIDER.md](SPIDER.md) for more details about spatial data generation and the full YAML schema and examples.

## Acknowledgements
- [TPC-H](https://www.tpc.org/tpch/)
- [SpiderWeb: A Spatial Data Generator on the Web](https://dl.acm.org/doi/10.1145/3397536.3422351)
- [tpchgen-rs for inspiration and baseline performance](https://datafusion.apache.org/blog/2025/04/10/fastest-tpch-generator/)

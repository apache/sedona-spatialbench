# SpatialBench Data Generator CLI

See the main [README.md](https://github.com/wherobots/sedona-spatialbench) for full documentation.

## Installation

### Install Using Python

Install this tool with Python:
```shell
pip install spatialbench-cli
```

### Install Using Rust

[Install Rust](https://www.rust-lang.org/tools/install) and this tool:

```shell
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
cargo install spatialbench-cli
```

## CLI Usage

We tried to make the `spatialbench-cli` experience as close to `dbgen` as possible for no other
reason than maybe make it easier for you to have a drop-in replacement.

```shell
$ spatialbench-cli -h
TPC-H Data Generator

Usage: spatialbench-cli [OPTIONS]

Options:
  -s, --scale-factor <SCALE_FACTOR>
          Scale factor to address (default: 1) [default: 1]
  -o, --output-dir <OUTPUT_DIR>
          Output directory for generated files (default: current directory) [default: .]
  -T, --tables <TABLES>
          Which tables to generate (default: all) [possible values: region, nation, supplier, customer, part, partsupp, orders, lineitem]
  -p, --parts <PARTS>
          Number of parts to generate (manual parallel generation) [default: 1]
      --part <PART>
          Which part to generate (1-based, only relevant if parts > 1) [default: 1]
  -f, --format <FORMAT>
          Output format: tbl, csv, parquet (default: tbl) [default: tbl] [possible values: tbl, csv, parquet]
  -n, --num-threads <NUM_THREADS>
          The number of threads for parallel generation, defaults to the number of CPUs [default: 8]
  -c, --parquet-compression <PARQUET_COMPRESSION>
          Parquet block compression format. Default is SNAPPY [default: SNAPPY]
  -v, --verbose
          Verbose output (default: false)
      --stdout
          Write the output to stdout instead of a file
  -h, --help
          Print help (see more with '--help')
```

For example generating a dataset with a scale factor of 1 (1GB) can be done like this:
```shell
$ spatialbench-cli -s 1 --output-dir=/tmp/tpch
```

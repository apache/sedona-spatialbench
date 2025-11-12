---
title: Contributors Guide
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

# Contributors Guide

This guide details how to set up your development environment as a SpatialBench contributor.

## Fork and clone the repository

Your first step is to create a personal copy of the repository and connect it to the main project.

1. Fork the repository

   * Navigate to the official [SpatialBench GitHub repository](https://github.com/apache/sedona-spatialbench).
   * Click the **Fork** button in the top-right corner. This creates a complete copy of the project in your own GitHub account.

2. Clone your fork

   * Next, clone your newly created fork to your local machine. This command downloads the repository into a new folder named `sedona-spatialbench`.
   * Replace `YourUsername` with your actual GitHub username.

    ```shell
    git clone https://github.com/YourUsername/sedona-spatialbench.git
    cd sedona-spatialbench
    ```

3. Configure the remotes

   * Your local repository needs to know where the original project is so you can pull in updates. You'll add a remote link, traditionally named upstream, to the main SpatialBench repository.
   * Your fork is automatically configured as the origin remote.

    ```shell
    # Add the main repository as the "upstream" remote
    git remote add upstream https://github.com/apache/sedona-spatialbench.git
    ```

4. Verify the configuration

   * Run the following command to verify that you have two remotes configured correctly: origin (your fork) and upstream (the main repository).

    ```shell
    git remote -v
    ```

   * The output should look like this:

    ```shell
    origin    https://github.com/YourUsername/sedona-spatialbench.git (fetch)
    origin    https://github.com/YourUsername/sedona-spatialbench.git (push)
    upstream  https://github.com/apache/sedona-spatialbench.git (fetch)
    upstream  https://github.com/apache/sedona-spatialbench.git (push)
    ``` 

## Development Setup

SpatialBench is written in Rust and is a standard cargo workspace. You can install a recent version of the Rust compiler and cargo from rustup.rs.

To run tests:

```shell
cargo test
```

A local development version of the CLI can be run with:

```shell
cargo run --bin spatialbench-cli
```

## Debugging

### IDE

Debugging Rust code is most easily done by writing or finding a test that triggers the desired behavior and running it using the Debug selection in your IDE with the [rust-analyzer](https://www.jetbrains.com/help/fleet/using-rust-analyzer.html) extension.

### Verbose CLI Output

When debugging the SpatialBench CLI, you can enable verbose output to see detailed logging:

Enable verbose output (info level logging),

```shell
cargo run --bin spatialbench-cli -- --scale-factor 1 --verbose
```

Or using environment variables for more granular control,
```shell
RUST_LOG=debug cargo run --bin spatialbench-cli -- --scale-factor 1
```

The `--verbose` flag sets the log level to info and ignores the RUST_LOG environment variable. When not specified, logging is configured via `RUST_LOG`.

### Logging Levels

You can control logging granularity using `RUST_LOG`:

```shell
# Show only errors
RUST_LOG=error cargo run --bin spatialbench-cli -- --scale-factor 1

# Show warnings and errors
RUST_LOG=warn cargo run --bin spatialbench-cli -- --scale-factor 1

# Show info, warnings, and errors
RUST_LOG=info cargo run --bin spatialbench-cli -- --scale-factor 1

# Show debug output
RUST_LOG=debug cargo run --bin spatialbench-cli -- --scale-factor 1

# Show trace output (very verbose)
RUST_LOG=trace cargo run --bin spatialbench-cli -- --scale-factor 1

# Show debug output for specific modules
RUST_LOG=spatialbench=debug cargo run --bin spatialbench-cli -- --scale-factor 1
```

## Testing

We use cargo to run the Rust tests:

```shell
cargo test
```

You can run tests for a specific crate:

```shell
cd spatialbench
cargo test
```

## Linting

Install pre-commit. This will automatically run various checks (e.g., formatting) that will be needed to pass CI:

```shell
pre-commit install
```

Additionally, you should run clippy to catch common lints before pushing new Rust changes. This is not included in pre-commit, so this should be run manually. Fix any suggestions it makes, and run it again to make sure there are no other changes to make:

```shell
cargo clippy
```

## Documentation

To contribute to the SpatialBench documentation:

1. Clone the repository and create a fork.
2. Install the Documentation dependencies:
    ```shell
    pip install -r docs/requirements.txt
    ```
3. Make your changes to the documentation files.
4. Preview your changes locally using these commands:
   * `mkdocs serve` - Start the live-reloading docs server.
   * `mkdocs build` - Build the documentation site.
   * `mkdocs -h` - Print help message and exit.
5. Push your changes and open a pull request.
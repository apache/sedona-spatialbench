#!/usr/bin/env python3
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import argparse
import logging
import os
import shutil
import subprocess
from math import ceil, log
from pathlib import Path
from tempfile import mkdtemp
import concurrent.futures


def main():
    # take some args: output dir, scale factor, mb per file

    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=str, default=mkdtemp(prefix="spatialbench-data-"),
                        help="Output directory for generated data")
    parser.add_argument("--scale-factor", type=int, default=1, help="Scale factor for data generation")
    parser.add_argument("--mb-per-file", type=int, default=256, help="Rough megabytes per output file")
    args = parser.parse_args()

    generate_data(args.scale_factor, args.mb_per_file, args.output_dir)
    print(f"Data generated at {args.output_dir}")


def generate_data(scale_factor: int, target_mb: int, output_dir: str) -> dict[str, str]:
    """Generate SpatialBench data using spatialbench-cli and return table->filepath mapping"""
    # Aim for ~256 MB per partition file (on-disk). Use rough SF=1 size estimates per format.
    # These are estimates; actual sizes vary by codec/implementation.

    tables = [
        "building",
        "customer",
        "driver",
        "trip",
        "vehicle",
        "zone",
    ]

    size_mb_sf1 = {
        # Values from testing sf=1
        "building": 1.5,
        "customer": 1.7,
        "driver": 30.0 / 1024,
        "trip": 280.0,
        "vehicle": 4.0 / 1024,
        # step functioned table size
        "zone": 141.7,
    }

    # Compute partitions per table by scaling linearly with SF and dividing by target size.
    def parts_for(table: str) -> int:
        size_mb = size_mb_sf1.get(table, 1.0) * float(scale_factor)
        return max(1, int(ceil(size_mb / target_mb)))

    num_partitions = {table: parts_for(table) for table in tables}

    # Zone table doesn't scale linearly. It has a step function.
    if scale_factor < 10:
        zone_size_mb = 141.7
    elif scale_factor < 100:
        zone_size_mb = 2.09 * 1024
    elif scale_factor < 1000:
        zone_size_mb = 5.68 * 1024
    else:
        # TODO this number is wrong, but we don't have data for >1000
        zone_size_mb = 8.0 * 1024
    num_partitions["zone"] = max(1, int(ceil(zone_size_mb / target_mb)))

    # buildings scale sublinearly with sf: 20,000 × (1 + log₂(10)) rows
    buildings_rows_per_mb = 13367.47  # did some empirical testing
    building_size_mb = 20_000.0 * (1.0 + log(scale_factor, 2)) / buildings_rows_per_mb
    num_partitions["building"] = max(1, int(ceil(building_size_mb / target_mb)))

    return _generate_data(scale_factor, num_partitions, output_dir)


def _generate_data(scale_factor: int, num_partitions: dict[str, int], output_path: str) -> dict[str, str]:
    """util method for generating data using a CLI command.

    Most useful for benchmarks that use the TPC-H-ish data generation tools.
    After generation, repartitions the data using DuckDB for optimal performance.
    """
    try:
        tables = list(num_partitions.keys())
        # Ensure base directories exist
        Path(output_path).mkdir(parents=True, exist_ok=True)

        def run_one(table: str) -> None:

            result = subprocess.run(
                [
                    "spatialbench-cli",
                    "-s",
                    str(scale_factor),
                    f"--format=parquet",
                    f"--parts={num_partitions[table]}",
                    f"--tables={table}",
                    f"--output-dir={output_path}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            if result.stderr:
                logging.warning("Command errors:")
                logging.warning(result.stderr)

        # Launch all generation tasks in parallel threads
        futures = []
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=os.cpu_count() or 4
        ) as executor:
            for table in tables:
                futures.append(executor.submit(run_one, table))
            # Raise the first exception if any
            for fut in concurrent.futures.as_completed(futures):
                fut.result()
    except subprocess.CalledProcessError as e:
        logging.warning(f"Error running spatialbench-cli: {e}")
        logging.warning(f"Return code: {e.returncode}")
        if e.stdout:
            logging.warning(f"Stdout: {e.stdout}")
        if e.stderr:
            logging.warning(f"Stderr: {e.stderr}")
        raise

    return {table: f"{output_path}/{table}" for table in tables}


if __name__ == "__main__":
    main()

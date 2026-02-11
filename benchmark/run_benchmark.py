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

"""
SpatialBench Benchmark Runner

This script runs spatial benchmarks comparing SedonaDB, DuckDB, and GeoPandas
on the SpatialBench queries at a specified scale factor.
"""

import argparse
import json
import multiprocessing
import signal
import sys
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# Add spatialbench-queries directory to path to import query modules
# Use append (not insert) so installed packages like spatial_polars are found first
sys.path.append(str(Path(__file__).parent.parent / "spatialbench-queries"))

# Constants
QUERY_COUNT = 12
TABLES = ["building", "customer", "driver", "trip", "vehicle", "zone"]


@dataclass
class BenchmarkResult:
    """Result of a single query benchmark."""
    query: str
    engine: str
    time_seconds: float | None
    row_count: int | None
    status: str  # "success", "error", "timeout"
    error_message: str | None = None


@dataclass
class BenchmarkSuite:
    """Complete benchmark suite results."""
    engine: str
    scale_factor: float
    results: list[BenchmarkResult] = field(default_factory=list)
    total_time: float = 0.0
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    version: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return {
            "engine": self.engine,
            "version": self.version,
            "scale_factor": self.scale_factor,
            "timestamp": self.timestamp,
            "total_time": self.total_time,
            "results": [
                {
                    "query": r.query,
                    "time_seconds": r.time_seconds,
                    "row_count": r.row_count,
                    "status": r.status,
                    "error_message": r.error_message,
                }
                for r in self.results
            ],
        }


class QueryTimeoutError(Exception):
    """Raised when a query times out."""
    pass


def _run_query_in_process(
    result_queue: multiprocessing.Queue,
    engine_class: type,
    data_paths: dict[str, str],
    query_name: str,
    query_sql: str | None,
):
    """Worker function to run a query in a separate process.

    This allows us to forcefully terminate queries that hang or consume
    too much memory, which SIGALRM cannot do for native code.
    """
    try:
        # For Spatial Polars, ensure the package is imported first to register namespace
        if engine_class.__name__ == "SpatialPolarsBenchmark":
            import spatial_polars as _sp  # noqa: F401

        benchmark = engine_class(data_paths)
        benchmark.setup()
        try:
            start_time = time.perf_counter()
            row_count, _ = benchmark.execute_query(query_name, query_sql)
            elapsed = time.perf_counter() - start_time
            result_queue.put({
                "status": "success",
                "time_seconds": round(elapsed, 2),
                "row_count": row_count,
                "error_message": None,
            })
        finally:
            benchmark.teardown()
    except Exception as e:
        result_queue.put({
            "status": "error",
            "time_seconds": None,
            "row_count": None,
            "error_message": str(e),
        })


def get_data_paths(data_dir: str) -> dict[str, str]:
    """Get paths to all data tables.

    Supports two data formats:
    1. Directory format: table_name/*.parquet (e.g., building/building.1.parquet)
    2. Single file format: table_name.parquet (e.g., building.parquet)

    Returns directory paths for directories containing parquet files.
    Both DuckDB, pandas, and SedonaDB can read all parquet files from a directory.
    """
    data_path = Path(data_dir)
    paths = {}

    for table in TABLES:
        table_path = data_path / table
        # Check for directory format first (from HF: building/building.1.parquet)
        if table_path.is_dir():
            parquet_files = list(table_path.glob("*.parquet"))
            if parquet_files:
                # Return directory path - DuckDB, pandas, and SedonaDB all support reading
                # all parquet files from a directory
                paths[table] = str(table_path)
            else:
                paths[table] = str(table_path)
        # Then check for single file format (building.parquet)
        elif (data_path / f"{table}.parquet").exists():
            paths[table] = str(data_path / f"{table}.parquet")
        # Finally check for any matching parquet files
        else:
            matches = list(data_path.glob(f"{table}*.parquet"))
            if matches:
                paths[table] = str(matches[0])

    return paths


class BaseBenchmark(ABC):
    """Base class for benchmark runners."""

    def __init__(self, data_paths: dict[str, str], engine_name: str):
        self.data_paths = data_paths
        self.engine_name = engine_name

    @abstractmethod
    def setup(self) -> None:
        """Initialize the benchmark environment."""
        pass

    @abstractmethod
    def teardown(self) -> None:
        """Cleanup the benchmark environment."""
        pass

    @abstractmethod
    def execute_query(self, query_name: str, query: str | None) -> tuple[int, Any]:
        """Execute a query and return (row_count, result)."""
        pass

    def run_query(self, query_name: str, query: str | None = None, timeout: int = 1200) -> BenchmarkResult:
        """Run a single query with timeout handling."""
        start_time = time.perf_counter()
        try:
            with timeout_handler(timeout, query_name):
                row_count, _ = self.execute_query(query_name, query)
                elapsed = time.perf_counter() - start_time
                return BenchmarkResult(
                    query=query_name,
                    engine=self.engine_name,
                    time_seconds=round(elapsed, 2),
                    row_count=row_count,
                    status="success",
                )
        except (TimeoutError, QueryTimeoutError) as e:
            return BenchmarkResult(
                query=query_name,
                engine=self.engine_name,
                time_seconds=timeout,
                row_count=None,
                status="timeout",
                error_message=str(e),
            )
        except Exception as e:
            elapsed = time.perf_counter() - start_time
            # If elapsed time is close to or exceeds timeout, treat as timeout
            # This handles cases where native code (Rust/C) throws a different exception
            # when interrupted by SIGALRM
            if elapsed >= timeout * 0.95:  # 95% of timeout to account for timing variance
                return BenchmarkResult(
                    query=query_name,
                    engine=self.engine_name,
                    time_seconds=timeout,
                    row_count=None,
                    status="timeout",
                    error_message=f"Query timed out after {timeout}s (original error: {e})",
                )
            return BenchmarkResult(
                query=query_name,
                engine=self.engine_name,
                time_seconds=None,
                row_count=None,
                status="error",
                error_message=str(e),
            )


class DuckDBBenchmark(BaseBenchmark):
    """DuckDB benchmark runner."""

    def __init__(self, data_paths: dict[str, str]):
        super().__init__(data_paths, "duckdb")
        self._conn = None

    def setup(self) -> None:
        import duckdb
        self._conn = duckdb.connect()
        self._conn.execute("LOAD spatial;")
        self._conn.execute("SET enable_external_file_cache = false;")
        for table, path in self.data_paths.items():
            # DuckDB needs glob pattern for directories, add /*.parquet if path is a directory
            parquet_path = path
            if Path(path).is_dir():
                parquet_path = str(Path(path) / "*.parquet")
            self._conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{parquet_path}')")

    def teardown(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def execute_query(self, query_name: str, query: str | None) -> tuple[int, Any]:
        result = self._conn.execute(query).fetchall()
        return len(result), result


class GeoPandasBenchmark(BaseBenchmark):
    """GeoPandas benchmark runner."""

    def __init__(self, data_paths: dict[str, str]):
        super().__init__(data_paths, "geopandas")
        self._queries = None

    def setup(self) -> None:
        import importlib.util
        geopandas_path = Path(__file__).parent.parent / "spatialbench-queries" / "geopandas_queries.py"
        spec = importlib.util.spec_from_file_location("geopandas_queries", geopandas_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self._queries = {f"q{i}": getattr(module, f"q{i}") for i in range(1, QUERY_COUNT + 1)}

    def teardown(self) -> None:
        self._queries = None

    def execute_query(self, query_name: str, query: str | None) -> tuple[int, Any]:
        if query_name not in self._queries:
            raise ValueError(f"Query {query_name} not found")
        result = self._queries[query_name](self.data_paths)
        return len(result), result


class SedonaDBBenchmark(BaseBenchmark):
    """SedonaDB benchmark runner."""

    def __init__(self, data_paths: dict[str, str]):
        super().__init__(data_paths, "sedonadb")
        self._sedona = None

    def setup(self) -> None:
        import sedonadb
        self._sedona = sedonadb.connect()
        for table, path in self.data_paths.items():
            # SedonaDB needs glob pattern for directories
            parquet_path = path
            if Path(path).is_dir():
                parquet_path = str(Path(path) / "*.parquet")
            self._sedona.read_parquet(parquet_path).to_view(table, overwrite=True)

    def teardown(self) -> None:
        self._sedona = None

    def execute_query(self, query_name: str, query: str | None) -> tuple[int, Any]:
        result = self._sedona.sql(query).to_pandas()
        return len(result), result


class SpatialPolarsBenchmark(BaseBenchmark):
    """Spatial Polars benchmark runner."""

    def __init__(self, data_paths: dict[str, str]):
        super().__init__(data_paths, "spatial_polars")
        self._queries = None

    def setup(self) -> None:
        # spatial_polars package is already imported in _run_query_in_process
        # to register .spatial namespace before any module loading

        # Load query functions directly from the module
        import importlib.util
        query_file = Path(__file__).parent.parent / "spatialbench-queries" / "spatial_polars.py"
        spec = importlib.util.spec_from_file_location("spatial_polars_queries", query_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self._queries = {f"q{i}": getattr(module, f"q{i}") for i in range(1, QUERY_COUNT + 1)}

    def teardown(self) -> None:
        self._queries = None

    def execute_query(self, query_name: str, query: str | None) -> tuple[int, Any]:
        if query_name not in self._queries:
            raise ValueError(f"Query {query_name} not found")
        result = self._queries[query_name](self.data_paths)
        return len(result), result


def get_sql_queries(dialect: str) -> dict[str, str]:
    """Get SQL queries for a specific dialect from print_queries.py."""
    from print_queries import DuckDBSpatialBenchBenchmark, SedonaDBSpatialBenchBenchmark

    dialects = {
        "duckdb": DuckDBSpatialBenchBenchmark,
        "sedonadb": SedonaDBSpatialBenchBenchmark,
    }
    return dialects[dialect]().queries()


def run_query_isolated(
    engine_class: type,
    engine_name: str,
    data_paths: dict[str, str],
    query_name: str,
    query_sql: str | None,
    timeout: int,
) -> BenchmarkResult:
    """Run a single query in an isolated subprocess with hard timeout.

    This is more robust than SIGALRM because:
    1. Native code (C++/Rust) can be forcefully terminated
    2. Memory-hungry queries don't affect the main process
    3. Crashed queries don't invalidate the benchmark runner
    """
    result_queue = multiprocessing.Queue()
    process = multiprocessing.Process(
        target=_run_query_in_process,
        args=(result_queue, engine_class, data_paths, query_name, query_sql),
    )

    process.start()
    process.join(timeout=timeout)

    if process.is_alive():
        # Query exceeded timeout - forcefully terminate
        process.terminate()
        process.join(timeout=5)  # Give it 5 seconds to terminate gracefully

        if process.is_alive():
            # Still alive - kill it
            process.kill()
            process.join(timeout=2)

        return BenchmarkResult(
            query=query_name,
            engine=engine_name,
            time_seconds=timeout,
            row_count=None,
            status="timeout",
            error_message=f"Query {query_name} timed out after {timeout} seconds (process killed)",
        )

    # Process completed - get result from queue
    try:
        result_data = result_queue.get_nowait()
        return BenchmarkResult(
            query=query_name,
            engine=engine_name,
            time_seconds=result_data["time_seconds"],
            row_count=result_data["row_count"],
            status=result_data["status"],
            error_message=result_data["error_message"],
        )
    except Exception:
        # Process died without putting result in queue
        return BenchmarkResult(
            query=query_name,
            engine=engine_name,
            time_seconds=None,
            row_count=None,
            status="error",
            error_message=f"Query {query_name} crashed (process exit code: {process.exitcode})",
        )


def run_benchmark(
    engine: str,
    data_paths: dict[str, str],
    queries: list[str] | None,
    timeout: int,
    scale_factor: float,
    runs: int = 3,
) -> BenchmarkSuite:
    """Generic benchmark runner for any engine.

    Each query runs in an isolated subprocess to ensure:
    - Hard timeout enforcement (process can be killed)
    - Memory isolation (one query can't OOM the runner)
    - Crash isolation (one query crash doesn't affect others)

    If runs > 1 and the first run succeeds, additional runs are performed
    and the average time is reported for fair comparison.
    """

    from importlib.metadata import version as pkg_version

    # Engine configurations
    configs = {
        "duckdb": {
            "class": DuckDBBenchmark,
            "version_getter": lambda: __import__("duckdb").__version__,
            "queries_getter": lambda: get_sql_queries("duckdb"),
        },
        "geopandas": {
            "class": GeoPandasBenchmark,
            "version_getter": lambda: pkg_version("geopandas"),
            "queries_getter": lambda: {f"q{i}": None for i in range(1, QUERY_COUNT + 1)},
        },
        "sedonadb": {
            "class": SedonaDBBenchmark,
            "version_getter": lambda: pkg_version("sedonadb"),
            "queries_getter": lambda: get_sql_queries("sedonadb"),
        },
        "spatial_polars": {
            "class": SpatialPolarsBenchmark,
            "version_getter": lambda: pkg_version("spatial-polars"),
            "queries_getter": lambda: {f"q{i}": None for i in range(1, QUERY_COUNT + 1)},
        },
    }

    config = configs[engine]
    version = config["version_getter"]()

    # Format engine name for display
    display_name = engine.replace("_", " ").title()

    print(f"\n{'=' * 60}")
    print(f"Running {display_name} Benchmark")
    print(f"{'=' * 60}")
    print(f"{display_name} version: {version}")
    if runs > 1:
        print(f"Runs per query: {runs} (average will be reported)")

    suite = BenchmarkSuite(engine=engine, scale_factor=scale_factor, version=version)
    all_queries = config["queries_getter"]()
    engine_class = config["class"]

    for query_name, query_sql in all_queries.items():
        if queries and query_name not in queries:
            continue

        print(f"  Running {query_name}...", end=" ", flush=True)

        # First run
        result = run_query_isolated(
            engine_class=engine_class,
            engine_name=engine,
            data_paths=data_paths,
            query_name=query_name,
            query_sql=query_sql,
            timeout=timeout,
        )

        # If first run succeeded and we want multiple runs, do additional runs
        if result.status == "success" and runs > 1:
            run_times = [result.time_seconds]

            for run_num in range(2, runs + 1):
                additional_result = run_query_isolated(
                    engine_class=engine_class,
                    engine_name=engine,
                    data_paths=data_paths,
                    query_name=query_name,
                    query_sql=query_sql,
                    timeout=timeout,
                )
                if additional_result.status == "success":
                    run_times.append(additional_result.time_seconds)
                else:
                    # If any subsequent run fails, just use successful runs
                    break

            # Calculate average of all successful runs
            avg_time = round(sum(run_times) / len(run_times), 2)
            result = BenchmarkResult(
                query=query_name,
                engine=engine,
                time_seconds=avg_time,
                row_count=result.row_count,
                status="success",
                error_message=None,
            )
            print(f"{avg_time}s avg ({len(run_times)} runs, {result.row_count} rows)")
        elif result.status == "success":
            print(f"{result.time_seconds}s ({result.row_count} rows)")
        else:
            print(f"{result.status.upper()}: {result.error_message}")

        suite.results.append(result)
        if result.status == "success":
            suite.total_time += result.time_seconds

    return suite


def print_summary(results: list[BenchmarkSuite]) -> None:
    """Print a summary comparison table."""
    print(f"\n{'=' * 80}")
    print("BENCHMARK SUMMARY")
    print("=" * 80)

    all_queries = sorted(
        {r.query for suite in results for r in suite.results},
        key=lambda x: int(x[1:])
    )

    data = {
        suite.engine: {
            r.query: f"{r.time_seconds:.2f}s" if r.status == "success" else r.status.upper()
            for r in suite.results
        }
        for suite in results
    }

    engines = [s.engine for s in results]
    header = f"{'Query':<10}" + "".join(f"{e:<15}" for e in engines)
    print(header)
    print("-" * len(header))

    for query in all_queries:
        row = f"{query:<10}" + "".join(f"{data.get(e, {}).get(query, 'N/A'):<15}" for e in engines)
        print(row)

    print("-" * len(header))
    print(f"{'Total':<10}" + "".join(f"{s.total_time:.2f}s{'':<9}" for s in results))


def save_results(results: list[BenchmarkSuite], output_file: str) -> None:
    """Save results to JSON file."""
    output = {
        "benchmark": "spatialbench",
        "version": "0.1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "results": [suite.to_dict() for suite in results],
    }

    with open(output_file, "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nResults saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Run SpatialBench benchmarks comparing SedonaDB, DuckDB, GeoPandas, and Spatial Polars"
    )
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Path to directory containing benchmark data (parquet files)")
    parser.add_argument("--engines", type=str, default="duckdb,geopandas,sedonadb,spatial_polars",
                        help="Comma-separated list of engines to benchmark")
    parser.add_argument("--queries", type=str, default=None,
                        help="Comma-separated list of queries to run (e.g., q1,q2,q3)")
    parser.add_argument("--timeout", type=int, default=10,
                        help="Query timeout in seconds (default: 10)")
    parser.add_argument("--runs", type=int, default=3,
                        help="Number of runs per query for averaging (default: 3)")
    parser.add_argument("--output", type=str, default="benchmark_results.json",
                        help="Output file for results")
    parser.add_argument("--scale-factor", type=float, default=1,
                        help="Scale factor of the data (for reporting only)")

    args = parser.parse_args()

    engines = [e.strip().lower() for e in args.engines.split(",")]
    valid_engines = {"duckdb", "geopandas", "sedonadb", "spatial_polars"}

    for e in engines:
        if e not in valid_engines:
            print(f"Error: Unknown engine '{e}'. Valid options: {valid_engines}")
            sys.exit(1)

    queries = [q.strip().lower() for q in args.queries.split(",")] if args.queries else None

    data_paths = get_data_paths(args.data_dir)
    if not data_paths:
        print(f"Error: No data files found in {args.data_dir}")
        sys.exit(1)

    print("Data paths:")
    for table, path in data_paths.items():
        print(f"  {table}: {path}")

    results = [
        run_benchmark(engine, data_paths, queries, args.timeout, args.scale_factor, args.runs)
        for engine in engines
    ]

    print_summary(results)
    save_results(results, args.output)


if __name__ == "__main__":
    # Use 'spawn' on macOS to avoid issues with forking and native code
    # On Linux (GitHub Actions), 'fork' is default and usually works fine
    import platform
    if platform.system() == 'Darwin':
        try:
            multiprocessing.set_start_method('spawn', force=True)
        except RuntimeError:
            pass  # Already set
    main()

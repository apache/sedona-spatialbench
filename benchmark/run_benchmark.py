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
    table_load_time: float | None = None  # Time to load tables (e.g., for BigQuery)
    table_load_details: dict[str, Any] | None = None  # Per-table timing details

    def to_dict(self) -> dict[str, Any]:
        result = {
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
        if self.table_load_time is not None:
            result["table_load_time"] = self.table_load_time
        if self.table_load_details is not None:
            result["table_load_details"] = self.table_load_details
        return result


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


def get_bigquery_data_paths(gcs_base_path: str, scale_factor: float | None = None) -> dict[str, str]:
    """Get GCS paths to all data tables for BigQuery.

    Args:
        gcs_base_path: Base GCS path (e.g., gs://bucket/path/to/data or gs://bucket/path/)
        scale_factor: If provided and path ends with /, appends sf-{scale_factor}/ to the path

    Returns:
        Dict mapping table names to GCS paths
    """
    base = gcs_base_path.rstrip("/")

    # If scale_factor is provided and the base path looks like it needs a scale factor suffix
    # (i.e., doesn't already end with sf-X), append it
    if scale_factor is not None:
        sf_int = int(scale_factor)
        sf_suffix = f"sf-{sf_int}"
        if not base.endswith(sf_suffix):
            base = f"{base}/{sf_suffix}"

    paths = {}
    for table in TABLES:
        # Assume single parquet file per table
        paths[table] = f"{base}/{table}.parquet"
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


@dataclass
class TableLoadResult:
    """Result of loading a single table."""
    table: str
    external_table_time: float
    internal_table_time: float
    total_time: float
    status: str  # "success", "error"
    error_message: str | None = None


class BigQueryBenchmark(BaseBenchmark):
    """BigQuery benchmark runner using ADBC driver.

    This benchmark uses BigQuery's native GEOGRAPHY type with spherical coordinates.
    Data is loaded from GCS Parquet files through external tables, then copied to
    internal tables for optimized query performance.

    Uses the ADBC BigQuery driver which works with Application Default Credentials (ADC).
    Run `gcloud auth application-default login --project=<project>` to authenticate.

    Environment variables:
    - BIGQUERY_PROJECT_ID: GCP project ID (required)
    - BIGQUERY_DATASET_ID: BigQuery dataset ID (required)
    - BIGQUERY_GCS_BASE_PATH: Base GCS path for data (e.g., gs://bucket/path/), scale factor folder appended
    """

    # Class-level flag to indicate tables have been pre-loaded
    _tables_preloaded = False
    _preload_results: list["TableLoadResult"] = []
    _scale_factor: int | None = None  # Class-level scale factor for table suffixes

    def __init__(self, data_paths: dict[str, str], gcs_base_path: str | None = None,
                 skip_table_loading: bool = False, scale_factor: int | None = None):
        """Initialize BigQuery benchmark.

        Args:
            data_paths: Mapping of table names to GCS paths (gs://bucket/path/to/table.parquet)
            gcs_base_path: Base GCS path for parquet files. If provided, data_paths values
                           are treated as relative paths under this base.
            skip_table_loading: If True, skip table loading (assumes tables already exist)
            scale_factor: Scale factor for table name suffix (e.g., 1 creates trip_sf1)
        """
        super().__init__(data_paths, "bigquery")
        self._con = None
        self._project_id = None
        self._dataset_id = None
        self._gcs_base_path = gcs_base_path
        self._skip_table_loading = skip_table_loading or BigQueryBenchmark._tables_preloaded
        self._table_load_results: list[TableLoadResult] = []
        # Use instance scale_factor or fall back to class-level
        self._instance_scale_factor = scale_factor if scale_factor is not None else BigQueryBenchmark._scale_factor

    def _create_connection(self):
        """Create ADBC BigQuery connection."""
        import warnings
        import adbc_driver_bigquery.dbapi

        db_kwargs = {
            "adbc.bigquery.sql.project_id": self._project_id,
            "adbc.bigquery.sql.dataset_id": self._dataset_id,
        }
        # Suppress autocommit warning from ADBC
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Cannot disable autocommit")
            return adbc_driver_bigquery.dbapi.connect(db_kwargs=db_kwargs)

    @classmethod
    def preload_tables(cls, data_paths: dict[str, str], gcs_base_path: str | None = None,
                       scale_factor: int | None = None) -> list["TableLoadResult"]:
        """Pre-load tables to BigQuery before running queries.

        This is called once before running isolated query processes.
        Sets the class-level _tables_preloaded flag so subsequent instances
        skip table loading.

        Args:
            data_paths: Mapping of table names to GCS paths
            gcs_base_path: Base GCS path for parquet files
            scale_factor: Scale factor for table name suffix (e.g., 1 creates trip_sf1)

        Returns:
            List of TableLoadResult for each table
        """
        # Store scale factor at class level for subprocess access
        cls._scale_factor = scale_factor

        benchmark = cls(data_paths, gcs_base_path, skip_table_loading=False, scale_factor=scale_factor)
        benchmark._skip_table_loading = False  # Force loading for preload

        import os

        benchmark._project_id = os.environ.get("BIGQUERY_PROJECT_ID")
        benchmark._dataset_id = os.environ.get("BIGQUERY_DATASET_ID")

        if not benchmark._project_id:
            raise ValueError("BIGQUERY_PROJECT_ID environment variable is required")
        if not benchmark._dataset_id:
            raise ValueError("BIGQUERY_DATASET_ID environment variable is required")

        benchmark._con = benchmark._create_connection()

        # Load tables
        benchmark._load_tables()

        # Set class-level flag
        cls._tables_preloaded = True
        cls._preload_results = benchmark._table_load_results

        benchmark._con.close()
        return benchmark._table_load_results

    @classmethod
    def reset_preload_state(cls):
        """Reset the preload state (useful for testing)."""
        cls._tables_preloaded = False
        cls._preload_results = []
        cls._scale_factor = None

    def setup(self) -> None:
        """Initialize BigQuery ADBC connection and optionally load tables."""
        import os

        self._project_id = os.environ.get("BIGQUERY_PROJECT_ID")
        self._dataset_id = os.environ.get("BIGQUERY_DATASET_ID")

        if not self._project_id:
            raise ValueError("BIGQUERY_PROJECT_ID environment variable is required")
        if not self._dataset_id:
            raise ValueError("BIGQUERY_DATASET_ID environment variable is required")

        self._con = self._create_connection()

        # Only load tables if not skipping
        if not self._skip_table_loading:
            self._load_tables()

    def _get_gcs_path(self, table: str) -> str:
        """Get the GCS path for a table."""
        path = self.data_paths.get(table)
        if not path:
            raise ValueError(f"No path configured for table: {table}")

        if path.startswith("gs://"):
            return path

        if self._gcs_base_path:
            base = self._gcs_base_path.rstrip("/")
            return f"{base}/{path}"

        raise ValueError(f"Path {path} is not a GCS path and no gcs_base_path configured")

    def _load_tables(self) -> None:
        """Load all tables from GCS Parquet files into BigQuery internal tables."""
        self._table_load_results = []

        for table in self.data_paths.keys():
            result = self._load_single_table(table)
            self._table_load_results.append(result)
            if result.status == "error":
                raise RuntimeError(f"Failed to load table {table}: {result.error_message}")

    def _get_table_suffix(self) -> str:
        """Get the table name suffix based on scale factor."""
        sf = self._instance_scale_factor
        if sf is not None:
            return f"_sf{sf}"
        return ""

    def _load_single_table(self, table: str) -> TableLoadResult:
        """Load a single table from GCS to BigQuery internal table."""
        gcs_path = self._get_gcs_path(table)
        suffix = self._get_table_suffix()
        external_table_name = f"{table}{suffix}_external"
        internal_table_name = f"{table}{suffix}"

        full_external_table = f"{self._project_id}.{self._dataset_id}.{external_table_name}"
        full_internal_table = f"{self._project_id}.{self._dataset_id}.{internal_table_name}"

        try:
            # Step 1: Create external table pointing to GCS Parquet
            external_start = time.perf_counter()

            create_external_sql = f"""
            CREATE OR REPLACE EXTERNAL TABLE `{full_external_table}`
            OPTIONS (
              format = 'PARQUET',
              uris = ['{gcs_path}']
            )
            """
            with self._con.cursor() as cur:
                cur.execute(create_external_sql)
            external_time = time.perf_counter() - external_start

            # Step 2: Create internal table from external table
            internal_start = time.perf_counter()

            create_internal_sql = f"""
            CREATE OR REPLACE TABLE `{full_internal_table}` AS
            SELECT * FROM `{full_external_table}`
            """
            with self._con.cursor() as cur:
                cur.execute(create_internal_sql)
            internal_time = time.perf_counter() - internal_start

            return TableLoadResult(
                table=table,
                external_table_time=round(external_time, 3),
                internal_table_time=round(internal_time, 3),
                total_time=round(external_time + internal_time, 3),
                status="success",
            )

        except Exception as e:
            return TableLoadResult(
                table=table,
                external_table_time=0,
                internal_table_time=0,
                total_time=0,
                status="error",
                error_message=str(e),
            )

    def teardown(self) -> None:
        """Clean up BigQuery resources."""
        if self._con:
            # Optionally clean up tables here
            # For now, leave tables in place for inspection
            self._con.close()
            self._con = None

    def execute_query(self, query_name: str, query: str | None) -> tuple[int, Any]:
        """Execute a query on BigQuery and return row count and results."""
        if query is None:
            raise ValueError(f"Query {query_name} requires SQL")

        # Replace table names with suffixed versions
        modified_query = self._apply_table_suffix(query)
        with self._con.cursor() as cur:
            cur.execute(modified_query)
            result = cur.fetch_arrow_table()
        return len(result), result

    def _apply_table_suffix(self, query: str) -> str:
        """Replace bare table names with scale-factor-suffixed versions."""
        suffix = self._get_table_suffix()
        if not suffix:
            return query

        import re
        modified = query
        for table in TABLES:
            # Match table name as a whole word (not part of another identifier)
            # Handles: FROM table, JOIN table, table., table WHERE, etc.
            pattern = rf'\b{table}\b'
            replacement = f'{table}{suffix}'
            modified = re.sub(pattern, replacement, modified)
        return modified

    def get_table_load_times(self) -> dict[str, TableLoadResult]:
        """Get table load timing results for analysis."""
        return {r.table: r for r in self._table_load_results}

    def get_total_load_time(self) -> float:
        """Get total time spent loading all tables."""
        return sum(r.total_time for r in self._table_load_results)


def get_sql_queries(dialect: str) -> dict[str, str]:
    """Get SQL queries for a specific dialect from print_queries.py."""
    from print_queries import (
        BigQuerySpatialBenchBenchmark,
        DuckDBSpatialBenchBenchmark,
        SedonaDBSpatialBenchBenchmark,
    )

    dialects = {
        "duckdb": DuckDBSpatialBenchBenchmark,
        "sedonadb": SedonaDBSpatialBenchBenchmark,
        "bigquery": BigQuerySpatialBenchBenchmark,
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
    output_file: str | None = None,
) -> BenchmarkSuite:
    """Generic benchmark runner for any engine.

    Each query runs in an isolated subprocess to ensure:
    - Hard timeout enforcement (process can be killed)
    - Memory isolation (one query can't OOM the runner)
    - Crash isolation (one query crash doesn't affect others)

    If runs > 1 and the first run succeeds, additional runs are performed
    and the average time is reported for fair comparison.

    If output_file is provided, results are saved incrementally after each
    query so that partial results survive if the runner crashes mid-way.
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
        "bigquery": {
            "class": BigQueryBenchmark,
            "version_getter": lambda: pkg_version("google-cloud-bigquery"),
            "queries_getter": lambda: get_sql_queries("bigquery"),
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

    # BigQuery: Pre-load tables before running queries
    if engine == "bigquery":
        sf_int = int(scale_factor)
        print(f"  Pre-loading tables to BigQuery (scale factor: {sf_int})...")
        try:
            load_results = BigQueryBenchmark.preload_tables(data_paths, scale_factor=sf_int)
            table_load_time = sum(r.total_time for r in load_results)
            suite.table_load_time = round(table_load_time, 3)
            suite.table_load_details = {
                r.table: {
                    "external_table_time": r.external_table_time,
                    "internal_table_time": r.internal_table_time,
                    "total_time": r.total_time,
                    "status": r.status,
                }
                for r in load_results
            }
            print(f"  Table loading complete: {suite.table_load_time}s total")
            for r in load_results:
                suffix = f"_sf{sf_int}"
                print(f"    {r.table}{suffix}: {r.total_time}s (external: {r.external_table_time}s, internal: {r.internal_table_time}s)")
        except Exception as e:
            print(f"  Error loading tables: {e}")
            raise

    # Determine which queries will be run
    query_items = [
        (qname, qsql) for qname, qsql in all_queries.items()
        if not queries or qname in queries
    ]

    # Pre-populate all queries as "not_started" so even a total crash
    # (e.g. OOM killing the runner) leaves a file showing what was attempted
    for query_name, _ in query_items:
        suite.results.append(BenchmarkResult(
            query=query_name,
            engine=engine,
            time_seconds=None,
            row_count=None,
            status="not_started",
            error_message=None,
        ))
    if output_file:
        save_results([suite], output_file)

    # Install a SIGTERM handler so we flush results if the runner is shutting down
    def _sigterm_handler(signum, frame):
        print(f"\nReceived signal {signum}, saving partial results...", flush=True)
        if output_file:
            save_results([suite], output_file)
        sys.exit(128 + signum)

    prev_handler = signal.signal(signal.SIGTERM, _sigterm_handler)

    try:
        for idx, (query_name, query_sql) in enumerate(query_items):
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

            # Replace the pre-populated "not_started" entry with the actual result
            suite.results[idx] = result
            if result.status == "success":
                suite.total_time += result.time_seconds

            # Save partial results after each query so they survive crashes
            if output_file:
                save_results([suite], output_file)
    finally:
        signal.signal(signal.SIGTERM, prev_handler)

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
    total_row = f"{'Total':<10}" + "".join(f"{s.total_time:.2f}s{'':<9}" for s in results)
    print(total_row)

    # Show table load time for engines that have it (e.g., BigQuery)
    engines_with_load = [s for s in results if s.table_load_time is not None]
    if engines_with_load:
        load_row = f"{'Load Time':<10}" + "".join(
            f"{s.table_load_time:.2f}s{'':<9}" if s.table_load_time is not None else f"{'N/A':<15}"
            for s in results
        )
        print(load_row)
        combined_row = f"{'Combined':<10}" + "".join(
            f"{(s.total_time + (s.table_load_time or 0)):.2f}s{'':<9}"
            for s in results
        )
        print(combined_row)


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
        description="Run SpatialBench benchmarks comparing SedonaDB, DuckDB, GeoPandas, Spatial Polars, and BigQuery"
    )
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Path to directory containing benchmark data (parquet files). For BigQuery, use GCS paths.")
    parser.add_argument("--engines", type=str, default="duckdb,geopandas,sedonadb,spatial_polars",
                        help="Comma-separated list of engines to benchmark (add 'bigquery' for BigQuery)")
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
    parser.add_argument("--gcs-base-path", type=str, default=None,
                        help="Base GCS path for BigQuery data (e.g., gs://bucket/path/to/data)")

    args = parser.parse_args()

    engines = [e.strip().lower() for e in args.engines.split(",")]
    valid_engines = {"duckdb", "geopandas", "sedonadb", "spatial_polars", "bigquery"}

    for e in engines:
        if e not in valid_engines:
            print(f"Error: Unknown engine '{e}'. Valid options: {valid_engines}")
            sys.exit(1)

    # Get GCS base path from argument or environment variable
    import os
    gcs_base_path = args.gcs_base_path or os.environ.get("BIGQUERY_GCS_BASE_PATH")

    # Check BigQuery requirements
    if "bigquery" in engines and not gcs_base_path:
        print("Error: BigQuery engine requires --gcs-base-path argument or BIGQUERY_GCS_BASE_PATH environment variable")
        sys.exit(1)

    queries = [q.strip().lower() for q in args.queries.split(",")] if args.queries else None

    # Prepare data paths for local engines (non-BigQuery)
    local_engines = [e for e in engines if e != "bigquery"]
    bigquery_engines = [e for e in engines if e == "bigquery"]

    data_paths = {}
    bigquery_data_paths = {}

    if local_engines:
        data_paths = get_data_paths(args.data_dir)
        if not data_paths and local_engines:
            print(f"Error: No data files found in {args.data_dir}")
            sys.exit(1)

        print("Local data paths:")
        for table, path in data_paths.items():
            print(f"  {table}: {path}")

    if bigquery_engines:
        # Construct dataset ID from prefix + scale factor if not explicitly set
        dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
        if not dataset_id:
            dataset_prefix = os.environ.get("BIGQUERY_DATASET_PREFIX", "spatialbench_")
            sf_int = int(args.scale_factor)
            dataset_id = f"{dataset_prefix}sf{sf_int}"
            os.environ["BIGQUERY_DATASET_ID"] = dataset_id
            print(f"Using BigQuery dataset: {dataset_id}")

        bigquery_data_paths = get_bigquery_data_paths(gcs_base_path, args.scale_factor)
        print("BigQuery GCS paths:")
        for table, path in bigquery_data_paths.items():
            print(f"  {table}: {path}")

    results = []

    # Run local engine benchmarks
    for engine in local_engines:
        result = run_benchmark(engine, data_paths, queries, args.timeout, args.scale_factor, args.runs, args.output)
        results.append(result)

    # Run BigQuery benchmarks with GCS paths
    for engine in bigquery_engines:
        result = run_benchmark(engine, bigquery_data_paths, queries, args.timeout, args.scale_factor, args.runs, args.output)
        results.append(result)
        # Reset BigQuery preload state for potential re-runs
        BigQueryBenchmark.reset_preload_state()

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

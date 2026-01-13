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
import signal
import sys
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

# Add parent directory to path to import query modules
sys.path.insert(0, str(Path(__file__).parent.parent))

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
    scale_factor: int
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


@contextmanager
def timeout_handler(seconds: int, query_name: str):
    """Context manager for handling query timeouts (Unix only)."""
    def _handler(signum, frame):
        raise TimeoutError(f"Query {query_name} timed out after {seconds} seconds")
    
    if hasattr(signal, 'SIGALRM'):
        old_handler = signal.signal(signal.SIGALRM, _handler)
        signal.alarm(seconds)
        try:
            yield
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)
    else:
        # Windows: no timeout support
        yield


def get_data_paths(data_dir: str) -> dict[str, str]:
    """Get paths to all data tables."""
    data_path = Path(data_dir)
    paths = {}
    
    for table in TABLES:
        table_path = data_path / table
        if table_path.is_dir():
            parquet_files = list(table_path.glob("*.parquet"))
            paths[table] = str(table_path / "*.parquet") if parquet_files else str(table_path)
        elif (data_path / f"{table}.parquet").exists():
            paths[table] = str(data_path / f"{table}.parquet")
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
        except TimeoutError as e:
            return BenchmarkResult(
                query=query_name,
                engine=self.engine_name,
                time_seconds=None,
                row_count=None,
                status="timeout",
                error_message=str(e),
            )
        except Exception as e:
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
        self._conn.execute("INSTALL spatial; LOAD spatial;")
        self._conn.execute("SET enable_external_file_cache = false;")
        for table, path in self.data_paths.items():
            self._conn.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{path}')")
    
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
        geopandas_path = Path(__file__).parent.parent / "geopandas.py"
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
        import sedona.db
        self._sedona = sedona.db.connect()
        for table, path in self.data_paths.items():
            self._sedona.read_parquet(path).create_temp_view(table)
    
    def teardown(self) -> None:
        if self._sedona:
            self._sedona.close()
            self._sedona = None
    
    def execute_query(self, query_name: str, query: str | None) -> tuple[int, Any]:
        result = self._sedona.sql(query).collect()
        return len(result), result


def get_sql_queries(dialect: str) -> dict[str, str]:
    """Get SQL queries for a specific dialect from print_queries.py."""
    from print_queries import DuckDBSpatialBenchBenchmark, SedonaDBSpatialBenchBenchmark
    
    dialects = {
        "duckdb": DuckDBSpatialBenchBenchmark,
        "sedonadb": SedonaDBSpatialBenchBenchmark,
    }
    return dialects[dialect]().queries()


def run_benchmark(
    engine: str,
    data_paths: dict[str, str],
    queries: list[str] | None,
    timeout: int,
    scale_factor: int,
) -> BenchmarkSuite:
    """Generic benchmark runner for any engine."""
    
    # Engine configurations
    configs = {
        "duckdb": {
            "class": DuckDBBenchmark,
            "version_getter": lambda: __import__("duckdb").__version__,
            "queries_getter": lambda: get_sql_queries("duckdb"),
            "needs_sql": True,
        },
        "geopandas": {
            "class": GeoPandasBenchmark,
            "version_getter": lambda: __import__("geopandas").__version__,
            "queries_getter": lambda: {f"q{i}": None for i in range(1, QUERY_COUNT + 1)},
            "needs_sql": False,
        },
        "sedonadb": {
            "class": SedonaDBBenchmark,
            "version_getter": lambda: getattr(__import__("sedonadb"), "__version__", "unknown"),
            "queries_getter": lambda: get_sql_queries("sedonadb"),
            "needs_sql": True,
        },
    }
    
    config = configs[engine]
    version = config["version_getter"]()
    
    print(f"\n{'=' * 60}")
    print(f"Running {engine.title()} Benchmark")
    print(f"{'=' * 60}")
    print(f"{engine.title()} version: {version}")
    
    benchmark = config["class"](data_paths)
    suite = BenchmarkSuite(engine=engine, scale_factor=scale_factor, version=version)
    
    try:
        benchmark.setup()
        all_queries = config["queries_getter"]()
        
        for query_name, query_sql in all_queries.items():
            if queries and query_name not in queries:
                continue
            
            print(f"  Running {query_name}...", end=" ", flush=True)
            result = benchmark.run_query(query_name, query_sql, timeout)
            suite.results.append(result)
            
            if result.status == "success":
                print(f"{result.time_seconds}s ({result.row_count} rows)")
                suite.total_time += result.time_seconds
            else:
                print(f"{result.status.upper()}: {result.error_message}")
    finally:
        benchmark.teardown()
    
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
        description="Run SpatialBench benchmarks comparing SedonaDB, DuckDB, and GeoPandas"
    )
    parser.add_argument("--data-dir", type=str, required=True,
                        help="Path to directory containing benchmark data (parquet files)")
    parser.add_argument("--engines", type=str, default="duckdb,geopandas",
                        help="Comma-separated list of engines to benchmark")
    parser.add_argument("--queries", type=str, default=None,
                        help="Comma-separated list of queries to run (e.g., q1,q2,q3)")
    parser.add_argument("--timeout", type=int, default=600,
                        help="Query timeout in seconds (default: 600)")
    parser.add_argument("--output", type=str, default="benchmark_results.json",
                        help="Output file for results")
    parser.add_argument("--scale-factor", type=int, default=1,
                        help="Scale factor of the data (for reporting only)")
    
    args = parser.parse_args()
    
    engines = [e.strip().lower() for e in args.engines.split(",")]
    valid_engines = {"duckdb", "geopandas", "sedonadb"}
    
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
        run_benchmark(engine, data_paths, queries, args.timeout, args.scale_factor)
        for engine in engines
    ]
    
    print_summary(results)
    save_results(results, args.output)


if __name__ == "__main__":
    main()

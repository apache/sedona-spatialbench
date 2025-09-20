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
import inspect
import re
import sys


class SpatialBenchBenchmark:
    """A benchmark for the performance of analytical spatial queries on a spatial dataset.

    These queries are written in the Sedona/Spark SQL dialect. Because spatial functions are not as standardized as
    other analytical functions, many engines needs specific implementations of a couple of these queries where dialects
    vary slightly.

    To deal with these differences,  other engine-specific implementations of this benchmark subclass this class and
    override only the queries that need to be changed.

    """

    def queries(self) -> dict[str, str]:
        """
        Collects all methods of the subclass whose names start with 'q' followed by a number and have no arguments (other than self),
        and returns them as a dictionary of query functions, partially applied with the current instance.

        Returns:
            Dict[str, str]: A dictionary mapping query names to their corresponding functions.
        """

        queries = {}
        for name, method in inspect.getmembers(
                self.__class__, predicate=inspect.isfunction
        ):
            if re.fullmatch(r"q\d+", name):
                sig = inspect.signature(method)
                if len(sig.parameters) == 0:
                    queries[name] = method()
                else:
                    raise ValueError("Query methods must not take any arguments")

        # Sort queries numerically by extracting the number from the query name
        sorted_queries = dict(sorted(queries.items(), key=lambda x: int(x[0][1:])))

        return sorted_queries

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "SedonaSpark"

    @staticmethod
    def q1() -> str:
        return """
-- Q1: Find trips starting within 50km of Sedona city center, ordered by distance
SELECT
   t.t_tripkey, ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon, ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat, t.t_pickuptime,
   ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('POINT (-111.7610 34.8697)')) AS distance_to_center
FROM trip t
WHERE ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromText('POINT (-111.7610 34.8697)'), 0.45) -- 50km radius around Sedona center
ORDER BY distance_to_center ASC, t.t_tripkey ASC
               """

    @staticmethod
    def q2() -> str:
        return """
-- Q2: Count trips starting within Coconino County (Arizona) zone
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(ST_GeomFromWKB(t.t_pickuploc), (SELECT ST_GeomFromWKB(z.z_boundary) FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1))
               """

    @staticmethod
    def q3() -> str:
        return """
-- Q3: Monthly trip statistics within 15km radius of Sedona city center (10km base + 5km buffer)
SELECT
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month, COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_distance) AS avg_distance, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
   AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_DWithin(
             ST_GeomFromWKB(t.t_pickuploc),
             ST_GeomFromText('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))'), -- 10km bounding box around Sedona
             0.045 -- Additional 5km buffer
     )
GROUP BY pickup_month
ORDER BY pickup_month
"""

    @staticmethod
    def q4() -> str:
        return """
-- Q4: Zone distribution of top 1000 trips by tip amount
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM
   zone z
       JOIN (
       SELECT t.t_pickuploc
       FROM trip t
       ORDER BY t.t_tip DESC, t.t_tripkey ASC
           LIMIT 1000 -- Replace 1000 with x (how many top tips you want)
   ) top_trips ON ST_Within(ST_GeomFromWKB(top_trips.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q5() -> str:
        return """
-- Q5: Monthly travel patterns for repeat customers (convex hull of dropoff locations)
SELECT
   c.c_custkey, c.c_name AS customer_name,
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
   ST_Area(ST_ConvexHull(ST_Collect(ARRAY_AGG(ST_GeomFromWKB(t.t_dropoffloc))))) AS monthly_travel_hull_area,
   COUNT(*) as dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
ORDER BY dropoff_count DESC, c.c_custkey ASC
            """

    @staticmethod
    def q6() -> str:
        return """
-- Q6: Zone statistics for trips within 50km radius of Sedona city center
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups, AVG(t.t_totalamount) AS avg_distance,
   AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_Contains(ST_GeomFromText('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'), ST_GeomFromWKB(z.z_boundary)) -- 50km bounding box around Sedona
 AND ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q7() -> str:
        return """
-- Q7: Detect potential route detours by comparing reported vs. geometric distances
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       t.t_distance AS reported_distance_m,
       ST_Length(
               ST_MakeLine(
                       ST_GeomFromWKB(t.t_pickuploc),
                       ST_GeomFromWKB(t.t_dropoffloc)
               )
       ) / 0.000009 AS line_distance_m -- 1 meter = 0.000009 degree
   FROM trip t
)
SELECT
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
               """

    @staticmethod
    def q8() -> str:
        return """
-- Q8: Count nearby pickups for each building within 500m radius
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t JOIN building b ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045) -- ~500m
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
               """

    @staticmethod
    def q9() -> str:
        return """
-- Q9: Building Conflation (duplicate/overlap detection via IoU), deterministic order
WITH b1 AS (
   SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom
   FROM building
),
    b2 AS (
        SELECT b_buildingkey AS id, ST_GeomFromWKB(b_boundary) AS geom
        FROM building
    ),
    pairs AS (
        SELECT
            b1.id AS building_1,
            b2.id AS building_2,
            ST_Area(b1.geom) AS area1,
            ST_Area(b2.geom) AS area2,
            ST_Area(ST_Intersection(b1.geom, b2.geom)) AS overlap_area
        FROM b1
                 JOIN b2
                      ON b1.id < b2.id
                          AND ST_Intersects(b1.geom, b2.geom)
    )
SELECT
   building_1,
   building_2,
   area1,
   area2,
   overlap_area,
   CASE
       WHEN overlap_area = 0 THEN 0.0
       WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
       ELSE overlap_area / (area1 + area2 - overlap_area)
       END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC
               """

    @staticmethod
    def q10() -> str:
        return """
-- Q10: Zone statistics for trips starting within each zone
SELECT
   z.z_zonekey, z.z_name AS pickup_zone, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
   AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM zone z LEFT JOIN trip t ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
               """

    @staticmethod
    def q11() -> str:
        return """
-- Q11: Count trips that cross between different zones
SELECT COUNT(*) AS cross_zone_trip_count
FROM
   trip t
       JOIN zone pickup_zone ON ST_Within(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(pickup_zone.z_boundary))
       JOIN zone dropoff_zone ON ST_Within(ST_GeomFromWKB(t.t_dropoffloc), ST_GeomFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
               """

    @staticmethod
    def q12() -> str:
        # There is some odd bug with missing columns in EMR. Using CTEs to work around it.
        return """
-- Q12: Find 5 nearest buildings to each trip pickup location using KNN join
WITH trip_with_geom AS (
   SELECT t_tripkey, t_pickuploc, ST_GeomFromWKB(t_pickuploc) as pickup_geom
   FROM trip
),
    building_with_geom AS (
        SELECT b_buildingkey, b_name, b_boundary, ST_GeomFromWKB(b_boundary) as boundary_geom
        FROM building
    )
SELECT
   t.t_tripkey,
   t.t_pickuploc,
   b.b_buildingkey,
   b.b_name AS building_name,
   ST_Distance(t.pickup_geom, b.boundary_geom) AS distance_to_building
FROM trip_with_geom t JOIN building_with_geom b
                          ON ST_KNN(t.pickup_geom, b.boundary_geom, 5, FALSE)
ORDER BY distance_to_building ASC, b.b_buildingkey ASC
               """


class DatabricksSpatialBenchBenchmark(SpatialBenchBenchmark):
    """A Databricks-specific implementation of the SpatialBench benchmark.

    This class is used to run the SpatialBench benchmark using Databricks' spatial functions. It varies only as
    needed from the base class.

    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "Databricks"

    @staticmethod
    def q5() -> str:
        return """
-- Q5 (Databricks): NO ST_Collect function, using ST_Union_Agg instead. This is more expensive, but should give the same results.
SELECT
   c.c_custkey, c.c_name AS customer_name,
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
   ST_Area(ST_ConvexHull(ST_Union_Agg(ST_GeomFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area,
   COUNT(*) as dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
ORDER BY dropoff_count DESC, c.c_custkey ASC
               """

    @staticmethod
    def q8() -> str:
        return """
-- Q8 (Databricks): ST_MakeLine takes an array of points rather than varargs
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       t.t_distance AS reported_distance_m,
       ST_Length(
               ST_MakeLine(
                       Array(
                               ST_GeomFromWKB(t.t_pickuploc),
                               ST_GeomFromWKB(t.t_dropoffloc)
                       )
               )
       ) / 0.000009 AS line_distance_m -- 1 meter = 0.000009 degree
   FROM trip t
)
SELECT
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
               """

    @staticmethod
    def q12() -> str:
        return """
-- Q12 (Databricks): No KNN join, using cross join + ROW_NUMBER() window function instead.
-- Note: Databricks doesn't have  cross join lateral support.
SELECT
   t_tripkey,
   t_pickuploc,
   b_buildingkey,
   building_name,
   distance_to_building
FROM (
        SELECT
            t.t_tripkey,
            t.t_pickuploc,
            b.b_buildingkey,
            b.b_name AS building_name,
            ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) AS distance_to_building,
            ROW_NUMBER() OVER (
        PARTITION BY t.t_tripkey
        ORDER BY ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) ASC
    ) AS rn
        FROM trip t
                 JOIN building b
    ) AS ranked_buildings
WHERE rn <= 5
ORDER BY distance_to_building ASC, b_buildingkey ASC
               """


class DuckDBSpatialBenchBenchmark(SpatialBenchBenchmark):
    """A DuckDB-specific implementation of the SpatialBench benchmark.

    This class is used to run the SpatialBench benchmark using DuckDB's spatial extension. It varies only as
    needed from the base class.
    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "DuckDB"

    @staticmethod
    def q12() -> str:
        return """
-- Q12 (DuckDB): No KNN join, using cross join lateral instead.
SELECT
   t.t_tripkey,
   t.t_pickuploc,
   nb.b_buildingkey,
   nb.building_name,
   nb.distance_to_building
FROM trip t
        CROSS JOIN LATERAL (
   SELECT
       b.b_buildingkey,
       b.b_name AS building_name,
       ST_Distance(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary)) AS distance_to_building
   FROM building b
   ORDER BY distance_to_building
       LIMIT 5
) AS nb
ORDER BY nb.distance_to_building, nb.b_buildingkey
               """


class SedonaDBSpatialBenchBenchmark(SpatialBenchBenchmark):
    """A SedonaDB-specific implementation of the SpatialBench benchmark.

    This class is used to run the SpatialBench benchmark using SedonaDB's spatial functions.
    It inherits from the SpatialBenchBenchmark class and uses SedonaDB's spatial functions.

    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "SedonaDB"

    @staticmethod
    def q5() -> str:
        return """
-- Q5 (SedonaDB): In SedonaDB ST_Collect is an aggregate function so no need to use ARRAY_AGG first.
-- ST_Collect does not accept an array as input so we cannot use the query with ARRAY_AGG.
SELECT
    c.c_custkey, c.c_name AS customer_name,
    DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
    ST_Area(ST_ConvexHull(ST_Collect(ST_GeomFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area,
    COUNT(*) as dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
ORDER BY dropoff_count DESC, c.c_custkey ASC
               """


def main():
    query_classes = {
        "SedonaSpark": SpatialBenchBenchmark,
        "Databricks": DatabricksSpatialBenchBenchmark,
        "DuckDB": DuckDBSpatialBenchBenchmark,
        "SedonaDB": SedonaDBSpatialBenchBenchmark,
        "Geopandas": None  # Special case, we will catch this below
    }

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <dialect>")
        print(f"Available dialects: {', '.join(query_classes.keys())}")
        sys.exit(1)

    dialect_arg = sys.argv[1]

    if dialect_arg == "Geopandas":
        print("Geopandas does not support SQL queries directly. Please use the provided Python script geopandas.py.")
        sys.exit(0)

    if dialect_arg not in query_classes:
        print(f"Unknown dialect: {dialect_arg}")
        print(f"Available dialects: {', '.join(query_classes.keys())}")
        sys.exit(1)

    queries = query_classes[dialect_arg]().queries()

    for query in queries.values():
        print(query)


if __name__ == "__main__":
    main()
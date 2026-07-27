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
import sys

from print_queries import SpatialBenchBenchmark


class GeographySpatialBenchBenchmark:
    """The geography counterpart of the SpatialBench queries, in the Sedona/Spark SQL dialect.

    The underlying data is unchanged: the same EPSG:4326 lon/lat WKB columns the geometry
    suite reads. Only the edge interpretation changes -- geography edges are geodesics on a
    sphere rather than straight lines in a Cartesian plane -- so every measure comes back in
    real units. Distances and lengths are metres, areas are square metres, and the degree
    conversion factors the geometry suite carries (0.45 "= 50 km", /0.000009 "= 1 m") are
    gone. Those factors only hold near the equator along longitude, so the two suites select
    different rows by construction; see docs/geography-queries.md.

    Running these needs an engine with a GEOGRAPHY type whose measures are geodesic; support
    for the individual functions is still filling in, so check the engine's own reference.

    Q12 is documented in docs/geography-queries.md but deliberately absent here: it is a
    K-nearest-neighbour join, and ST_KNN has no geography form yet. It is added once one lands.
    """

    # Reuse the geometry suite's query-collection reflection *without* inheriting its SQL:
    # subclassing SpatialBenchBenchmark would silently reintroduce the geometry q12 into
    # this suite.
    queries = SpatialBenchBenchmark.queries

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "SedonaSpark (Geography)"

    @staticmethod
    def q1() -> str:
        return """
-- Q1 (Geography): Find trips starting within 50km of Sedona city center, ordered by geodesic distance
SELECT
   t.t_tripkey,
   -- A point's coordinates are identical under either edge interpretation, so the lon/lat
   -- projection stays on the geometry accessor, which is the more widely available one.
   ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon, ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat,
   t.t_pickuptime,
   ST_Distance(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKT('POINT (-111.7610 34.8697)')) AS distance_to_center -- metres
FROM trip t
WHERE ST_DWithin(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKT('POINT (-111.7610 34.8697)'), 50000) -- 50 km geodesic radius around Sedona center
ORDER BY distance_to_center ASC, t.t_tripkey ASC
LIMIT 100 -- Return only the 100 closest trips (bounded result set)
               """

    @staticmethod
    def q2() -> str:
        return """
-- Q2 (Geography): Count trips starting within Coconino County (Arizona) zone
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(ST_GeogFromWKB(t.t_pickuploc), (SELECT ST_GeogFromWKB(z.z_boundary) FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1))
               """

    @staticmethod
    def q3() -> str:
        return """
-- Q3 (Geography): Monthly trip statistics for a buffered box around Sedona city center
SELECT
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month, COUNT(t.t_tripkey) AS total_trips,
   AVG(t.t_distance) AS avg_distance, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
   AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_DWithin(
             ST_GeogFromWKB(t.t_pickuploc),
             -- Same ring as the geometry suite, but its edges are geodesics here.
             -- Spans ~26.5 km E-W by ~30 km N-S about the center; corners sit ~20 km out.
             ST_GeogFromWKT('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))'),
             5000 -- Additional 5 km geodesic buffer, so ~25 km at the corners
     )
GROUP BY pickup_month
ORDER BY pickup_month
"""

    @staticmethod
    def q4() -> str:
        return """
-- Q4 (Geography): Zone distribution of top 1000 trips by tip amount
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM
   zone z
       JOIN (
       SELECT t.t_pickuploc
       FROM trip t
       ORDER BY t.t_tip DESC, t.t_tripkey ASC
           LIMIT 1000 -- Replace 1000 with x (how many top tips you want)
   ) top_trips ON ST_Within(ST_GeogFromWKB(top_trips.t_pickuploc), ST_GeogFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q5() -> str:
        return """
-- Q5 (Geography): Monthly travel patterns for repeat customers (convex hull of dropoff locations)
SELECT
   c.c_custkey, c.c_name AS customer_name,
   DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
   ST_Area(ST_ConvexHull(ST_Collect(ARRAY_AGG(ST_GeogFromWKB(t.t_dropoffloc))))) AS monthly_travel_hull_area, -- m^2, spherical
   COUNT(*) as dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC, pickup_month ASC
LIMIT 100 -- Return only the top 100 repeat customer-months by travel-hull area (bounded result set)
            """

    @staticmethod
    def q6() -> str:
        return """
-- Q6 (Geography): Zone statistics for trips intersecting a bounding box
SELECT
   z.z_zonekey, z.z_name,
   COUNT(t.t_tripkey) AS total_pickups, AVG(t.t_distance) AS avg_distance,
   AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(ST_GeogFromWKT('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'), ST_GeogFromWKB(z.z_boundary))
 AND ST_Within(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
               """

    @staticmethod
    def q7() -> str:
        return """
-- Q7 (Geography): Detect potential route detours by comparing reported vs. geodesic distances
WITH trip_lengths AS (
   SELECT
       t.t_tripkey,
       t.t_distance AS reported_distance_m,
       ST_Length(
               ST_MakeLine(
                       ST_GeogFromWKB(t.t_pickuploc),
                       ST_GeogFromWKB(t.t_dropoffloc)
               )
       ) AS line_distance_m -- metres; no degree conversion factor needed
   FROM trip t
)
SELECT
   t.t_tripkey,
   t.reported_distance_m,
   t.line_distance_m,
   t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY detour_ratio DESC NULLS LAST, reported_distance_m DESC, t_tripkey ASC
LIMIT 100 -- Return only the top 100 highest-detour trips (bounded result set)
               """

    @staticmethod
    def q8() -> str:
        return """
-- Q8 (Geography): Count nearby pickups for each building within 500m radius
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t JOIN building b ON ST_DWithin(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(b.b_boundary), 500) -- 500 m geodesic
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
LIMIT 100 -- Return only the top 100 busiest buildings (bounded result set)
               """

    @staticmethod
    def q9() -> str:
        return """
-- Q9 (Geography): Building Conflation (duplicate/overlap detection via IoU), deterministic order
-- Areas are square metres. Needs geography ST_Intersection: available in SedonaDB, not yet in Apache Sedona.
WITH b1 AS (
   SELECT b_buildingkey AS id, ST_GeogFromWKB(b_boundary) AS geog
   FROM building
),
    b2 AS (
        SELECT b_buildingkey AS id, ST_GeogFromWKB(b_boundary) AS geog
        FROM building
    ),
    pairs AS (
        SELECT
            b1.id AS building_1,
            b2.id AS building_2,
            ST_Area(b1.geog) AS area1,
            ST_Area(b2.geog) AS area2,
            ST_Area(ST_Intersection(b1.geog, b2.geog)) AS overlap_area
        FROM b1
                 JOIN b2
                      ON b1.id < b2.id
                          AND ST_Intersects(b1.geog, b2.geog)
    )
SELECT
   building_1,
   building_2,
   area1,
   area2,
   overlap_area,
   -- A ratio of areas, so the unit change cancels: iou is dimensionless in both suites
   CASE
       WHEN overlap_area = 0 THEN 0.0
       WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
       ELSE overlap_area / (area1 + area2 - overlap_area)
       END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC
LIMIT 100 -- Return only the top 100 most-overlapping building pairs (bounded result set)
               """

    @staticmethod
    def q10() -> str:
        return """
-- Q10 (Geography): Zone statistics for trips starting within each zone
SELECT
   z.z_zonekey, z.z_name AS pickup_zone, AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
   AVG(t.t_distance) AS avg_distance, COUNT(t.t_tripkey) AS num_trips
FROM zone z LEFT JOIN trip t ON ST_Within(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
LIMIT 100 -- Return only the top 100 zones by average trip duration (bounded result set)
               """

    @staticmethod
    def q11() -> str:
        return """
-- Q11 (Geography): Count trips that cross between different zones
SELECT COUNT(*) AS cross_zone_trip_count
FROM
   trip t
       JOIN zone pickup_zone ON ST_Within(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(pickup_zone.z_boundary))
       JOIN zone dropoff_zone ON ST_Within(ST_GeogFromWKB(t.t_dropoffloc), ST_GeogFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
               """


class SedonaDBGeographySpatialBenchBenchmark(GeographySpatialBenchBenchmark):
    """A SedonaDB-specific implementation of the geography SpatialBench benchmark.

    As in the geometry suite, only Q5 differs: SedonaDB spells the aggregate ST_Collect_Agg
    rather than ST_Collect(ARRAY_AGG(...)).
    """

    def dialect(self) -> str:
        """Return the dialect of the benchmark."""
        return "SedonaDB (Geography)"

    @staticmethod
    def q5() -> str:
        return """
-- Q5 (SedonaDB, Geography): SedonaDB uses ST_Collect_Agg (with _Agg suffix) for aggregate functions.
SELECT
    c.c_custkey, c.c_name AS customer_name,
    DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
    ST_Area(ST_ConvexHull(ST_Collect_Agg(ST_GeogFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area, -- m^2, spherical
    COUNT(*) as dropoff_count
FROM trip t JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC, pickup_month ASC
LIMIT 100 -- Return only the top 100 repeat customer-months by travel-hull area (bounded result set)
               """


def main():
    query_classes = {
        "SedonaSpark": GeographySpatialBenchBenchmark,
        "SedonaDB": SedonaDBGeographySpatialBenchBenchmark,
    }

    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <dialect>")
        print(f"Available dialects: {', '.join(query_classes.keys())}")
        sys.exit(1)

    dialect_arg = sys.argv[1]

    if dialect_arg not in query_classes:
        print(f"Unknown dialect: {dialect_arg}")
        print(f"Available dialects: {', '.join(query_classes.keys())}")
        sys.exit(1)

    queries = query_classes[dialect_arg]().queries()

    for query in queries.values():
        print(query)


if __name__ == "__main__":
    main()

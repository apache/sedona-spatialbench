---
title: Geography Queries
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

# Run the SpatialBench Geography Queries


SpatialBench's [main query suite](https://sedona.apache.org/spatialbench/queries/) is a **geometry** benchmark: the WKB columns are
decoded with `ST_GeomFromWKB`, predicates use planar (2D Euclidean) edges, and every distance
threshold is an angle in degrees.

This notebook defines the **geography** counterpart of the same 12 queries. The data is
unchanged — the same EPSG:4326 longitude/latitude WKB columns — but the columns are decoded
with `ST_GeogFromWKB`, so edges are interpreted as geodesics on a sphere and every measure
comes back in real-world units.

## Why a second suite

The geometry suite has to fake real-world units. It filters trips "within 50 km" of Sedona
using `ST_DWithin(..., 0.45)`, converts metres to degrees with `1 m = 0.000009 degree`, and
reports building overlap areas in square degrees.

A degree of latitude is roughly 111 km everywhere, but a degree of longitude shrinks with the
cosine of the latitude. So `0.45` degrees is about 50 km tall and only about 41 km wide at
Sedona (34.87°N), and about 25 km wide at 60°N. Since SpatialBench data is generated across
eight continent-sized boxes spanning latitudes −56° to 78°, a degree-based radius is a
different real-world shape in every part of the dataset.

The geography suite replaces all of that with metres and square metres:

| | Geometry suite | Geography suite |
|---|---|---|
| Point/polygon constructor | `ST_GeomFromWKB`, `ST_GeomFromText` | `ST_GeogFromWKB`, `ST_GeogFromWKT` |
| Edge interpretation | straight lines in a plane | geodesics on a sphere |
| `ST_DWithin` threshold | degrees (`0.45`, `0.045`, `0.0045`) | metres (`50000`, `5000`, `500`) |
| `ST_Distance`, `ST_Length` | degrees | metres |
| `ST_Area` | square degrees | square metres |
| Unit conversion in SQL | `/ 0.000009` | none needed |

!!! note "The two suites are not row-for-row comparable"

    A 0.45° planar radius and a 50 000 m geodesic radius select different trips, so Q1
    returns a different 100 rows in each suite. That is the point of the exercise, not a
    discrepancy: each suite has its own ground truth, and results should only ever be
    compared within a suite.

## The query module

The same SQL is available outside this notebook from the query module, which prints the suite
for a given dialect:

```bash
python3 spatialbench-queries/print_geography_queries.py SedonaDB
```

Running the queries needs an engine with a `GEOGRAPHY` type whose measures are geodesic;
support for the individual functions is still filling in across engines, so check your
engine's own reference for what it implements. Q12 is defined below but is not carried in the
module and is not executed here: it is a K-nearest-neighbour join, and `ST_KNN` has no
geography form yet.


## Before you start



```python
%pip install -r ~/sedona-spatialbench/docs/requirements.txt

```

Additionally, install the SpatialBench CLI and generate the synthetic data on your machine:

```
# SpatialBench CLI
cargo install --path ./spatialbench-cli
# Generate the benchmarking data to the sf1-parquet directory
spatialbench-cli -s 1 --format=parquet --output-dir sf1-parquet
```

Alternatively, download pre-generated data from [Hugging Face](https://huggingface.co/datasets/apache-sedona/spatialbench) instead of generating it (tables are published under `v<version>/sf<scale>/`; scale factors `sf0.1`, `sf1`, `sf10`, `sf100`):

```
pip install huggingface-hub
hf download apache-sedona/spatialbench --repo-type dataset --include "v0.1.0/sf1/**" --local-dir spatialbench-data
```

If you use the Hugging Face download, set `DATA_DIR = "spatialbench-data/v0.1.0/sf1"` in the data-loading cell below.



```python
import sedona.db

```


```python
sd = sedona.db.connect()

```


```python
import os

# The CLI writes flat files to sf1-parquet/; the Hugging Face download puts
# partitioned tables under spatialbench-data/v0.1.0/sf1/. Point DATA_DIR at
# whichever you used -- both layouts load below.
DATA_DIR = "../sf1-parquet"

for table in ["building", "customer", "driver", "trip", "vehicle", "zone"]:
    flat = f"{DATA_DIR}/{table}.parquet"
    source = flat if os.path.exists(flat) else f"{DATA_DIR}/{table}/*.parquet"
    sd.read_parquet(source).to_view(table)

```

## Q1: Find trips starting within 50km of Sedona city center, ordered by distance

**Real-life scenario:** Identify and rank trips by proximity to a city center for urban
planning and transportation analysis.

Unlike the geometry version, the 50 km radius is a true geodesic radius rather than a 0.45°
angular one, and `distance_to_center` is in metres.

**Spatial query characteristics tested:**

1. Geodesic distance-based spatial filtering (`ST_DWithin`, metres)
2. Geodesic distance calculation to a fixed point
3. Coordinate extraction (`ST_X`, `ST_Y`)
4. Ordering by spatial distance



```python
sd.sql("""
SELECT
    t.t_tripkey,
    -- A point's coordinates are identical under either edge interpretation, so the lon/lat
    -- projection stays on the geometry accessor, which is the more widely available one.
    ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon,
    ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat,
    t.t_pickuptime,
    ST_Distance(
        ST_GeogFromWKB(t.t_pickuploc),
        ST_GeogFromWKT('POINT (-111.7610 34.8697)')
    ) AS distance_to_center -- metres
FROM trip t
WHERE ST_DWithin(
    ST_GeogFromWKB(t.t_pickuploc),
    ST_GeogFromWKT('POINT (-111.7610 34.8697)'),
    50000 -- 50 km geodesic radius around Sedona center
)
ORDER BY distance_to_center ASC, t.t_tripkey ASC
LIMIT 100 -- Return only the 100 closest trips (bounded result set)
""").show(3)

```

## Q2: Count trips starting within Coconino County

**Real-life scenario:** Count trip activity inside an administrative boundary.

The county polygon comes from Overture data with planar edges; reading it as a geography
reinterprets those edges as geodesics, which can move the boundary by a few metres relative
to the geometry suite and change the count for pickups that fall essentially on the border.

**Spatial query characteristics tested:**

1. Point-in-polygon spatial predicate on the sphere (`ST_Intersects`)
2. Scalar subquery producing the query geography



```python
sd.sql("""
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(
    ST_GeogFromWKB(t.t_pickuploc),
    (SELECT ST_GeogFromWKB(z.z_boundary) FROM zone z WHERE z.z_name = 'Coconino County' LIMIT 1)
)
""").show(3)

```

## Q3: Monthly trip statistics for a buffered box around Sedona city center

**Real-life scenario:** Track monthly demand, revenue and trip duration in a metro area.

The polygon literal is the same ring as in the geometry suite, but its edges are geodesics
here, so it is a spherical quadrilateral rather than a planar box.

The ring is centred on Sedona and spans roughly **26.5 km east–west by 30 km north–south** —
about 13 km and 15 km from the centre along each axis, with the corners ~20 km out. The 5 000 m
buffer extends that to about 25 km at the corners.

**Spatial query characteristics tested:**

1. Geodesic buffered-polygon filtering (`ST_DWithin`, metres)
2. Temporal grouping with `DATE_TRUNC`
3. Multiple aggregations over a spatial filter



```python
sd.sql("""
SELECT
    DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
    COUNT(t.t_tripkey) AS total_trips,
    AVG(t.t_distance) AS avg_distance,
    AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
    AVG(t.t_fare) AS avg_fare
FROM trip t
WHERE ST_DWithin(
    ST_GeogFromWKB(t.t_pickuploc),
    -- ~26.5 km E-W by ~30 km N-S about the center; corners sit ~20 km out
    ST_GeogFromWKT('POLYGON((-111.9060 34.7347, -111.6160 34.7347, -111.6160 35.0047, -111.9060 35.0047, -111.9060 34.7347))'),
    5000 -- Additional 5 km geodesic buffer, so ~25 km at the corners
)
GROUP BY pickup_month
ORDER BY pickup_month
""").show(3)

```

## Q4: Zone distribution of top 1000 trips by tip amount

**Real-life scenario:** Find which neighbourhoods generate the most generous tips.

**Spatial query characteristics tested:**

1. Point-in-polygon spatial join on the sphere (`ST_Within`)
2. Spatial join against a `LIMIT`-bounded subquery
3. Aggregation on the spatial join result



```python
sd.sql("""
SELECT z.z_zonekey, z.z_name, COUNT(*) AS trip_count
FROM zone z
JOIN (
    SELECT t.t_pickuploc
    FROM trip t
    ORDER BY t.t_tip DESC, t.t_tripkey ASC
    LIMIT 1000 -- Replace 1000 with x (how many top tips you want)
) top_trips
ON ST_Within(ST_GeogFromWKB(top_trips.t_pickuploc), ST_GeogFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
""").show(3)

```

## Q5: Monthly travel patterns for repeat customers

**Real-life scenario:** Measure how large an area each frequent customer travels across in a
month, from the convex hull of their dropoff locations.

In the geography suite `monthly_travel_hull_area` is a spherical area in square metres, and
the hull itself is computed with geodesic edges.

**Spatial query characteristics tested:**

1. Geography aggregation into a collection (`ST_Collect_Agg`)
2. Spherical convex hull (`ST_ConvexHull`)
3. Spherical area (`ST_Area`, m²)
4. Grouped aggregation with `HAVING`



```python
sd.sql("""
SELECT
    c.c_custkey,
    c.c_name AS customer_name,
    DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
    ST_Area(ST_ConvexHull(ST_Collect_Agg(ST_GeogFromWKB(t.t_dropoffloc)))) AS monthly_travel_hull_area, -- m^2
    COUNT(*) AS dropoff_count
FROM trip t
JOIN customer c ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5 -- Only include repeat customers for meaningful hulls
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC, pickup_month ASC
LIMIT 100 -- Return only the top 100 repeat customer-months by travel-hull area (bounded result set)
""").show(3)

```

## Q6: Zone statistics for trips intersecting a bounding box

**Real-life scenario:** Summarise trip activity for every zone touching a region of interest.

**Spatial query characteristics tested:**

1. Two spatial predicates in one join (`ST_Intersects` and `ST_Within`)
2. Polygon-polygon intersection test on the sphere
3. Aggregation over a doubly-filtered join



```python
sd.sql("""
SELECT
    z.z_zonekey, z.z_name,
    COUNT(t.t_tripkey) AS total_pickups,
    AVG(t.t_distance) AS avg_distance,
    AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(
        ST_GeogFromWKT('POLYGON((-112.2110 34.4197, -111.3110 34.4197, -111.3110 35.3197, -112.2110 35.3197, -112.2110 34.4197))'),
        ST_GeogFromWKB(z.z_boundary)
    )
  AND ST_Within(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
""").show(3)

```

## Q7: Detect potential route detours

**Real-life scenario:** Flag trips whose reported distance greatly exceeds the straight-line
distance between pickup and dropoff, a signal for detours or fare padding.

This is the query the geography type helps most. The geometry version measures a planar line
length in degrees and divides by `0.000009` to pretend it is metres; the geography version
measures the geodesic directly, so `line_distance_m` and `detour_ratio` are meaningful
everywhere in the dataset rather than only near the equator.

**Spatial query characteristics tested:**

1. Line construction from two points (`ST_MakeLine`)
2. Geodesic length (`ST_Length`, metres)
3. Ratio computation with division-by-zero guarding



```python
sd.sql("""
WITH trip_lengths AS (
    SELECT
        t.t_tripkey,
        t.t_distance AS reported_distance_m,
        ST_Length(
            ST_MakeLine(
                ST_GeogFromWKB(t.t_pickuploc),
                ST_GeogFromWKB(t.t_dropoffloc)
            )
        ) AS line_distance_m -- metres, no degree conversion needed
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
""").show(3)

```

## Q8: Count nearby pickups for each building within a 500m radius

**Real-life scenario:** Find the buildings that generate the most pickup activity.

The 500 m radius is a true geodesic distance from the building footprint, so the catchment is
the same physical size for a building in Norway as for one in Kenya — unlike the geometry
suite, where `0.0045°` is a much narrower band at high latitude.

**Spatial query characteristics tested:**

1. Geodesic distance spatial join between points and polygons (`ST_DWithin`, metres)
2. Aggregation on spatial join result



```python
sd.sql("""
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t
JOIN building b
ON ST_DWithin(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(b.b_boundary), 500) -- 500 m geodesic
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
LIMIT 100 -- Return only the top 100 busiest buildings (bounded result set)
""").show(3)

```

## Q9: Building Conflation (duplicate/overlap detection via IoU)

**Real-life scenario:** Detect duplicate or overlapping building footprints to find data
quality issues.

`area1`, `area2` and `overlap_area` are square metres here rather than square degrees, which
makes them directly interpretable. `iou` is a ratio of areas, so it is dimensionless in both
suites — though the values still differ, because the ratio of spherical areas is not the ratio
of planar ones.

**Spatial query characteristics tested:**

1. Polygon-polygon self-join with a spatial predicate (`ST_Intersects`)
2. Spherical overlay (`ST_Intersection`)
3. Spherical area (`ST_Area`, m²)



```python
sd.sql("""
WITH b1 AS (
    SELECT b_buildingkey AS id, ST_GeogFromWKB(b_boundary) AS geog FROM building
),
b2 AS (
    SELECT b_buildingkey AS id, ST_GeogFromWKB(b_boundary) AS geog FROM building
),
pairs AS (
    SELECT
        b1.id AS building_1,
        b2.id AS building_2,
        ST_Area(b1.geog) AS area1,        -- m^2
        ST_Area(b2.geog) AS area2,        -- m^2
        ST_Area(ST_Intersection(b1.geog, b2.geog)) AS overlap_area -- m^2
    FROM b1
    JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geog, b2.geog)
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
LIMIT 100 -- Return only the top 100 most-overlapping building pairs (bounded result set)
""").show(3)

```

## Q10: Zone statistics for trips starting within each zone

**Real-life scenario:** Rank zones by average trip duration, keeping zones with no trips.

**Spatial query characteristics tested:**

1. Left spatial join on the sphere (`ST_Within`), preserving unmatched zones
2. Aggregation with `NULL` handling in the ordering



```python
sd.sql("""
SELECT
    z.z_zonekey,
    z.z_name AS pickup_zone,
    AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
    AVG(t.t_distance) AS avg_distance,
    COUNT(t.t_tripkey) AS num_trips
FROM zone z
LEFT JOIN trip t ON ST_Within(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(z.z_boundary))
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
LIMIT 100 -- Return only the top 100 zones by average trip duration (bounded result set)
""").show(3)

```

## Q11: Count trips that cross between different zones

**Real-life scenario:** Quantify inter-zone travel demand for transit planning.

**Spatial query characteristics tested:**

1. Two independent point-in-polygon spatial joins on the sphere
2. Non-spatial filter on the join result



```python
sd.sql("""
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON ST_Within(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(pickup_zone.z_boundary))
JOIN zone dropoff_zone ON ST_Within(ST_GeogFromWKB(t.t_dropoffloc), ST_GeogFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
""").show(3)

```

## Q12: Rank trip pickups by average distance to their 5 nearest buildings

**Real-life scenario:** Find the most isolated pickups — those farthest from any surrounding
building — for coverage and service-gap analysis.

`avg_distance_to_5_nearest` is in metres, and the neighbour ranking itself is by geodesic
distance rather than planar degrees, so the "5 nearest buildings" are genuinely the 5 nearest
on the ground.

**Spatial query characteristics tested:**

1. K-nearest-neighbour spatial join on the sphere (`ST_KNN`)
2. Geodesic point-to-polygon distance (`ST_Distance`, metres)
3. Aggregation over the KNN result


```sql
WITH trip_with_geog AS (
    SELECT t_tripkey, ST_GeogFromWKB(t_pickuploc) AS pickup_geog
    FROM trip
),
building_with_geog AS (
    SELECT ST_GeogFromWKB(b_boundary) AS boundary_geog
    FROM building
),
knn AS (
    SELECT
        t.t_tripkey,
        ST_Distance(t.pickup_geog, b.boundary_geog) AS distance_to_building -- metres
    FROM trip_with_geog t
    JOIN building_with_geog b ON ST_KNN(t.pickup_geog, b.boundary_geog, 5, TRUE) -- TRUE = rank by spherical distance
)
SELECT
    t_tripkey,
    AVG(distance_to_building) AS avg_distance_to_5_nearest
FROM knn
GROUP BY t_tripkey
ORDER BY avg_distance_to_5_nearest DESC, t_tripkey ASC
LIMIT 100 -- Return only the top 100 most-isolated pickups (bounded result set)
```

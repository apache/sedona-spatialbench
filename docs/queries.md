# Run the SpatialBench Queries

This notebook contains the queries that make up the SpatialBench benchmark.

SpatialBench is a benchmark for assessing geospatial SQL analytics query performance across database systems. The queries represent common, real-world geospatial analytics tasks and are designed to test a wide range of spatial functions and join conditions.

The benchmark uses a realistic but synthetic, transportation-themed dataset to ensure the queries reflect practical use cases. By running these queries, you can evaluate and compare the relative performance of different spatial query engines in a consistent and unbiased manner.

## Before you start

Before running this notebook, ensure that you have installed the packages in the `requirements.txt` file:


```python
%pip install -r ~/sedona-spatialbench/docs/requirements.txt
```

    ...
    ...
    Note: you may need to restart the kernel to use updated packages.


Additionally, install the SpatialBench CLI and generate the synthetic data on your machine:

```
# SpatialBench CLI
cargo install --path ./spatialbench-cli
# Generate the benchmarking data to the sf1-parquet directory
spatialbench-cli -s 1 --format=parquet --output-dir sf1-parquet
```


```python
import sedona.db
```


```python
sd = sedona.db.connect()
```


```python
sd.read_parquet(f"../sf1-parquet/building.parquet").to_view("building")
sd.read_parquet(f"../sf1-parquet/customer.parquet").to_view("customer")
sd.read_parquet(f"../sf1-parquet/driver.parquet").to_view("driver")
sd.read_parquet(f"../sf1-parquet/trip.parquet").to_view("trip")
sd.read_parquet(f"../sf1-parquet/vehicle.parquet").to_view("vehicle")
sd.read_parquet(f"../sf1-parquet/zone.parquet").to_view("zone")
```

## Q1: Find trips starting within 50km of Sedona city center, ordered by distance

**Real-life scenario:** Identify and rank trips by proximity to a city center for urban planning and transportation analysis.

This query finds all taxi or rideshare trips that started within 50 kilometers of downtown Sedona, Arizona. For each qualifying trip, it shows the trip ID, pickup coordinates, pickup time, and calculates the exact distance from the pickup location to Sedona's city center. The results are sorted to show the trips that picked up closest to downtown Sedona first, making it easy to see which rides originated nearest to the city center.

**Spatial query characteristics tested:**

1. Distance-based spatial filtering (ST_DWithin)
2. Distance calculation to a fixed point
3. Coordinate extraction (ST_X, ST_Y)
4. Ordering by spatial distance


```python
sd.sql("""
SELECT
    t.t_tripkey,
    ST_X(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lon,
    ST_Y(ST_GeomFromWKB(t.t_pickuploc)) AS pickup_lat,
    t.t_pickuptime,
    ST_Distance(
        ST_GeomFromWKB(t.t_pickuploc),
        ST_GeomFromText('POINT (-111.7610 34.8697)')
    ) AS distance_to_center
FROM trip t
WHERE ST_DWithin(
    ST_GeomFromWKB(t.t_pickuploc),
    ST_GeomFromText('POINT (-111.7610 34.8697)'),
    0.45 -- 50km radius around Sedona center in degrees
)
ORDER BY distance_to_center ASC, t.t_tripkey ASC
""").show(3)
```

    ┌───────────┬────────────────┬──────────────┬─────────────────────┬──────────────────────┐
    │ t_tripkey ┆   pickup_lon   ┆  pickup_lat  ┆     t_pickuptime    ┆  distance_to_center  │
    │   int64   ┆     float64    ┆    float64   ┆      timestamp      ┆        float64       │
    ╞═══════════╪════════════════╪══════════════╪═════════════════════╪══════════════════════╡
    │   1451371 ┆ -111.791052127 ┆ 34.826733457 ┆ 1998-08-12T06:47:01 ┆  0.05243333056935387 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │   2047835 ┆ -111.706967009 ┆ 34.883889472 ┆ 1992-04-08T07:36:09 ┆ 0.055865062714050374 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │   3936870 ┆ -111.827619221 ┆ 34.882950924 ┆ 1998-11-10T13:32:07 ┆  0.06792427838042854 │
    └───────────┴────────────────┴──────────────┴─────────────────────┴──────────────────────┘


## Q2: Count trips starting within Coconino County (Arizona) zone

**Real-life scenario:** Count all trips originating within a specific administrative boundary (county) for regional transportation statistics.

This query counts how many taxi or rideshare trips started within Coconino County, Arizona. It does this by checking if each trip's pickup location falls inside the county's geographic boundaries. The result is a simple count showing the total number of trips that originated anywhere within Coconino County.

**Spatial query characteristics tested:**

1. Point-in-polygon spatial filtering (ST_Intersects)
2. Subquery with spatial geometry selection
3. Simple aggregation on spatially filtered data


```python
sd.sql("""
SELECT COUNT(*) AS trip_count_in_coconino_county
FROM trip t
WHERE ST_Intersects(
    ST_GeomFromWKB(t.t_pickuploc),
    (
        SELECT ST_GeomFromWKB(z.z_boundary)
        FROM zone z
        WHERE z.z_name = 'Coconino County'
        LIMIT 1
    )
)
""").show(3)
```

    ┌───────────────────────────────┐
    │ trip_count_in_coconino_county │
    │             int64             │
    ╞═══════════════════════════════╡
    │                           541 │
    └───────────────────────────────┘


## Q3: Monthly trip statistics within a 15km radius of the Sedona city center

**Real-life scenario:** Track monthly travel trends and performance metrics in a metropolitan area with seasonal analysis.

This query analyzes taxi and rideshare trip patterns around Sedona, Arizona, by grouping trips into monthly summaries. It looks at all trips that started within a 15-kilometer area around Sedona (a 10km box plus 5km buffer) and calculates key statistics for each month, including total number of trips, average trip distance, average trip duration, and average fare. The results are organized chronologically by month, allowing you to see seasonal trends and changes in ride patterns over time in the Sedona area.

**Spatial query characteristics tested:**

1. Distance-based spatial filtering (ST_DWithin) with buffer
2. Temporal grouping (monthly aggregation)
3. Multiple statistical aggregations on spatially filtered data


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
    ST_GeomFromWKB(t.t_pickuploc),
    ST_GeomFromText('POLYGON((
        -111.9060 34.7347, -111.6160 34.7347,
        -111.6160 35.0047, -111.9060 35.0047,
        -111.9060 34.7347
    ))'), -- Bounding box around Sedona
    0.045 -- Additional 5km buffer in degrees
)
GROUP BY pickup_month
ORDER BY pickup_month
""").show(3)
```

    ┌─────────────────────┬─────────────┬──────────────┬─────────────────────────────────┬─────────────┐
    │     pickup_month    ┆ total_trips ┆ avg_distance ┆           avg_duration          ┆   avg_fare  │
    │      timestamp      ┆    int64    ┆  decimal128  ┆             duration            ┆  decimal128 │
    ╞═════════════════════╪═════════════╪══════════════╪═════════════════════════════════╪═════════════╡
    │ 1992-04-01T00:00:00 ┆           2 ┆  0.000020000 ┆ 0 days 1 hours 23 mins 47.000 … ┆ 0.000075000 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 1992-07-01T00:00:00 ┆           1 ┆  0.000010000 ┆ 0 days 0 hours 58 mins 58.000 … ┆ 0.000040000 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │ 1994-02-01T00:00:00 ┆           2 ┆  0.000020000 ┆ 0 days 1 hours 23 mins 50.000 … ┆ 0.000050000 │
    └─────────────────────┴─────────────┴──────────────┴─────────────────────────────────┴─────────────┘


## Q4: Zone distribution of top 1000 trips by tip amount

**Real-life scenario:** Analyze the geographic distribution of high-value trips (by tip amount) to understand premium service areas.

This query identifies which neighborhoods or zones produced the most generous tippers by analyzing the top 1000 highest-tipping trips. It first finds the 1000 trips with the largest tips, then determines which geographic zones or neighborhoods those pickup locations fall within, and counts how many of these high-tip trips originated from each area. The results show a ranking of zones by the number of big tippers they produced, helping identify the most lucrative pickup areas for drivers seeking high-tip rides.

**Spatial query characteristics tested:**

1. Subquery with ordering and limiting
2. Point-in-polygon spatial join (ST_Within)
3. Aggregation on spatially joined results
4. Multi-step query with spatial filtering and grouping


```python
sd.sql("""
SELECT
    z.z_zonekey,
    z.z_name,
    COUNT(*) AS trip_count
FROM
    zone z
    JOIN (
        SELECT t.t_pickuploc
        FROM trip t
        ORDER BY t.t_tip DESC, t.t_tripkey ASC
        LIMIT 1000
    ) top_trips
    ON ST_Within(
        ST_GeomFromWKB(top_trips.t_pickuploc),
        ST_GeomFromWKB(z.z_boundary)
    )
GROUP BY z.z_zonekey, z.z_name
ORDER BY trip_count DESC, z.z_zonekey ASC
""").show(3)
```

    ┌───────────┬─────────────────────────────────┬────────────┐
    │ z_zonekey ┆              z_name             ┆ trip_count │
    │   int64   ┆               utf8              ┆    int64   │
    ╞═══════════╪═════════════════════════════════╪════════════╡
    │     65008 ┆ Ndélé                           ┆         35 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    │    147530 ┆ 乐山市                          ┆         27 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┤
    │    150276 ┆ 锡林郭勒盟 ᠰᠢᠯᠢ ᠶᠢᠨ ᠭᠣᠣᠯ ᠠᠶᠢᠮᠠᠭ ┆         19 │
    └───────────┴─────────────────────────────────┴────────────┘


## Q5: Monthly travel patterns for repeat customers (convex hull of dropoff locations)

**Real-life scenario:** Analyze the geographic spread of travel patterns for frequent customers to understand their mobility behavior.

This query analyzes the monthly travel patterns of frequent customers by measuring how much geographic area they cover with their trips. For each customer who took more than five trips in a month, it calculates the size of the "travel hull" - the area enclosed by connecting all their dropoff locations that month. The results reveal which customers have the most expansive travel patterns, helping to identify power users who cover large geographic areas versus those who stick to smaller, local areas.

**Spatial query characteristics tested:**

1. Spatial aggregation (ST_Collect/ARRAY_AGG)
2. Convex hull computation (ST_ConvexHull)
3. Area calculation on complex geometries
4. Temporal and customer-based grouping with spatial operations


```python
sd.sql("""
SELECT
    c.c_custkey,
    c.c_name AS customer_name,
    DATE_TRUNC('month', t.t_pickuptime) AS pickup_month,
    ST_Area(
        ST_ConvexHull(ST_Collect(ST_GeomFromWKB(t.t_dropoffloc)))
    ) AS monthly_travel_hull_area,
    COUNT(*) as dropoff_count
FROM trip t
JOIN customer c
    ON t.t_custkey = c.c_custkey
GROUP BY c.c_custkey, c.c_name, pickup_month
HAVING dropoff_count > 5 -- Only include repeat customers
ORDER BY monthly_travel_hull_area DESC, c.c_custkey ASC
""").show(3)
```

    ┌───────────┬────────────────────┬─────────────────────┬────────────────────┬───────────────┐
    │ c_custkey ┆    customer_name   ┆     pickup_month    ┆ monthly_travel_hul ┆ dropoff_count │
    │   int64   ┆        utf8        ┆      timestamp      ┆       l_area…      ┆     int64     │
    ╞═══════════╪════════════════════╪═════════════════════╪════════════════════╪═══════════════╡
    │     25975 ┆ Customer#000025975 ┆ 1992-02-01T00:00:00 ┆ 34941.303419053635 ┆            10 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │     12061 ┆ Customer#000012061 ┆ 1997-03-01T00:00:00 ┆  34607.53871953154 ┆            14 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │     21418 ┆ Customer#000021418 ┆ 1993-08-01T00:00:00 ┆  34465.32323910264 ┆             9 │
    └───────────┴────────────────────┴─────────────────────┴────────────────────┴───────────────┘


## Q6: Zone statistics for trips within a 50km radius of the Sedona city center

**Real-life scenario:** Analyze trip patterns in zones within a metropolitan area around a specific city center.

This query analyzes ride activity across all neighborhoods and zones within a 50-kilometer area around Sedona, Arizona. It identifies which zones had the most pickup activity by counting total trips that started in each region. Also, it calculates the average trip cost and duration for rides originating from each zone. The results are ranked by pickup volume, showing which neighborhoods or areas generate the most ride demand and their typical trip characteristics within the greater Sedona region.

**Spatial query characteristics tested:**

1. Polygon containment check (ST_Contains) with bounding box
2. Point-in-polygon spatial join (ST_Within)


```python
sd.sql("""
SELECT
    z.z_zonekey,
    z.z_name,
    COUNT(t.t_tripkey) AS total_pickups,
    AVG(t.t_distance) AS avg_distance, -- Corrected from t_totalamount
    AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration
FROM trip t, zone z
WHERE ST_Intersects(
    ST_GeomFromText('POLYGON((
        -112.2110 34.4197, -111.3110 34.4197,
        -111.3110 35.3197, -112.2110 35.3197,
        -112.2110 34.4197
    ))'), -- Bounding box around Sedona
    ST_GeomFromWKB(z.z_boundary)
  )
  AND ST_Within(
    ST_GeomFromWKB(t.t_pickuploc),
    ST_GeomFromWKB(z.z_boundary)
  )
GROUP BY z.z_zonekey, z.z_name
ORDER BY total_pickups DESC, z.z_zonekey ASC
""").show(3)
```

    ┌───────────┬─────────────────┬───────────────┬──────────────┬────────────────────────────────────┐
    │ z_zonekey ┆      z_name     ┆ total_pickups ┆ avg_distance ┆            avg_duration            │
    │   int64   ┆       utf8      ┆     int64     ┆  decimal128  ┆              duration              │
    ╞═══════════╪═════════════════╪═══════════════╪══════════════╪════════════════════════════════════╡
    │     30084 ┆ Coconino County ┆           541 ┆  0.000030406 ┆ 0 days 1 hours 45 mins 16.591 secs │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │     30083 ┆ Yavapai County  ┆           292 ┆  0.000027157 ┆ 0 days 1 hours 36 mins 43.647 secs │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │     29488 ┆ Gila County     ┆            39 ┆  0.000021282 ┆ 0 days 1 hours 16 mins 59.769 secs │
    └───────────┴─────────────────┴───────────────┴──────────────┴────────────────────────────────────┘


## Q7: Detect potential route detours by comparing reported vs. geometric distances

**Real-life scenario:** Identify suspicious trips where the reported distance significantly exceeds the straight-line distance, potentially indicating fare manipulation.

This query analyzes how much taxi and rideshare trips deviate from the most direct route by comparing the actual reported trip distance to the straight-line distance between pickup and dropoff points. It calculates a "detour ratio" that shows how much longer the actual route was compared to flying in a straight line.  For example, a ratio of 1.5 means the trip was 50% longer than the direct path. The results are sorted to show the trips with the highest detour ratios first, helping identify routes that took significant detours due to traffic, road layouts, or other factors.

**Spatial query characteristics tested:**

1. Line geometry creation (ST_MakeLine)
2. Length calculation (ST_Length)
3. Coordinate system conversion and distance calculations
4. Ratio-based filtering on geometric vs. reported measurements


```python
sd.sql("""
WITH trip_lengths AS (
    SELECT
        t.t_tripkey,
        t.t_distance AS reported_distance_m,
        ST_Length(
            ST_MakeLine(
                ST_GeomFromWKB(t.t_pickuploc),
                ST_GeomFromWKB(t.t_dropoffloc)
            )
        ) * 111111 AS line_distance_m -- Approx. meters per degree
    FROM trip t
)
SELECT
    t.t_tripkey,
    t.reported_distance_m,
    t.line_distance_m,
    t.reported_distance_m / NULLIF(t.line_distance_m, 0) AS detour_ratio
FROM trip_lengths t
ORDER BY
    detour_ratio DESC NULLS LAST,
    reported_distance_m DESC,
    t_tripkey ASC
""").show(3)
```

    ┌───────────┬─────────────────────┬────────────────────┬──────────────────────┐
    │ t_tripkey ┆ reported_distance_m ┆   line_distance_m  ┆     detour_ratio     │
    │   int64   ┆      decimal128     ┆       float64      ┆        float64       │
    ╞═══════════╪═════════════════════╪════════════════════╪══════════════════════╡
    │   4688563 ┆             0.00010 ┆ 11111.114941555596 ┆ 8.999996897341038e-9 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │   2380123 ┆             0.00010 ┆ 11111.114983939786 ┆ 8.999996863009868e-9 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │   3077131 ┆             0.00010 ┆ 11111.115027455284 ┆ 8.999996827762339e-9 │
    └───────────┴─────────────────────┴────────────────────┴──────────────────────┘


## Q8: Count nearby pickups for each building within a 500m radius

**Real-life scenario:** Count how many trips start within 500 meters of each building.

This query identifies which buildings generate the most taxi and rideshare pickup activity by counting trips that started within 500 meters of each building. It analyzes the relationship between specific buildings (like hotels, shopping centers, airports, or office buildings) and ride demand in their immediate vicinity. The results are ranked to show which buildings are the biggest trip generators, helping identify key pickup hotspots and understand how different types of buildings drive transportation demand.

**Spatial query characteristics tested:**

1. Distance spatial join between points and polygons
2. Aggregation on spatial join result


```python
sd.sql("""
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t
JOIN building b
ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045) -- ~500m
GROUP BY b.b_buildingkey, b.b_name
ORDER BY nearby_pickup_count DESC, b.b_buildingkey ASC
""").show(3)
```

    ┌───────────────┬────────┬─────────────────────┐
    │ b_buildingkey ┆ b_name ┆ nearby_pickup_count │
    │     int64     ┆  utf8  ┆        int64        │
    ╞═══════════════╪════════╪═════════════════════╡
    │          3779 ┆ linen  ┆                  42 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │         19135 ┆ misty  ┆                  36 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │          4416 ┆ sienna ┆                  26 │
    └───────────────┴────────┴─────────────────────┘


## Q9: Building Conflation (duplicate/overlap detection via IoU)

**Real-life scenario:** Detect duplicate or overlapping building footprints in GIS datasets to identify data quality issues.

This query identifies overlapping buildings by calculating how much their footprints intersect with each other. For every pair of buildings that touch or overlap, it measures the total area of each building and the area where they overlap, then calculates an "Intersection over Union" (IoU) score that ranges from 0 to 1. The results are sorted by IoU score to show the most significantly overlapping building pairs first, which could help identify data quality issues, adjacent structures, or buildings that share common areas like courtyards or parking lots.

**Spatial query characteristics tested:**

1. Self-join with spatial intersection (ST_Intersects)
2. Area calculations (ST_Area)
3. Geometric intersection operations (ST_Intersection)
4. Complex geometric ratio calculations (IoU - Intersection over Union)


```python
sd.sql("""
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
    JOIN b2 ON b1.id < b2.id AND ST_Intersects(b1.geom, b2.geom)
)
SELECT
   building_1,
   building_2,
   area1,
   area2,
   overlap_area,
   CASE
       WHEN (area1 + area2 - overlap_area) = 0 THEN 1.0
       ELSE overlap_area / (area1 + area2 - overlap_area)
   END AS iou
FROM pairs
ORDER BY iou DESC, building_1 ASC, building_2 ASC
""").show(3)
```

    ┌────────────┬────────────┬───┬───────────────────────┬────────────────────┐
    │ building_1 ┆ building_2 ┆ … ┆      overlap_area     ┆         iou        │
    │    int64   ┆    int64   ┆   ┆        float64        ┆       float64      │
    ╞════════════╪════════════╪═══╪═══════════════════════╪════════════════════╡
    │       2285 ┆      15719 ┆ … ┆ 2.3709162946727276e-6 ┆ 0.9056816071717889 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │       7562 ┆      18534 ┆ … ┆  5.855106543747764e-6 ┆ 0.8450437137796769 │
    ├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │       2285 ┆      13658 ┆ … ┆ 1.9770693222933237e-6 ┆  0.737899157380637 │
    └────────────┴────────────┴───┴───────────────────────┴────────────────────┘


## Q10: Zone statistics for trips starting within each zone

**Real-life scenario:** Analyze trip patterns and performance metrics for each administrative zone (like city districts or neighborhoods).

This query analyzes trip patterns across all geographic zones by calculating average trip duration, distance, and volume for rides originating from each area. It uses a left join to include all zones in the results, even those with no pickup activity, showing which neighborhoods generate longer trips on average versus shorter local rides. The results are sorted by average trip duration to identify zones where people tend to take longer journeys, which could indicate more isolated areas, have limited local amenities, or serve as departure points for longer-distance travel.

**Spatial query characteristics tested:**

1. Point-in-polygon spatial join (ST_Within)
2. Aggregation with multiple metrics (average duration, distance, count)
3. LEFT JOIN to include zones with no trips


```python
sd.sql("""
SELECT
    z.z_zonekey,
    z.z_name AS pickup_zone,
    AVG(t.t_dropofftime - t.t_pickuptime) AS avg_duration,
    AVG(t.t_distance) AS avg_distance,
    COUNT(t.t_tripkey) AS num_trips
FROM
    zone z
    LEFT JOIN trip t
    ON ST_Within(
        ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(z.z_boundary)
    )
GROUP BY z.z_zonekey, z.z_name
ORDER BY avg_duration DESC NULLS LAST, z.z_zonekey ASC
""").show(3)
```

    ┌───────────┬────────────────┬────────────────────────────────────┬──────────────┬───────────┐
    │ z_zonekey ┆   pickup_zone  ┆            avg_duration            ┆ avg_distance ┆ num_trips │
    │   int64   ┆      utf8      ┆              duration              ┆  decimal128  ┆   int64   │
    ╞═══════════╪════════════════╪════════════════════════════════════╪══════════════╪═══════════╡
    │     31558 ┆ Benewah County ┆ 4 days 13 hours 3 mins 34.000 secs ┆  0.002180000 ┆         2 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    │    119540 ┆ Kreis Unna     ┆ 2 days 4 hours 52 mins 44.000 secs ┆  0.001050000 ┆         1 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
    │     59928 ┆ Ndhiwa         ┆ 2 days 4 hours 19 mins 39.000 secs ┆  0.001040000 ┆         1 │
    └───────────┴────────────────┴────────────────────────────────────┴──────────────┴───────────┘


## Q11: Count trips that cross between different zones

**Real-life scenario:** Identify inter-district or inter-city trips to understand cross-boundary travel patterns.

This query counts how many trips crossed zone boundaries by starting in one geographic zone and ending in a different zone. It identifies each trip's pickup and dropoff zones, then filters to only include trips where the pickup zone is different from the dropoff zone. The result shows the total number of inter-zone trips, helping measure how much travel occurs between different neighborhoods, districts, or areas rather than staying within the same local zone.

**Spatial query characteristics tested:**

1. Multiple point-in-polygon spatial joins
2. Filtering based on spatial relationship results


```python
sd.sql("""
SELECT COUNT(*) AS cross_zone_trip_count
FROM
    trip t
    JOIN zone pickup_zone
        ON ST_Within(
            ST_GeomFromWKB(t.t_pickuploc),
            ST_GeomFromWKB(pickup_zone.z_boundary)
        )
    JOIN zone dropoff_zone
        ON ST_Within(
            ST_GeomFromWKB(t.t_dropoffloc),
            ST_GeomFromWKB(dropoff_zone.z_boundary)
        )
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
""").show(3)
```

    ┌───────────────────────┐
    │ cross_zone_trip_count │
    │         int64         │
    ╞═══════════════════════╡
    │                176391 │
    └───────────────────────┘


## Q12: Find five nearest buildings to each trip pickup location using KNN join
**Real-life scenario:** Identify the closest landmarks or buildings to each trip start point for location context and navigation.

This query finds the 5 closest buildings to each trip pickup location using spatial nearest neighbor analysis. For every trip, it identifies the five buildings that are geographically closest to where the passenger was picked up and calculates the exact distance to each of those buildings. The results show which buildings are most commonly near pickup points, helping understand the relationship between trip origins and nearby landmarks, businesses, or residential structures that might influence ride demand patterns.

**Spatial query characteristics tested:**

1. K-nearest neighbor (KNN) spatial join
2. Distance calculations between points and polygons
3. Ranking and limiting results based on spatial proximity


```python
sd.sql("""
WITH trip_with_geom AS (
    SELECT
        t_tripkey,
        t_pickuploc,
        ST_GeomFromWKB(t_pickuploc) as pickup_geom
    FROM trip
),
building_with_geom AS (
    SELECT
        b_buildingkey,
        b_name,
        b_boundary,
        ST_GeomFromWKB(b_boundary) as boundary_geom
    FROM building
)
SELECT
    t.t_tripkey,
    t.t_pickuploc,
    b.b_buildingkey,
    b.b_name AS building_name,
    ST_Distance(t.pickup_geom, b.boundary_geom) AS distance_to_building
FROM trip_with_geom t
JOIN building_with_geom b
    ON ST_KNN(t.pickup_geom, b.boundary_geom, 5, FALSE)
ORDER BY t.t_tripkey ASC, distance_to_building ASC, b.b_buildingkey ASC
""").show(3)
```

    ┌───────────┬─────────────────────────────────┬───────────────┬───────────────┬────────────────────┐
    │ t_tripkey ┆           t_pickuploc           ┆ b_buildingkey ┆ building_name ┆ distance_to_buildi │
    │   int64   ┆              binary             ┆     int64     ┆      utf8     ┆         ng…        │
    ╞═══════════╪═════════════════════════════════╪═══════════════╪═══════════════╪════════════════════╡
    │         1 ┆ 01010000009f3c318dd43735405930… ┆         15870 ┆ purple        ┆  0.984633987957188 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │         1 ┆ 01010000009f3c318dd43735405930… ┆          6800 ┆ ghost         ┆  1.205725156670704 │
    ├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
    │         1 ┆ 01010000009f3c318dd43735405930… ┆          8384 ┆ lavender      ┆ 1.4195012994942622 │
    └───────────┴─────────────────────────────────┴───────────────┴───────────────┴────────────────────┘


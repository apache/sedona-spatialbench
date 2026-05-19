# 运行 SpatialBench 查询

本笔记本包含构成 SpatialBench 基准测试的所有查询。

SpatialBench 是一个用于评估各类数据库系统中地理空间 SQL 分析查询性能的基准测试。这些查询代表了常见的真实世界地理空间分析任务，旨在测试各种空间函数和连接条件。

该基准测试使用了一个真实但合成的、以交通运输为主题的数据集，以确保查询能够反映实际的使用场景。通过运行这些查询，你可以在一致且中立的基础上评估并比较不同空间查询引擎的相对性能。

## 在开始之前

在运行此笔记本前，请确保已安装 `requirements.txt` 中列出的依赖：


```python
%pip install -r ~/sedona-spatialbench/docs/requirements.txt
```

    ...
    ...
    Note: you may need to restart the kernel to use updated packages.


此外，请安装 SpatialBench CLI 并在本机生成合成数据：

```
# SpatialBench CLI
cargo install --path ./spatialbench-cli
# 将基准测试数据生成到 sf1-parquet 目录
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

## Q1：在距离 Sedona 市中心 50 公里以内的行程，按距离排序

**真实场景：** 根据与市中心的距离对行程进行识别和排序，用于城市规划和交通分析。

该查询找出所有从亚利桑那州 Sedona 市中心 50 公里范围内出发的出租车或网约车行程。对于每一个符合条件的行程，结果会显示行程 ID、上车经纬度、上车时间，并计算上车点到 Sedona 市中心的精确距离。结果按距离升序排序，最靠近市中心的行程排在最前面，便于查看哪些行程最贴近市中心。

**被测试的空间查询特性：**

1. 基于距离的空间过滤（ST_DWithin）
2. 到固定点的距离计算
3. 坐标提取（ST_X、ST_Y）
4. 按空间距离排序


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
    0.45 -- Sedona 中心 50km 半径，以度为单位
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


## Q2：统计 Coconino 县（Arizona）区域内起始的行程数量

**真实场景：** 统计某个特定行政边界（县）内起始的所有行程数量，用于区域交通统计。

该查询统计有多少出租车或网约车行程在亚利桑那州 Coconino 县内出发。其做法是判断每个行程的上车点是否落在该县的地理边界内。结果是一个简单的总数，表示在 Coconino 县范围内出发的行程总数。

**被测试的空间查询特性：**

1. 点在多边形内的空间过滤（ST_Intersects）
2. 包含空间几何选择的子查询
3. 对经空间过滤的数据进行简单聚合


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


## Q3：Sedona 市中心 15 公里半径内的月度行程统计

**真实场景：** 跟踪都市区的月度出行趋势和性能指标，并进行季节性分析。

该查询通过将行程按月度汇总，分析 Sedona 周边的出租车和网约车行程模式。它会查看所有在 Sedona 周边 15 公里范围内（即 10km 的边界框加上 5km 的缓冲区）起始的行程，并为每个月份计算关键统计量，包括总行程数、平均行驶距离、平均行程时长以及平均车费。结果按月份按时间顺序排列，让你可以观察到 Sedona 地区出行模式的季节性变化趋势。

**被测试的空间查询特性：**

1. 带缓冲区的基于距离空间过滤（ST_DWithin）
2. 时间维度的分组（月度聚合）
3. 在空间过滤后的数据上进行多种统计聚合


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
    ))'), -- Sedona 周边的边界框
    0.045 -- 额外 5km 缓冲（以度计）
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


## Q4：按小费金额排序的前 1000 个行程在各区域的分布

**真实场景：** 分析高价值行程（按小费金额）的地理分布，从而了解高端服务区域。

该查询分析前 1000 个小费最高的行程，识别出哪些街区或区域产生了最多的“慷慨小费客”。它先找出小费金额最高的 1000 个行程，再判断这些上车点落在哪些地理区域或街区内，并统计每个区域中此类“高小费”行程的数量。结果按高额小费数量进行排名，便于识别出对司机最具吸引力的高小费上车区域。

**被测试的空间查询特性：**

1. 包含排序和限制的子查询
2. 点在多边形内的空间连接（ST_Within）
3. 在空间连接结果上进行聚合
4. 包含空间过滤和分组的多步查询


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
    │    150276 ┆ 锡林郭勒盟 ᠰᠢᠯᠢ ᠶᠢᠨ ᠭᠣᠣᠯ ᠠᠶᠢᠮᠠᠭ ┆         19 │
    └───────────┴─────────────────────────────────┴────────────┘


## Q5：重复客户的月度出行模式（基于下车点的凸包）

**真实场景：** 分析常客出行模式的地理分布范围，以了解他们的出行行为。

该查询通过测量常客每月行程所覆盖的地理范围，分析他们的月度出行模式。对于每个在某个月内乘坐超过 5 次行程的客户，它会计算其“出行凸包”的面积——即连接当月所有下车点形成的区域的面积。结果可以揭示出哪些客户的出行覆盖范围最广，帮助识别覆盖大面积区域的“重度用户”以及那些只在较小的本地区域内活动的用户。

**被测试的空间查询特性：**

1. 空间聚合（ST_Collect / ARRAY_AGG）
2. 凸包计算（ST_ConvexHull）
3. 复杂几何上的面积计算
4. 结合时间和客户维度的分组与空间运算


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
HAVING dropoff_count > 5 -- 仅保留重复客户
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


## Q6：Sedona 市中心 50 公里半径内各区域的行程统计

**真实场景：** 分析特定城市中心周边都市区内各区域的行程模式。

该查询分析以亚利桑那州 Sedona 为中心、半径 50 公里范围内所有街区和区域的乘车活动。它通过统计每个区域内起始的行程总数，识别出上车活动最频繁的区域；同时，还计算每个区域内行程的平均费用和平均时长。结果按上车次数排序，揭示出大 Sedona 区域内哪些街区或区域产生的乘车需求最旺盛及其典型的行程特征。

**被测试的空间查询特性：**

1. 带边界框的多边形包含检查（ST_Contains）
2. 点在多边形内的空间连接（ST_Within）


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
    ))'), -- Sedona 周边的边界框
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


## Q7：通过对比上报距离与几何距离来检测可能的绕路

**真实场景：** 找出上报行驶距离明显超过两点直线距离的可疑行程，可能存在车费操纵嫌疑。

该查询通过比较行程上报的实际距离与上车点到下车点的直线距离，分析出租车和网约车行程偏离最短路径的程度。它会计算一个“绕路比”（detour ratio），用于反映实际路线比直线距离长多少。例如，比值为 1.5 表示该行程比直线距离多走了 50%。结果按绕路比从高到低排序，便于识别因交通、道路布局或其他原因而显著绕路的行程。

**被测试的空间查询特性：**

1. 线几何的构建（ST_MakeLine）
2. 长度计算（ST_Length）
3. 坐标系转换和距离计算
4. 基于几何与上报值之比的过滤


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
        ) * 111111 AS line_distance_m -- 每度约对应的米数
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


## Q8：统计每栋建筑物 500 米半径内的上车次数

**真实场景：** 统计每栋建筑物 500 米范围内起始的行程数量。

该查询通过统计在每栋建筑物 500 米范围内起始的行程数量，识别哪些建筑物附近产生的出租车和网约车上车活动最多。它分析特定建筑物（如酒店、购物中心、机场或办公楼）与其周边乘车需求之间的关系。结果按数量排序，可以帮助识别出最重要的上车热点建筑物，并理解不同类型的建筑物如何影响交通需求。

**被测试的空间查询特性：**

1. 点和多边形之间基于距离的空间连接
2. 在空间连接结果上的聚合


```python
sd.sql("""
SELECT b.b_buildingkey, b.b_name, COUNT(*) AS nearby_pickup_count
FROM trip t
JOIN building b
ON ST_DWithin(ST_GeomFromWKB(t.t_pickuploc), ST_GeomFromWKB(b.b_boundary), 0.0045) -- 约 500m
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


## Q9：通过 IoU 进行建筑物合并（检测重复/重叠的建筑物轮廓）

**真实场景：** 检测 GIS 数据集中重复或重叠的建筑物轮廓，识别数据质量问题。

该查询通过计算建筑物轮廓之间的相互重叠面积来识别重叠的建筑物。对于每一对相互接触或相互重叠的建筑物，它会测量每栋建筑物的总面积以及它们的重叠面积，然后计算 0 到 1 之间的“交并比”（Intersection over Union，IoU）。结果按 IoU 从高到低排序，可揭示出最显著重叠的建筑物对，这有助于识别数据质量问题、相邻结构，或共享公共区域（如院子或停车场）的建筑物。

**被测试的空间查询特性：**

1. 带空间相交的自连接（ST_Intersects）
2. 面积计算（ST_Area）
3. 几何相交操作（ST_Intersection）
4. 复杂几何比值的计算（IoU——交并比）


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


## Q10：每个区域内起始行程的统计指标

**真实场景：** 分析每个行政区域（如市辖区或街区）的行程模式与性能指标。

该查询通过计算每个区域内起始行程的平均时长、平均距离和数量，分析各个地理区域的行程模式。它使用左连接（LEFT JOIN）来保留所有区域，包括那些没有上车活动的区域，从而展示出哪些街区平均行程较长，哪些则以较短的本地行程为主。结果按平均时长降序排序，便于识别人们倾向于走长途的区域——这些区域可能较为偏远、本地配套有限，或作为长途出行的出发地。

**被测试的空间查询特性：**

1. 点在多边形内的空间连接（ST_Within）
2. 多指标聚合（平均时长、距离、数量）
3. 使用 LEFT JOIN 保留无行程的区域


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


## Q11：统计跨区域出行的行程数量

**真实场景：** 识别跨行政区或跨城市的行程，以了解跨边界的出行模式。

该查询通过判断行程的上车区域与下车区域是否相同，统计有多少行程跨越了区域边界。它会找出每个行程的上车区域和下车区域，然后仅保留上车区域与下车区域不同的行程。结果是跨区域行程的总数，可用于衡量在不同街区、行政区或区域之间发生的出行量，而非仅停留在同一本地区域内的出行量。

**被测试的空间查询特性：**

1. 多次点在多边形内的空间连接
2. 基于空间关系结果进行过滤


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


## Q12：使用 KNN 连接找出每个行程上车点最近的 5 栋建筑物
**真实场景：** 找出每个行程起点最近的地标或建筑物，为定位和导航提供上下文。

该查询通过空间最近邻分析，找出每个行程上车点最近的 5 栋建筑物。对于每个行程，它会识别出与上车点地理距离最近的 5 栋建筑物，并计算到每栋建筑物的精确距离。结果可以揭示哪些建筑物常常出现在上车点附近，有助于理解行程起点与附近地标、商业设施或住宅建筑之间的关系，以及它们如何影响出行需求模式。

**被测试的空间查询特性：**

1. K 近邻（KNN）空间连接
2. 点和多边形之间的距离计算
3. 基于空间邻近性进行排序与限制


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

---
title: 地理查询
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

# 运行 SpatialBench 地理查询

SpatialBench 的[主查询套件](https://sedona.apache.org/spatialbench/zh/queries/)是一个 **geometry（几何）** 基准：WKB 列通过
`ST_GeomFromWKB` 解码，谓词使用平面（二维欧几里得）边，所有距离阈值都是以度为单位的角度。

本笔记本定义这同样 12 条查询的 **geography（地理）** 版本。数据本身没有变化——仍是同样的
EPSG:4326 经纬度 WKB 列——但列改用 `ST_GeogFromWKB` 解码，因此边被解释为球面上的大地线，
所有度量结果都以真实世界的单位返回。

## 为什么需要第二套查询

geometry 套件不得不伪造真实世界的单位。它用 `ST_DWithin(..., 0.45)` 筛选 Sedona “50 公里内”
的行程，用 `1 米 = 0.000009 度` 把米换算成度，并以平方度报告建筑物的重叠面积。

一个纬度差大致在各处都约等于 111 公里，但一个经度差会随纬度的余弦而缩小。因此在 Sedona
（北纬 34.87°），`0.45` 度约为 50 公里高、却只有约 41 公里宽；在北纬 60° 处仅约 25 公里宽。
由于 SpatialBench 的数据生成在八个大陆级别的范围框内、跨越南纬 56° 到北纬 78°，以度为基准的
半径在数据集的每个区域都对应着不同的真实形状。

geography 套件把这些全部替换为米和平方米：

| | geometry 套件 | geography 套件 |
|---|---|---|
| 点/面构造函数 | `ST_GeomFromWKB`、`ST_GeomFromText` | `ST_GeogFromWKB`、`ST_GeogFromWKT` |
| 边的解释 | 平面上的直线 | 球面上的大地线 |
| `ST_DWithin` 阈值 | 度（`0.45`、`0.045`、`0.0045`） | 米（`50000`、`5000`、`500`） |
| `ST_Distance`、`ST_Length` | 度 | 米 |
| `ST_Area` | 平方度 | 平方米 |
| SQL 中的单位换算 | `/ 0.000009` | 无需换算 |

!!! note "两套查询的结果无法逐行比较"

    0.45° 的平面半径与 50 000 米的大地线半径会选出不同的行程，因此 Q1 在两套查询中返回的
    是不同的 100 行。这正是本套件的意义所在，而不是结果不一致：每套查询都有各自的标准答案，
    结果只应在同一套件内部进行比较。

## 查询模块

在本笔记本之外，也可以通过查询模块获取同样的 SQL，它会按指定方言打印整套查询：

```bash
python3 spatialbench-queries/print_geography_queries.py SedonaDB
```

运行这些查询需要一个具备 `GEOGRAPHY` 类型、且其度量基于大地线的引擎——各引擎对具体函数的支持
仍在逐步完善，请查阅所用引擎自身的参考文档以确认其实现范围。Q12 在下文中给出了定义，但未纳入
该模块，也不会在此执行：它是一个 K 最近邻连接，而 `ST_KNN` 目前还没有 geography 形式。

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

或者，你也可以从 [Hugging Face](https://huggingface.co/datasets/apache-sedona/spatialbench) 下载预生成的数据，而无需自行生成（数据表按 `v<版本>/sf<规模>/` 组织；提供 `sf0.1`、`sf1`、`sf10`、`sf100`）：

```
pip install huggingface-hub
hf download apache-sedona/spatialbench --repo-type dataset --include "v0.1.0/sf1/**" --local-dir spatialbench-data
```

如果使用 Hugging Face 下载的数据，请在下方的数据加载单元格中设置 `DATA_DIR = "spatialbench-data/v0.1.0/sf1"`。


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

## Q1：查找从 Sedona 市中心 50 公里范围内出发的行程，并按距离排序

**实际场景：** 按照与市中心的距离识别并排序行程，用于城市规划与交通分析。

与 geometry 版本不同，这里的 50 公里是真正的大地线半径，而不是 0.45° 的角度半径，且
`distance_to_center` 以米为单位。

**所考察的空间查询特性：**

1. 基于大地线距离的空间过滤（`ST_DWithin`，米）
2. 到固定点的大地线距离计算
3. 坐标提取（`ST_X`、`ST_Y`）
4. 按空间距离排序


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

## Q2：统计从 Coconino County 出发的行程数

**实际场景：** 统计某一行政边界内的行程活动。

县级多边形来自 Overture 数据，其边为平面边；将它作为 geography 读取会把这些边重新解释为
大地线，相对 geometry 套件可能使边界偏移几米，从而改变那些几乎正好落在边界上的上车点的计数。

**所考察的空间查询特性：**

1. 球面上的点在多边形内谓词（`ST_Intersects`）
2. 由标量子查询产生查询用 geography


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

## Q3：Sedona 市中心周边带缓冲区范围的每月行程统计

**实际场景：** 跟踪某都市区每月的需求、收入与行程时长。

多边形字面量与 geometry 套件中的环相同，但这里它的边是大地线，因此它是一个球面四边形而不是
平面矩形。

该环以 Sedona 为中心，东西向约 **26.5 公里**、南北向约 **30 公里**——沿两个方向距中心分别约
13 公里与 15 公里，四角约在 20 公里处。再加上 5 000 米缓冲后，四角处可达约 25 公里。

**所考察的空间查询特性：**

1. 基于大地线的多边形缓冲过滤（`ST_DWithin`，米）
2. 使用 `DATE_TRUNC` 的时间分组
3. 在空间过滤结果上进行多项聚合


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

## Q4：小费最高的 1000 次行程的区域分布

**实际场景：** 找出小费最丰厚的街区。

**所考察的空间查询特性：**

1. 球面上的点在多边形内空间连接（`ST_Within`）
2. 与 `LIMIT` 限定子查询的空间连接
3. 在空间连接结果上进行聚合


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

## Q5：回头客的每月出行范围

**实际场景：** 通过下车点的凸包，衡量每位高频乘客在一个月内的出行覆盖范围有多大。

在 geography 套件中，`monthly_travel_hull_area` 是以平方米为单位的球面面积，且凸包本身也是
用大地线边计算的。

**所考察的空间查询特性：**

1. geography 聚合为集合（`ST_Collect_Agg`）
2. 球面凸包（`ST_ConvexHull`）
3. 球面面积（`ST_Area`，平方米）
4. 带 `HAVING` 的分组聚合


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

## Q6：与某一范围框相交的区域的行程统计

**实际场景：** 汇总所有与目标区域接触的区域的行程活动。

**所考察的空间查询特性：**

1. 在一次连接中使用两个空间谓词（`ST_Intersects` 与 `ST_Within`）
2. 球面上的多边形-多边形相交判断
3. 在双重过滤的连接结果上进行聚合


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

## Q7：检测潜在的绕路行程

**实际场景：** 标记报告距离远超上下车点直线距离的行程，作为绕路或虚增车费的信号。

这是 geography 类型帮助最大的一条查询。geometry 版本以度为单位测量平面线段长度，再除以
`0.000009` 假装它是米；geography 版本直接测量大地线，因此 `line_distance_m` 与
`detour_ratio` 在整个数据集范围内都有意义，而不仅仅在赤道附近成立。

**所考察的空间查询特性：**

1. 由两点构造线（`ST_MakeLine`）
2. 大地线长度（`ST_Length`，米）
3. 带除零保护的比值计算


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

## Q8：统计每栋建筑 500 米范围内的上车次数

**实际场景：** 找出产生最多上车活动的建筑物。

500 米半径是自建筑轮廓起算的真实大地线距离，因此挪威的一栋建筑与肯尼亚的一栋建筑拥有相同
物理尺度的覆盖范围——这与 geometry 套件不同，在那里 `0.0045°` 在高纬度会形成窄得多的条带。

**所考察的空间查询特性：**

1. 点与多边形之间基于大地线距离的空间连接（`ST_DWithin`，米）
2. 在空间连接结果上进行聚合


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

## Q9：建筑物融合（通过 IoU 检测重复/重叠）

**实际场景：** 检测重复或重叠的建筑轮廓，以发现数据质量问题。

这里 `area1`、`area2` 与 `overlap_area` 的单位是平方米而非平方度，因此可以直接解读。`iou`
是面积之比，在两套查询中都是无量纲的——不过取值仍会不同，因为球面面积之比并不等于平面面积
之比。

**所考察的空间查询特性：**

1. 带空间谓词的多边形-多边形自连接（`ST_Intersects`）
2. 球面叠加运算（`ST_Intersection`）
3. 球面面积（`ST_Area`，平方米）


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

## Q10：各区域内出发行程的统计

**实际场景：** 按平均行程时长对区域排序，同时保留没有行程的区域。

**所考察的空间查询特性：**

1. 球面上的左空间连接（`ST_Within`），保留未匹配的区域
2. 排序中带 `NULL` 处理的聚合


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

## Q11：统计跨区域的行程数

**实际场景：** 量化区域间的出行需求，用于公共交通规划。

**所考察的空间查询特性：**

1. 球面上两次独立的点在多边形内空间连接
2. 在连接结果上的非空间过滤


```python
sd.sql("""
SELECT COUNT(*) AS cross_zone_trip_count
FROM trip t
JOIN zone pickup_zone ON ST_Within(ST_GeogFromWKB(t.t_pickuploc), ST_GeogFromWKB(pickup_zone.z_boundary))
JOIN zone dropoff_zone ON ST_Within(ST_GeogFromWKB(t.t_dropoffloc), ST_GeogFromWKB(dropoff_zone.z_boundary))
WHERE pickup_zone.z_zonekey != dropoff_zone.z_zonekey
""").show(3)

```

## Q12：按到最近 5 栋建筑的平均距离对上车点排序

**实际场景：** 找出最孤立的上车点——距离周边任何建筑都最远的位置——用于覆盖度与服务空白分析。

`avg_distance_to_5_nearest` 以米为单位，且近邻排序本身依据大地线距离而非平面度数，因此
“最近的 5 栋建筑”是地面上真正最近的 5 栋。

**所考察的空间查询特性：**

1. 球面上的 K 最近邻空间连接（`ST_KNN`）
2. 点到多边形的大地线距离（`ST_Distance`，米）
3. 在 KNN 结果上进行聚合


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

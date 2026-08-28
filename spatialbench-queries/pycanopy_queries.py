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

from __future__ import annotations

import polars as pl
import shapely

import pycanopy as pc


def q1(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q1 (PyCanopy): Trips starting within ~50km of Sedona city center."""
    center = (-111.7610, 34.8697)
    radius = 0.45  # degrees (~50km, planar)

    sf = pc.SpatialFrame.scan_parquet(
        data_paths["trip"], geometry_col="t_pickuploc", geometry_kind="point"
    )
    # The select keeps the scan narrow and carries the decoded coordinates out of the source
    near = (
        sf.lazy()
        .within_distance_of_point(center[0], center[1], radius)
        .select("t_tripkey", "t_pickuptime", "_x", "_y")
        .collect()
    )
    result = near.with_columns(
        distance_to_center=(
            (pl.col("_x") - center[0]) ** 2 + (pl.col("_y") - center[1]) ** 2
        ).sqrt()
    ).select(
        "t_tripkey",
        pl.col("_x").alias("pickup_lon"),
        pl.col("_y").alias("pickup_lat"),
        "t_pickuptime",
        "distance_to_center",
    )
    del sf, near
    return result.lazy().sort(["distance_to_center", "t_tripkey"]).head(100).collect()


def q2(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q2 (PyCanopy): Count trips starting within Coconino County zone."""
    zone = (
        pc.SpatialFrame.scan_parquet(
            data_paths["zone"], geometry_col="z_boundary", geometry_kind="polygon"
        )
        .lazy()
        .filter(pl.col("z_name") == "Coconino County")
        .limit(1)
        .select("z_boundary")
        .collect()
    )
    # from_wkb keeps a MultiPolygon whole rather than exploding it into parts
    poly = shapely.from_wkb(zone["z_boundary"][0])

    sf = pc.SpatialFrame.scan_parquet(
        data_paths["trip"], geometry_col="t_pickuploc", geometry_kind="point"
    )
    count = sf.lazy().points_within_distance_of_polygon(poly, 0.0).count()
    return pl.DataFrame({"trip_count_in_coconino_county": [count]})


def q3(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q3 (PyCanopy): Monthly trip stats within ~5km of a ~26.5x30 km box around Sedona."""
    distance = 0.045  # degrees (~5km)
    base_poly = shapely.Polygon(
        [
            (-111.9060, 34.7347),
            (-111.6160, 34.7347),
            (-111.6160, 35.0047),
            (-111.9060, 35.0047),
            (-111.9060, 34.7347),
        ]
    )
    agg_cols = ["t_pickuptime", "t_dropofftime", "t_distance", "t_fare"]

    sf = pc.SpatialFrame.scan_parquet(
        data_paths["trip"], geometry_col="t_pickuploc", geometry_kind="point"
    )
    # The deferred source prunes to the aggregated columns and frees each WKB batch after decode
    filtered = (
        sf.lazy()
        .points_within_distance_of_polygon(base_poly, distance)
        .select(agg_cols)
        .collect()
    )
    filtered = filtered.with_columns(
        pickup_month=pl.col("t_pickuptime").dt.truncate("1mo"),
        duration_seconds=(
            pl.col("t_dropofftime") - pl.col("t_pickuptime")
        ).dt.total_seconds(),
    )
    return (
        filtered.group_by("pickup_month")
        .agg(
            total_trips=pl.len(),
            avg_distance=pl.col("t_distance").mean(),
            avg_duration=pl.col("duration_seconds").mean(),
            avg_fare=pl.col("t_fare").mean(),
        )
        .sort("pickup_month")
    )


def q4(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q4 (PyCanopy): Zone distribution of the top 1000 trips by tip amount."""
    top_n = 1000

    top, zone = pl.collect_all(
        [
            pl.scan_parquet(data_paths["trip"])
            .select(["t_tripkey", "t_tip", "t_pickuploc"])
            .top_k(top_n, by=["t_tip", "t_tripkey"], reverse=[False, True])
            .select(["t_tripkey", "t_pickuploc"]),
            pl.scan_parquet(data_paths["zone"]).select(
                ["z_zonekey", "z_name", "z_boundary"]
            ),
        ]
    )

    qx, qy = pc.wkb_points_to_xy(top["t_pickuploc"])
    query_df = top.select("t_tripkey").with_columns(
        pl.Series("qx", qx), pl.Series("qy", qy)
    )
    del top

    sf = pc.SpatialFrame.from_wkb_polygons(zone, "z_boundary")

    return (
        sf.lazy()
        .within_join(query_df, "qx", "qy")
        .group_by(["z_zonekey", "z_name"])
        .agg(trip_count=pc.agg.count())
        .sort(["trip_count", "z_zonekey"], descending=[True, False])
    )


def q5(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q5 (PyCanopy): Monthly travel hull area for repeat customers (convex hull of dropoffs)."""
    min_trips = 5

    trip, cust = pl.collect_all(
        [
            pl.scan_parquet(data_paths["trip"]).select(
                ["t_custkey", "t_dropoffloc", "t_pickuptime"]
            ),
            pl.scan_parquet(data_paths["customer"]).select(["c_custkey", "c_name"]),
        ]
    )

    dx, dy = pc.wkb_points_to_xy(trip["t_dropoffloc"])
    t = (
        trip.select(["t_custkey", "t_pickuptime"])
        .with_columns(
            pl.Series("dx", dx),
            pl.Series("dy", dy),
            pickup_month=pl.col("t_pickuptime").dt.truncate("1mo"),
        )
        .select(["t_custkey", "pickup_month", "dx", "dy"])
    )
    del trip
    grouped = (
        t.group_by(["t_custkey", "pickup_month"])
        .agg(trip_count=pl.len(), dxs=pl.col("dx"), dys=pl.col("dy"))
        .filter(pl.col("trip_count") > min_trips)
    )

    areas = pc.Engine.group_convex_hull_areas(grouped["dxs"], grouped["dys"])
    grouped = grouped.with_columns(
        monthly_travel_hull_area=pl.Series(
            "monthly_travel_hull_area", areas, dtype=pl.Float64
        )
    )
    grouped = grouped.join(cust, left_on="t_custkey", right_on="c_custkey", how="inner")
    grouped = (
        grouped.lazy()
        .sort(
            ["monthly_travel_hull_area", "t_custkey", "pickup_month"],
            descending=[True, False, False],
        )
        .head(100)
        .collect()
    )

    return grouped.select(
        [
            "t_custkey",
            "c_name",
            "pickup_month",
            "monthly_travel_hull_area",
            "trip_count",
        ]
    ).rename(
        {
            "t_custkey": "c_custkey",
            "c_name": "customer_name",
            "trip_count": "dropoff_count",
        }
    )


def q6(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q6 (PyCanopy): Zone statistics for trips intersecting a bounding box."""
    bbox = (-112.2110, 34.4197, -111.3110, 35.3197)  # min_x, min_y, max_x, max_y
    trip_cols = ["t_pickuploc", "t_distance", "t_pickuptime", "t_dropofftime"]

    zone, trip = pl.collect_all(
        [
            pl.scan_parquet(data_paths["zone"]).select(
                ["z_zonekey", "z_name", "z_boundary"]
            ),
            pl.scan_parquet(data_paths["trip"]).select(trip_cols),
        ]
    )
    zsf = pc.SpatialFrame.from_wkb_polygons(zone, "z_boundary")
    cand_sf = zsf.range_filter(*bbox)
    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    qdf = trip.select(["t_distance", "t_pickuptime", "t_dropofftime"]).with_columns(
        pl.Series("qx", qx),
        pl.Series("qy", qy),
        # t_distance is decimal(15,5); average it as float so the result keeps full
        # precision (a decimal mean stays at scale 5 and rounds off the answer).
        t_distance=pl.col("t_distance").cast(pl.Float64),
        duration_seconds=(
            pl.col("t_dropofftime") - pl.col("t_pickuptime")
        ).dt.total_seconds(),
    )
    del trip

    return (
        cand_sf.lazy()
        .within_join(qdf, "qx", "qy")
        .group_by(["z_zonekey", "z_name"])
        .agg(
            total_pickups=pc.agg.count(),
            avg_distance=pc.agg.mean("t_distance"),
            avg_duration=pc.agg.mean("duration_seconds"),
        )
        .sort(["total_pickups", "z_zonekey"], descending=[True, False])
    )


def q7(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q7 (PyCanopy): Detect route detours by comparing reported vs straight-line distance."""
    deg_per_m = 0.000009  # 1 meter ~= 0.000009 degrees

    trip = (
        pl.scan_parquet(data_paths["trip"])
        .select(["t_tripkey", "t_distance", "t_pickuploc", "t_dropoffloc"])
        .collect()
    )
    line_m = (
        pc.wkb_point_distance(trip["t_pickuploc"], trip["t_dropoffloc"]) / deg_per_m
    )

    df = trip.select("t_tripkey", "t_distance").with_columns(
        pl.Series("line_distance_m", line_m),
        reported_distance_m=pl.col("t_distance").cast(pl.Float64),
    )
    del trip
    df = df.with_columns(
        detour_ratio=pl.when(pl.col("line_distance_m") != 0.0)
        .then(pl.col("reported_distance_m") / pl.col("line_distance_m"))
        .otherwise(None)
    )
    return (
        df.lazy()
        .select("t_tripkey", "reported_distance_m", "line_distance_m", "detour_ratio")
        .sort(
            ["detour_ratio", "reported_distance_m", "t_tripkey"],
            descending=[True, True, False],
            nulls_last=True,
        )
        .head(100)
        .collect()
    )


def q8(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q8 (PyCanopy): Count trip pickups within ~500m of each building."""
    threshold = 0.0045  # degrees (~500m)

    buildings, trip = pl.collect_all(
        [
            pl.scan_parquet(data_paths["building"]).select(
                ["b_buildingkey", "b_name", "b_boundary"]
            ),
            pl.scan_parquet(data_paths["trip"]).select(["t_pickuploc"]),
        ]
    )
    sf = pc.SpatialFrame.from_wkb_polygons(buildings, "b_boundary")

    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    query_df = pl.DataFrame({"qx": qx, "qy": qy})
    del trip

    counts = (
        sf.lazy()
        .polygon_within_distance_join(query_df, "qx", "qy", distance=threshold)
        .group_by(["b_buildingkey", "b_name"])
        .agg(nearby_pickup_count=pc.agg.count())
    )
    return (
        counts.lazy()
        .sort(["nearby_pickup_count", "b_buildingkey"], descending=[True, False])
        .head(100)
        .collect()
    )


def q9(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q9 (PyCanopy): Building conflation via IoU (intersection over union) detection."""
    sf = pc.SpatialFrame.scan_parquet(
        data_paths["building"], geometry_col="b_boundary", geometry_kind="polygon"
    )
    pairs = sf.lazy().intersects_pairs("b_buildingkey").collect()
    return (
        pairs.select(
            pl.col("b_buildingkey_1").alias("building_1"),
            pl.col("b_buildingkey_2").alias("building_2"),
            pl.col("area_left").alias("area1"),
            pl.col("area_right").alias("area2"),
            "overlap_area",
            "iou",
        )
        .sort(["iou", "building_1", "building_2"], descending=[True, False, False])
        .head(100)
    )


def q10(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q10 (PyCanopy): Per-zone trip statistics, retaining zones with no trips."""
    trip_cols = ["t_pickuploc", "t_pickuptime", "t_dropofftime", "t_distance"]

    zone, trip = pl.collect_all(
        [
            pl.scan_parquet(data_paths["zone"]).select(
                ["z_zonekey", "z_name", "z_boundary"]
            ),
            pl.scan_parquet(data_paths["trip"]).select(trip_cols),
        ]
    )
    sf = pc.SpatialFrame.from_wkb_polygons(zone, "z_boundary")

    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    qdf = trip.with_columns(
        pl.Series("qx", qx),
        pl.Series("qy", qy),
        # t_distance is decimal(15,5); average it as float so the result keeps full
        # precision (a decimal mean stays at scale 5 and rounds off the answer).
        t_distance=pl.col("t_distance").cast(pl.Float64),
        duration_seconds=(
            pl.col("t_dropofftime") - pl.col("t_pickuptime")
        ).dt.total_seconds(),
    ).select(["qx", "qy", "t_distance", "duration_seconds"])
    del trip

    agg = (
        sf.lazy()
        .within_join(qdf, "qx", "qy")
        .group_by(["z_zonekey", "z_name"])
        .agg(
            avg_duration=pc.agg.mean("duration_seconds"),
            avg_distance=pc.agg.mean("t_distance"),
            num_trips=pc.agg.count(),
        )
    )

    all_zones = zone.select(["z_zonekey", "z_name"])
    result = (
        all_zones.join(agg, on=["z_zonekey", "z_name"], how="left")
        .with_columns(num_trips=pl.col("num_trips").fill_null(0))
        .rename({"z_name": "pickup_zone"})
    )
    return (
        result.lazy()
        .sort(["avg_duration", "z_zonekey"], descending=[True, False], nulls_last=True)
        .head(100)
        .collect()
    )


def q11(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q11 (PyCanopy): Count trips that start and end in different zones."""
    trip, zone = pl.collect_all(
        [
            pl.scan_parquet(data_paths["trip"]).select(
                ["t_tripkey", "t_pickuploc", "t_dropoffloc"]
            ),
            pl.scan_parquet(data_paths["zone"]).select(["z_zonekey", "z_boundary"]),
        ]
    )
    sf = pc.SpatialFrame.from_wkb_polygons(zone, "z_boundary")

    px, py = pc.wkb_points_to_xy(trip["t_pickuploc"])
    dx, dy = pc.wkb_points_to_xy(trip["t_dropoffloc"])
    keys = trip.select("t_tripkey")
    pickup_df = keys.with_columns(pl.Series("px", px), pl.Series("py", py))
    dropoff_df = keys.with_columns(pl.Series("dx", dx), pl.Series("dy", dy))
    del trip

    pickup_batches = (
        sf.lazy()
        .within_join(pickup_df, "px", "py")
        .select(["t_tripkey", "z_zonekey"])
        .collect_batched()
    )
    dropoff_batches = (
        sf.lazy()
        .within_join(dropoff_df, "dx", "dy")
        .select(["t_tripkey", "z_zonekey"])
        .collect_batched()
    )

    # Aligned morsels carry the same trips on each side and per-morsel counts sum to the global count
    count = 0
    for pickup, dropoff in zip(pickup_batches, dropoff_batches, strict=True):
        count += (
            pickup.rename({"z_zonekey": "pickup_zone"})
            .join(
                dropoff.rename({"z_zonekey": "dropoff_zone"}),
                on="t_tripkey",
                how="inner",
            )
            .filter(pl.col("pickup_zone") != pl.col("dropoff_zone"))
            .height
        )
    return pl.DataFrame({"cross_zone_trip_count": [count]})


def q12(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q12 (PyCanopy): Rank trip pickups by average distance to their 5 nearest buildings.

    For each pickup, averages the distances to its 5 nearest buildings to produce one row per trip.
    Ordered by that average descending (most isolated pickups first), bounded to the top 100.
    Output columns: t_tripkey, avg_distance_to_5_nearest
    """
    k = 5

    buildings, trip = pl.collect_all(
        [
            pl.scan_parquet(data_paths["building"]).select(
                ["b_buildingkey", "b_boundary"]
            ),
            pl.scan_parquet(data_paths["trip"]).select(["t_tripkey", "t_pickuploc"]),
        ]
    )
    sf = pc.SpatialFrame.from_wkb_polygons(buildings, "b_boundary")

    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    query_df = trip.select("t_tripkey").with_columns(
        pl.Series("qx", qx), pl.Series("qy", qy)
    )
    del trip

    joined = (
        sf.lazy()
        .polygon_knn_join(query_df, "qx", "qy", k=k)
        .select(["t_tripkey", "distance_to_polygon"])
    )

    candidates = []
    for morsel in joined.collect_batched():
        averages = morsel.group_by("t_tripkey").agg(
            avg_distance_to_5_nearest=pl.col("distance_to_polygon").mean()
        )
        candidates.append(
            averages.lazy()
            .sort(["avg_distance_to_5_nearest", "t_tripkey"], descending=[True, False])
            .head(100)
            .collect()
        )
    return (
        pl.concat(candidates, how="vertical", rechunk=False)
        .lazy()
        .sort(["avg_distance_to_5_nearest", "t_tripkey"], descending=[True, False])
        .head(100)
        .collect()
    )

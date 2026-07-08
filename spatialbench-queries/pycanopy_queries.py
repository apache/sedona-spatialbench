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

    trip = pl.read_parquet(data_paths["trip"], columns=["t_tripkey", "t_pickuploc", "t_pickuptime"])
    sf = pc.SpatialFrame.from_wkb_points(trip, "t_pickuploc")
    center_df = pl.DataFrame({"cx": [center[0]], "cy": [center[1]]})
    joined = sf.lazy().within_distance_join(center_df, "cx", "cy", distance=radius).collect()
    return (
        joined.with_columns(
            distance_to_center=((pl.col("_x") - pl.col("cx")) ** 2 + (pl.col("_y") - pl.col("cy")) ** 2).sqrt()
        )
        .select(
            "t_tripkey",
            pl.col("_x").alias("pickup_lon"),
            pl.col("_y").alias("pickup_lat"),
            "t_pickuptime",
            "distance_to_center",
        )
        .sort(["distance_to_center", "t_tripkey"])
    )


def q2(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q2 (PyCanopy): Count trips starting within Coconino County zone."""
    zone = pl.read_parquet(data_paths["zone"], columns=["z_name", "z_boundary"])
    target = zone.filter(pl.col("z_name") == "Coconino County").head(1)
    if target.height == 0:
        return pl.DataFrame({"trip_count_in_coconino_county": [0]})
    poly = shapely.from_wkb(target["z_boundary"].to_numpy())[0]

    trip = pl.read_parquet(data_paths["trip"], columns=["t_pickuploc"])
    sf = pc.SpatialFrame.from_wkb_points(trip, "t_pickuploc")
    idx = sf.engine.points_within_distance_of_polygon(poly, 0.0)
    return pl.DataFrame({"trip_count_in_coconino_county": [len(idx)]})


def q3(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q3 (PyCanopy): Monthly trip stats within ~5km of a 10km bounding box around Sedona."""
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
    cols = ["t_pickuploc", "t_pickuptime", "t_dropofftime", "t_distance", "t_fare"]

    trip = pl.read_parquet(data_paths["trip"], columns=cols)
    sf = pc.SpatialFrame.from_wkb_points(trip, "t_pickuploc")
    filtered = sf.points_within_distance_of_polygon(base_poly, distance)
    filtered = filtered.with_columns(
        pickup_month=pl.col("t_pickuptime").dt.truncate("1mo"),
        duration_seconds=(pl.col("t_dropofftime") - pl.col("t_pickuptime")).dt.total_seconds(),
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

    trip = pl.read_parquet(data_paths["trip"], columns=["t_tripkey", "t_tip", "t_pickuploc"])

    top_keys = (
        trip.select(["t_tripkey", "t_tip"])
        .sort(["t_tip", "t_tripkey"], descending=[True, False])
        .head(top_n)
        .select("t_tripkey")
    )
    top = top_keys.join(trip.select(["t_tripkey", "t_pickuploc"]), on="t_tripkey", how="left")
    qx, qy = pc.wkb_points_to_xy(top["t_pickuploc"])
    query_df = top.select("t_tripkey").with_columns(pl.Series("qx", qx), pl.Series("qy", qy))

    zone = pl.read_parquet(data_paths["zone"], columns=["z_zonekey", "z_name", "z_boundary"])
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

    trip = pl.read_parquet(data_paths["trip"], columns=["t_custkey", "t_dropoffloc", "t_pickuptime"])
    cust = pl.read_parquet(data_paths["customer"], columns=["c_custkey", "c_name"])

    dx, dy = pc.wkb_points_to_xy(trip["t_dropoffloc"])
    t = trip.with_columns(
        pl.Series("dx", dx),
        pl.Series("dy", dy),
        pickup_month=pl.col("t_pickuptime").dt.truncate("1mo"),
    )
    joined = t.join(cust, left_on="t_custkey", right_on="c_custkey", how="inner")
    grouped = (
        joined.group_by(["t_custkey", "c_name", "pickup_month"])
        .agg(trip_count=pl.len(), dxs=pl.col("dx"), dys=pl.col("dy"))
        .filter(pl.col("trip_count") > min_trips)
    )

    areas = pc.Engine.group_convex_hull_areas(grouped["dxs"], grouped["dys"])
    grouped = grouped.with_columns(
        monthly_travel_hull_area=pl.Series("monthly_travel_hull_area", areas, dtype=pl.Float64)
    ).sort(["trip_count", "t_custkey"], descending=[True, False])

    return grouped.select(["t_custkey", "c_name", "pickup_month", "monthly_travel_hull_area"]).rename(
        {"t_custkey": "c_custkey", "c_name": "customer_name"}
    )


def q6(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q6 (PyCanopy): Zone statistics for trips intersecting a bounding box."""
    bbox = (-112.2110, 34.4197, -111.3110, 35.3197)  # min_x, min_y, max_x, max_y
    trip_cols = ["t_pickuploc", "t_totalamount", "t_pickuptime", "t_dropofftime"]

    zone = pl.read_parquet(data_paths["zone"], columns=["z_zonekey", "z_name", "z_boundary"])
    zsf = pc.SpatialFrame.from_wkb_polygons(zone, "z_boundary")
    cand_sf = zsf.range_filter(*bbox)
    if cand_sf.engine.n == 0:
        return pl.DataFrame(
            schema={
                "z_zonekey": pl.Int64,
                "z_name": pl.Utf8,
                "total_pickups": pl.UInt32,
                "avg_distance": pl.Float64,
                "avg_duration": pl.Float64,
            }
        )

    trip = pl.read_parquet(data_paths["trip"], columns=trip_cols)
    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    qdf = trip.select(["t_totalamount", "t_pickuptime", "t_dropofftime"]).with_columns(
        pl.Series("qx", qx),
        pl.Series("qy", qy),
        duration_seconds=(pl.col("t_dropofftime") - pl.col("t_pickuptime")).dt.total_seconds(),
    )

    return (
        cand_sf.lazy()
        .within_join(qdf, "qx", "qy")
        .group_by(["z_zonekey", "z_name"])
        .agg(
            total_pickups=pc.agg.count(),
            avg_distance=pc.agg.mean("t_totalamount"),
            avg_duration=pc.agg.mean("duration_seconds"),
        )
        .sort(["total_pickups", "z_zonekey"], descending=[True, False])
    )


def q7(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q7 (PyCanopy): Detect route detours by comparing reported vs straight-line distance."""
    deg_per_m = 0.000009  # 1 meter ~= 0.000009 degrees

    trip = pl.read_parquet(
        data_paths["trip"], columns=["t_tripkey", "t_distance", "t_pickuploc", "t_dropoffloc"]
    )
    line_m = pc.wkb_point_distance(trip["t_pickuploc"], trip["t_dropoffloc"]) / deg_per_m

    df = trip.select("t_tripkey", "t_distance").with_columns(
        pl.Series("line_distance_m", line_m),
        reported_distance_m=pl.col("t_distance").cast(pl.Float64),
    )
    df = df.with_columns(
        detour_ratio=pl.when(pl.col("line_distance_m") != 0.0)
        .then(pl.col("reported_distance_m") / pl.col("line_distance_m"))
        .otherwise(None)
    )
    return df.select("t_tripkey", "reported_distance_m", "line_distance_m", "detour_ratio").sort(
        ["detour_ratio", "reported_distance_m", "t_tripkey"],
        descending=[True, True, False],
        nulls_last=True,
    )


def q8(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q8 (PyCanopy): Count trip pickups within ~500m of each building."""
    threshold = 0.0045  # degrees (~500m)

    buildings = pl.read_parquet(data_paths["building"], columns=["b_buildingkey", "b_name", "b_boundary"])
    sf = pc.SpatialFrame.from_wkb_polygons(buildings, "b_boundary")

    trip = pl.read_parquet(data_paths["trip"], columns=["t_pickuploc"])
    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    query_df = pl.DataFrame({"qx": qx, "qy": qy})

    return (
        sf.lazy()
        .polygon_within_distance_join(query_df, "qx", "qy", distance=threshold)
        .group_by(["b_buildingkey", "b_name"])
        .agg(nearby_pickup_count=pc.agg.count())
        .sort(["nearby_pickup_count", "b_buildingkey"], descending=[True, False])
    )


def q9(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q9 (PyCanopy): Building conflation via IoU (intersection over union) detection."""
    buildings = pl.read_parquet(data_paths["building"], columns=["b_buildingkey", "b_boundary"])
    sf = pc.SpatialFrame.from_wkb_polygons(buildings, "b_boundary")
    pairs = sf.intersects_pairs(key_col="b_buildingkey")
    if pairs.height == 0:
        return pl.DataFrame(
            schema={
                "building_1": pl.Int64,
                "building_2": pl.Int64,
                "area1": pl.Float64,
                "area2": pl.Float64,
                "overlap_area": pl.Float64,
                "iou": pl.Float64,
            }
        )
    return pairs.select(
        pl.col("b_buildingkey_1").alias("building_1"),
        pl.col("b_buildingkey_2").alias("building_2"),
        pl.col("area_left").alias("area1"),
        pl.col("area_right").alias("area2"),
        "overlap_area",
        "iou",
    ).sort(["iou", "building_1", "building_2"], descending=[True, False, False])


def q10(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q10 (PyCanopy): Per-zone trip statistics, retaining zones with no trips."""
    trip_cols = ["t_pickuploc", "t_pickuptime", "t_dropofftime", "t_distance"]

    zone = pl.read_parquet(data_paths["zone"], columns=["z_zonekey", "z_name", "z_boundary"])
    sf = pc.SpatialFrame.from_wkb_polygons(zone, "z_boundary")

    trip = pl.read_parquet(data_paths["trip"], columns=trip_cols)
    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    qdf = trip.with_columns(
        pl.Series("qx", qx),
        pl.Series("qy", qy),
        duration_seconds=(pl.col("t_dropofftime") - pl.col("t_pickuptime")).dt.total_seconds(),
    ).select(["qx", "qy", "t_distance", "duration_seconds"])

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
    return result.sort(["avg_duration", "z_zonekey"], descending=[True, False], nulls_last=True)


def q11(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q11 (PyCanopy): Count trips that start and end in different zones."""
    trip = pl.read_parquet(data_paths["trip"], columns=["t_tripkey", "t_pickuploc", "t_dropoffloc"])
    zone = pl.read_parquet(data_paths["zone"], columns=["z_zonekey", "z_boundary"])
    sf = pc.SpatialFrame.from_wkb_polygons(zone, "z_boundary")

    px, py = pc.wkb_points_to_xy(trip["t_pickuploc"])
    dx, dy = pc.wkb_points_to_xy(trip["t_dropoffloc"])
    keys = trip.select("t_tripkey")
    pickup_df = keys.with_columns(pl.Series("px", px), pl.Series("py", py))
    dropoff_df = keys.with_columns(pl.Series("dx", dx), pl.Series("dy", dy))

    pickup_batches = (
        sf.lazy().within_join(pickup_df, "px", "py").select(["t_tripkey", "z_zonekey"]).collect_batched()
    )
    dropoff_batches = (
        sf.lazy().within_join(dropoff_df, "dx", "dy").select(["t_tripkey", "z_zonekey"]).collect_batched()
    )

    # Aligned morsels carry the same trips on each side so per-morsel counts sum to the global count
    count = 0
    for pickup, dropoff in zip(pickup_batches, dropoff_batches):
        count += (
            pickup.rename({"z_zonekey": "pickup_zone"})
            .join(dropoff.rename({"z_zonekey": "dropoff_zone"}), on="t_tripkey", how="inner")
            .filter(pl.col("pickup_zone") != pl.col("dropoff_zone"))
            .height
        )
    return pl.DataFrame({"cross_zone_trip_count": [count]})


def q12(data_paths: dict[str, str]) -> pl.DataFrame:
    """Q12 (PyCanopy): The 5 nearest buildings to each trip pickup location."""
    k = 5

    buildings = pl.read_parquet(data_paths["building"], columns=["b_buildingkey", "b_name", "b_boundary"])
    sf = pc.SpatialFrame.from_wkb_polygons(buildings, "b_boundary")

    trip = pl.read_parquet(data_paths["trip"], columns=["t_tripkey", "t_pickuploc"])
    qx, qy = pc.wkb_points_to_xy(trip["t_pickuploc"])
    query_df = trip.select("t_tripkey", "t_pickuploc").with_columns(pl.Series("qx", qx), pl.Series("qy", qy))

    return (
        sf.lazy()
        .polygon_knn_join(query_df, "qx", "qy", k=k, sorted_output=True)
        .select("t_tripkey", "t_pickuploc", "b_buildingkey", "b_name", "distance_to_polygon")
        .collect()
        .rename({"b_name": "building_name", "distance_to_polygon": "distance_to_building"})
    )

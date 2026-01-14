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
from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd

from pandas import DataFrame
from shapely import wkb
from shapely.geometry import LineString, MultiPoint, Point, Polygon


def q1(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q1 (GeoPandas): Trips starting within 50km of Sedona city center."""
    trip_df = pd.read_parquet(data_paths["trip"])[
        ["t_tripkey", "t_pickuploc", "t_pickuptime"]
    ]
    trip_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_pickuploc"], crs="EPSG:4326"
    )
    trip_gdf = gpd.GeoDataFrame(trip_df, geometry="pickup_geom", crs="EPSG:4326")
    trip_gdf["pickup_lon"] = trip_gdf.geometry.x
    trip_gdf["pickup_lat"] = trip_gdf.geometry.y
    center = Point(-111.7610, 34.8697)
    trip_gdf["distance_to_center"] = trip_gdf.geometry.distance(center)
    filtered = trip_gdf[
        trip_gdf["distance_to_center"].notna()
        & (trip_gdf["distance_to_center"] <= 0.45)
        ]
    return filtered.sort_values(  # type: ignore[no-any-return]
        ["distance_to_center", "t_tripkey"], ascending=[True, True]
    )[
        [
            "t_tripkey",
            "pickup_lon",
            "pickup_lat",
            "t_pickuptime",
            "distance_to_center",
        ]
    ].reset_index(drop=True)


def q2(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q2 (GeoPandas): Count trips starting within Coconino County zone.

    Finds the first zone row where z_name == 'Coconino County' and counts trips whose
    pickup point intersects that polygon. Returns single-row DataFrame with
    trip_count_in_coconino_county.
    """
    trip_df = pd.read_parquet(data_paths["trip"])
    zone_df = pd.read_parquet(data_paths["zone"])
    target = zone_df[zone_df["z_name"] == "Coconino County"].head(1)
    if target.empty:
        return pd.DataFrame({"trip_count_in_coconino_county": [0]})
    poly = wkb.loads(target.iloc[0]["z_boundary"])
    trip_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_pickuploc"], crs="EPSG:4326"
    )
    # Ensure intersects is called on a GeoSeries, not a Series
    pickup_geoms = gpd.GeoSeries(trip_df["pickup_geom"], crs="EPSG:4326")
    count = int(pickup_geoms.intersects(poly).sum())
    return pd.DataFrame({"trip_count_in_coconino_county": [count]})


def q3(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q3 (GeoPandas): Monthly trip stats within 15km (10km box + 5km buffer) of Sedona center.

    Implements: filter trips whose pickup location is within 0.045 degrees (~5km) of the 10km bounding
    box polygon (approximating ST_DWithin(pickup_point, polygon, 0.045)). Then aggregates monthly:
      * total_trips   = COUNT(t_tripkey)
      * avg_distance  = AVG(t_distance) (set NaN if column absent)
      * avg_duration  = AVG(t_dropofftime - t_pickuptime) in seconds
      * avg_fare      = AVG(t_fare) (set NaN if column absent)
    Ordered by pickup_month ASC.
    Returns columns: pickup_month, total_trips, avg_distance, avg_duration, avg_fare
    """
    trip_df = pd.read_parquet(data_paths["trip"])

    trip_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_pickuploc"], crs="EPSG:4326"
    )
    trips_gdf = gpd.GeoDataFrame(trip_df, geometry="pickup_geom", crs="EPSG:4326")

    base_poly = Polygon(
        [
            (-111.9060, 34.7347),
            (-111.6160, 34.7347),
            (-111.6160, 35.0047),
            (-111.9060, 35.0047),
            (-111.9060, 34.7347),
        ]
    )

    distances = trips_gdf["pickup_geom"].distance(base_poly)
    mask = distances <= 0.045
    filtered = trips_gdf.loc[mask]

    filtered["_duration_seconds"] = (
            filtered["t_dropofftime"] - filtered["t_pickuptime"]
    ).dt.total_seconds()

    filtered["pickup_month"] = (
        filtered["t_pickuptime"].dt.to_period("M").dt.to_timestamp()
    )

    agg = (
        filtered.groupby("pickup_month", as_index=False)
        .agg(
            total_trips=("t_tripkey", "count"),
            avg_distance=("t_distance", "mean"),
            avg_duration=("_duration_seconds", "mean"),
            avg_fare=("t_fare", "mean"),
        )
        .sort_values("pickup_month")
        .reset_index(drop=True)
    )
    return cast(DataFrame, agg)


def q4(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q4 (GeoPandas): Zone distribution of top 1000 trips by tip amount.

    Steps:
      * Select top 1000 trips ordered by t_tip DESC, t_tripkey ASC.
      * Spatial join (pickup point within zone polygon).
      * Group by z_zonekey, z_name counting trips.
      * Order by trip_count DESC, z_zonekey ASC.
    Returns columns: z_zonekey, z_name, trip_count.
    """
    trip_df = pd.read_parquet(data_paths["trip"])
    if "t_tip" not in trip_df.columns:
        return pd.DataFrame(columns=["z_zonekey", "z_name", "trip_count"])
    top_trips = trip_df.sort_values(
        ["t_tip", "t_tripkey"], ascending=[False, True]
    ).head(1000)
    top_trips["pickup_geom"] = gpd.GeoSeries.from_wkb(
        top_trips["t_pickuploc"], crs="EPSG:4326"
    )
    top_gdf = gpd.GeoDataFrame(top_trips, geometry="pickup_geom", crs="EPSG:4326")
    zone_df = pd.read_parquet(data_paths["zone"])[
        ["z_zonekey", "z_name", "z_boundary"]
    ]
    zone_df["zone_geom"] = gpd.GeoSeries.from_wkb(
        zone_df["z_boundary"], crs="EPSG:4326"
    )
    zones_gdf = gpd.GeoDataFrame(zone_df, geometry="zone_geom", crs="EPSG:4326")[
        ["z_zonekey", "z_name", "zone_geom"]
    ]

    result = (
        gpd.sjoin(top_gdf, zones_gdf, how="inner", predicate="within")
        .groupby(["z_zonekey", "z_name"], as_index=False)
        .size()
        .rename(columns={"size": "trip_count"})
        .sort_values(["trip_count", "z_zonekey"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return result  # type: ignore[no-any-return]


def q5(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q5 (GeoPandas): Monthly travel patterns for repeat customers (convex hull of dropoff points)."""
    trip_df = pd.read_parquet(data_paths["trip"])
    cust_df = pd.read_parquet(data_paths["customer"])
    trip_df["dropoff_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_dropoffloc"], crs="EPSG:4326"
    )
    joined = trip_df.merge(
        cust_df[["c_custkey", "c_name"]],
        left_on="t_custkey",
        right_on="c_custkey",
        how="inner",
    )
    joined["pickup_month"] = (
        joined["t_pickuptime"].dt.to_period("M").dt.to_timestamp()
    )
    grouped = (
        joined.groupby(["c_custkey", "c_name", "pickup_month"], as_index=False)
        .agg(
            trip_count=("t_tripkey", "count"),
            dropoff_points=("dropoff_geom", lambda x: list(x)),
        )
        .loc[lambda d: d["trip_count"] > 5]
    )
    grouped["monthly_travel_hull_area"] = gpd.GeoSeries(
        grouped["dropoff_points"].map(MultiPoint), crs="EPSG:4326"
    ).convex_hull.area

    result = (
        grouped.sort_values(["trip_count", "c_custkey"], ascending=[False, True])[
            ["c_custkey", "c_name", "pickup_month", "monthly_travel_hull_area"]
        ]
        .rename(columns={"c_name": "customer_name"})
        .reset_index(drop=True)
    )
    return result


def q6(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q6 (GeoPandas): Zone statistics for trips within 50km bounding box around Sedona.

    Mirrors original SQL intent:
      * Filter zones fully contained in the provided bounding box polygon.
      * Count trips whose pickup point lies within each zone (inner semantics: zones with 0 pickups excluded).
      * Compute:
          total_pickups = COUNT(t_tripkey)
          avg_distance  = AVG(t_totalamount) (matches original aliasing; falls back to t_distance if needed)
          avg_duration  = AVG(t_dropofftime - t_pickuptime) in seconds
      * Order by total_pickups DESC, z_zonekey ASC.
    Returns DataFrame with columns: z_zonekey, z_name, total_pickups, avg_distance, avg_duration
    """
    trip_df = pd.read_parquet(data_paths["trip"])
    zone_df = pd.read_parquet(data_paths["zone"])

    trip_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_pickuploc"], crs="EPSG:4326"
    )
    pickup_points = gpd.GeoDataFrame(
        trip_df, geometry="pickup_geom", crs="EPSG:4326"
    )

    zone_df["zone_geom"] = gpd.GeoSeries.from_wkb(
        zone_df["z_boundary"], crs="EPSG:4326"
    )
    zones_gdf = gpd.GeoDataFrame(zone_df, geometry="zone_geom", crs="EPSG:4326")[
        ["z_zonekey", "z_name", "zone_geom"]
    ]

    bbox_poly = Polygon(
        [
            (-112.2110, 34.4197),
            (-111.3110, 34.4197),
            (-111.3110, 35.3197),
            (-112.2110, 35.3197),
            (-112.2110, 34.4197),
        ]
    )

    candidate_zones = zones_gdf[
        zones_gdf["zone_geom"].notna()
        & zones_gdf["zone_geom"].intersects(bbox_poly)
        ]

    distance_col = (
        "t_totalamount"
        if "t_totalamount" in trip_df.columns
        else ("t_distance" if "t_distance" in trip_df.columns else None)
    )

    result = (
        gpd.sjoin(pickup_points, candidate_zones, how="inner", predicate="within")
        .assign(
            _duration_seconds=lambda d: (
                    d["t_dropofftime"] - d["t_pickuptime"]
            ).dt.total_seconds(),
            _distance_metric=lambda d: d[distance_col] if distance_col else pd.NA,
        )
        .groupby(["z_zonekey", "z_name"], as_index=False)
        .agg(
            total_pickups=("t_tripkey", "count"),
            avg_distance=("_distance_metric", "mean"),
            avg_duration=("_duration_seconds", "mean"),
        )
        .sort_values(["total_pickups", "z_zonekey"], ascending=[False, True])
        .reset_index(drop=True)
    )
    return result  # type: ignore[no-any-return]


def q7(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q7 (GeoPandas): Detect potential route detours by comparing reported vs geometric distances.

    Mirrors SQL semantics:
      * Join trip with driver and vehicle
      * Filter trips where t_distance > 0
      * reported_distance_m = t_distance (coerced to float)
      * line_distance_m = length of straight line between pickup and dropoff (meters)
      * detour_ratio = (reported_distance_m) / line_distance_m (NULL if line_distance_m==0)
      * Ordered by detour_ratio DESC, reported_distance_m DESC, t_tripkey ASC
    """
    trip_df = pd.read_parquet(data_paths["trip"])
    trip_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_pickuploc"], crs="EPSG:4326"
    )
    trip_df["dropoff_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_dropoffloc"], crs="EPSG:4326"
    )
    trip_df["reported_distance_m"] = trip_df["t_distance"].astype(float)
    pickup_vals = trip_df["pickup_geom"].to_numpy()
    dropoff_vals = trip_df["dropoff_geom"].to_numpy()
    line_lengths = np.fromiter(
        (
            LineString([pg, dg]).length / 0.000009  # 1 meter = 0.000009 degree
            if (pg is not None and dg is not None)
            else np.nan
            for pg, dg in zip(pickup_vals, dropoff_vals, strict=False)
        ),
        dtype=float,
        count=len(trip_df),
    )
    trip_df["line_distance_m"] = line_lengths
    trip_df["detour_ratio"] = np.divide(
        trip_df["reported_distance_m"].to_numpy(dtype=float, copy=False),
        line_lengths,
        out=np.full_like(
            trip_df["reported_distance_m"].to_numpy(dtype=float, copy=False), np.nan
        ),
        where=(line_lengths != 0.0),
    )
    result = (
        trip_df[
            [
                "t_tripkey",
                "reported_distance_m",
                "line_distance_m",
                "detour_ratio",
            ]
        ]
        .sort_values(
            ["detour_ratio", "reported_distance_m", "t_tripkey"],
            ascending=[False, False, True],
            na_position="last",
        )
        .reset_index(drop=True)
    )
    return result


def q8(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q8 (GeoPandas): Count nearby pickups for each building within ~500m."""
    trips_df = pd.read_parquet(data_paths["trip"])
    trips_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trips_df["t_pickuploc"], crs="EPSG:4326"
    )
    pickups_gdf = gpd.GeoDataFrame(
        trips_df, geometry="pickup_geom", crs="EPSG:4326"
    )

    buildings_df = pd.read_parquet(data_paths["building"])
    buildings_df["boundary_geom"] = gpd.GeoSeries.from_wkb(
        buildings_df["b_boundary"], crs="EPSG:4326"
    )
    buildings_gdf = gpd.GeoDataFrame(
        buildings_df, geometry="boundary_geom", crs="EPSG:4326"
    )

    threshold = 0.0045  # degrees (~500m)
    result = (
        buildings_gdf.sjoin(pickups_gdf, predicate="dwithin", distance=threshold)
        .groupby(["b_buildingkey", "b_name"], as_index=False)
        .size()
        .rename(columns={"size": "nearby_pickup_count"})
        .sort_values(
            ["nearby_pickup_count", "b_buildingkey"], ascending=[False, True]
        )
        .reset_index(drop=True)
    )
    return result  # type: ignore[no-any-return]


def q9(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q9 (GeoPandas): Building conflation via IoU (intersection over union) detection.

    Uses spatial self-join (predicate='intersects') to find overlapping (intersecting) building boundary polygons.
    Robust to differing GeoPandas suffix behaviors by detecting column names and falling back to index_right.
    Output columns: building_1, building_2, area1, area2, overlap_area, iou ordered by
    iou DESC, building_1 ASC, building_2 ASC.
    """
    buildings_df = pd.read_parquet(data_paths["building"])
    buildings_df["boundary_geom"] = gpd.GeoSeries.from_wkb(
        buildings_df["b_boundary"], crs="EPSG:4326"
    )
    bdf = gpd.GeoDataFrame(buildings_df, geometry="boundary_geom", crs="EPSG:4326")[
        ["b_buildingkey", "boundary_geom"]
    ].rename(columns={"b_buildingkey": "building_key"})

    pairs = gpd.sjoin(bdf, bdf, how="inner", predicate="intersects")

    left_key_candidates = ["building_key_left", "building_key_1", "building_key"]
    right_key_candidates = ["building_key_right", "building_key_2"]
    left_key_col = next(c for c in left_key_candidates if c in pairs.columns)
    right_key_col = next(
        (c for c in right_key_candidates if c in pairs.columns), None
    )
    if right_key_col is None:
        pairs["_building_key_right_temp"] = bdf.loc[
            pairs["index_right"], "building_key"
        ].to_numpy()
        right_key_col = "_building_key_right_temp"

    pairs = pairs.rename(
        columns={left_key_col: "building_1", right_key_col: "building_2"}
    ).rename_geometry("boundary_geom_1")
    pairs["boundary_geom_2"] = bdf.loc[
        pairs["index_right"], "boundary_geom"
    ].to_numpy()

    # Filter to only building_1 < building_2 (exclude self-pairs)
    pairs = pairs[pairs["building_1"] < pairs["building_2"]]

    # Compute metrics
    boundary_geom_1_gs = gpd.GeoSeries(pairs["boundary_geom_1"], crs=pairs.crs)
    boundary_geom_2_gs = gpd.GeoSeries(pairs["boundary_geom_2"], crs=pairs.crs)
    pairs["area1"] = boundary_geom_1_gs.area
    pairs["area2"] = boundary_geom_2_gs.area
    intersection = boundary_geom_1_gs.intersection(boundary_geom_2_gs)
    pairs["overlap_area"] = intersection.area
    overlap = pairs["overlap_area"].to_numpy(dtype=float, copy=False)
    area1 = pairs["area1"].to_numpy(dtype=float, copy=False)
    area2 = pairs["area2"].to_numpy(dtype=float, copy=False)
    union = area1 + area2 - overlap
    iou = np.divide(overlap, union, out=np.zeros_like(overlap), where=union != 0.0)
    mask_union_zero = (union == 0.0) & (overlap > 0.0)
    if mask_union_zero.any():
        iou[mask_union_zero] = 1.0
    pairs["iou"] = iou
    result = (
        pairs[["building_1", "building_2", "area1", "area2", "overlap_area", "iou"]]
        .sort_values(
            ["iou", "building_1", "building_2"], ascending=[False, True, True]
        )
        .reset_index(drop=True)
    )
    return cast(DataFrame, result)


def q10(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q10 (GeoPandas): Zone stats for trips starting within each zone.

    Produces columns: z_zonekey, pickup_zone (z_name), avg_duration (seconds), avg_distance, num_trips
    Ordered by avg_duration DESC (NULLS last), z_zonekey ASC.
    Zones with zero trips retained (avg_* = NaN, num_trips = 0).
    """
    trip_df = pd.read_parquet(data_paths["trip"])
    zone_df = pd.read_parquet(data_paths["zone"])

    trip_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trip_df["t_pickuploc"], crs="EPSG:4326"
    )
    pickup_points = gpd.GeoDataFrame(
        trip_df, geometry="pickup_geom", crs="EPSG:4326"
    )

    zone_df["zone_geom"] = gpd.GeoSeries.from_wkb(
        zone_df["z_boundary"], crs="EPSG:4326"
    )
    zones_gdf = gpd.GeoDataFrame(zone_df, geometry="zone_geom", crs="EPSG:4326")

    aggregations = {
        "duration_seconds": "mean",
        "t_distance": "mean",
        "t_tripkey": "count",
    }
    result = (
        gpd.sjoin(pickup_points, zones_gdf, how="right", predicate="within")
        .assign(
            duration_seconds=lambda d: (
                    d["t_dropofftime"] - d["t_pickuptime"]
            ).dt.total_seconds()
        )
        .groupby(["z_zonekey", "z_name"], dropna=False)
        .agg(aggregations)
        .rename(
            columns={
                "duration_seconds": "avg_duration",
                "t_distance": "avg_distance",
                "t_tripkey": "num_trips",
            }
        )
        .reset_index()
        .assign(num_trips=lambda d: d["num_trips"].fillna(0).astype(int))
        .sort_values(
            by=["avg_duration", "z_zonekey"],
            ascending=[False, True],
            na_position="last",
        )
        .rename(columns={"z_name": "pickup_zone"})
        .reset_index(drop=True)
    )
    return result  # type: ignore[no-any-return]


def q11(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q11 (GeoPandas): Count trips that cross between different zones.

    Returns a single-row DataFrame with column: cross_zone_trip_count
    """
    trip_df = pd.read_parquet(data_paths["trip"])
    zone_df = pd.read_parquet(data_paths["zone"])

    pickup_df = trip_df[["t_tripkey", "t_pickuploc"]].copy()
    pickup_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        pickup_df["t_pickuploc"], crs="EPSG:4326"
    )
    pickup_points = gpd.GeoDataFrame(
        pickup_df, geometry="pickup_geom", crs="EPSG:4326"
    )

    dropoff_df = trip_df[["t_tripkey", "t_dropoffloc"]].copy()
    dropoff_df["dropoff_geom"] = gpd.GeoSeries.from_wkb(
        dropoff_df["t_dropoffloc"], crs="EPSG:4326"
    )
    dropoff_points = gpd.GeoDataFrame(
        dropoff_df, geometry="dropoff_geom", crs="EPSG:4326"
    )

    zones_pickup = (
        zone_df[["z_zonekey", "z_boundary"]]
        .rename(columns={"z_zonekey": "pickup_zonekey"})
        .copy()
    )
    zones_pickup["zone_geom"] = gpd.GeoSeries.from_wkb(
        zones_pickup["z_boundary"], crs="EPSG:4326"
    )
    zones_gdf = gpd.GeoDataFrame(
        zones_pickup, geometry="zone_geom", crs="EPSG:4326"
    )

    zones_dropoff = (
        zone_df[["z_zonekey", "z_boundary"]]
        .rename(columns={"z_zonekey": "dropoff_zonekey"})
        .copy()
    )
    zones_dropoff["zone_geom"] = gpd.GeoSeries.from_wkb(
        zones_dropoff["z_boundary"], crs="EPSG:4326"
    )
    zones2_gdf = gpd.GeoDataFrame(
        zones_dropoff, geometry="zone_geom", crs="EPSG:4326"
    )

    pickup_join = gpd.sjoin(
        pickup_points,
        zones_gdf,
        how="left",
        predicate="within",
    )
    dropoff_join = gpd.sjoin(
        dropoff_points,
        zones2_gdf,
        how="left",
        predicate="within",
    )

    merged = pickup_join[["t_tripkey", "pickup_zonekey"]].merge(
        dropoff_join[["t_tripkey", "dropoff_zonekey"]], on="t_tripkey", how="inner"
    )

    mask = (
            merged["pickup_zonekey"].notna()
            & merged["dropoff_zonekey"].notna()
            & (merged["pickup_zonekey"] != merged["dropoff_zonekey"])
    )
    count = int(mask.sum())
    return pd.DataFrame({"cross_zone_trip_count": [count]})


def q12(data_paths: dict[str, str]) -> DataFrame:  # type: ignore[override]
    """Q12 (GeoPandas): Find 5 nearest buildings to each trip pickup location (NLJ, memory-efficient).

    Uses a Python loop (nested loop join) to avoid materializing the full cross join.
    Optionally uses STRtree to shortlist candidate buildings for each pickup point.
    For each pickup, computes distances to candidates, selects 5 closest (ties by building key ASC).
    Output columns: t_tripkey, t_pickuploc, b_buildingkey, building_name, distance_to_building
    """
    trips_df = pd.read_parquet(data_paths["trip"])
    buildings_df = pd.read_parquet(data_paths["building"])

    trips_df["pickup_geom"] = gpd.GeoSeries.from_wkb(
        trips_df["t_pickuploc"], crs="EPSG:4326"
    )
    buildings_df["boundary_geom"] = gpd.GeoSeries.from_wkb(
        buildings_df["b_boundary"], crs="EPSG:4326"
    )
    trips_gdf = gpd.GeoDataFrame(trips_df, geometry="pickup_geom", crs="EPSG:4326")
    buildings_gdf = gpd.GeoDataFrame(
        buildings_df, geometry="boundary_geom", crs="EPSG:4326"
    )

    pickup_geoms = trips_gdf["pickup_geom"].to_list()
    building_geoms = buildings_gdf["boundary_geom"].to_list()
    building_keys = buildings_gdf["b_buildingkey"].to_numpy()
    building_names = buildings_gdf["b_name"].to_numpy()

    results = []
    # Since geopandas doesn't support KNN join, we had to choose either a cross join + filter or a NLJ.
    # The cross join would be more pandas-esque, but would require too much memory.
    # The NLJ is arguably methodologically unfair (a hand optimization) but the only way to
    # actually get the query to run.
    for i, pt in enumerate(pickup_geoms):
        dists = [pt.distance(geom) for geom in building_geoms]
        # Sort by distance, then building key
        nearest_idx = np.lexsort((building_keys, dists))[:5]
        for idx in nearest_idx:
            results.append(
                {
                    "t_tripkey": trips_gdf.iloc[i]["t_tripkey"],
                    "t_pickuploc": trips_gdf.iloc[i]["t_pickuploc"],
                    "b_buildingkey": building_keys[idx],
                    "building_name": building_names[idx],
                    "distance_to_building": dists[idx],
                }
            )
    return (
        pd.DataFrame(results)
        .sort_values(
            ["distance_to_building", "b_buildingkey"], ascending=[True, True]
        )
        .reset_index(drop=True)
    )

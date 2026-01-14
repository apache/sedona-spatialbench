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

import polars as pl
import shapely
from polars import DataFrame

import spatial_polars  # NOQA:F401 needed to add spatial namespace to polars dataframes

# for Q12 Spatial polars uses scipy's KDtree for KNN joins. 
# Scipy must be installed for this to work.
# `pip install spatial-polars[knn]`
# which is essentially the same as
# `pip install spatial-polars scipy`

def q1(data_paths: dict[str, str]) -> DataFrame:
    """Q1 (Spatial Polars): Trips starting within 50km of Sedona city center."""
    center_point = shapely.Point(-111.7610, 34.8697)

    return (
        pl.scan_parquet(data_paths["trip"])
        .with_columns(
            pl.col("t_pickuploc").spatial.from_WKB(),
        )
        .filter(
            pl.col("t_pickuploc").spatial.dwithin(center_point, 0.45),
        )
        .select(
            pl.col("t_tripkey"),
            pl.col("t_pickuploc").spatial.get_x().alias("pickup_lon"),
            pl.col("t_pickuploc").spatial.get_y().alias("pickup_lat"),
            pl.col("t_pickuptime"),
            pl.col("t_pickuploc")
            .spatial.distance(center_point)
            .alias("distance_to_center"),
        )
        .sort(
            "distance_to_center",
            "t_tripkey",
        )
        .collect(engine="streaming")
    )


def q2(data_paths: dict[str, str]) -> DataFrame:
    """Q2 (Spatial Polars): Count trips starting within Coconino County zone.

    Finds the first zone row where z_name == 'Coconino County' and counts trips whose
    pickup point intersects that polygon. Returns single-row DataFrame with
    trip_count_in_coconino_county.
    """
    return (
        pl.scan_parquet(data_paths["trip"])
        .with_columns(
            pl.col("t_pickuploc").spatial.from_WKB(),
        )
        .filter(
            pl.col("t_pickuploc").spatial.intersects(
                pl.scan_parquet(data_paths["zone"])
                .with_columns(
                    pl.col("z_boundary").spatial.from_WKB(),
                )
                .filter(
                    pl.col("z_name") == "Coconino County",
                )
                .select(
                    pl.col("z_boundary"),
                )
                .collect(engine="streaming")
                .to_series()
                .spatial.to_shapely_array(),
            ),
        )
        .select(
            pl.len().alias("trip_count_in_coconino_county"),
        )
        .collect(engine="streaming")
    )


def q3(data_paths: dict[str, str]) -> DataFrame:
    """Q3 (Spatial Polars): Monthly trip stats within 15km (10km box + 5km buffer) of Sedona center.

    Implements: filter trips whose pickup location is within 0.045 degrees (~5km) of the 10km bounding
    box polygon (approximating ST_DWithin(pickup_point, polygon, 0.045)). Then aggregates monthly:
      * total_trips   = COUNT(t_tripkey)
      * avg_distance  = AVG(t_distance) (set NaN if column absent)
      * avg_duration  = AVG(t_dropofftime - t_pickuptime) in seconds
      * avg_fare      = AVG(t_fare) (set NaN if column absent)
    Ordered by pickup_month ASC.
    Returns columns: pickup_month, total_trips, avg_distance, avg_duration, avg_fare
    """
    bbox = shapely.Polygon(
        (
            (-111.9060, 34.7347),
            (-111.6160, 34.7347),
            (-111.6160, 35.0047),
            (-111.9060, 35.0047),
            (-111.9060, 34.7347),
        )
    )
    return (
        pl.scan_parquet(data_paths["trip"])
        .with_columns(
            pl.col("t_pickuploc").spatial.from_WKB(),
        )
        .filter(
            pl.col("t_pickuploc").spatial.dwithin(
                bbox,
                0.045,
            ),
        )
        .with_columns(
            pl.col("t_pickuptime").dt.truncate("1mo").alias("pickup_month"),
        )
        .group_by(
            "pickup_month",
        )
        .agg(
            pl.col("t_tripkey").len().alias("total_trips"),
            pl.col("t_distance").mean().alias("avg_distance"),
            (pl.col("t_dropofftime") - pl.col("t_pickuptime"))
            .mean()
            .alias("avg_duration"),
            pl.col("t_fare").mean().alias("avg_fare"),
        )
        .sort(
            "pickup_month",
        )
        .collect(engine="streaming")
    )


def q4(data_paths: dict[str, str]) -> DataFrame:
    """Q4 (Spatial Polars): Zone distribution of top 1000 trips by tip amount.

    Steps:
      * Select top 1000 trips ordered by t_tip DESC, t_tripkey ASC.
      * Spatial join (pickup point within zone polygon).
      * Group by z_zonekey, z_name counting trips.
      * Order by trip_count DESC, z_zonekey ASC.
    Returns columns: z_zonekey, z_name, trip_count.
    """
    return (
        pl.scan_parquet(data_paths["zone"])
        .with_columns(
            pl.col("z_boundary").spatial.from_WKB(),
        )
        .collect(engine="streaming")
        .spatial.join(
            pl.scan_parquet(data_paths["trip"])
            .with_columns(
                pl.col("t_pickuploc").spatial.from_WKB(),
            )
            .collect(engine="streaming")
            .sort(
                pl.col("t_tip"),
                pl.col("t_tripkey"),
                descending=[True, False],
            )
            .head(1000),
            how="inner",
            predicate="intersects",
            left_on="z_boundary",
            right_on="t_pickuploc",
            suffix="_boundary",
        )
        .group_by(
            pl.col("z_zonekey"),
            pl.col("z_name"),
        )
        .agg(
            pl.len().alias("trip_count"),
        )
        .sort(
            pl.col("trip_count"),
            pl.col("z_zonekey"),
            descending=[True, False],
        )
    )


def q5(data_paths: dict[str, str]) -> DataFrame:
    """Q5 (Spatial Polars): Monthly travel patterns for repeat customers (convex hull of dropoff points)."""
    return (
        pl.scan_parquet(data_paths["trip"])
        .with_columns(
            pl.col("t_dropoffloc").spatial.from_WKB(),
        )
        .join(
            pl.scan_parquet(data_paths["customer"]),
            how="inner",
            left_on="t_custkey",
            right_on="c_custkey",
            coalesce=False,
        )
        .group_by(
            pl.col("c_custkey"),
            pl.col("c_name"),
            pl.col("t_pickuptime").dt.truncate("1mo").alias("pickup_month"),
        )
        .agg(
            pl.len().alias("dropoff_count"),
            pl.col("t_dropoffloc"),
        )
        .filter(
            pl.col("dropoff_count") > 5,
        )
        .with_columns(
            pl.col("t_dropoffloc")
            .spatial.to_geometrycollection()
            .spatial.convex_hull()
            .spatial.area()
            .alias("monthly_travel_hull_area"),
        )
        .select(
            pl.col("c_custkey"),
            pl.col("c_name").alias("customer_name"),
            pl.col("pickup_month"),
            pl.col("monthly_travel_hull_area"),
            pl.col("dropoff_count"),
        )
        .sort(
            pl.col("monthly_travel_hull_area"),
            pl.col("c_custkey"),
            descending=[True, False],
        )
        .collect(engine="streaming")
    )


def q6(data_paths: dict[str, str]) -> DataFrame:
    """Q6 (Spatial Polars): Zone statistics for trips within 50km bounding box around Sedona.

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
    aoi = shapely.Polygon(
        [
            (-112.2110, 34.4197),
            (-111.3110, 34.4197),
            (-111.3110, 35.3197),
            (-112.2110, 35.3197),
            (-112.2110, 34.4197),
        ]
    )

    return (
        pl.scan_parquet(data_paths["zone"])
        .with_columns(
            pl.col("z_boundary").spatial.from_WKB(),
        )
        .filter(
            pl.col("z_boundary").spatial.intersects(
                aoi,
            ),
        )
        .collect(engine="streaming")
        .spatial.join(
            pl.scan_parquet(data_paths["trip"])
            .with_columns(
                pl.col("t_pickuploc").spatial.from_WKB(),
            )
            .collect(engine="streaming"),
            predicate="intersects",
            left_on="z_boundary",
            right_on="t_pickuploc",
        )
        .group_by(
            "z_zonekey",
            "z_name",
        )
        .agg(
            pl.col("t_tripkey").len().alias("total_pickups"),
            pl.col("t_distance").mean().alias("avg_distance"),
            (pl.col("t_dropofftime") - pl.col("t_pickuptime"))
            .mean()
            .alias("avg_duration"),
        )
        .sort(
            pl.col("total_pickups"),
            pl.col("z_zonekey"),
            descending=[True, False],
        )
    )


def q7(data_paths: dict[str, str]) -> DataFrame:
    """Q7 (Spatial Polars): Detect potential route detours by comparing reported vs geometric distances.

    Mirrors SQL semantics:
      * Join trip with driver and vehicle
      * Filter trips where t_distance > 0
      * reported_distance_m = t_distance (coerced to float)
      * line_distance_m = length of straight line between pickup and dropoff (meters)
      * detour_ratio = (reported_distance_m) / line_distance_m (NULL if line_distance_m==0)
      * Ordered by detour_ratio DESC, reported_distance_m DESC, t_tripkey ASC
    """
    return (
        pl.scan_parquet(data_paths["trip"])
        .with_columns(
            pl.col("t_pickuploc").spatial.from_WKB(),
            pl.col("t_dropoffloc").spatial.from_WKB(),
        )
        .with_columns(
            (
                pl.struct(("t_pickuploc", "t_dropoffloc")).spatial.distance() * 111111
            ).alias("line_distance_m"),
            pl.col("t_distance").alias("reported_distance_m"),
        )
        .select(
            pl.col("t_tripkey"),
            pl.col("reported_distance_m"),
            pl.col("line_distance_m"),
            (
                pl.col("reported_distance_m")
                / pl.when(
                    pl.col("line_distance_m") == 0,
                )
                .then(None)
                .otherwise(pl.col("line_distance_m"))
            ).alias("detour_ratio"),
        )
        .sort(
            pl.col("detour_ratio"),
            pl.col("reported_distance_m"),
            pl.col("t_tripkey"),
            descending=[True, True, False],
            nulls_last=[True, False, False],
        )
        .collect(engine="streaming")
    )


def q8(data_paths: dict[str, str]) -> DataFrame:
    """Q8 (Spatial Polars): Count nearby pickups for each building within ~500m."""
    return (
        pl.scan_parquet(data_paths["trip"])
        .with_columns(
            pl.col("t_pickuploc").spatial.from_WKB(),
        )
        .collect(engine="streaming")
        .spatial.join(
            pl.scan_parquet(data_paths["building"])
            .with_columns(
                pl.col("b_boundary").spatial.from_WKB(),
            )
            .collect(engine="streaming"),
            left_on="t_pickuploc",
            right_on="b_boundary",
            predicate="dwithin",
            distance=0.0045,
        )
        .group_by(
            pl.col("b_buildingkey"),
            pl.col("b_name"),
        )
        .agg(
            pl.len().alias("nearby_pickup_count"),
        )
        .sort(
            pl.col("nearby_pickup_count"),
            pl.col("b_buildingkey"),
            descending=[True, False],
        )
    )


def q9(data_paths: dict[str, str]) -> DataFrame:
    """Q9 (Spatial Polars): Building conflation via IoU (intersection over union) detection.

    Uses spatial self-join (predicate='intersects') to find overlapping (intersecting) building boundary polygons.
    Robust to differing GeoPandas suffix behaviors by detecting column names and falling back to index_right.
    Output columns: building_1, building_2, area1, area2, overlap_area, iou ordered by
    iou DESC, building_1 ASC, building_2 ASC.
    """
    building_lf = pl.scan_parquet(data_paths["building"])
    b1_df = (
        building_lf.with_columns(
            pl.col("b_boundary").spatial.from_WKB(),
        )
        .select(
            pl.col("b_buildingkey").alias("id"),
            pl.col("b_boundary"),
        )
        .collect(engine="streaming")
    )

    b2_df = (
        building_lf.with_columns(
            pl.col("b_boundary").spatial.from_WKB(),
        )
        .select(
            pl.col("b_buildingkey").alias("id"),
            pl.col("b_boundary"),
        )
        .collect(engine="streaming")
    )

    return (
        b1_df.spatial.join(
            b2_df,
            predicate="intersects",
            on="b_boundary",
            suffix=("_2"),
        )
        .filter(
            pl.col("id") < pl.col("id_2"),
        )
        .select(
            pl.col("id").alias("building_1"),
            pl.col("id_2").alias("building_2"),
            pl.col("b_boundary").spatial.area().alias("area1"),
            pl.col("b_boundary_2").spatial.area().alias("area2"),
            pl.struct(("b_boundary", "b_boundary_2"))
            .spatial.intersection()
            .spatial.area()
            .alias("overlap_area"),
        )
        .with_columns(
            pl.when((pl.col("area1") + pl.col("area2") - pl.col("overlap_area")) == 0)
            .then(1)
            .otherwise(
                pl.col("overlap_area")
                / (pl.col("area1") + pl.col("area2") - pl.col("overlap_area"))
            )
            .alias("iou")
        )
        .sort(
            pl.col("iou"),
            pl.col("building_1"),
            pl.col("building_2"),
            descending=[True, False, False],
        )
    )


def q10(data_paths: dict[str, str]) -> DataFrame:
    """Q10 (Spatial Polars): Zone stats for trips starting within each zone.

    Produces columns: z_zonekey, pickup_zone (z_name), avg_duration (seconds), avg_distance, num_trips
    Ordered by avg_duration DESC (NULLS last), z_zonekey ASC.
    Zones with zero trips retained (avg_* = NaN, num_trips = 0).
    """
    return (
        pl.scan_parquet(data_paths["zone"])
        .with_columns(
            pl.col("z_boundary").spatial.from_WKB(),
        )
        .collect(engine="streaming")
        .spatial.join(
            pl.scan_parquet(data_paths["trip"])
            .with_columns(
                pl.col("t_pickuploc").spatial.from_WKB(),
            )
            .collect(engine="streaming"),
            left_on="z_boundary",
            right_on="t_pickuploc",
            predicate="intersects",
            how="left",
        )
        .group_by(
            pl.col("z_zonekey"),
            pl.col("z_name").alias("pickup_zone"),
        )
        .agg(
            (pl.col("t_dropofftime") - pl.col("t_pickuptime"))
            .mean()
            .alias("avg_duration"),
            pl.col("t_distance").mean().alias("avg_distance"),
            pl.col("t_tripkey").count().alias("num_trips"),
        )
        .sort(
            pl.col("avg_duration"),
            pl.col("z_zonekey"),
            descending=[True, False],
            nulls_last=[True, False],
        )
    )


def q11(data_paths: dict[str, str]) -> DataFrame:
    """Q11 (Spatial Polars): Count trips that cross between different zones.

    Returns a single-row DataFrame with column: cross_zone_trip_count
    """
    trip_df = (
        pl.scan_parquet(data_paths["trip"])
        .with_columns(
            pl.col("t_pickuploc").spatial.from_WKB(),
            pl.col("t_dropoffloc").spatial.from_WKB(),
        )
        .collect(engine="streaming")
    )

    zone_df = (
        pl.scan_parquet(data_paths["zone"])
        .with_columns(
            pl.col("z_boundary").spatial.from_WKB(),
        )
        .collect(engine="streaming")
    )

    return (
        trip_df.spatial.join(
            zone_df,
            left_on="t_pickuploc",
            right_on="z_boundary",
            predicate="intersects",
        )
        .spatial.join(
            zone_df,
            left_on="t_dropoffloc",
            right_on="z_boundary",
            predicate="intersects",
            suffix="_dropoff",
        )
        .filter(
            pl.col("z_zonekey") != pl.col("z_zonekey_dropoff"),
        )
        .select(
            pl.len().alias("cross_zone_trip_count"),
        )
    )


def q12(data_paths: dict[str, str]) -> DataFrame:
    """Q12 (Spatial Polars): Find 5 nearest buildings to each trip pickup location.

    Spatial polars uses scipy's KDtree for KNN joins. Scipy must be installed for this to work.
    `pip install spatial-polars[knn]`

    For each pickup, computes distances to candidates, selects 5 closest (ties by building key ASC).
    Output columns: t_tripkey, t_pickuploc, b_buildingkey, building_name, distance_to_building
    """

    return (
        pl.scan_parquet(data_paths["trip"])
        .select(
            pl.col("t_tripkey"),
            pl.col("t_pickuploc").spatial.from_WKB().alias("pickup_geom"),
        )
        .collect(engine="streaming")
        .spatial.centroid_knn_join(
            pl.scan_parquet(data_paths["building"])
            .select(
                pl.col("b_buildingkey"),
                pl.col("b_name"),
                pl.col("b_boundary").spatial.from_WKB().alias("boundary_geom"),
            )
            .collect(engine="streaming"),
            left_on="pickup_geom",
            right_on="boundary_geom",
            k=5,
        )
        .lazy()
        .select(
            pl.col("t_tripkey"),
            pl.col("pickup_geom").struct.field("wkb_geometry").alias("t_pickuploc"),
            pl.col("b_buildingkey"),
            pl.col("b_name").alias("building_name"),
            pl.struct(("pickup_geom", "boundary_geom"))
            .spatial.distance()
            .alias("distance_to_building"),
        )
        .sort(
            pl.col("t_tripkey"),
            pl.col("distance_to_building"),
            pl.col("b_buildingkey"),
            descending=[False, False, True],
        )
        .collect(engine="streaming")
    )

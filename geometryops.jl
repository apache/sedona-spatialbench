import GeometryOps as GO, GeoInterface as GI, GeoFormatTypes as GFT
import GeoParquet, Parquet2, WellKnownGeometry

import Tables, Dates, Statistics
using DataFrames
# function q1(data_paths::Dict{String, String})

data_paths = Dict{String, String}()
for file in ["trip", "building", "customer", "driver", "vehicle", "zone"]
    data_paths[file] = joinpath(@__DIR__, "$file.parquet")
end

"""
    q1(data_paths::Dict{String, String})::DataFrame

Q1 (GeometryOps): Find trips starting within 50km of Sedona city center.
"""
q1(args...) = q1_naive(args...)

# TODO: this needs a branch-and-bound is within distance algorithm.
function q1_naive(data_paths::Dict{String, String})
    trip_pq = Parquet2.Dataset(data_paths["trip"])#[!, ["t_tripkey", "t_pickuploc", "t_pickuptime"]]
    trip_df = DataFrame(trip_pq; copycols = false)[!, ["t_tripkey", "t_pickuploc", "t_pickuptime"]]
    trip_df.geometry = GO.tuples(GFT.WellKnownBinary.((GFT.Geom(),), trip_df.t_pickuploc); crs = GFT.EPSG(4326))
    metadata!(trip_df, GI.GEOINTERFACE_CRS_KEY, GFT.EPSG(4326))
    metadata!(trip_df, GI.GEOINTERFACE_GEOMETRYCOLUMNS_KEY, (:geometry,))

    trip_df.pickup_lon = GI.x.(trip_df.geometry)
    trip_df.pickup_lat = GI.y.(trip_df.geometry)
    trip_df.distance_to_center = GO.distance.(trip_df.geometry, ((-111.7610, 34.8697),))

    filtered = filter(:distance_to_center => x -> isfinite(x) && x <= 0.45, trip_df)
    return sort!(filtered, [:distance_to_center, :t_tripkey])[!, ["t_tripkey", "pickup_lon", "pickup_lat", "t_pickuptime", "distance_to_center"]]
end


"""
    q2(data_paths::Dict{String, String})::DataFrame

Q2 (GeometryOps): Count trips starting within Coconino County zone.

Finds the first zone row where z_name == 'Coconino County' and counts trips whose
pickup point intersects that polygon. Returns single-row DataFrame with
trip_count_in_coconino_county.

TODO:
- [ ] Implement tree accelerated point-in-polygon in GeometryOps, preferably with a prepared,
      NaturallyIndexedRing implementation
"""
q2(args...) = q2_naive(args...)
# TODO: this needs tree accelerated point-in-polygon - probably requires 
# fleshing out the Prepared interface.
function q2_naive(data_paths::Dict{String, String})
    trip_pq = Parquet2.Dataset(data_paths["trip"])#[!, ["t_tripkey", "t_pickuploc", "t_pickuptime"]]
    trip_df = DataFrame(trip_pq; copycols = false)[!, ["t_tripkey", "t_pickuploc", "t_pickuptime"]]
    trip_df.geometry = GO.tuples(GFT.WellKnownBinary.((GFT.Geom(),), trip_df.t_pickuploc); crs = GFT.EPSG(4326))

    zone_df = DataFrame(Parquet2.Dataset(data_paths["zone"]); copycols = false)
    target_idx = findfirst(==("Coconino County"), zone_df.z_name)
    if isnothing(target_idx)
        return DataFrame(trip_count_in_coconino_county = [0])
    end
    target_poly = GFT.WellKnownBinary(GFT.Geom(), zone_df.z_boundary[target_idx]) |> GO.tuples

    trip_loc_tree = GO.STRtree(trip_df.geometry)
    target_extent = GI.extent(target_poly)

    count = 0
    GO.SpatialTreeInterface.depth_first_search(GO.intersects(target_extent), trip_loc_tree) do i
        if GO.intersects(trip_df.geometry[i], target_poly)
            count += 1
        end
    end

    return DataFrame(trip_count_in_coconino_county = [count])
end


"""
    q3(data_paths::Dict{String, String})::DataFrame

Q3 (GeometryOps): Monthly trip stats within 15km (10km box + 5km buffer) of Sedona center.

"""
q3(args...) = q3_naive(args...)
# TODO: this needs a branch-and-bound is within distance algorithm.
function q3_naive(data_paths::Dict{String, String})
    trip_pq = Parquet2.Dataset(data_paths["trip"])#[!, ["t_tripkey", "t_pickuploc", "t_pickuptime"]]
    trip_df = DataFrame(trip_pq; copycols = false)
    trip_df.geometry = GO.tuples(GFT.WellKnownBinary.((GFT.Geom(),), trip_df.t_pickuploc); crs = GFT.EPSG(4326))

    base_poly = GI.Polygon(
        [GI.LinearRing([
            (-111.9060, 34.7347),
            (-111.6160, 34.7347),
            (-111.6160, 35.0047),
            (-111.9060, 35.0047),
            (-111.9060, 34.7347),
        ])]
    )

    trip_df.distance = GO.distance.(trip_df.geometry, (base_poly,))
    
    filtered = filter(:distance => <=(0.045), trip_df)

    filtered.duration_seconds = (filtered.t_dropofftime - filtered.t_pickuptime)
    filtered.pickup_month = (Dates.month.(filtered.t_pickuptime))
    
    grouped = groupby(filtered, :pickup_month)
    agg = combine(grouped, :t_tripkey => length => :total_trips, :distance => Statistics.mean => :avg_distance, :duration_seconds => Statistics.mean => :avg_duration, :t_fare => Statistics.mean => :avg_fare)
    sort!(agg, :pickup_month)
    return agg
end

"""
    q4(data_paths::Dict{String, String})::DataFrame

Q4 (GeometryOps): Zone distribution of top 1000 trips by tip amount.
"""
q4(args...) = q4_naive(args...)

function q4_naive(data_paths::Dict{String, String})
    # Algorithm to do coverage like things
    # Do a dual query
    # Store the result in a vector where the index is of the point and the value 
    # is the index of the zone
    # and have a sentinel value (0) for no zone yet
    # if the point has already been assigned a zone, but intersection returns
    # valid for another zone, then update the value to the new zone
    # IF AND ONLY IF the new zone is less than the current zone
    # OR: pseudorandom selection
end

"""
    q5(data_paths::Dict{String, String})::DataFrame

Q5 (GeometryOps): Monthly travel patterns for repeat customers (convex hull of dropoff locations)
"""
q5(args...) = q5_naive(args...)

function q5_naive(data_paths::Dict{String, String})
    
end
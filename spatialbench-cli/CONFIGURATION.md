# SpatialBench Configuration

SpatialBench configuration allows you to customize spatial data generation for your benchmark workloads. The spatial geometry generation is powered by the Spider module, which creates Points, Boxes, and Polygons using deterministic random distributions.

Spider is designed for benchmark reproducibility:
- Generates millions of geometries per second.
- Uses seeds for deterministic output.

Reference: [SpiderWeb: A Spatial Data Generator on the Web](https://dl.acm.org/doi/10.1145/3397536.3422351) by Katiyar et al., SIGSPATIAL 2020.

## Supported Distribution Types

| Type         | Description                                                  | Implementation Details                                                                                                                                                                                             |
|--------------|--------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `UNIFORM`    | Uniformly distributed points in `[0,1]²`                     | Uses `rand_unit()` to generate independent random X and Y coordinates. Each coordinate is a uniform random value between 0.0 and 1.0.                                                                              |
| `NORMAL`     | 2D Gaussian distribution with configurable `mu` and `sigma`  | Uses Box-Muller transform to generate normal distributions. Both X and Y coordinates use the same `mu` and `sigma` parameters. Values are clamped to [0,1]².                                                       |
| `DIAGONAL`   | Points clustered along a diagonal                            | With probability `percentage`, generates points exactly on y=x diagonal. Otherwise, generates points with normal noise around the diagonal using `buffer` as standard deviation.                                   |
| `BIT`        | Points in a grid with `2^digits` resolution                  | Uses recursive binary subdivision. Each bit position has `probability` chance of being set. Creates a deterministic grid pattern with resolution 2^digits × 2^digits.                                              |
| `SIERPINSKI` | Fractal pattern using Sierpinski triangle                    | Uses chaos game algorithm with 10 iterations. Randomly moves toward one of three triangle vertices (0,0), (1,0), or (0.5,√3/2). Creates fractal-like clustering patterns.                                          |
| `THOMAS`     | Gaussian Neyman–Scott cluster process                        | Defines parent centers, each spawning offspring points with Gaussian spread. Parent weights follow a configurable Pareto distribution.                                                                             |
| `HIERTHOMAS` | Hierarchical Thomas process                                  | First selects a city (Pareto-weighted), then a subcluster within the city (Pareto-weighted), and finally generates a point with Gaussian jitter around the subcluster. Models realistic urban/suburban clustering. |

![image.png](../images/spatial_distributions.png)

## Geometry Types

| Type      | Description | Implementation Details |
|-----------|-------------|------------------------|
| `point`   | Single coordinate point | Direct output of generated coordinates after affine transform. |
| `box`     | Rectangular polygon | Creates a rectangle centered on generated coordinates. Width and height are randomized between 0 and the configured `width`/`height` values. |
| `polygon` | Regular polygon | Creates a polygon with 3 to `maxseg` sides, centered on generated coordinates with radius `polysize`. Number of sides is randomized. |

## Using Configuration in the CLI

```bash
spatialbench-cli -s 1 --tables trip,building --config spatialbench-config.yaml
```

If --config is omitted, SpatialBench will try a local default and then fall back to built-ins (see [Configuration Resolution & Logging](#configuration-resolution--logging)).

## Expected Config File Structure

At the top level, the YAML may define:

```yaml
trip:      # (optional) Config for Trip pickup points
building:  # (optional) Config for Building polygons
```

Each entry must conform to the configuration schema:

```yaml
<name>:
  dist_type: <string>        # Distribution algorithm: uniform | normal | diagonal | bit | sierpinski
  geom_type: <string>        # Geometry type: point | box | polygon
  dim: <int>                 # Dimensions (always 2 for 2D spatial data)
  seed: <int>                # Random seed for deterministic generation
  width: <float>             # Box width (used only when geom_type = box)
  height: <float>            # Box height (used only when geom_type = box)
  maxseg: <int>              # Maximum polygon segments (used only when geom_type = polygon)
  polysize: <float>          # Polygon radius/size (used only when geom_type = polygon)
  params:                    # Distribution-specific parameters
    type: <string>           # Parameter type: none | normal | diagonal | bit
    ...                      # Additional fields depend on type (see table below)
```

### Configuration Field Details

| Field | Type | Required | Description                                                                                                                                  |
|-------|------|----------|----------------------------------------------------------------------------------------------------------------------------------------------|
| `dist_type` | string | Yes | **Distribution Algorithm**: Controls how coordinates are generated in the unit square [0,1]² before applying affine transforms.              |
| `geom_type` | string | Yes | **Geometry Type**: Determines the final spatial geometry output format.                                                                      |
| `dim` | int | Yes | **Dimensions**: Always 2 for 2D spatial data. Controls the dimensionality of generated coordinates.                                          |
| `seed` | int | Yes | **Random Seed**: Ensures reproducible generation. Each record uses a deterministic hash of this seed combined with the record index.         |
| `width` | float | Yes | **Box Width**: Maximum width of generated boxes (in unit square coordinates). Actual width is randomized between 0 and this value.           |
| `height` | float | Yes | **Box Height**: Maximum height of generated boxes (in unit square coordinates). Actual height is randomized between 0 and this value.        |
| `maxseg` | int | Yes | **Max Polygon Segments**: Maximum number of sides for generated polygons. Minimum is 3, actual count is randomized between 3 and this value. |
| `polysize` | float | Yes | **Polygon Size**: Radius of generated polygons from their center point (in unit square coordinates).                                         |
| `params` | object | Yes | **Distribution Parameters**: Specific parameters for the chosen distribution type.                                                           |

## Supported Distribution Parameters

| Variant      | Field                  | Type   | Description                                                                                                                                        |
|--------------|------------------------|--------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `None`       | `--`                   | `--`   | **No Parameters**: Used for Uniform and Sierpinski distributions that don't require additional configuration.                                      |
| `Normal`     | `mu`                   | float  | **Mean**: Center point of the 2D Gaussian distribution in [0,1]². Applied to both X and Y coordinates.                                             |
|              | `sigma`                | float  | **Standard Deviation**: Spread of the 2D Gaussian distribution. Controls how clustered the points are around the mean.                             |
| `Diagonal`   | `percentage`           | float  | **Diagonal Percentage**: Fraction of points (0.0–1.0) that lie exactly on the diagonal line y=x.                                                   |
|              | `buffer`               | float  | **Buffer Width**: Standard deviation for the normal distribution used to generate noise around the diagonal for non-diagonal points.               |
| `Bit`        | `probability`          | float  | **Bit Probability**: Probability (0.0–1.0) of setting each bit in the recursive binary subdivision. Controls the density of the grid pattern.      |
|              | `digits`               | int    | **Bit Digits**: Number of bits used in the recursive subdivision. Creates a 2^digits × 2^digits grid resolution. Higher values create finer grids. |
| `Thomas`     | `parents`              | int    | **Number of Parent Clusters**: Top-level centers of activity (hotspots).                                                                           |
|              | `mean_offspring`       | float  | **Mean Offspring**: Global scaling factor for density; influences relative number of points per parent.                                            |
|              | `sigma`                | float  | **Cluster StdDev**: Spread of points around each parent center in unit coordinates. Smaller = tighter clusters.                                    |
|              | `pareto_alpha`         | float  | **Pareto Shape (α)**: Tail parameter. Smaller values (≈1.0–1.5) → heavier skew in cluster sizes.                                                   |
|              | `pareto_xm`            | float  | **Pareto Scale (xm)**: Minimum offspring weight per parent. Larger values raise the floor of cluster sizes.                                        |
| `HierThomas` | `cities`               | int    | **Number of Cities**: Top-level clusters representing major regions of activity.                                                                   |
|              | `sub_mean`             | float  | **Subcluster Mean**: Average number of subclusters per city (normally distributed).                                                                |
|              | `sub_sd`               | float  | **Subcluster StdDev**: Variability in number of subclusters per city.                                                                              |
|              | `sub_min`              | int    | **Minimum Subclusters**: Lower bound for subclusters per city.                                                                                     |
|              | `sub_max`              | int    | **Maximum Subclusters**: Upper bound for subclusters per city.                                                                                     |
|              | `sigma_city`           | float  | **City Spread**: StdDev of subcluster centers around the city center. Larger = more spread-out neighborhoods.                                      |
|              | `sigma_sub`            | float  | **Subcluster Spread**: StdDev of final points around the subcluster center. Smaller = tighter hotspots.                                            |
|              | `pareto_alpha_city`    | float  | **City Pareto Shape (α)**: Controls skew in city sizes. Smaller values → some cities dominate.                                                     |
|              | `pareto_xm_city`       | float  | **City Pareto Scale (xm)**: Minimum weight per city.                                                                                               |
|              | `pareto_alpha_sub`     | float  | **Subcluster Pareto Shape (α)**: Controls skew in subcluster sizes within each city.                                                               |
|              | `pareto_xm_sub`        | float  | **Subcluster Pareto Scale (xm)**: Minimum weight per subcluster.                                                                                   |

## Default Configs

The repository includes a ready-to-use default file:
[`spatialbench-config.yml`](../spatialbench-config.yml).

These defaults are automatically used if no `--config` is passed and the file exists in the current working directory.

## Deterministic Generation

SpatialBench ensures reproducible generation through deterministic seeding:

- **Global Seed**: The `seed` field in configuration provides the base for all random generation
- **Record-Specific Seeds**: Each record uses a deterministic hash combining the global seed with the record index
- **Hash Algorithm**: Uses a SplitMix64-like algorithm for fast, high-quality deterministic hashing
- **Reproducibility**: Same seed + same record index always produces identical output

This allows for:
- Exact reproduction of datasets across different runs
- Parallel generation of different parts that combine correctly
- Consistent benchmark results for performance testing

## Configuration Resolution & Logging

When SpatialBench starts, it resolves configuration in this order:

1. Explicit config: If --config <path> is provided, that file is used.
2. Local default: If no flag is provided, SpatialBench looks for ./spatialbench-config.yml in the current directory.
3. Built-ins: If neither is found, it uses compiled defaults from the built-in configuration.

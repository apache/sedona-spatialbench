import os

import numpy as np
import rasterio
from rasterio.transform import from_origin


def create_multiband_landsat_like_cog(
    filename: str,
    width: int = 512,
    height: int = 512,
    res: int = 30,
    crs: str = "EPSG:4326"
):
    """
    Create a synthetic multi-band Cloud-Optimized GeoTIFF (COG) resembling Landsat data.
    Includes: Coastal, Blue, Green, Red, NIR, SWIR1, SWIR2, Thermal1, Thermal2

    Band Order:
    1. Coastal (B1)
    2. Blue (B2)
    3. Green (B3)
    4. Red (B4)
    5. NIR (B5)
    6. SWIR1 (B6)
    7. SWIR2 (B7)
    8. Thermal IR 1 (B10)
    9. Thermal IR 2 (B11)
    """

    # Simulate realistic value ranges for each band
    band_ranges = {
        "Coastal": (0.05, 0.2),
        "Blue": (0.05, 0.25),
        "Green": (0.1, 0.3),
        "Red": (0.1, 0.4),
        "NIR": (0.2, 0.6),
        "SWIR1": (0.15, 0.5),
        "SWIR2": (0.2, 0.55),
        "Thermal1": (290, 320),  # Kelvin
        "Thermal2": (290, 320)
    }

    transform = from_origin(100.0, 40.0, res, res)  # top-left corner and pixel size

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": len(band_ranges),
        "dtype": "float32",
        "crs": crs,
        "transform": transform,
        "tiled": True,
        "blockxsize": 512,
        "blockysize": 512,
        "compress": "DEFLATE"
    }

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with rasterio.open(filename, "w", **profile) as dst:
        for idx, (band_name, (low, high)) in enumerate(band_ranges.items(), start=1):
            band_data = np.random.uniform(low, high, (height, width)).astype("float32")
            dst.write(band_data, idx)
            dst.set_band_description(idx, band_name)

    print(f"Saved: {filename}")

# Example usage:
create_multiband_landsat_like_cog("output/synthetic_landsat_multiband.tif")
"""
Explains the tile size calculation and latitude effects for EE downloads.
Run this script to understand the math behind the tiling system.

Empirically determined values (Feb 2026):
- EE limit: exactly 50,331,648 bytes
- EE uses ~5.02 bytes/band (not 4.0) due to GeoTIFF overhead
- At 196 bands: 226px works, 227px fails
"""
import math

print("=== EE Download Limit (Empirically Determined) ===")
print()

EE_DOWNLOAD_LIMIT_BYTES = 50_331_648  # Exact limit from EE error messages
EE_BYTES_PER_BAND = 5.02  # Measured: includes GeoTIFF structure overhead
EE_SAFETY_MARGIN = 0.95  # 5% safety margin
DEFAULT_NUM_BANDS = 240  # Conservative for future growth

print(
    f"EE Limit: {EE_DOWNLOAD_LIMIT_BYTES:,} bytes ({EE_DOWNLOAD_LIMIT_BYTES/1e6:.1f}MB)"
)
print(f"Bytes per band: {EE_BYTES_PER_BAND} (Float32 + GeoTIFF overhead)")
print(f"Safety margin: {EE_SAFETY_MARGIN*100:.0f}%")
print(f"Default bands: {DEFAULT_NUM_BANDS}")
print()

scale_meters = 10

for num_bands in [196, 210, 240]:
    max_bytes = EE_DOWNLOAD_LIMIT_BYTES * EE_SAFETY_MARGIN
    bytes_per_pixel = num_bands * EE_BYTES_PER_BAND
    max_pixels_total = max_bytes / bytes_per_pixel
    max_pixels_per_side = int(math.sqrt(max_pixels_total))
    max_meters = max_pixels_per_side * scale_meters
    max_km = max_meters / 1000

    print(
        f"{num_bands} bands: {max_pixels_per_side}px × {max_pixels_per_side}px = {max_km:.2f}km × {max_km:.2f}km"
    )

print()
print("=== Latitude-Aware Tile Sizing ===")
print()
print("With latitude adjustment, tile size in degrees varies:")
print("  - Latitude (height): Always ~0.021° (~2.33km)")
print("  - Longitude (width): 0.021° / cos(lat) - LARGER at high latitudes")
print()

for lat in [0, 15, 30, 45, 60, 75]:
    cos_lat = max(math.cos(math.radians(lat)), 0.1)
    max_lat_deg = max_meters / 111_000
    max_lon_deg = max_meters / (111_000 * cos_lat)

    # Actual tile size in km
    tile_km_lon = max_lon_deg * 111 * cos_lat  # Should be ~2.33km
    tile_km_lat = max_lat_deg * 111

    print(
        f"  Lat {lat:2d}°: lon {max_lon_deg:.4f}° × lat {max_lat_deg:.4f}° = {tile_km_lon:.2f}km × {tile_km_lat:.2f}km"
    )

print()
print("=== Tile Count Comparison (for 10km × 10km area) ===")
print()
print("          Old (fixed)    New (lat-aware)")

for lat in [0, 30, 45, 60]:
    # Old method: fixed 0.021° for both dimensions
    old_tile_deg = max_meters / 111_000
    old_cols = math.ceil(0.09 / old_tile_deg)  # ~10km in degrees
    old_rows = math.ceil(0.09 / old_tile_deg)
    old_tiles = old_cols * old_rows

    # New method: latitude-aware
    cos_lat = max(math.cos(math.radians(lat)), 0.1)
    new_lon_deg = max_meters / (111_000 * cos_lat)
    new_lat_deg = max_meters / 111_000

    # 10km in degrees at this latitude
    lon_10km_deg = 10 / (111 * cos_lat)
    lat_10km_deg = 10 / 111

    new_cols = math.ceil(lon_10km_deg / new_lon_deg)
    new_rows = math.ceil(lat_10km_deg / new_lat_deg)
    new_tiles = new_cols * new_rows

    savings = ((old_tiles - new_tiles) / old_tiles) * 100 if old_tiles > 0 else 0
    print(
        f"  Lat {lat:2d}°:   {old_tiles:3d} tiles       {new_tiles:3d} tiles    ({savings:+.0f}% change)"
    )

print()
print("=== Summary ===")
print("Latitude-aware tiling gives SAME tile count at all latitudes")
print("because tiles are always ~2.33km × ~2.33km in real-world distance.")
print("Old method created unnecessarily small tiles at high latitudes.")

"""Compare COG (area-multiplied) vs live image outputs. Fresh random features in Yamoussoukro."""
import sys, os, time, random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import ee

try:
    ee.Reset()
except Exception:
    pass
project = os.environ.get("PROJECT", None)
ee.Initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")

import geopandas as gpd
import pandas as pd
import tempfile
from shapely.geometry import Polygon

COG_GCS = "gs://whisp_bucket/whisp_cogs/gaul1_1210_i16_noyearly_10m_20260223.tif"
AREA_SCALE_FACTOR = 10  # Area band = pixelArea_ha * 10 (Int16)
SKIP_BANDS = ["Area", "admin_code"]  # Don't multiply these by area

# Yamoussoukro bbox
YAMA_BBOX = {"min_lon": -5.45, "max_lon": -5.15, "min_lat": 6.65, "max_lat": 7.05}


def generate_random_polygons(n, seed):
    """Generate n random small polygons (~4-10ha) within Yamoussoukro bbox."""
    rng = random.Random(seed)
    features = []
    for _ in range(n):
        cx = rng.uniform(YAMA_BBOX["min_lon"] + 0.01, YAMA_BBOX["max_lon"] - 0.01)
        cy = rng.uniform(YAMA_BBOX["min_lat"] + 0.01, YAMA_BBOX["max_lat"] - 0.01)
        dx = rng.uniform(0.002, 0.003)
        dy = rng.uniform(0.002, 0.003)
        poly = Polygon(
            [
                (cx - dx, cy - dy),
                (cx + dx, cy - dy),
                (cx + dx, cy + dy),
                (cx - dx, cy + dy),
                (cx - dx, cy - dy),
            ]
        )
        features.append(poly)
    return gpd.GeoDataFrame(geometry=features, crs="EPSG:4326")


def cog_to_hectares_image(cog_gcs_path):
    """
    Convert int16 COG image to hectare-equivalent image matching live pipeline.

    For each data band: multiply binary 0/1 by (Area_band / AREA_SCALE_FACTOR)
    to get area in hectares per pixel. Then reduceRegions(sum) gives hectares.

    Skip: Area (already the area), admin_code (categorical integer).
    """
    raw = ee.Image.loadGeoTIFF(cog_gcs_path)
    band_names = raw.bandNames()

    # Area band in true hectares (divide by scale factor)
    area_ha = raw.select("Area").divide(AREA_SCALE_FACTOR).toFloat()

    # Build list of converted bands
    converted = [area_ha]  # Area band stays as-is (now in true ha)

    # Get all band names except Area
    other_bands = band_names.remove("Area")

    def multiply_band(band_name):
        band_name = ee.String(band_name)
        band = raw.select(band_name).toFloat()
        # Check if this band should be skipped (admin_code)
        is_admin = band_name.equals("admin_code")
        # If admin: keep as-is. Otherwise: multiply by area_ha
        return ee.Algorithms.If(
            is_admin, band, band.multiply(area_ha).rename(band_name)
        )

    # Map over other bands
    other_converted = other_bands.map(multiply_band)

    # Build final image by adding each converted band
    # We need to use iterate to build up the image
    def add_band(img_to_add, accumulator):
        return ee.Image(accumulator).addBands(ee.Image(img_to_add))

    result = other_converted.iterate(add_band, area_ha)
    return ee.Image(result)


# --- Generate fresh features ---
ts = int(time.time())
tmp_dir = tempfile.gettempdir()
gj_shared = os.path.join(tmp_dir, "compare_shared.geojson")
gdf_shared = generate_random_polygons(250, seed=ts + 55555)
gdf_shared.to_file(gj_shared, driver="GeoJSON")
print(f"Shared set: 250 fresh polygons (seed={ts + 55555})")
print()

# --- Build images ---
print("=== Building images ===")
print("Loading COG and converting to hectares...")
t0 = time.time()
cog_ha_img = cog_to_hectares_image(COG_GCS)
cog_ha_bands = cog_ha_img.bandNames().getInfo()
print(f"  COG-ha bands: {len(cog_ha_bands)}, first 5: {cog_ha_bands[:5]}")
print(f"  Build time: {time.time() - t0:.1f}s")

print("Loading live image (non-yearly)...")
t0 = time.time()
import io
from contextlib import redirect_stdout
from openforis_whisp.datasets import combine_datasets

with redirect_stdout(io.StringIO()):
    live_img = combine_datasets(exclude_yearly=True)
live_bands = live_img.bandNames().getInfo()
print(f"  Live bands: {len(live_bands)}, first 5: {live_bands[:5]}")
print(f"  Build time: {time.time() - t0:.1f}s")
print()

# --- Run stats ---
print("=== Running stats (shared 10 features) ===")
from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

print("COG-ha image stats...")
t0 = time.time()
try:
    df_cog = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=gj_shared,
        mode="concurrent",
        whisp_image=cog_ha_img,
    )
    print(f"  Shape: {df_cog.shape}, Time: {time.time() - t0:.1f}s")
except Exception as e:
    print(f"  FAILED: {e}")
    import traceback

    traceback.print_exc()
    df_cog = None

print()
print("Live image stats...")
t0 = time.time()
try:
    df_live = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=gj_shared,
        mode="concurrent",
        whisp_image=live_img,
    )
    print(f"  Shape: {df_live.shape}, Time: {time.time() - t0:.1f}s")
except Exception as e:
    print(f"  FAILED: {e}")
    df_live = None

# --- Compare values ---
if df_cog is not None and df_live is not None:
    print()
    print("=" * 70)
    print("=== VALUE COMPARISON: COG-ha vs Live ===")
    print("=" * 70)

    common_cols = [c for c in df_cog.columns if c in df_live.columns]
    numeric_common = [
        c
        for c in common_cols
        if pd.api.types.is_numeric_dtype(df_cog[c])
        and pd.api.types.is_numeric_dtype(df_live[c])
    ]

    # Skip metadata columns for data comparison
    skip_meta = {"external_id", "Centroid_lon", "Centroid_lat", "plotId"}
    data_cols = [c for c in numeric_common if c not in skip_meta]

    print(
        f"\n  Comparing {len(data_cols)} data columns across all {len(df_cog)} features"
    )
    print()

    # Per-column comparison across ALL features
    matches = 0
    mismatches = 0
    close_matches = 0  # within 5%
    print(
        f"  {'Column':<40s}  {'COG-ha (mean)':>14s}  {'Live (mean)':>14s}  {'Diff%':>8s}  Status"
    )
    print(f"  {'-'*40}  {'-'*14}  {'-'*14}  {'-'*8}  {'-'*10}")
    for col in data_cols:
        v_cog_mean = df_cog[col].mean()
        v_live_mean = df_live[col].mean()
        try:
            vc = float(v_cog_mean)
            vl = float(v_live_mean)
            if abs(vl) < 0.001 and abs(vc) < 0.001:
                status = "MATCH"
                pct = 0.0
                matches += 1
            else:
                denom = max(abs(vl), 0.001)
                pct = abs(vc - vl) / denom * 100
                if pct < 1.0:
                    status = "MATCH"
                    matches += 1
                elif pct < 5.0:
                    status = "CLOSE"
                    close_matches += 1
                else:
                    status = "MISMATCH"
                    mismatches += 1
        except (TypeError, ValueError):
            pct = float("nan")
            status = "??"
        print(
            f"  {col:<40s}  {str(round(vc, 3)):>14s}  {str(round(vl, 3)):>14s}  {pct:>7.1f}%  {status}"
        )

    print()
    print(
        f"  Summary: {matches} match (<1%), {close_matches} close (1-5%), {mismatches} mismatch (>5%)"
    )
    print()

    # Show first feature detail for key columns
    key_cols = [
        "Area",
        "GFC_TC_2020",
        "Forest_FDaP",
        "ESA_TC_2020",
        "Cocoa_FDaP",
        "Cocoa_ETH",
        "In_waterbody",
    ]
    available_key = [
        c for c in key_cols if c in df_cog.columns and c in df_live.columns
    ]
    if available_key:
        print("  First feature detail (key columns):")
        for col in available_key:
            vc = df_cog[col].iloc[0]
            vl = df_live[col].iloc[0]
            print(f"    {col:<30s}: COG-ha={str(vc):>12s}  Live={str(vl):>12s}")

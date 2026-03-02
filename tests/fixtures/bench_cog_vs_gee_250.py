"""Benchmark: GEE concurrent with COG-as-image vs live image (non-yearly).

Generates 250 fresh random polygons per benchmark within Yamoussoukro bbox
to ensure zero GEE caching.
"""
import sys, os, time, random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import ee

# Init with high-volume endpoint (needed for concurrent mode)
try:
    ee.Reset()
except Exception:
    pass
project = os.environ.get("PROJECT", None)
ee.Initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")

import geopandas as gpd
import tempfile
from shapely.geometry import Polygon

COG_GCS = "gs://whisp_bucket/whisp_cogs/gaul1_1210_i16_noyearly_10m_20260223.tif"

# Yamoussoukro approximate bbox
YAMA_BBOX = {"min_lon": -5.45, "max_lon": -5.15, "min_lat": 6.65, "max_lat": 7.05}


def generate_random_polygons(n, seed):
    """Generate n random small polygons (~4-10ha) within Yamoussoukro bbox."""
    rng = random.Random(seed)
    features = []
    for _ in range(n):
        # Random center
        cx = rng.uniform(YAMA_BBOX["min_lon"] + 0.01, YAMA_BBOX["max_lon"] - 0.01)
        cy = rng.uniform(YAMA_BBOX["min_lat"] + 0.01, YAMA_BBOX["max_lat"] - 0.01)
        # Random size (~200-300m sides = 4-9 ha)
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


# Generate two completely independent sets with different seeds
tmp_dir = tempfile.gettempdir()
gj_a = os.path.join(tmp_dir, "bench_fresh_a.geojson")
gj_b = os.path.join(tmp_dir, "bench_fresh_b.geojson")

ts = int(time.time())
gdf_a = generate_random_polygons(250, seed=ts)
gdf_b = generate_random_polygons(250, seed=ts + 99999)
gdf_a.to_file(gj_a, driver="GeoJSON")
gdf_b.to_file(gj_b, driver="GeoJSON")
print(f"Chunk A: 250 fresh random polygons (seed={ts}) -> {gj_a}")
print(f"Chunk B: 250 fresh random polygons (seed={ts + 99999}) -> {gj_b}")
print()

results = {}

# --- Benchmark 1: GEE concurrent with COG as ee.Image (chunk A) ---
print("--- Benchmark 1: GEE concurrent + COG as ee.Image (chunk A, 250 features) ---")
try:
    from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

    print(f"  Loading COG as EE image: {COG_GCS}")
    whisp_image_cog = ee.Image.loadGeoTIFF(COG_GCS)
    print(f"  Band count: {whisp_image_cog.bandNames().size().getInfo()}")

    t0 = time.time()
    df_cog_gee = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=gj_a,
        mode="concurrent",
        whisp_image=whisp_image_cog,
    )
    t_cog_gee = time.time() - t0
    print(f"  Time: {t_cog_gee:.1f}s ({t_cog_gee/250:.2f}s/feature)")
    print(f"  Result shape: {df_cog_gee.shape}")
    results["gee_cog_image_s"] = t_cog_gee
except Exception as e:
    print(f"  FAILED: {e}")
    import traceback

    traceback.print_exc()

print()

# --- Benchmark 2: GEE concurrent with live combine_datasets (chunk B) ---
print(
    "--- Benchmark 2: GEE concurrent + live combine_datasets (chunk B, 250 features) ---"
)
try:
    from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
    from openforis_whisp.datasets import combine_datasets
    import io
    from contextlib import redirect_stdout

    print("  Building non-yearly whisp image via combine_datasets()...")
    with redirect_stdout(io.StringIO()):
        whisp_image_ny = combine_datasets(exclude_yearly=True)

    t0 = time.time()
    df_gee_ny = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=gj_b,
        mode="concurrent",
        whisp_image=whisp_image_ny,
    )
    t_gee_ny = time.time() - t0
    print(f"  Time: {t_gee_ny:.1f}s ({t_gee_ny/250:.2f}s/feature)")
    print(f"  Result shape: {df_gee_ny.shape}")
    results["gee_live_noyearly_s"] = t_gee_ny
except Exception as e:
    print(f"  FAILED: {e}")
    import traceback

    traceback.print_exc()

print()

# --- Summary ---
print("=" * 65)
print("SUMMARY (250 features each, different subsets, non-yearly)")
print("=" * 65)
for k, v in sorted(results.items()):
    print(f"  {k:<30s}: {v:>8.1f}s")

if "gee_cog_image_s" in results and "gee_live_noyearly_s" in results:
    r = results["gee_live_noyearly_s"] / results["gee_cog_image_s"]
    if r > 1:
        print(f"\n  GEE COG-image is {r:.1f}x faster than GEE live")
    else:
        print(f"\n  GEE live is {1/r:.1f}x faster than GEE COG-image")

"""Benchmark: 4-way comparison on CI COGs at 10, 50, 250 features.

Uses the same polygon parameters as bench_ci_full.py (4-50 ha, 4-20 vertices)
spread across all of CI.

Compares:
  A) exactextract raw (VRT + /vsicurl/ range requests, no post-processing)
  B) GEE concurrent + COG mosaic (loadGeoTIFF, server-side)
  C) GEE concurrent + live image (combine_datasets, server-side)
  D) whisp_stats_cog_local (full pipeline: cached VRT → exactextract → format)
"""
import sys, os, time, random, math

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import ee

try:
    ee.Reset()
except Exception:
    pass
project = os.environ.get("PROJECT", None)
ee.Initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")

import geopandas as gpd
import tempfile
from shapely.geometry import Polygon

# CI bounding box
CI_BBOX = {"min_lon": -8.6, "max_lon": -2.5, "min_lat": 4.35, "max_lat": 10.74}

# Polygon parameters (standardized)
MIN_HA, MAX_HA = 4, 50
MIN_VERTS, MAX_VERTS = 4, 20

# Feature counts
FEATURE_COUNTS = [10, 50, 250]

# GCS config
BUCKET = "whisp_bucket"
COG_PREFIX = "whisp_cogs/CI_gaul1_"


def generate_random_polygons(n, seed, bbox=CI_BBOX):
    """Generate random polygons (4-50 ha, 4-20 vertices) within bbox."""
    rng = random.Random(seed)
    features = []
    for _ in range(n):
        cx = rng.uniform(bbox["min_lon"] + 0.02, bbox["max_lon"] - 0.02)
        cy = rng.uniform(bbox["min_lat"] + 0.02, bbox["max_lat"] - 0.02)
        target_ha = rng.uniform(MIN_HA, MAX_HA)
        radius_m = math.sqrt(target_ha * 10000 / math.pi)
        radius_deg = radius_m / 111000
        n_verts = rng.randint(MIN_VERTS, MAX_VERTS)
        coords = []
        for i in range(n_verts):
            angle = 2 * math.pi * i / n_verts
            r = radius_deg * rng.uniform(0.6, 1.4)
            x = cx + r * math.cos(angle)
            y = cy + r * math.sin(angle)
            coords.append((x, y))
        coords.append(coords[0])
        poly = Polygon(coords)
        if poly.is_valid:
            features.append(poly)
        else:
            poly = poly.buffer(0)
            if not poly.is_empty:
                features.append(poly)
    return gpd.GeoDataFrame(geometry=features[:n], crs="EPSG:4326")


# ============================================================
# Step 1: Build VRT from all CI COG shards (one-time setup)
# ============================================================
print("Building VRT from CI COG shards in GCS...")
from openforis_whisp.export_cog import create_vrt_from_gcs, _create_area_band_vrt

t0 = time.time()
vrt_path = create_vrt_from_gcs(BUCKET, COG_PREFIX)
t_vrt = time.time() - t0
print(f"  VRT ready in {t_vrt:.1f}s: {vrt_path}")

# Create area-band VRT for weighted_sum
area_vrt = _create_area_band_vrt(vrt_path)
print(f"  Area band VRT: {area_vrt}")

# Read metadata
import rasterio

with rasterio.open(vrt_path) as src:
    n_bands_vrt = src.count
    width, height = src.width, src.height
print(f"  VRT: {n_bands_vrt} bands, {width}x{height} pixels")

# ============================================================
# Step 2: Load GEE images (COG mosaic + live)
# ============================================================
print("\nLoading GEE COG mosaic...")
from openforis_whisp.export_cog import load_country_cog

t0 = time.time()
gee_cog_image = load_country_cog(["CI"], date_str="20260223")
t_load_cog = time.time() - t0
print(f"  Loaded in {t_load_cog:.1f}s")

print("Building live image...")
import io
from contextlib import redirect_stdout
from openforis_whisp.datasets import combine_datasets

t0 = time.time()
with redirect_stdout(io.StringIO()):
    live_image = combine_datasets(national_codes=["CI"], exclude_yearly=True)
t_build_live = time.time() - t0
print(f"  Built in {t_build_live:.1f}s")

# ============================================================
# Step 3: Run benchmarks
# ============================================================
import exactextract
from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.export_cog import whisp_stats_cog_local

tmp_dir = tempfile.gettempdir()
all_results = []

for n in FEATURE_COUNTS:
    print(f"\n{'='*70}")
    print(f"  {n} features, {MIN_HA}-{MAX_HA} ha, {MIN_VERTS}-{MAX_VERTS} vertices")
    print(f"{'='*70}")

    # Generate 4 independent sets (different seeds)
    seed_base = int(time.time() * 1000) % 1_000_000
    gdf_ee = generate_random_polygons(n, seed=seed_base)
    gdf_cog = generate_random_polygons(n, seed=seed_base + 11111)
    gdf_live = generate_random_polygons(n, seed=seed_base + 77777)
    gdf_cl = generate_random_polygons(n, seed=seed_base + 33333)

    gj_ee = os.path.join(tmp_dir, f"bench_ee_{n}.geojson")
    gj_cog = os.path.join(tmp_dir, f"bench_cog_{n}.geojson")
    gj_live = os.path.join(tmp_dir, f"bench_live_{n}.geojson")
    gj_cl = os.path.join(tmp_dir, f"bench_cl_{n}.geojson")
    gdf_ee.to_file(gj_ee, driver="GeoJSON")
    gdf_cog.to_file(gj_cog, driver="GeoJSON")
    gdf_live.to_file(gj_live, driver="GeoJSON")
    gdf_cl.to_file(gj_cl, driver="GeoJSON")

    gj_size_kb = os.path.getsize(gj_ee) / 1024
    print(f"  GeoJSON: ~{gj_size_kb:.0f} KB per set")

    # --- A) exactextract raw via VRT/vsicurl ---
    print(f"\n  A) exactextract raw (VRT + /vsicurl/ range requests)...")
    try:
        t0 = time.time()
        result_ee = exactextract.exact_extract(
            rast=vrt_path,
            vec=gdf_ee,
            weights=area_vrt,
            ops=["weighted_sum", "sum"],
            output="pandas",
        )
        t_ee = time.time() - t0
        print(
            f"     {t_ee:.1f}s ({t_ee/n:.3f}s/feat, {n/t_ee:.0f} feat/s) shape={result_ee.shape}"
        )
    except Exception as e:
        print(f"     FAILED: {e}")
        t_ee = None

    # --- B) GEE + COG mosaic (loadGeoTIFF) ---
    print(f"  B) GEE concurrent + COG mosaic...")
    try:
        t0 = time.time()
        df_cog = whisp_formatted_stats_geojson_to_df(
            input_geojson_filepath=gj_cog,
            mode="concurrent",
            whisp_image=gee_cog_image,
        )
        t_cog = time.time() - t0
        print(
            f"     {t_cog:.1f}s ({t_cog/n:.3f}s/feat, {n/t_cog:.0f} feat/s) shape={df_cog.shape}"
        )
    except Exception as e:
        print(f"     FAILED: {e}")
        import traceback

        traceback.print_exc()
        t_cog = None

    # --- C) GEE + live combine_datasets ---
    print(f"  C) GEE concurrent + live image...")
    try:
        t0 = time.time()
        df_live = whisp_formatted_stats_geojson_to_df(
            input_geojson_filepath=gj_live,
            mode="concurrent",
            whisp_image=live_image,
        )
        t_live = time.time() - t0
        print(
            f"     {t_live:.1f}s ({t_live/n:.3f}s/feat, {n/t_live:.0f} feat/s) shape={df_live.shape}"
        )
    except Exception as e:
        print(f"     FAILED: {e}")
        t_live = None

    # --- D) whisp_stats_cog_local (full pipeline: cached VRT → exactextract → formatted output) ---
    print(f"  D) cog-local (cached VRT + exactextract + post-processing)...")
    try:
        t0 = time.time()
        df_cl = whisp_stats_cog_local(
            input_geojson_filepath=gj_cl,
            iso2_codes=["CI"],
            verbose=False,
        )
        t_cl = time.time() - t0
        print(
            f"     {t_cl:.1f}s ({t_cl/n:.3f}s/feat, {n/t_cl:.0f} feat/s) shape={df_cl.shape}"
        )
    except Exception as e:
        print(f"     FAILED: {e}")
        import traceback

        traceback.print_exc()
        t_cl = None

    all_results.append((n, t_ee, t_cog, t_live, t_cl, gj_size_kb))

# ============================================================
# Summary
# ============================================================
print(f"\n\n{'='*110}")
print(
    f"  BENCHMARK SUMMARY: CI full-country | Polygons: {MIN_HA}-{MAX_HA} ha, {MIN_VERTS}-{MAX_VERTS} vertices"
)
print(f"  A) exactextract raw via VRT+/vsicurl/ (local CPU, range requests to GCS)")
print(f"  B) GEE concurrent + COG mosaic (loadGeoTIFF, server-side)")
print(f"  C) GEE concurrent + live image (combine_datasets, server-side)")
print(f"  D) cog-local full pipeline (cached VRT → exactextract → formatted output)")
print(f"{'='*110}")
print(
    f"  {'Feat':>5}  {'GJ KB':>6}  {'A:exactext':>11}  {'B:GEE+COG':>10}  {'C:GEE+live':>11}  {'D:cog-local':>12}  {'B/A':>5}  {'C/B':>5}  {'D/A':>5}"
)
print(
    f"  {'-'*5}  {'-'*6}  {'-'*11}  {'-'*10}  {'-'*11}  {'-'*12}  {'-'*5}  {'-'*5}  {'-'*5}"
)

for n, t_ee, t_cog, t_live, t_cl, sz in all_results:
    ee_str = f"{t_ee:.1f}s" if t_ee else "FAIL"
    cog_str = f"{t_cog:.1f}s" if t_cog else "FAIL"
    live_str = f"{t_live:.1f}s" if t_live else "FAIL"
    cl_str = f"{t_cl:.1f}s" if t_cl else "FAIL"
    ba = f"{t_cog/t_ee:.2f}x" if (t_ee and t_cog) else "N/A"
    cb = f"{t_live/t_cog:.1f}x" if (t_cog and t_live) else "N/A"
    da = f"{t_cl/t_ee:.2f}x" if (t_ee and t_cl) else "N/A"
    print(
        f"  {n:>5}  {sz:>5.0f}  {ee_str:>11}  {cog_str:>10}  {live_str:>11}  {cl_str:>12}  {ba:>5}  {cb:>5}  {da:>5}"
    )

print()

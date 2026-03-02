"""Benchmark: CI full-country COG mosaic (14 admin regions) performance.

Tests extraction speed at multiple feature counts using random polygons
spread across all of Cote d'Ivoire (not just Yamoussoukro).

Compares:
  A) COG mosaic via load_country_cog() (60 sharded files)
  B) Live combine_datasets() (on-the-fly from 49 GEE assets)
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

# Full CI bounding box (approximate)
CI_BBOX = {"min_lon": -8.6, "max_lon": -2.5, "min_lat": 4.35, "max_lat": 10.74}

# Feature counts to test
FEATURE_COUNTS = [10, 50, 250, 1000, 5000]


MIN_HA, MAX_HA = 4, 50
MIN_VERTS, MAX_VERTS = 4, 20


def generate_random_polygons(
    n,
    seed,
    bbox,
    min_ha=MIN_HA,
    max_ha=MAX_HA,
    min_verts=MIN_VERTS,
    max_verts=MAX_VERTS,
):
    """Generate random polygons within a bounding box."""
    rng = random.Random(seed)
    features = []
    for _ in range(n):
        cx = rng.uniform(bbox["min_lon"] + 0.02, bbox["max_lon"] - 0.02)
        cy = rng.uniform(bbox["min_lat"] + 0.02, bbox["max_lat"] - 0.02)

        target_ha = rng.uniform(min_ha, max_ha)
        radius_m = math.sqrt(target_ha * 10000 / math.pi)
        radius_deg = radius_m / 111000

        n_verts = rng.randint(min_verts, max_verts)
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


def run_benchmark(label, geojson_path, whisp_image, n):
    """Run a single benchmark, return time in seconds or None on failure."""
    from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

    try:
        t0 = time.time()
        df = whisp_formatted_stats_geojson_to_df(
            input_geojson_filepath=geojson_path,
            mode="concurrent",
            whisp_image=whisp_image,
        )
        elapsed = time.time() - t0
        print(
            f"  {label}: {elapsed:.1f}s ({elapsed/n:.3f}s/feat, {n/elapsed:.0f} feat/s) shape={df.shape}"
        )
        return elapsed
    except Exception as e:
        print(f"  {label}: FAILED - {e}")
        import traceback

        traceback.print_exc()
        return None


# ============================================================
# Prepare images
# ============================================================
print("Loading CI COG mosaic (14 admin regions, 60 files)...")
t0 = time.time()
from openforis_whisp.export_cog import load_country_cog

cog_image = load_country_cog(["CI"], date_str="20260223")
t_load = time.time() - t0
n_bands_cog = cog_image.bandNames().size().getInfo()
print(f"  Loaded in {t_load:.1f}s, {n_bands_cog} bands\n")

print("Building live image via combine_datasets(exclude_yearly=True)...")
import io
from contextlib import redirect_stdout
from openforis_whisp.datasets import combine_datasets

t0 = time.time()
with redirect_stdout(io.StringIO()):
    live_image = combine_datasets(national_codes=["CI"], exclude_yearly=True)
t_build = time.time() - t0
n_bands_live = live_image.bandNames().size().getInfo()
print(f"  Built in {t_build:.1f}s, {n_bands_live} bands\n")

# ============================================================
# Run benchmarks at each feature count
# ============================================================
all_results = []
tmp_dir = tempfile.gettempdir()

for n in FEATURE_COUNTS:
    print(f"\n{'='*65}")
    print(f"  {n} features across all of CI")
    print(f"{'='*65}")

    # Generate two independent sets (different seeds for COG vs live)
    seed_base = int(time.time() * 1000) % 1_000_000
    gdf_a = generate_random_polygons(n, seed=seed_base, bbox=CI_BBOX)
    gdf_b = generate_random_polygons(n, seed=seed_base + 77777, bbox=CI_BBOX)

    gj_a = os.path.join(tmp_dir, f"bench_ci_cog_{n}.geojson")
    gj_b = os.path.join(tmp_dir, f"bench_ci_live_{n}.geojson")
    gdf_a.to_file(gj_a, driver="GeoJSON")
    gdf_b.to_file(gj_b, driver="GeoJSON")

    # Input stats
    size_a_kb = os.path.getsize(gj_a) / 1024
    size_b_kb = os.path.getsize(gj_b) / 1024
    verts_a = [len(g.exterior.coords) for g in gdf_a.geometry]
    areas_a = [g.area * 111000 * 111000 / 10000 for g in gdf_a.geometry]  # rough ha
    print(
        f"  Input A: {size_a_kb:.0f} KB, verts {min(verts_a)}-{max(verts_a)}, area ~{min(areas_a):.0f}-{max(areas_a):.0f} ha"
    )
    print(f"  Input B: {size_b_kb:.0f} KB")

    t_cog = run_benchmark(f"COG mosaic ({n}feat)", gj_a, cog_image, n)
    t_live = run_benchmark(f"Live image ({n}feat)", gj_b, live_image, n)

    all_results.append((n, t_cog, t_live, size_a_kb, size_b_kb))

# ============================================================
# Summary table
# ============================================================
print(f"\n\n{'='*80}")
print(f"  BENCHMARK SUMMARY: CI full-country COG vs Live (concurrent mode)")
print(f"  COG: 14 admin regions, 60 sharded files, int16, 49 bands, 10m")
print(f"  Live: combine_datasets(national_codes=['CI'], exclude_yearly=True)")
print(f"  Polygons: {MIN_HA}-{MAX_HA} ha, {MIN_VERTS}-{MAX_VERTS} vertices")
print(f"{'='*100}")
print(
    f"  {'Features':>8}  {'Input KB':>9}  {'COG (s)':>9}  {'Live (s)':>9}  {'Speedup':>8}  {'COG feat/s':>11}  {'Live feat/s':>12}"
)
print(f"  {'-'*8}  {'-'*9}  {'-'*9}  {'-'*9}  {'-'*8}  {'-'*11}  {'-'*12}")

for n, t_cog, t_live, sz_a, sz_b in all_results:
    cog_str = f"{t_cog:.1f}" if t_cog else "FAIL"
    live_str = f"{t_live:.1f}" if t_live else "FAIL"
    sz_str = f"{sz_a:.0f}"
    if t_cog and t_live:
        speedup = t_live / t_cog
        cog_fps = n / t_cog
        live_fps = n / t_live
        print(
            f"  {n:>8}  {sz_str:>9}  {cog_str:>9}  {live_str:>9}  {speedup:>7.1f}x  {cog_fps:>10.0f}  {live_fps:>11.0f}"
        )
    else:
        print(
            f"  {n:>8}  {sz_str:>9}  {cog_str:>9}  {live_str:>9}  {'N/A':>8}  {'N/A':>11}  {'N/A':>12}"
        )

print()

"""Benchmark: COG-local (prebuilt COG + exactextract) vs GEE concurrent.

Compares the two full pipeline approaches on CI (Côte d'Ivoire):
  A) whisp_stats_cog_local  — cached VRT from prebuilt GCS COGs → exactextract → formatted output
  B) GEE concurrent         — server-side reduceRegions with live combine_datasets image

Both return identical formatted DataFrames (same columns, same post-processing).
Random polygons (4-50 ha, 4-20 vertices) spread across all of CI.

Usage:
    python tests/fixtures/bench_coglocal_vs_gee.py
"""
import sys
import os
import time
import random
import math
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import geopandas as gpd
from shapely.geometry import Polygon

# CI bounding box
CI_BBOX = {"min_lon": -8.6, "max_lon": -2.5, "min_lat": 4.35, "max_lat": 10.74}

MIN_HA, MAX_HA = 4, 50
MIN_VERTS, MAX_VERTS = 4, 20

FEATURE_COUNTS = [10, 50, 250]


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


def main():
    """Run the benchmark (must be called inside __name__ == '__main__' guard)."""
    import io
    from contextlib import redirect_stdout
    import ee

    try:
        ee.Reset()
    except Exception:
        pass
    ee.Initialize(
        project="ee-andyarnellgee",
        opt_url="https://earthengine-highvolume.googleapis.com",
    )
    print("EE initialized (high-volume endpoint)\n")

    # ============================================================
    # One-time setup: pre-build GEE live image and pre-warm VRT
    # ============================================================
    print("--- Setup ---")

    from openforis_whisp.datasets import combine_datasets

    print("Building live combine_datasets image (exclude_yearly=True)...")
    t0 = time.time()
    with redirect_stdout(io.StringIO()):
        live_image = combine_datasets(national_codes=["CI"], exclude_yearly=True)
    t_build = time.time() - t0
    print(f"  Live image built in {t_build:.1f}s")

    from openforis_whisp.export_cog import whisp_stats_cog_local, create_vrt_from_gcs

    print("Pre-warming VRT cache...")
    t0 = time.time()
    vrt_path = create_vrt_from_gcs("whisp_bucket", "whisp_cogs/CI_gaul1_")
    t_vrt = time.time() - t0
    print(f"  VRT ready in {t_vrt:.1f}s (cached for all runs)\n")

    # ============================================================
    # Run benchmarks
    # ============================================================
    from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

    tmp_dir = tempfile.gettempdir()
    all_results = []

    for n in FEATURE_COUNTS:
        print(f"{'='*70}")
        print(
            f"  {n} features | {MIN_HA}-{MAX_HA} ha | {MIN_VERTS}-{MAX_VERTS} vertices"
        )
        print(f"{'='*70}")

        # Generate independent polygon sets (different seeds to avoid caching)
        seed_base = int(time.time() * 1000) % 1_000_000
        gdf_cog = generate_random_polygons(n, seed=seed_base)
        gdf_gee = generate_random_polygons(n, seed=seed_base + 77777)

        gj_cog = os.path.join(tmp_dir, f"bench_cog_{n}.geojson")
        gj_gee = os.path.join(tmp_dir, f"bench_gee_{n}.geojson")
        gdf_cog.to_file(gj_cog, driver="GeoJSON")
        gdf_gee.to_file(gj_gee, driver="GeoJSON")

        gj_kb = os.path.getsize(gj_cog) / 1024
        print(f"  GeoJSON: ~{gj_kb:.0f} KB per set\n")

        # --- A) COG-local: cached VRT → exactextract → formatted output ---
        # max_extract_workers=1 avoids ProcessPoolExecutor spawn issues on Windows
        print(f"  A) cog-local (exactextract on prebuilt COGs)...")
        t_cog = None
        try:
            t0 = time.time()
            df_cog = whisp_stats_cog_local(
                input_geojson_filepath=gj_cog,
                iso2_codes=["CI"],
                max_extract_workers=1,
                verbose=False,
            )
            t_cog = time.time() - t0
            print(
                f"     {t_cog:.1f}s  ({t_cog/n:.3f}s/feat, {n/t_cog:.0f} feat/s)  shape={df_cog.shape}"
            )
        except Exception as e:
            print(f"     FAILED: {e}")
            import traceback

            traceback.print_exc()

        # --- B) GEE concurrent + live image ---
        print(f"  B) GEE concurrent (live combine_datasets)...")
        t_gee = None
        try:
            t0 = time.time()
            df_gee = whisp_formatted_stats_geojson_to_df(
                input_geojson_filepath=gj_gee,
                mode="concurrent",
                whisp_image=live_image,
            )
            t_gee = time.time() - t0
            print(
                f"     {t_gee:.1f}s  ({t_gee/n:.3f}s/feat, {n/t_gee:.0f} feat/s)  shape={df_gee.shape}"
            )
        except Exception as e:
            print(f"     FAILED: {e}")
            import traceback

            traceback.print_exc()

        all_results.append((n, t_cog, t_gee, gj_kb))
        print()

    # ============================================================
    # Summary table
    # ============================================================
    print(f"\n{'='*85}")
    print(f"  BENCHMARK SUMMARY: COG-local vs GEE concurrent (CI full country)")
    print(
        f"  A) cog-local  — prebuilt GCS COGs → cached VRT → exactextract → formatted output"
    )
    print(f"  B) GEE concurrent — live combine_datasets, server-side reduceRegions")
    print(
        f"  Polygons: {MIN_HA}-{MAX_HA} ha, {MIN_VERTS}-{MAX_VERTS} vertices, random across CI"
    )
    print(f"{'='*85}")
    print(
        f"  {'Feat':>5}  {'GJ KB':>6}  {'A:cog-local':>12}  {'B:GEE conc':>11}  {'Speedup':>8}  {'A feat/s':>9}  {'B feat/s':>9}"
    )
    print(f"  {'-'*5}  {'-'*6}  {'-'*12}  {'-'*11}  {'-'*8}  {'-'*9}  {'-'*9}")

    for n, t_cog, t_gee, sz in all_results:
        cog_str = f"{t_cog:.1f}s" if t_cog else "FAIL"
        gee_str = f"{t_gee:.1f}s" if t_gee else "FAIL"
        if t_cog and t_gee:
            speedup = t_gee / t_cog
            cog_fps = n / t_cog
            gee_fps = n / t_gee
            print(
                f"  {n:>5}  {sz:>5.0f}  {cog_str:>12}  {gee_str:>11}  {speedup:>7.1f}x  {cog_fps:>8.0f}  {gee_fps:>8.0f}"
            )
        else:
            print(
                f"  {n:>5}  {sz:>5.0f}  {cog_str:>12}  {gee_str:>11}  {'N/A':>8}  {'N/A':>9}  {'N/A':>9}"
            )

    print()


if __name__ == "__main__":
    main()

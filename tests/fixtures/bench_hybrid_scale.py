"""
Benchmark hybrid mode at scale: 50, 100, 250 features.

Uses generate_random_polygons() from utils.py with unique seeds per test
to avoid GEE caching. Plots are scattered across Côte d'Ivoire.

Compares:
1. Full COG: load_country_cog() → all bands from COG
2. Hybrid:   load_country_cog_hybrid() → static COG + dynamic live GEE
3. Full GEE: combine_datasets() → all bands live from GEE
"""

import ee
import json
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

ee.Initialize(
    project="ee-andyarnellgee",
    opt_url="https://earthengine-highvolume.googleapis.com",
)

from src.openforis_whisp.export_cog import load_country_cog, load_country_cog_hybrid
from src.openforis_whisp.advanced_stats import whisp_stats_geojson_to_df_concurrent
from src.openforis_whisp.utils import generate_random_polygons

ISO2 = ["CI"]
DATE_STR = "20260223"
OUT_DIR = os.path.join(os.path.dirname(__file__), "_hybrid_bench")
os.makedirs(OUT_DIR, exist_ok=True)

# Côte d'Ivoire approximate land bounds
CI_BOUNDS = [-8.6, 4.4, -2.5, 10.7]


def make_geojson(n_features, mode):
    """Generate fresh random GeoJSON and save to file. Unique seed per call."""
    seed = (int(time.time()) + hash(mode)) % (2**32 - 1)
    geojson = generate_random_polygons(
        bounds=CI_BOUNDS,
        num_polygons=n_features,
        min_area_ha=1,
        max_area_ha=10,
        seed=seed,
    )
    path = os.path.join(OUT_DIR, f"{mode}_{n_features}.geojson")
    with open(path, "w") as f:
        json.dump(geojson, f)
    return path


def run_bench(label, n_features, mode):
    """Run a single benchmark. mode: 'cog', 'hybrid', 'gee'."""
    path = make_geojson(n_features, mode)

    t0 = time.time()

    if mode == "cog":
        img = load_country_cog(ISO2, date_str=DATE_STR)
        df = whisp_stats_geojson_to_df_concurrent(
            path, whisp_image=img, national_codes=ISO2, batch_size=10
        )
    elif mode == "hybrid":
        img = load_country_cog_hybrid(ISO2, date_str=DATE_STR)
        df = whisp_stats_geojson_to_df_concurrent(
            path, whisp_image=img, national_codes=ISO2, batch_size=10
        )
    elif mode == "gee":
        df = whisp_stats_geojson_to_df_concurrent(
            path, national_codes=ISO2, batch_size=10
        )

    elapsed = time.time() - t0
    print(f"  {label}: {elapsed:.1f}s | {len(df)} rows, {len(df.columns)} cols")
    return elapsed, len(df)


if __name__ == "__main__":
    sizes = [1000]
    n_runs = 3
    results = {}

    for n in sizes:
        all_runs = {"cog": [], "hybrid": [], "gee": []}

        for run_idx in range(1, n_runs + 1):
            print(f"\n{'='*60}")
            print(f"  {n} FEATURES — RUN {run_idx}/{n_runs}")
            print(f"{'='*60}")

            t_cog, _ = run_bench(f"Full COG   ({n}) run {run_idx}", n, "cog")
            t_hybrid, _ = run_bench(f"Hybrid     ({n}) run {run_idx}", n, "hybrid")
            t_gee, _ = run_bench(f"Full GEE   ({n}) run {run_idx}", n, "gee")

            all_runs["cog"].append(t_cog)
            all_runs["hybrid"].append(t_hybrid)
            all_runs["gee"].append(t_gee)

        results[n] = {k: sum(v) / len(v) for k, v in all_runs.items()}
        results[n]["runs"] = all_runs

    # Summary table
    print(f"\n{'='*60}")
    print(f"SUMMARY — AVERAGES over {n_runs} runs (seconds)")
    print(f"{'='*60}")
    print(
        f"{'Features':>10} {'Full COG':>10} {'Hybrid':>10} {'Full GEE':>10} {'Hybrid/COG':>12} {'Hybrid/GEE':>12}"
    )
    for n in sizes:
        r = results[n]
        ratio_cog = r["hybrid"] / r["cog"] if r["cog"] > 0 else 0
        ratio_gee = r["hybrid"] / r["gee"] if r["gee"] > 0 else 0
        print(
            f"{n:>10} {r['cog']:>10.1f} {r['hybrid']:>10.1f} {r['gee']:>10.1f}"
            f" {ratio_cog:>11.2f}x {ratio_gee:>11.2f}x"
        )

    # Per-run detail
    print(f"\nPer-run detail:")
    for n in sizes:
        runs = results[n]["runs"]
        for i in range(n_runs):
            print(
                f"  Run {i+1}: COG={runs['cog'][i]:.1f}s  Hybrid={runs['hybrid'][i]:.1f}s  GEE={runs['gee'][i]:.1f}s"
            )

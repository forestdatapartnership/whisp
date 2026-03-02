"""
Speed test: Combined Int16 + exclude_yearly for 200 random polygons.

Uses Whisp's generate_random_polygons() to create test features,
then benchmarks 4 modes of whisp_stats_local():
  1. Default (float32, all bands)
  2. exclude_yearly only (float32, ~49 bands)
  3. int16 only (int16, all bands)
  4. Both exclude_yearly + int16 (~49 bands, int16)

"""

import time
import json
import tempfile
import shutil
import os
import sys
from pathlib import Path

# Ensure package is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

import ee

ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")

from openforis_whisp.utils import generate_random_polygons
from openforis_whisp.local_stats import whisp_stats_local

# --- Configuration ---
NUM_FEATURES = 200
# Côte d'Ivoire bounds (good tropical coverage with commodity data)
BOUNDS = [-7.5, 5.0, -5.0, 7.5]
SEED = 42  # Reproducible
MIN_AREA_HA = 1
MAX_AREA_HA = 10
SCALE = 10

CONFIGS = [
    {
        "label": "float32 + all bands (default)",
        "output_dtype": "float32",
        "exclude_yearly": False,
    },
    {
        "label": "float32 + exclude_yearly",
        "output_dtype": "float32",
        "exclude_yearly": True,
    },
    {"label": "int16 + all bands", "output_dtype": "int16", "exclude_yearly": False},
    {
        "label": "int16 + exclude_yearly (combined)",
        "output_dtype": "int16",
        "exclude_yearly": True,
    },
]


def main():
    print(f"{'='*70}")
    print(f"Speed test: 4 modes x {NUM_FEATURES} random polygons")
    print(f"Bounds: {BOUNDS}  Scale: {SCALE}m  Seed: {SEED}")
    print(f"{'='*70}\n")

    # Step 1: Generate random polygons
    print(f"Generating {NUM_FEATURES} random polygons...")
    t0 = time.time()
    geojson = generate_random_polygons(
        bounds=BOUNDS,
        num_polygons=NUM_FEATURES,
        min_area_ha=MIN_AREA_HA,
        max_area_ha=MAX_AREA_HA,
        seed=SEED,
    )
    gen_time = time.time() - t0
    print(f"  Generated in {gen_time:.1f}s")
    print(f"  Features: {len(geojson['features'])}")

    # Save to temp file (reused across all configs)
    tmp_geojson = Path(tempfile.mkdtemp()) / "test_200.geojson"
    with open(tmp_geojson, "w") as f:
        json.dump(geojson, f)
    print(f"  Saved to: {tmp_geojson}\n")

    # Step 2: Run each configuration
    results = []
    for cfg in CONFIGS:
        label = cfg["label"]
        print(f"--- {label} ---")

        output_dir = Path(tempfile.mkdtemp())

        t_start = time.time()
        try:
            df = whisp_stats_local(
                input_geojson_filepath=str(tmp_geojson),
                output_dir=str(output_dir),
                scale=SCALE,
                output_dtype=cfg["output_dtype"],
                exclude_yearly=cfg["exclude_yearly"],
                cleanup_files=True,
                verbose=True,
            )
            elapsed = time.time() - t_start

            # Gather stats
            num_cols = len(df.columns)
            num_rows = len(df)

            # Check area column for sanity
            area_col = "Area" if "Area" in df.columns else None
            area_mean = df[area_col].mean() if area_col else None

            result = {
                "label": label,
                "time_s": elapsed,
                "rows": num_rows,
                "columns": num_cols,
                "area_mean": area_mean,
                "error": None,
            }
            print(
                f"  Time: {elapsed:.1f}s | Rows: {num_rows} | Cols: {num_cols} | Area mean: {area_mean:.3f} ha"
            )

        except Exception as e:
            elapsed = time.time() - t_start
            result = {
                "label": label,
                "time_s": elapsed,
                "rows": 0,
                "columns": 0,
                "area_mean": None,
                "error": str(e),
            }
            print(f"  ERROR after {elapsed:.1f}s: {e}")

        results.append(result)

        # Cleanup output dir
        try:
            shutil.rmtree(output_dir, ignore_errors=True)
        except Exception:
            pass

        print()

    # Cleanup temp geojson
    try:
        shutil.rmtree(tmp_geojson.parent, ignore_errors=True)
    except Exception:
        pass

    # Summary table
    print(f"\n{'='*70}")
    print("SUMMARY")
    print(f"{'='*70}")
    print(f"{'Config':<40} {'Time':>8} {'Cols':>6} {'Rows':>6} {'Speedup':>8}")
    print(f"{'-'*40} {'-'*8} {'-'*6} {'-'*6} {'-'*8}")

    baseline_time = results[0]["time_s"] if results[0]["error"] is None else None

    for r in results:
        if r["error"]:
            print(f"{r['label']:<40} {'ERROR':>8}")
            continue
        speedup = ""
        if baseline_time and r["time_s"] > 0:
            speedup = f"{baseline_time / r['time_s']:.2f}x"
        print(
            f"{r['label']:<40} {r['time_s']:>7.1f}s {r['columns']:>6} {r['rows']:>6} {speedup:>8}"
        )

    # Area comparison
    print(f"\nArea comparison (mean ha):")
    for r in results:
        if r["area_mean"] is not None:
            print(f"  {r['label']:<40} {r['area_mean']:.4f}")

    if all(r["area_mean"] is not None for r in results):
        base_area = results[0]["area_mean"]
        for r in results[1:]:
            pct_diff = abs(r["area_mean"] - base_area) / base_area * 100
            print(f"  {r['label']:<40} diff: {pct_diff:.4f}%")


if __name__ == "__main__":
    main()

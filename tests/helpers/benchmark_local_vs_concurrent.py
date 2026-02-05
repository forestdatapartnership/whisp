"""
Benchmark script to compare local vs concurrent mode timings.

Tests various combinations of:
- Number of features (3, 10)
- max_workers / max_concurrent settings
- batch_size settings
"""

import os
import sys
import time
import json
import tempfile
import random
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import ee
import pandas as pd
from shapely.geometry import Polygon

# Initialize EE with high-volume endpoint
try:
    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")
    print("✅ Earth Engine initialized (high-volume endpoint)\n")
except Exception:
    ee.Initialize()
    print("⚠️ Using standard Earth Engine endpoint\n")


def create_test_geojson(num_features: int, output_path: str) -> str:
    """Create test GeoJSON with small polygons (guaranteed to work with EE download limits)."""
    features = []

    # Use small fixed-size polygons that won't exceed EE limits
    # These are ~0.001 degrees which is about 100m x 100m at equator
    base_coords = [
        (-2.5, -60.5),  # Amazon
        (1.0, 20.0),  # Congo
        (5.0, 105.0),  # SE Asia
    ]

    for i in range(num_features):
        # Cycle through regions
        base_lat, base_lon = base_coords[i % len(base_coords)]

        # Add small offset for each feature
        offset = i * 0.05
        center_lat = base_lat + offset
        center_lon = base_lon + offset

        # Create very small polygon (~1 ha = 0.003 degrees at equator)
        size = 0.003
        coords = [
            [center_lon - size, center_lat - size],
            [center_lon + size, center_lat - size],
            [center_lon + size, center_lat + size],
            [center_lon - size, center_lat + size],
            [center_lon - size, center_lat - size],
        ]

        features.append(
            {
                "type": "Feature",
                "properties": {"id": i + 1},
                "geometry": {"type": "Polygon", "coordinates": [coords]},
            }
        )

    geojson = {"type": "FeatureCollection", "features": features}

    with open(output_path, "w") as f:
        json.dump(geojson, f)

    return output_path


def run_concurrent_mode(
    geojson_path: str, batch_size: int, max_concurrent: int
) -> tuple:
    """Run concurrent mode and return (time, num_rows, num_cols)."""
    from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

    start = time.time()
    df = whisp_formatted_stats_geojson_to_df(
        geojson_path,
        mode="concurrent",
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        unit_type="ha",
    )
    elapsed = time.time() - start
    return elapsed, len(df), len(df.columns)


def run_local_mode(
    geojson_path: str,
    output_dir: str,
    max_download_workers: int,
    max_extract_workers: int,
    chunk_size: int,
) -> tuple:
    """Run local mode and return (time, num_rows, num_cols)."""
    from openforis_whisp.local_stats import whisp_stats_local

    start = time.time()
    df = whisp_stats_local(
        geojson_path,
        output_dir=output_dir,
        max_download_workers=max_download_workers,
        max_extract_workers=max_extract_workers,
        chunk_size=chunk_size,
        unit_type="ha",
        cleanup_files=True,
        verbose=False,  # Suppress output for clean benchmark
    )
    elapsed = time.time() - start
    return elapsed, len(df), len(df.columns)


def run_benchmark():
    """Run comprehensive benchmark comparing modes."""

    # Test configurations
    feature_counts = [3, 10]  # Number of features to test

    # Concurrent mode configurations: (batch_size, max_concurrent)
    concurrent_configs = [
        (10, 20),
        (15, 30),
    ]

    # Local mode configurations: (max_download_workers, max_extract_workers, chunk_size)
    local_configs = [
        (15, 4, 10),
        (30, 8, 25),
    ]

    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        for num_features in feature_counts:
            print(f"\n{'='*70}")
            print(f"BENCHMARKING WITH {num_features} FEATURES")
            print(f"{'='*70}")

            # Create test GeoJSON
            geojson_path = os.path.join(tmpdir, f"test_{num_features}.geojson")
            create_test_geojson(num_features, geojson_path)
            print(f"📁 Created test GeoJSON: {geojson_path}\n")

            # Test concurrent mode configurations
            print("-" * 70)
            print("CONCURRENT MODE BENCHMARKS")
            print("-" * 70)

            for batch_size, max_concurrent in concurrent_configs:
                config_name = f"batch={batch_size}, max_concurrent={max_concurrent}"
                print(f"\nTesting: {config_name}")

                try:
                    elapsed, rows, cols = run_concurrent_mode(
                        geojson_path, batch_size, max_concurrent
                    )
                    print(f"  ✅ {elapsed:.1f}s ({rows} rows, {cols} cols)")
                    results.append(
                        {
                            "mode": "concurrent",
                            "features": num_features,
                            "config": config_name,
                            "time_seconds": elapsed,
                            "rows": rows,
                            "cols": cols,
                        }
                    )
                except Exception as e:
                    print(f"  ❌ Error: {str(e)[:50]}")
                    results.append(
                        {
                            "mode": "concurrent",
                            "features": num_features,
                            "config": config_name,
                            "time_seconds": None,
                            "error": str(e)[:100],
                        }
                    )

            # Test local mode configurations
            print("\n" + "-" * 70)
            print("LOCAL MODE BENCHMARKS")
            print("-" * 70)

            for i, (dl_workers, ext_workers, chunk_size) in enumerate(local_configs):
                config_name = f"dl_workers={dl_workers}, ext_workers={ext_workers}, chunk={chunk_size}"
                print(f"\nTesting: {config_name}")

                local_output_dir = os.path.join(tmpdir, f"local_{num_features}_{i}")
                os.makedirs(local_output_dir, exist_ok=True)

                try:
                    elapsed, rows, cols = run_local_mode(
                        geojson_path,
                        local_output_dir,
                        dl_workers,
                        ext_workers,
                        chunk_size,
                    )
                    print(f"  ✅ {elapsed:.1f}s ({rows} rows, {cols} cols)")
                    results.append(
                        {
                            "mode": "local",
                            "features": num_features,
                            "config": config_name,
                            "time_seconds": elapsed,
                            "rows": rows,
                            "cols": cols,
                        }
                    )
                except Exception as e:
                    print(f"  ❌ Error: {str(e)[:50]}")
                    results.append(
                        {
                            "mode": "local",
                            "features": num_features,
                            "config": config_name,
                            "time_seconds": None,
                            "error": str(e)[:100],
                        }
                    )

    # Summary
    print("\n" + "=" * 70)
    print("BENCHMARK SUMMARY")
    print("=" * 70)

    df_results = pd.DataFrame(results)

    # Group by features and mode
    for num_features in feature_counts:
        print(f"\n📊 {num_features} Features:")
        subset = df_results[df_results["features"] == num_features]

        for mode in ["concurrent", "local"]:
            mode_subset = subset[subset["mode"] == mode]
            if not mode_subset.empty:
                valid = mode_subset[mode_subset["time_seconds"].notna()]
                if not valid.empty:
                    best = valid.loc[valid["time_seconds"].idxmin()]
                    worst = valid.loc[valid["time_seconds"].idxmax()]
                    avg = valid["time_seconds"].mean()
                    print(f"\n  {mode.upper()} MODE:")
                    print(f"    Best:  {best['time_seconds']:.1f}s - {best['config']}")
                    print(
                        f"    Worst: {worst['time_seconds']:.1f}s - {worst['config']}"
                    )
                    print(f"    Avg:   {avg:.1f}s")

    # Save results
    output_path = Path(__file__).parent.parent.parent / "benchmark_results.csv"
    df_results.to_csv(output_path, index=False)
    print(f"\n📁 Results saved to: {output_path}")

    return df_results


if __name__ == "__main__":
    run_benchmark()

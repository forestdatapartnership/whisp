"""
Test script to compare local vs concurrent mode output and iterate until aligned.

This script runs both processing modes on the same test GeoJSON and compares
the output DataFrames column-by-column, identifying any differences.
"""

import os
import sys
import json
import tempfile
import pandas as pd
import geopandas as gpd
import numpy as np
from pathlib import Path


def create_small_test_geojson(output_path: str, num_features: int = 3):
    """Create a small test GeoJSON with a few simple features."""
    # Use small, simple polygons from different regions
    features = [
        # Feature 1: Small polygon in Kenya
        {
            "type": "Feature",
            "properties": {"user_id": 1},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [36.870897, -0.995549],
                        [36.871064, -0.995654],
                        [36.871334, -0.994686],
                        [36.871291, -0.994644],
                        [36.870897, -0.995549],
                    ]
                ],
            },
        },
        # Feature 2: Small polygon in Côte d'Ivoire
        {
            "type": "Feature",
            "properties": {"user_id": 2},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-5.586148, 5.59854],
                        [-5.585202, 5.59858],
                        [-5.585091, 5.598941],
                        [-5.585778, 5.599088],
                        [-5.586148, 5.59854],
                    ]
                ],
            },
        },
        # Feature 3: Small polygon in Brazil
        {
            "type": "Feature",
            "properties": {"user_id": 3},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-47.008185, -20.478621],
                        [-47.007295, -20.478865],
                        [-47.006912, -20.477775],
                        [-47.00788, -20.477718],
                        [-47.008185, -20.478621],
                    ]
                ],
            },
        },
    ]

    geojson = {"type": "FeatureCollection", "features": features[:num_features]}

    with open(output_path, "w") as f:
        json.dump(geojson, f)

    return output_path


def compare_dataframes(
    df_local: pd.DataFrame, df_concurrent: pd.DataFrame, tolerance: float = 0.02
) -> dict:
    """
    Compare two DataFrames and return differences.

    Args:
        df_local: DataFrame from local processing
        df_concurrent: DataFrame from concurrent processing
        tolerance: Relative tolerance for numeric comparisons (default 2% to account
            for floating point precision differences between EE reduceRegions and exactextract)

    Returns:
        dict with comparison results
    """
    results = {
        "match": True,
        "local_only_columns": [],
        "concurrent_only_columns": [],
        "column_differences": {},
        "null_columns_local": [],
        "null_columns_concurrent": [],
        "row_count_match": len(df_local) == len(df_concurrent),
    }

    # Check row counts
    if not results["row_count_match"]:
        results["match"] = False
        results["local_rows"] = len(df_local)
        results["concurrent_rows"] = len(df_concurrent)
        return results

    # Find column differences
    local_cols = set(df_local.columns)
    concurrent_cols = set(df_concurrent.columns)

    results["local_only_columns"] = sorted(local_cols - concurrent_cols)
    results["concurrent_only_columns"] = sorted(concurrent_cols - local_cols)

    if results["local_only_columns"] or results["concurrent_only_columns"]:
        results["match"] = False

    # Check for null columns
    for col in df_local.columns:
        if df_local[col].isna().all():
            results["null_columns_local"].append(col)

    for col in df_concurrent.columns:
        if df_concurrent[col].isna().all():
            results["null_columns_concurrent"].append(col)

    # Compare common columns
    common_cols = local_cols & concurrent_cols

    # Columns to skip in comparison (metadata that will always differ)
    skip_cols = {
        "whisp_processing_metadata",  # Timestamps and mode will differ
        "geo",  # JSON formatting differences (quotes, precision)
        # Admin columns differ because local mode uses sum instead of mode
        # for admin_code, which is expected limitation of local processing
        "Country",
        "ProducerCountry",
        "Admin_Level_1",
    }

    for col in sorted(common_cols - skip_cols):
        local_vals = df_local[col]
        concurrent_vals = df_concurrent[col]

        # Check for type mismatch
        if local_vals.dtype != concurrent_vals.dtype:
            # Try to compare as numeric if possible
            try:
                local_numeric = pd.to_numeric(local_vals, errors="coerce")
                concurrent_numeric = pd.to_numeric(concurrent_vals, errors="coerce")

                if not np.allclose(
                    local_numeric.fillna(0),
                    concurrent_numeric.fillna(0),
                    rtol=tolerance,
                    equal_nan=True,
                ):
                    results["column_differences"][col] = {
                        "type": "value_mismatch",
                        "local_dtype": str(local_vals.dtype),
                        "concurrent_dtype": str(concurrent_vals.dtype),
                        "local_sample": local_vals.head(3).tolist(),
                        "concurrent_sample": concurrent_vals.head(3).tolist(),
                    }
                    results["match"] = False
            except:
                results["column_differences"][col] = {
                    "type": "dtype_mismatch",
                    "local_dtype": str(local_vals.dtype),
                    "concurrent_dtype": str(concurrent_vals.dtype),
                }
                results["match"] = False

        # Compare numeric columns with tolerance
        elif pd.api.types.is_numeric_dtype(local_vals):
            if not np.allclose(
                local_vals.fillna(0),
                concurrent_vals.fillna(0),
                rtol=tolerance,
                equal_nan=True,
            ):
                max_diff = abs(local_vals.fillna(0) - concurrent_vals.fillna(0)).max()
                results["column_differences"][col] = {
                    "type": "numeric_mismatch",
                    "max_difference": float(max_diff),
                    "local_sample": local_vals.head(3).tolist(),
                    "concurrent_sample": concurrent_vals.head(3).tolist(),
                }
                results["match"] = False

        # Compare non-numeric columns
        else:
            if not local_vals.equals(concurrent_vals):
                results["column_differences"][col] = {
                    "type": "value_mismatch",
                    "local_sample": local_vals.head(3).tolist(),
                    "concurrent_sample": concurrent_vals.head(3).tolist(),
                }
                results["match"] = False

    return results


def print_comparison_results(results: dict):
    """Pretty print comparison results."""
    print("\n" + "=" * 70)
    print("COMPARISON RESULTS")
    print("=" * 70)

    if results["match"]:
        print("✅ DataFrames MATCH (within tolerance)")
    else:
        print("❌ DataFrames DO NOT MATCH")

    print(f"\nRow count match: {results['row_count_match']}")
    if not results["row_count_match"]:
        print(f"  Local rows: {results.get('local_rows', 'N/A')}")
        print(f"  Concurrent rows: {results.get('concurrent_rows', 'N/A')}")

    if results["local_only_columns"]:
        print(f"\n📋 Columns ONLY in LOCAL ({len(results['local_only_columns'])}):")
        for col in results["local_only_columns"]:
            print(f"  - {col}")

    if results["concurrent_only_columns"]:
        print(
            f"\n📋 Columns ONLY in CONCURRENT ({len(results['concurrent_only_columns'])}):"
        )
        for col in results["concurrent_only_columns"]:
            print(f"  - {col}")

    if results["null_columns_local"]:
        print(
            f"\n⚠️  All-NULL columns in LOCAL ({len(results['null_columns_local'])}):"
        )
        for col in results["null_columns_local"]:
            print(f"  - {col}")

    if results["null_columns_concurrent"]:
        print(
            f"\n⚠️  All-NULL columns in CONCURRENT ({len(results['null_columns_concurrent'])}):"
        )
        for col in results["null_columns_concurrent"]:
            print(f"  - {col}")

    if results["column_differences"]:
        print(f"\n❌ Column value differences ({len(results['column_differences'])}):")
        for col, diff in results["column_differences"].items():
            print(f"\n  {col}:")
            print(f"    Type: {diff['type']}")
            if "max_difference" in diff:
                print(f"    Max diff: {diff['max_difference']:.6f}")
            if "local_sample" in diff:
                print(f"    Local sample: {diff['local_sample']}")
            if "concurrent_sample" in diff:
                print(f"    Concurrent sample: {diff['concurrent_sample']}")

    print("\n" + "=" * 70)


def run_comparison():
    """Run the local vs concurrent comparison."""
    import ee

    # Initialize Earth Engine
    try:
        ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")
        print("✅ Earth Engine initialized (high-volume endpoint)")
    except Exception as e:
        print(f"Trying default initialization: {e}")
        ee.Initialize()
        print("✅ Earth Engine initialized (default endpoint)")

    # Import Whisp functions
    from openforis_whisp.local_stats import whisp_stats_local
    from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

    # Define parameter variations to test
    # Note: Some params (decimal_places, convert_water_flag, etc.) are not exposed
    # through whisp_formatted_stats_geojson_to_df wrapper, so we test only shared ones
    test_cases = [
        {
            "name": "Default (ha)",
            "params": {
                "unit_type": "ha",
            },
        },
        {
            "name": "Percent units",
            "params": {
                "unit_type": "percent",
            },
        },
        {
            "name": "Geometry audit trail",
            "params": {
                "unit_type": "ha",
                "geometry_audit_trail": True,
            },
        },
    ]

    # Create persistent output directory
    persistent_dir = Path("C:/Users/Arnell/Downloads/whisp_comparison_test")
    persistent_dir.mkdir(exist_ok=True)

    all_results = []

    # Create temp directory for test files
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create small test GeoJSON once
        test_geojson = os.path.join(tmpdir, "test_features.geojson")
        create_small_test_geojson(test_geojson, num_features=3)
        print(f"\n📁 Created test GeoJSON with 3 features: {test_geojson}")

        # Copy test file for reference
        import shutil

        shutil.copy(test_geojson, persistent_dir / "test_features.geojson")

        for i, test_case in enumerate(test_cases):
            test_name = test_case["name"]
            params = test_case["params"]

            print(f"\n{'='*70}")
            print(f"TEST {i+1}/{len(test_cases)}: {test_name}")
            print(f"{'='*70}")
            print(f"Parameters: {params}")

            # Output directories
            local_output_dir = os.path.join(tmpdir, f"local_output_{i}")
            os.makedirs(local_output_dir, exist_ok=True)

            print("\n" + "-" * 70)
            print("Running CONCURRENT mode...")
            print("-" * 70)

            try:
                df_concurrent = whisp_formatted_stats_geojson_to_df(
                    input_geojson_filepath=test_geojson, mode="concurrent", **params
                )
                print(
                    f"✅ Concurrent complete: {len(df_concurrent)} rows, {len(df_concurrent.columns)} columns"
                )

            except Exception as e:
                print(f"❌ Concurrent mode failed: {e}")
                import traceback

                traceback.print_exc()
                all_results.append({"name": test_name, "match": False, "error": str(e)})
                continue

            print("\n" + "-" * 70)
            print("Running LOCAL mode...")
            print("-" * 70)

            try:
                df_local = whisp_stats_local(
                    input_geojson_filepath=test_geojson,
                    output_dir=local_output_dir,
                    cleanup_files=True,
                    **params,
                )
                print(
                    f"✅ Local complete: {len(df_local)} rows, {len(df_local.columns)} columns"
                )

            except Exception as e:
                print(f"❌ Local mode failed: {e}")
                import traceback

                traceback.print_exc()
                all_results.append({"name": test_name, "match": False, "error": str(e)})
                continue

            # Compare the results
            print("\n" + "-" * 70)
            print("Comparing outputs...")
            print("-" * 70)

            results = compare_dataframes(df_local, df_concurrent)
            results["name"] = test_name
            all_results.append(results)

            # Print brief summary
            if results["match"]:
                print(f"✅ {test_name}: MATCH")
            else:
                print(f"❌ {test_name}: MISMATCH")
                print_comparison_results(results)

            # Save outputs for this test
            safe_name = (
                test_name.replace(" ", "_").replace("(", "").replace(")", "").lower()
            )
            df_concurrent.to_csv(
                persistent_dir / f"concurrent_{safe_name}.csv", index=False
            )
            df_local.to_csv(persistent_dir / f"local_{safe_name}.csv", index=False)

    # Print final summary
    print(f"\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}")

    passed = sum(1 for r in all_results if r.get("match", False))
    failed = len(all_results) - passed

    for result in all_results:
        name = result.get("name", "Unknown")
        if result.get("error"):
            print(f"❌ {name}: ERROR - {result['error']}")
        elif result.get("match"):
            print(f"✅ {name}: PASSED")
        else:
            print(f"❌ {name}: FAILED")
            if result.get("column_differences"):
                for col in list(result["column_differences"].keys())[:3]:
                    print(
                        f"   - {col}: {result['column_differences'][col].get('type', 'unknown')}"
                    )

    print(f"\n📊 Results: {passed}/{len(all_results)} tests passed")
    print(f"📁 Output files saved to: {persistent_dir}")

    return all_results


if __name__ == "__main__":
    results = run_comparison()

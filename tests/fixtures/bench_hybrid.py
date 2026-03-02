"""
Benchmark hybrid mode (COG static + GEE live dynamic) vs full COG and full GEE.

Compares:
1. Full COG: load_country_cog() → reduceRegions (all bands from COG)
2. Hybrid:   load_country_cog_hybrid() → reduceRegions (static from COG + dynamic from GEE)
3. Full GEE: combine_datasets() → reduceRegions (all bands live from GEE)

Uses CI (Côte d'Ivoire) test data.
"""

import ee
import time
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

ee.Initialize(
    project="ee-andyarnellgee",
    opt_url="https://earthengine-highvolume.googleapis.com",
)

from src.openforis_whisp.export_cog import load_country_cog, load_country_cog_hybrid
from src.openforis_whisp.advanced_stats import whisp_stats_geojson_to_df_concurrent

GEOJSON = os.path.join(os.path.dirname(__file__), "test_2features.geojson")
ISO2 = ["CI"]
DATE_STR = "20260223"


def bench_full_cog():
    """Full COG: all bands from pre-computed COG."""
    print("\n=== Full COG ===")
    t0 = time.time()
    img = load_country_cog(ISO2, date_str=DATE_STR)
    df = whisp_stats_geojson_to_df_concurrent(
        GEOJSON, whisp_image=img, national_codes=ISO2, batch_size=10
    )
    elapsed = time.time() - t0
    print(f"  Time: {elapsed:.1f}s | Columns: {len(df.columns)} | Rows: {len(df)}")
    return df, elapsed


def bench_hybrid():
    """Hybrid: static bands from COG + dynamic bands from live GEE."""
    print("\n=== Hybrid (COG static + GEE live dynamic) ===")
    t0 = time.time()
    img = load_country_cog_hybrid(ISO2, date_str=DATE_STR)
    df = whisp_stats_geojson_to_df_concurrent(
        GEOJSON, whisp_image=img, national_codes=ISO2, batch_size=10
    )
    elapsed = time.time() - t0
    print(f"  Time: {elapsed:.1f}s | Columns: {len(df.columns)} | Rows: {len(df)}")
    return df, elapsed


def bench_full_gee():
    """Full GEE: all bands computed live."""
    print("\n=== Full GEE (live) ===")
    t0 = time.time()
    df = whisp_stats_geojson_to_df_concurrent(
        GEOJSON, national_codes=ISO2, batch_size=10
    )
    elapsed = time.time() - t0
    print(f"  Time: {elapsed:.1f}s | Columns: {len(df.columns)} | Rows: {len(df)}")
    return df, elapsed


if __name__ == "__main__":
    # Run benchmarks
    df_cog, t_cog = bench_full_cog()
    df_hybrid, t_hybrid = bench_hybrid()
    df_gee, t_gee = bench_full_gee()

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Full COG:    {t_cog:.1f}s")
    print(f"Hybrid:      {t_hybrid:.1f}s")
    print(f"Full GEE:    {t_gee:.1f}s")

    # Compare dynamic band values between hybrid and full GEE
    dynamic_cols = [
        "RADD_after_2020",
        "DIST_after_2020",
        "GLAD-L_after_2020",
        "GLAD-S2_after_2020",
        "MODIS_fire_after_2020",
    ]
    existing_dynamic = [
        c for c in dynamic_cols if c in df_hybrid.columns and c in df_gee.columns
    ]

    if existing_dynamic:
        print(f"\nDynamic band comparison (hybrid vs full GEE):")
        for col in existing_dynamic:
            hybrid_vals = df_hybrid[col].astype(float).values
            gee_vals = df_gee[col].astype(float).values
            match = all(abs(h - g) < 0.01 for h, g in zip(hybrid_vals, gee_vals))
            print(
                f"  {col}: {'MATCH' if match else 'DIFFER'} (hybrid={hybrid_vals}, gee={gee_vals})"
            )

"""Test Int16 vs Float32 local_stats pipeline.

Runs whisp_stats_local with both output_dtype options on the same feature,
comparing download size, timing, and accuracy.
"""

import time
import os


def main():
    import ee

    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")

    from openforis_whisp.local_stats import whisp_stats_local

    GEOJSON = "tests/fixtures/test_10ha.geojson"
    OUT_FLOAT32 = "tests/fixtures/int16_test/float32"
    OUT_INT16 = "tests/fixtures/int16_test/int16"

    # ---- Float32 (current) ----
    print("=" * 60)
    print("  Float32 (current)")
    print("=" * 60)
    t0 = time.time()
    df_f32 = whisp_stats_local(
        input_geojson_filepath=GEOJSON,
        output_dir=OUT_FLOAT32,
        national_codes=["ci"],
        scale=10,
        cleanup_files=True,
        output_dtype="float32",
        verbose=True,
    )
    t_f32 = time.time() - t0
    print(f"Float32 time: {t_f32:.1f}s")
    print(f"Float32 columns: {len(df_f32.columns)}")

    # ---- Int16 (new) ----
    print()
    print("=" * 60)
    print("  Int16 (new)")
    print("=" * 60)
    t0 = time.time()
    df_i16 = whisp_stats_local(
        input_geojson_filepath=GEOJSON,
        output_dir=OUT_INT16,
        national_codes=["ci"],
        scale=10,
        cleanup_files=True,
        output_dtype="int16",
        verbose=True,
    )
    t_i16 = time.time() - t0
    print(f"Int16 time: {t_i16:.1f}s")
    print(f"Int16 columns: {len(df_i16.columns)}")

    # ---- Comparison ----
    print()
    print("=" * 60)
    print("  Comparison")
    print("=" * 60)
    print(
        f"Time:  Float32={t_f32:.1f}s  Int16={t_i16:.1f}s  " f"ratio={t_f32/t_i16:.2f}x"
    )

    # Compare numeric columns
    common_cols = sorted(set(df_f32.columns) & set(df_i16.columns))
    numeric_cols = [
        c
        for c in common_cols
        if df_f32[c].dtype in ("float64", "float32", "int64", "int32")
    ]

    print(f"\nCommon numeric columns: {len(numeric_cols)}")
    if numeric_cols:
        print(f"{'Column':<30} {'Float32':>10} {'Int16':>10} {'Diff':>10} {'%Diff':>8}")
        print("-" * 70)
        for col in numeric_cols[:20]:  # Show first 20
            v_f32 = df_f32[col].iloc[0]
            v_i16 = df_i16[col].iloc[0]
            diff = abs(v_i16 - v_f32)
            pct = abs(diff / v_f32 * 100) if v_f32 != 0 else 0
            print(f"{col:<30} {v_f32:>10.4f} {v_i16:>10.4f} {diff:>10.5f} {pct:>7.3f}%")

    # Show columns only in one
    only_f32 = set(df_f32.columns) - set(df_i16.columns)
    only_i16 = set(df_i16.columns) - set(df_f32.columns)
    if only_f32:
        print(f"\nColumns only in Float32: {sorted(only_f32)[:10]}")
    if only_i16:
        print(f"\nColumns only in Int16: {sorted(only_i16)[:10]}")


if __name__ == "__main__":
    main()

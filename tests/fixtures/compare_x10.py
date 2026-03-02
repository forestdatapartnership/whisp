"""Compare x10 scaled Int16 area vs Int32 area vs current sum."""

import time
import os
import numpy as np
import rasterio
from exactextract import exact_extract

SRC_TIF = "tests/fixtures/local_10m_test/feature_1.tif"
GEOJSON = "tests/fixtures/test_10ha.geojson"
OUT_DIR = "tests/fixtures/local_10m_test"

# Create area_int16 with x10 scaling (area_m2 * 10)
with rasterio.open(SRC_TIF) as src:
    area_data = src.read(1)
    profile = src.profile.copy()

scaled_x10 = np.round(area_data * 10).astype(np.int16)
path_x10 = os.path.join(OUT_DIR, "area_int16_x10.tif")
p = profile.copy()
p.update(dtype="int16", count=1)
with rasterio.open(path_x10, "w", **p) as dst:
    dst.write(scaled_x10, 1)

err_x10 = np.abs(area_data * 10 - scaled_x10.astype(float)).max()
print(f"area_m2 range: {area_data.min():.2f} - {area_data.max():.2f}")
print(f"x10 scaled:    {scaled_x10.min()} - {scaled_x10.max()} (fits Int16 max 32767)")
print(
    f"x10 rounding error: {err_x10:.3f} units = {err_x10/10:.4f} m2 ({err_x10/10/area_data.max()*100:.4f}%)"
)
print()

# Compare old /10 vs new x10
scaled_div10 = np.round(area_data / 10).astype(np.int16)
err_div10 = np.abs(area_data / 10 - scaled_div10.astype(float)).max()
print(
    f"OLD /10: val={scaled_div10.min()}-{scaled_div10.max()}, err={err_div10*10:.2f} m2 ({err_div10*10/area_data.max()*100:.2f}%)"
)
print(
    f"NEW x10: val={scaled_x10.min()}-{scaled_x10.max()}, err={err_x10/10:.4f} m2 ({err_x10/10/area_data.max()*100:.4f}%)"
)
print()

# Paths
binary_i16 = os.path.join(OUT_DIR, "binary_int16.tif")
binary_i8 = os.path.join(OUT_DIR, "binary_int8.tif")
area_i32 = os.path.join(OUT_DIR, "area_int32.tif")
ctx = os.path.join(OUT_DIR, "context_int16.tif")

# Warm up
exact_extract(rast=SRC_TIF, vec=GEOJSON, ops=["sum"], output="pandas")
exact_extract(rast=binary_i8, vec=GEOJSON, ops=["sum"], output="pandas")

N = 10

# A) Current sum on area-baked float32
times_A = []
for _ in range(N):
    t0 = time.time()
    exact_extract(rast=SRC_TIF, vec=GEOJSON, ops=["sum", "median"], output="pandas")
    times_A.append(time.time() - t0)

# B) ws Int16 binary + Int32 area
times_B = []
for _ in range(N):
    t0 = time.time()
    exact_extract(
        rast=binary_i16,
        vec=GEOJSON,
        ops=["weighted_sum"],
        weights=area_i32,
        output="pandas",
    )
    exact_extract(rast=ctx, vec=GEOJSON, ops=["median"], output="pandas")
    times_B.append(time.time() - t0)

# D) ws Int8 binary + Int32 area
times_D = []
for _ in range(N):
    t0 = time.time()
    exact_extract(
        rast=binary_i8,
        vec=GEOJSON,
        ops=["weighted_sum"],
        weights=area_i32,
        output="pandas",
    )
    exact_extract(rast=ctx, vec=GEOJSON, ops=["median"], output="pandas")
    times_D.append(time.time() - t0)

# F) ws Int16 binary + Int16 x10
times_F = []
for _ in range(N):
    t0 = time.time()
    exact_extract(
        rast=binary_i16,
        vec=GEOJSON,
        ops=["weighted_sum"],
        weights=path_x10,
        output="pandas",
    )
    exact_extract(rast=ctx, vec=GEOJSON, ops=["median"], output="pandas")
    times_F.append(time.time() - t0)

# G) ws Int8 binary + Int16 x10
times_G = []
for _ in range(N):
    t0 = time.time()
    exact_extract(
        rast=binary_i8,
        vec=GEOJSON,
        ops=["weighted_sum"],
        weights=path_x10,
        output="pandas",
    )
    exact_extract(rast=ctx, vec=GEOJSON, ops=["median"], output="pandas")
    times_G.append(time.time() - t0)

# ---- Accuracy ----
df_A = exact_extract(rast=SRC_TIF, vec=GEOJSON, ops=["sum"], output="pandas")
df_i32 = exact_extract(
    rast=binary_i8, vec=GEOJSON, ops=["weighted_sum"], weights=area_i32, output="pandas"
)
df_x10 = exact_extract(
    rast=binary_i8, vec=GEOJSON, ops=["weighted_sum"], weights=path_x10, output="pandas"
)

# Key bands (1-indexed in original TIF)
key_bands = [
    (1, "Area"),
    (45, "GFC_TC_2020"),
    (25, "ESA_TC_2020"),
    (96, "EUFO_2020"),
    (194, "TMF_regrowth_2023"),
    (195, "nCI_Cocoa_bnetd"),
]

print("--- Accuracy (key bands, hectares) ---")
print(
    f"{'Band':<22} {'A) sum':>10} {'D) i32':>10} {'G) x10':>10} {'D-A':>12} {'G-A':>12}"
)
for bn, name in key_bands:
    vA = df_A[f"band_{bn}_sum"].values[0] / 10000
    vD = df_i32[f"band_{bn}_weight_weighted_sum"].values[0] / 10000
    vG = df_x10[f"band_{bn}_weight_weighted_sum"].values[0] / 100000  # x10 scaling
    print(
        f"{name:<22} {vA:>10.4f} {vD:>10.4f} {vG:>10.4f} {vD-vA:>12.6f} {vG-vA:>12.6f}"
    )

# Max diff across ALL data bands (1-195)
max_dI32, max_dX10 = 0, 0
max_pI32, max_pX10 = 0, 0
for bn in range(1, 196):
    vA = df_A[f"band_{bn}_sum"].values[0] / 10000
    vD = df_i32[f"band_{bn}_weight_weighted_sum"].values[0] / 10000
    vG = df_x10[f"band_{bn}_weight_weighted_sum"].values[0] / 100000
    dD, dG = abs(vA - vD), abs(vA - vG)
    max_dI32 = max(max_dI32, dD)
    max_dX10 = max(max_dX10, dG)
    if vA > 0:
        max_pI32 = max(max_pI32, dD / vA * 100)
        max_pX10 = max(max_pX10, dG / vA * 100)

print(f"\nMax diff across all 195 data bands:")
print(f"  Int32 area:     {max_dI32:.6f} ha  ({max_pI32:.4f}%)")
print(f"  Int16 x10 area: {max_dX10:.6f} ha  ({max_pX10:.4f}%)")

# ---- Speed summary ----
baseline = np.mean(times_A)
print(f"\n--- Speed (avg of {N} runs) ---")
for label, ts in [
    ("A) sum float32 area-baked", times_A),
    ("B) ws Int16 + Int32 area", times_B),
    ("D) ws Int8 + Int32 area", times_D),
    ("F) ws Int16 + Int16 x10 area", times_F),
    ("G) ws Int8 + Int16 x10 area", times_G),
]:
    avg = np.mean(ts)
    mn = np.min(ts)
    print(f"  {label:35s} avg={avg:.3f}s  min={mn:.3f}s  ({avg/baseline:.2f}x)")

# File sizes
print(f"\n--- File sizes ---")
for name, path in [
    ("Original float32 (area-baked)", SRC_TIF),
    ("binary_int16", binary_i16),
    ("binary_int8", binary_i8),
    ("area_int32", area_i32),
    ("area_int16_x10", path_x10),
    ("context_int16", ctx),
]:
    sz = os.path.getsize(path)
    print(f"  {name:35s} {sz/1024:8.1f} KB")

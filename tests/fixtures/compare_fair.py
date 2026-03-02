"""
Fair comparison: sum on area-baked TIF vs weighted_sum on binary 0/1 + area weights.

Converts the EE-downloaded float32 TIF (area baked in) to:
  - binary_int16.tif: 195 bands, 0/1 Int16 (data bands binarized)
  - binary_int8.tif: 195 bands, 0/1 Int8
  - area_int32.tif: 1 band, pixel area in m2 (Int32)
  - area_int16.tif: 1 band, pixel area / 10 (Int16, scaled)

Then compares extraction speed and accuracy across methods.
"""

import time
import os
import numpy as np
import rasterio
from exactextract import exact_extract

SRC_TIF = "tests/fixtures/local_10m_test/feature_1.tif"
GEOJSON = "tests/fixtures/test_10ha.geojson"
OUT_DIR = "tests/fixtures/local_10m_test"

# Band indices (1-based) — from EE band order
AREA_BAND = 1
ADMIN_BAND = 196
WATER_BAND = 197
NUM_BANDS = 197

# Bands to keep as binary data (exclude Area, admin_code, In_waterbody)
DATA_BANDS = [i for i in range(1, NUM_BANDS + 1) if i not in (ADMIN_BAND, WATER_BAND)]
# admin + waterbody kept in a separate "context" raster
CONTEXT_BANDS = [ADMIN_BAND, WATER_BAND]

BAND_NAMES = [
    "Area",
    "Oil_palm_Descals",
    "European_Primary_Forest",
    "ESA_fire_before_2020",
    "ESA_fire_2001",
    "ESA_fire_2002",
    "ESA_fire_2003",
    "ESA_fire_2004",
    "ESA_fire_2005",
    "ESA_fire_2006",
    "ESA_fire_2007",
    "ESA_fire_2008",
    "ESA_fire_2009",
    "ESA_fire_2010",
    "ESA_fire_2011",
    "ESA_fire_2012",
    "ESA_fire_2013",
    "ESA_fire_2014",
    "ESA_fire_2015",
    "ESA_fire_2016",
    "ESA_fire_2017",
    "ESA_fire_2018",
    "ESA_fire_2019",
    "ESA_fire_2020",
    "ESA_TC_2020",
    "ESRI_crop_gain_2020_2023",
    "ESRI_2023_TC",
    "Cocoa_ETH",
    "Cocoa_2023_FDaP",
    "Cocoa_FDaP",
    "Coffee_FDaP",
    "Coffee_FDaP_2023",
    "Forest_FDaP",
    "Oil_palm_2023_FDaP",
    "Oil_palm_FDaP",
    "Rubber_2023_FDaP",
    "Rubber_FDaP",
    "GFT_naturally_regenerating",
    "GFT_planted_plantation",
    "GFT_primary",
    "DIST_after_2020",
    "DIST_year_2024",
    "DIST_year_2025",
    "DIST_year_2026",
    "GFC_TC_2020",
    "GFC_loss_after_2020",
    "GFC_loss_before_2020",
    "GFC_loss_year_2001",
    "GFC_loss_year_2002",
    "GFC_loss_year_2003",
    "GFC_loss_year_2004",
    "GFC_loss_year_2005",
    "GFC_loss_year_2006",
    "GFC_loss_year_2007",
    "GFC_loss_year_2008",
    "GFC_loss_year_2009",
    "GFC_loss_year_2010",
    "GFC_loss_year_2011",
    "GFC_loss_year_2012",
    "GFC_loss_year_2013",
    "GFC_loss_year_2014",
    "GFC_loss_year_2015",
    "GFC_loss_year_2016",
    "GFC_loss_year_2017",
    "GFC_loss_year_2018",
    "GFC_loss_year_2019",
    "GFC_loss_year_2020",
    "GFC_loss_year_2021",
    "GFC_loss_year_2022",
    "GFC_loss_year_2023",
    "GFC_loss_year_2024",
    "GLAD-L_after_2020",
    "GLAD-L_before_2020",
    "GLAD-L_year_2017",
    "GLAD-L_year_2018",
    "GLAD-L_year_2019",
    "GLAD-L_year_2020",
    "GLAD-L_year_2021",
    "GLAD-L_year_2022",
    "GLAD-L_year_2023",
    "GLAD-L_year_2025",
    "GLAD-L_year_2026",
    "GLAD_Primary",
    "GLAD-S2_after_2020",
    "GLAD-S2_before_2020",
    "GLAD-S2_year_2019",
    "GLAD-S2_year_2020",
    "GLAD-S2_year_2021",
    "GLAD-S2_year_2022",
    "GLAD-S2_year_2023",
    "GLAD-S2_year_2024",
    "GLAD-S2_year_2025",
    "GLAD-S2_year_2026",
    "IFL_2020",
    "IIASA_planted_plantation",
    "EUFO_2020",
    "TMF_plant",
    "TMF_undist",
    "GFW_logging_before_2020",
    "MODIS_fire_after_2020",
    "MODIS_fire_before_2020",
    "MODIS_fire_2000",
    "MODIS_fire_2001",
    "MODIS_fire_2002",
    "MODIS_fire_2003",
    "MODIS_fire_2004",
    "MODIS_fire_2005",
    "MODIS_fire_2006",
    "MODIS_fire_2007",
    "MODIS_fire_2008",
    "MODIS_fire_2009",
    "MODIS_fire_2010",
    "MODIS_fire_2011",
    "MODIS_fire_2012",
    "MODIS_fire_2013",
    "MODIS_fire_2014",
    "MODIS_fire_2015",
    "MODIS_fire_2016",
    "MODIS_fire_2017",
    "MODIS_fire_2018",
    "MODIS_fire_2019",
    "MODIS_fire_2020",
    "MODIS_fire_2021",
    "MODIS_fire_2022",
    "MODIS_fire_2023",
    "MODIS_fire_2024",
    "MODIS_fire_2025",
    "RADD_after_2020",
    "RADD_before_2020",
    "RADD_year_2019",
    "RADD_year_2020",
    "RADD_year_2021",
    "RADD_year_2022",
    "RADD_year_2023",
    "RADD_year_2024",
    "RADD_year_2025",
    "RADD_year_2026",
    "Rubber_RBGE",
    "Soy_Song_2020",
    "TMF_def_after_2020",
    "TMF_def_before_2020",
    "TMF_def_2000",
    "TMF_def_2001",
    "TMF_def_2002",
    "TMF_def_2003",
    "TMF_def_2004",
    "TMF_def_2005",
    "TMF_def_2006",
    "TMF_def_2007",
    "TMF_def_2008",
    "TMF_def_2009",
    "TMF_def_2010",
    "TMF_def_2011",
    "TMF_def_2012",
    "TMF_def_2013",
    "TMF_def_2014",
    "TMF_def_2015",
    "TMF_def_2016",
    "TMF_def_2017",
    "TMF_def_2018",
    "TMF_def_2019",
    "TMF_def_2020",
    "TMF_def_2021",
    "TMF_def_2022",
    "TMF_def_2023",
    "TMF_def_2024",
    "TMF_deg_after_2020",
    "TMF_deg_before_2020",
    "TMF_deg_2000",
    "TMF_deg_2001",
    "TMF_deg_2002",
    "TMF_deg_2003",
    "TMF_deg_2004",
    "TMF_deg_2005",
    "TMF_deg_2006",
    "TMF_deg_2007",
    "TMF_deg_2008",
    "TMF_deg_2009",
    "TMF_deg_2010",
    "TMF_deg_2011",
    "TMF_deg_2012",
    "TMF_deg_2013",
    "TMF_deg_2014",
    "TMF_deg_2015",
    "TMF_deg_2016",
    "TMF_deg_2017",
    "TMF_deg_2018",
    "TMF_deg_2019",
    "TMF_deg_2020",
    "TMF_deg_2021",
    "TMF_deg_2022",
    "TMF_deg_2023",
    "TMF_deg_2024",
    "TMF_regrowth_2023",
    "nCI_Cocoa_bnetd",
    "admin_code",
    "In_waterbody",
]

KEY_COLS = [
    "Area",
    "GFC_TC_2020",
    "EUFO_2020",
    "TMF_undist",
    "ESA_TC_2020",
    "TMF_regrowth_2023",
    "nCI_Cocoa_bnetd",
]


# ============================================================================
# Step 1: Create split TIFs from the EE-downloaded float32
# ============================================================================


def create_split_tifs():
    """Convert area-baked float32 TIF into binary + area + context TIFs."""
    with rasterio.open(SRC_TIF) as src:
        profile = src.profile.copy()
        area_data = src.read(AREA_BAND)  # ~98.6 m2 per pixel at 10m

        # --- Area Int32 ---
        area_int32_path = os.path.join(OUT_DIR, "area_int32.tif")
        p = profile.copy()
        p.update(dtype="int32", count=1)
        with rasterio.open(area_int32_path, "w", **p) as dst:
            dst.write(np.round(area_data).astype(np.int32), 1)
            dst.set_band_description(1, "pixel_area_m2")

        # --- Area Int16 scaled (m2 / 10) ---
        area_int16_path = os.path.join(OUT_DIR, "area_int16_scaled.tif")
        scaled = np.round(area_data / 10).astype(np.int16)
        p = profile.copy()
        p.update(dtype="int16", count=1)
        with rasterio.open(area_int16_path, "w", **p) as dst:
            dst.write(scaled, 1)
            dst.set_band_description(1, "pixel_area_m2_div10")

        # --- Binary Int16 (all 195 data bands → 0/1) ---
        binary_int16_path = os.path.join(OUT_DIR, "binary_int16.tif")
        p = profile.copy()
        p.update(dtype="int16", count=len(DATA_BANDS))
        with rasterio.open(binary_int16_path, "w", **p) as dst:
            for out_idx, src_band in enumerate(DATA_BANDS):
                data = src.read(src_band)
                binary = (data > 0).astype(np.int16)
                dst.write(binary, out_idx + 1)
                dst.set_band_description(out_idx + 1, BAND_NAMES[src_band - 1])

        # --- Binary Int8 (same, but Int8 = half the size) ---
        binary_int8_path = os.path.join(OUT_DIR, "binary_int8.tif")
        p = profile.copy()
        p.update(dtype="int8", count=len(DATA_BANDS))
        with rasterio.open(binary_int8_path, "w", **p) as dst:
            for out_idx, src_band in enumerate(DATA_BANDS):
                data = src.read(src_band)
                binary = (data > 0).astype(np.int8)
                dst.write(binary, out_idx + 1)
                dst.set_band_description(out_idx + 1, BAND_NAMES[src_band - 1])

        # --- Context (admin_code + In_waterbody, Int16) ---
        context_path = os.path.join(OUT_DIR, "context_int16.tif")
        p = profile.copy()
        p.update(dtype="int16", count=len(CONTEXT_BANDS))
        with rasterio.open(context_path, "w", **p) as dst:
            for out_idx, src_band in enumerate(CONTEXT_BANDS):
                data = src.read(src_band)
                dst.write(data.astype(np.int16), out_idx + 1)
                dst.set_band_description(out_idx + 1, BAND_NAMES[src_band - 1])

    # File sizes
    for name in [
        "area_int32.tif",
        "area_int16_scaled.tif",
        "binary_int16.tif",
        "binary_int8.tif",
        "context_int16.tif",
    ]:
        path = os.path.join(OUT_DIR, name)
        sz = os.path.getsize(path) / 1024
        print(f"  {name:30s} {sz:8.1f} KB")

    print(f"\n  Area range: {area_data.min():.2f} - {area_data.max():.2f} m2")
    print(f"  Scaled Int16: {scaled.min()} - {scaled.max()} (m2/10)")
    err = np.abs(area_data / 10 - scaled.astype(float)).max()
    print(
        f"  Max scaling error: {err:.2f} units = {err * 10:.1f} m2 ({err * 10 / area_data.max() * 100:.2f}%)"
    )

    return {
        "area_int32": area_int32_path,
        "area_int16": area_int16_path,
        "binary_int16": binary_int16_path,
        "binary_int8": binary_int8_path,
        "context": context_path,
    }


# ============================================================================
# Step 2: Extraction methods
# ============================================================================


def method_sum_areabaked(n_runs=1):
    """Current approach: sum on area-baked float32 TIF (+ median for categorical)."""
    times = []
    result = None
    for _ in range(n_runs):
        start = time.time()
        df = exact_extract(
            rast=SRC_TIF, vec=GEOJSON, ops=["sum", "median"], output="pandas"
        )
        times.append(time.time() - start)
        if result is None:
            result = {}
            for i, name in enumerate(BAND_NAMES):
                bn = i + 1
                if name in ("admin_code", "In_waterbody"):
                    result[name] = df[f"band_{bn}_median"].values[0]
                else:
                    # sum of area-baked values / 10000 = hectares
                    result[name] = df[f"band_{bn}_sum"].values[0] / 10000
    return result, times


def method_weighted_sum(binary_path, area_path, scale_factor, context_path, n_runs=1):
    """weighted_sum on binary 0/1 with area weights (+ median for context)."""
    times = []
    result = None
    for _ in range(n_runs):
        start = time.time()
        df_ws = exact_extract(
            rast=binary_path,
            vec=GEOJSON,
            ops=["weighted_sum"],
            weights=area_path,
            output="pandas",
        )
        df_ctx = exact_extract(
            rast=context_path,
            vec=GEOJSON,
            ops=["median"],
            output="pandas",
        )
        times.append(time.time() - start)

        if result is None:
            result = {}
            # Map binary bands (DATA_BANDS order)
            for out_idx, src_band in enumerate(DATA_BANDS):
                name = BAND_NAMES[src_band - 1]
                col = f"band_{out_idx + 1}_weight_weighted_sum"
                result[name] = df_ws[col].values[0] * scale_factor / 10000  # to ha
            # Map context bands
            for out_idx, src_band in enumerate(CONTEXT_BANDS):
                name = BAND_NAMES[src_band - 1]
                col = f"band_{out_idx + 1}_median"
                result[name] = df_ctx[col].values[0]
    return result, times


# ============================================================================
# Main
# ============================================================================

if __name__ == "__main__":
    print("=" * 80)
    print("FAIR COMPARISON: 10m, 1 feature (10ha), same downloaded data")
    print("=" * 80)

    # Create TIFs
    print("\n--- Creating split TIFs ---")
    paths = create_split_tifs()

    # Warm-up
    print("\n--- Warm-up ---")
    exact_extract(rast=SRC_TIF, vec=GEOJSON, ops=["sum"], output="pandas")
    exact_extract(rast=paths["binary_int16"], vec=GEOJSON, ops=["sum"], output="pandas")
    exact_extract(rast=paths["binary_int8"], vec=GEOJSON, ops=["sum"], output="pandas")
    print("Done")

    N = 10
    print(f"\n--- Running {N} iterations each ---")

    # Method A: Current - sum on area-baked float32
    rA, tA = method_sum_areabaked(N)
    print(
        f"  A) sum (float32 area-baked):      avg={np.mean(tA):.3f}s  min={np.min(tA):.3f}s"
    )

    # Method B: weighted_sum, Int16 binary + Int32 area
    rB, tB = method_weighted_sum(
        paths["binary_int16"], paths["area_int32"], 1, paths["context"], N
    )
    print(
        f"  B) ws (Int16 binary + Int32 area): avg={np.mean(tB):.3f}s  min={np.min(tB):.3f}s"
    )

    # Method C: weighted_sum, Int16 binary + Int16 scaled area
    rC, tC = method_weighted_sum(
        paths["binary_int16"], paths["area_int16"], 10, paths["context"], N
    )
    print(
        f"  C) ws (Int16 binary + Int16 area): avg={np.mean(tC):.3f}s  min={np.min(tC):.3f}s"
    )

    # Method D: weighted_sum, Int8 binary + Int32 area
    rD, tD = method_weighted_sum(
        paths["binary_int8"], paths["area_int32"], 1, paths["context"], N
    )
    print(
        f"  D) ws (Int8 binary + Int32 area):  avg={np.mean(tD):.3f}s  min={np.min(tD):.3f}s"
    )

    # Method E: weighted_sum, Int8 binary + Int16 scaled area
    rE, tE = method_weighted_sum(
        paths["binary_int8"], paths["area_int16"], 10, paths["context"], N
    )
    print(
        f"  E) ws (Int8 binary + Int16 area):  avg={np.mean(tE):.3f}s  min={np.min(tE):.3f}s"
    )

    # Results comparison
    print(f"\n--- Accuracy (key columns, in hectares) ---")
    methods = {
        "A) sum f32": rA,
        "B) ws i16+i32": rB,
        "C) ws i16+i16": rC,
        "D) ws i8+i32": rD,
        "E) ws i8+i16": rE,
    }

    header = (
        f"{'Band':<22}"
        + "".join(f"{m:>15}" for m in methods)
        + f"{'B-A diff':>12}"
        + f"{'C-A diff':>12}"
    )
    print(header)
    print("-" * len(header))

    for col in KEY_COLS:
        vals = {}
        for label, r in methods.items():
            vals[label] = r.get(col, 0) or 0

        fmt = ".3f" if col not in ("admin_code", "In_waterbody") else ".0f"
        parts = f"{col:<22}"
        for label in methods:
            parts += f"{vals[label]:>15{fmt}}"
        # Diff B-A and C-A
        dBA = vals["B) ws i16+i32"] - vals["A) sum f32"]
        dCA = vals["C) ws i16+i16"] - vals["A) sum f32"]
        parts += f"{dBA:>12.4f}{dCA:>12.4f}"
        print(parts)

    # admin_code
    print(
        f"\n  admin_code: A={rA.get('admin_code', 'N/A'):.0f}  B={rB.get('admin_code', 'N/A'):.0f}  (should match)"
    )
    print(
        f"  In_waterbody: A={rA.get('In_waterbody', 'N/A'):.1f}  B={rB.get('In_waterbody', 'N/A'):.1f}"
    )

    # Max diff across ALL bands
    print(f"\n--- Max difference across all {len(DATA_BANDS)} data bands ---")
    max_d = {"B-A": 0, "C-A": 0, "D-A": 0, "E-A": 0}
    max_pct = {"B-A": 0, "C-A": 0, "D-A": 0, "E-A": 0}
    for name in BAND_NAMES:
        if name in ("admin_code", "In_waterbody"):
            continue
        vA = rA.get(name, 0) or 0
        for tag, rX in [("B-A", rB), ("C-A", rC), ("D-A", rD), ("E-A", rE)]:
            vX = rX.get(name, 0) or 0
            d = abs(vA - vX)
            max_d[tag] = max(max_d[tag], d)
            if vA > 0:
                max_pct[tag] = max(max_pct[tag], d / vA * 100)

    for tag in max_d:
        print(
            f"  {tag}: max abs diff = {max_d[tag]:.6f} ha, max rel diff = {max_pct[tag]:.4f}%"
        )

    # File size comparison
    print(f"\n--- File sizes ---")
    print(f"  Original float32 (area-baked):  {os.path.getsize(SRC_TIF)/1024:8.1f} KB")
    for name, path in paths.items():
        print(f"  {name:30s}:  {os.path.getsize(path)/1024:8.1f} KB")

    # Speed summary
    print(f"\n--- Speed summary (avg of {N} runs) ---")
    labels_times = [
        ("A) sum (float32 area-baked)", tA),
        ("B) ws (Int16 binary + Int32 area)", tB),
        ("C) ws (Int16 binary + Int16 area)", tC),
        ("D) ws (Int8 binary + Int32 area)", tD),
        ("E) ws (Int8 binary + Int16 area)", tE),
    ]
    baseline = np.mean(tA)
    for label, ts in labels_times:
        avg = np.mean(ts)
        ratio = avg / baseline if baseline > 0 else 0
        print(f"  {label:40s} {avg:.3f}s  ({ratio:.2f}x vs A)")

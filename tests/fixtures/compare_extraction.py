"""Compare extraction speed: current sum vs weighted_sum with Int16 scaled area."""

import time
import numpy as np
import rasterio
from exactextract import exact_extract

TIF_PATH = "tests/fixtures/local_10m_test/feature_1.tif"
GEOJSON_PATH = "tests/fixtures/test_10ha.geojson"

# Band names from EE image (197 bands)
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

CATEGORICAL = {"admin_code", "In_waterbody"}
KEY_COLS = [
    "Area",
    "GFC_TC_2020",
    "EUFO_2020",
    "TMF_undist",
    "TMF_regrowth_2023",
    "nCI_Cocoa_bnetd",
    "admin_code",
]


def method_current_sum():
    """Current method: ops=['sum', 'median'] on the multiband float32 TIF."""
    ops = ["sum", "median"]
    start = time.time()
    df = exact_extract(rast=TIF_PATH, vec=GEOJSON_PATH, ops=ops, output="pandas")
    elapsed = time.time() - start

    # Rename columns using band names
    rename = {}
    for i, name in enumerate(BAND_NAMES):
        for op in ops:
            old = f"band_{i+1}_{op}"
            if old in df.columns:
                rename[old] = f"{name}_{op}"
    df = df.rename(columns=rename)

    # Build result dict: sum for area bands, median for categorical
    result = {}
    for name in BAND_NAMES:
        if name in CATEGORICAL:
            col = f"{name}_median"
            result[name] = df[col].values[0] if col in df.columns else None
        else:
            col = f"{name}_sum"
            result[name] = df[col].values[0] if col in df.columns else None

    return result, elapsed


def create_area_tifs():
    """Extract band 1 (Area) into separate Int32 and scaled Int16 TIFs."""
    with rasterio.open(TIF_PATH) as src:
        area_data = src.read(1)  # Band 1 = Area (pixel area in m2, float32)
        profile = src.profile.copy()
        transform = src.transform
        crs = src.crs

    # Int32 area (full precision m2)
    int32_path = "tests/fixtures/local_10m_test/area_int32.tif"
    p32 = profile.copy()
    p32.update(dtype="int32", count=1)
    with rasterio.open(int32_path, "w", **p32) as dst:
        dst.write(area_data.astype(np.int32), 1)

    # Scaled Int16 area (area_m2 / 10 — fits in Int16 for 10m pixels: ~100m2/10 = 10)
    int16_path = "tests/fixtures/local_10m_test/area_int16_scaled.tif"
    scaled = (area_data / 10).astype(np.int16)
    p16 = profile.copy()
    p16.update(dtype="int16", count=1)
    with rasterio.open(int16_path, "w", **p16) as dst:
        dst.write(scaled, 1)

    print(f"Area float32 range: {area_data.min():.1f} - {area_data.max():.1f} m2")
    print(
        f"Area int32 range:   {area_data.astype(np.int32).min()} - {area_data.astype(np.int32).max()} m2"
    )
    print(f"Area int16 scaled:  {scaled.min()} - {scaled.max()} (area_m2/10)")
    rounding_err = np.abs(area_data / 10 - scaled.astype(float)).max()
    print(f"Max rounding error: {rounding_err:.2f} units = {rounding_err * 10:.1f} m2")

    return int32_path, int16_path


def method_weighted_sum(area_path, scale_factor=1):
    """weighted_sum approach: binary bands * area weights, then convert to ha."""
    start = time.time()

    # Run weighted_sum on all bands
    df_ws = exact_extract(
        rast=TIF_PATH,
        vec=GEOJSON_PATH,
        ops=["weighted_sum"],
        weights=area_path,
        output="pandas",
    )

    # Run median on all bands (for categorical — fast, just pick what we need)
    df_med = exact_extract(
        rast=TIF_PATH,
        vec=GEOJSON_PATH,
        ops=["median"],
        output="pandas",
    )

    elapsed = time.time() - start

    # Build result dict
    result = {}
    for i, name in enumerate(BAND_NAMES):
        bn = i + 1
        if name in CATEGORICAL:
            col = f"band_{bn}_median"
            result[name] = df_med[col].values[0] if col in df_med.columns else None
        else:
            col = f"band_{bn}_weight_weighted_sum"
            if col in df_ws.columns:
                result[name] = df_ws[col].values[0] * scale_factor / 10000  # to ha
            else:
                result[name] = None

    return result, elapsed


if __name__ == "__main__":
    print("=" * 80)
    print("EXTRACTION SPEED COMPARISON: 10m resolution, 1 feature (10ha)")
    print("=" * 80)

    # Create area TIFs
    print("\n--- Creating area band TIFs ---")
    int32_path, int16_path = create_area_tifs()

    # Run warm-up (first call is slower due to file caching)
    print("\n--- Warm-up run ---")
    _ = exact_extract(rast=TIF_PATH, vec=GEOJSON_PATH, ops=["sum"], output="pandas")
    print("Done")

    # Method 1: Current sum approach
    print("\n--- Method 1: Current sum + median (float32 TIF) ---")
    r_sum, t_sum = method_current_sum()
    print(f"Time: {t_sum:.3f}s")

    # Method 2: weighted_sum with Int32 area
    print("\n--- Method 2: weighted_sum + Int32 area ---")
    r_ws32, t_ws32 = method_weighted_sum(int32_path, scale_factor=1)
    print(f"Time: {t_ws32:.3f}s")

    # Method 3: weighted_sum with scaled Int16 area
    print("\n--- Method 3: weighted_sum + Int16 scaled area (m2/10) ---")
    r_ws16, t_ws16 = method_weighted_sum(int16_path, scale_factor=10)
    print(f"Time: {t_ws16:.3f}s")

    # Run each 5 times for timing
    print("\n--- Timing (5 runs each) ---")
    times = {"sum": [], "ws_int32": [], "ws_int16": []}
    for _ in range(5):
        _, t = method_current_sum()
        times["sum"].append(t)
        _, t = method_weighted_sum(int32_path, 1)
        times["ws_int32"].append(t)
        _, t = method_weighted_sum(int16_path, 10)
        times["ws_int16"].append(t)

    for name, ts in times.items():
        avg = np.mean(ts)
        mn = np.min(ts)
        mx = np.max(ts)
        print(f"  {name:12s}: avg={avg:.3f}s  min={mn:.3f}s  max={mx:.3f}s")

    # Compare results
    print("\n--- Results Comparison ---")
    header = f"{'Band':<25} {'sum':>12} {'ws_int32':>12} {'ws_int16':>12} {'sum-ws32':>12} {'sum-ws16':>12}"
    print(header)
    print("-" * len(header))

    for col in KEY_COLS:
        v1 = r_sum.get(col)
        v2 = r_ws32.get(col)
        v3 = r_ws16.get(col)

        if col in CATEGORICAL:
            s1 = f"{v1:.0f}" if v1 is not None else "N/A"
            s2 = f"{v2:.0f}" if v2 is not None else "N/A"
            s3 = f"{v3:.0f}" if v3 is not None else "N/A"
            d1 = ""
            d2 = ""
        else:
            s1 = f"{v1:.4f}" if v1 is not None else "N/A"
            s2 = f"{v2:.4f}" if v2 is not None else "N/A"
            s3 = f"{v3:.4f}" if v3 is not None else "N/A"
            d1 = f"{v1 - v2:.6f}" if v1 is not None and v2 is not None else ""
            d2 = f"{v1 - v3:.6f}" if v1 is not None and v3 is not None else ""

        print(f"{col:<25} {s1:>12} {s2:>12} {s3:>12} {d1:>12} {d2:>12}")

    # Compare ALL non-zero bands
    print("\n--- All non-zero bands difference check ---")
    max_diff_32 = 0
    max_diff_16 = 0
    nonzero_count = 0
    for name in BAND_NAMES:
        if name in CATEGORICAL:
            continue
        v1 = r_sum.get(name, 0) or 0
        v2 = r_ws32.get(name, 0) or 0
        v3 = r_ws16.get(name, 0) or 0
        if v1 != 0 or v2 != 0 or v3 != 0:
            nonzero_count += 1
            d32 = abs(v1 - v2)
            d16 = abs(v1 - v3)
            max_diff_32 = max(max_diff_32, d32)
            max_diff_16 = max(max_diff_16, d16)
            if d32 > 0.001 or d16 > 0.001:
                print(
                    f"  {name:<30} sum={v1:.4f}  ws32={v2:.4f}  ws16={v3:.4f}  d32={d32:.6f}  d16={d16:.6f}"
                )

    print(f"\nNon-zero bands: {nonzero_count}")
    print(f"Max diff (sum vs ws_int32): {max_diff_32:.6f} ha")
    print(f"Max diff (sum vs ws_int16): {max_diff_16:.6f} ha")

    print("\n--- Summary ---")
    print(f"current local pipeline (full):  ~20.6s (incl. EE download)")
    t_avg_sum = np.mean(times["sum"])
    t_avg_32 = np.mean(times["ws_int32"])
    t_avg_16 = np.mean(times["ws_int16"])
    print(f"sum (post-download):            {t_avg_sum:.3f}s")
    print(f"weighted_sum Int32 area:         {t_avg_32:.3f}s")
    print(f"weighted_sum Int16 scaled area:  {t_avg_16:.3f}s")
    if t_avg_sum > 0:
        print(f"Speedup ws_int32 vs sum: {t_avg_sum/t_avg_32:.1f}x")
        print(f"Speedup ws_int16 vs sum: {t_avg_sum/t_avg_16:.1f}x")

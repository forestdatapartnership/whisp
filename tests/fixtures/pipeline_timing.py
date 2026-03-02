"""Pipeline timing: current sum vs weighted_sum (Int8+Int16x10) approach.

Tests 1, 5, 10 features from geojson_example.geojson (all < 50ha).
Breaks down: bbox, EE download, split (binary+area), VRT build, stats.
Compares sum on Float32 area-baked TIF vs weighted_sum on Int8 binary + Int16 area.
"""

import time
import os
import glob
import numpy as np
import geopandas as gpd
import rasterio
from rio_vrt import build_vrt as rio_build_vrt


def split_tif_to_binary_and_area(tif_path, out_dir, n_data_bands=195):
    """Split a Float32 area-baked TIF into Int8 binary + Int16 x10 area + Int16 context.

    Returns (binary_path, area_path, context_path).
    """
    basename = os.path.splitext(os.path.basename(tif_path))[0]
    binary_path = os.path.join(out_dir, f"{basename}_bin.tif")
    area_path = os.path.join(out_dir, f"{basename}_area.tif")
    ctx_path = os.path.join(out_dir, f"{basename}_ctx.tif")

    with rasterio.open(tif_path) as src:
        profile = src.profile.copy()
        # Area from band 1, scaled x10 → Int16
        area_data = src.read(1)
        area_scaled = np.round(area_data * 10).astype(np.int16)

        p = profile.copy()
        p.update(dtype="int16", count=1)
        with rasterio.open(area_path, "w", **p) as dst:
            dst.write(area_scaled, 1)

        # Binary Int16 (bands 1..n_data_bands) — using Int16 since rio_vrt lacks Int8
        p = profile.copy()
        p.update(dtype="int16", count=n_data_bands)
        with rasterio.open(binary_path, "w", **p) as dst:
            for i in range(n_data_bands):
                data = src.read(i + 1)
                dst.write((data > 0).astype(np.int16), i + 1)

        # Context Int16 (last 2 bands: admin_code, In_waterbody)
        total_bands = src.count
        p = profile.copy()
        p.update(dtype="int16", count=2)
        with rasterio.open(ctx_path, "w", **p) as dst:
            dst.write(src.read(total_bands - 1).astype(np.int16), 1)
            dst.write(src.read(total_bands).astype(np.int16), 2)

    return binary_path, area_path, ctx_path


def make_vrt(tif_paths, vrt_path):
    """Build a VRT from TIF paths. Returns single TIF path if only 1 file."""
    if len(tif_paths) == 1:
        return tif_paths[0]
    rio_build_vrt(vrt_path, tif_paths)
    return vrt_path


def main():
    import ee
    from exactextract import exact_extract

    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")

    from openforis_whisp.datasets import combine_datasets
    from openforis_whisp.local_stats import (
        convert_geojson_to_ee_bbox_obscured,
        download_geotiffs_for_feature_collection,
        create_vrt_from_folder,
        delete_all_files_in_folder,
    )

    SRC_GEOJSON = "tests/fixtures/geojson_example.geojson"
    BASE_DIR = "tests/fixtures/timing_test"
    SCALE = 10
    N_DATA_BANDS = 195  # bands 1-195 are data, 196=admin_code, 197=In_waterbody

    print("Compiling whisp image...")
    image = combine_datasets(national_codes=["ci"], include_context_bands=True)
    band_names = image.bandNames().getInfo()
    num_bands = len(band_names)
    print(f"Image: {num_bands} bands\n")

    gdf_all = gpd.read_file(SRC_GEOJSON)

    subsets = {
        "1 feat (9ha)": [9],
        "5 feat (0.2-40ha)": [1, 4, 9, 10, 11],
        "10 feat (0.2-40ha)": [1, 3, 4, 7, 8, 9, 10, 11, 13, 16],
    }

    gdf_all["area_ha_approx"] = gdf_all.geometry.area * (111320**2) / 10000
    for label, indices in subsets.items():
        areas = gdf_all.iloc[indices]["area_ha_approx"]
        print(
            f"  {label}: total ~{areas.sum():.0f} ha, "
            f"range {areas.min():.1f}-{areas.max():.1f} ha"
        )
    print()

    results = []

    for label, indices in subsets.items():
        print(f"{'='*70}")
        print(f"  {label}")
        print(f"{'='*70}")

        gdf_sub = gdf_all.iloc[indices].copy()
        out_dir = os.path.join(BASE_DIR, label.split("(")[0].strip().replace(" ", "_"))
        os.makedirs(out_dir, exist_ok=True)
        for ext in ["*.tif", "*.vrt"]:
            delete_all_files_in_folder(out_dir, ext, verbose=False)

        sub_geojson = os.path.join(out_dir, "subset.geojson")
        gdf_sub.to_file(sub_geojson, driver="GeoJSON")

        # --- 1) Obscured bbox ---
        t0 = time.time()
        obscured_fc = convert_geojson_to_ee_bbox_obscured(
            sub_geojson,
            extension_range=(0.002, 0.005),
            shift_geometries=True,
            shift_proportion=0.5,
            pixel_length=0.0001,
            verbose=False,
        )
        t_bbox = time.time() - t0

        # --- 2) EE Download ---
        t0 = time.time()
        geotiff_paths = download_geotiffs_for_feature_collection(
            feature_collection=obscured_fc,
            output_dir=out_dir,
            image=image,
            scale=SCALE,
            max_workers=10,
            num_bands=num_bands,
        )
        t_download = time.time() - t0
        total_mb = sum(os.path.getsize(p) for p in geotiff_paths) / (1024 * 1024)

        # --- 3a) VRT creation (current: all-band Float32) ---
        t0 = time.time()
        vrt_path = create_vrt_from_folder(out_dir, verbose=False)
        t_vrt_current = time.time() - t0

        # --- 3b) Split each TIF → binary Int8 + area Int16 x10 + context Int16 ---
        t0 = time.time()
        bin_paths, area_paths, ctx_paths = [], [], []
        for tif in geotiff_paths:
            bp, ap, cp = split_tif_to_binary_and_area(tif, out_dir, N_DATA_BANDS)
            bin_paths.append(bp)
            area_paths.append(ap)
            ctx_paths.append(cp)
        t_split = time.time() - t0

        # Build VRTs for binary, area, context
        t0 = time.time()
        bin_vrt = make_vrt(bin_paths, os.path.join(out_dir, "binary.vrt"))
        area_vrt = make_vrt(area_paths, os.path.join(out_dir, "area.vrt"))
        ctx_vrt = make_vrt(ctx_paths, os.path.join(out_dir, "context.vrt"))
        t_vrt_ws = time.time() - t0

        # --- 4) Stats: run both methods N times ---
        N_REPEAT = 3
        ts_sum, ts_ws = [], []
        for _ in range(N_REPEAT):
            # Current: sum+median on Float32 VRT
            t0 = time.time()
            df_sum = exact_extract(
                rast=vrt_path, vec=sub_geojson, ops=["sum", "median"], output="pandas"
            )
            ts_sum.append(time.time() - t0)

            # WS: weighted_sum on Int8 binary + Int16 area, plus median on context
            t0 = time.time()
            df_ws = exact_extract(
                rast=bin_vrt,
                vec=sub_geojson,
                ops=["weighted_sum"],
                weights=area_vrt,
                output="pandas",
            )
            df_ctx = exact_extract(
                rast=ctx_vrt, vec=sub_geojson, ops=["median"], output="pandas"
            )
            ts_ws.append(time.time() - t0)

        avg_sum = np.mean(ts_sum)
        avg_ws = np.mean(ts_ws)

        total_current = t_bbox + t_download + t_vrt_current + avg_sum
        total_ws = t_bbox + t_download + t_split + t_vrt_ws + avg_ws

        # Area accuracy check (first feature)
        area_sum = df_sum["band_1_sum"].values[0] / 10000
        area_ws = df_ws["band_1_weight_weighted_sum"].values[0] / 100000  # x10 scaling
        diff = abs(area_ws - area_sum)

        row = {
            "label": label,
            "n": len(gdf_sub),
            "mb": total_mb,
            "dl": t_download,
            "vrt_cur": t_vrt_current,
            "split": t_split,
            "vrt_ws": t_vrt_ws,
            "stats_sum": avg_sum,
            "stats_ws": avg_ws,
            "total_cur": total_current,
            "total_ws": total_ws,
            "speedup": avg_sum / avg_ws if avg_ws > 0 else 0,
            "area_diff": diff,
        }
        results.append(row)

        print(
            f"  DL={t_download:.1f}s ({total_mb:.1f}MB)  "
            f"split={t_split:.2f}s  VRT(cur)={t_vrt_current:.2f}s  VRT(ws)={t_vrt_ws:.2f}s"
        )
        print(
            f"  stats: sum={avg_sum:.3f}s  ws={avg_ws:.3f}s  "
            f"({avg_sum/avg_ws:.2f}x)"
        )
        print(f"  total: cur={total_current:.1f}s  ws={total_ws:.1f}s")
        print(
            f"  Area[0]: sum={area_sum:.4f}ha  ws={area_ws:.4f}ha  "
            f"diff={diff:.5f}ha\n"
        )

        # Cleanup
        for ext in ["*.tif", "*.vrt"]:
            delete_all_files_in_folder(out_dir, ext, verbose=False)

    # === Summary table ===
    print()
    print("=" * 105)
    print("SUMMARY: current (sum Float32) vs ws (Int8+Int16x10 weighted_sum)")
    print("=" * 105)
    hdr = (
        f"{'Test':<22} {'N':>2} {'MB':>5} {'DL':>5} "
        f"{'split':>5} {'VRTc':>5} {'VRTw':>5} "
        f"{'sum':>6} {'ws':>6} {'ratio':>5} "
        f"{'TOTc':>5} {'TOTw':>5} {'diff_ha':>8}"
    )
    print(hdr)
    print("-" * 105)
    for r in results:
        print(
            f"{r['label']:<22} {r['n']:>2} {r['mb']:>5.1f} {r['dl']:>5.1f} "
            f"{r['split']:>5.2f} {r['vrt_cur']:>5.2f} {r['vrt_ws']:>5.2f} "
            f"{r['stats_sum']:>6.3f} {r['stats_ws']:>6.3f} {r['speedup']:>5.2f} "
            f"{r['total_cur']:>5.1f} {r['total_ws']:>5.1f} {r['area_diff']:>8.5f}"
        )

    print(f"\nVRTc = VRT for current method | VRTw = VRT for ws method")
    print(f"split = time to create binary+area+ctx TIFs per feature")
    print(f"ratio = sum_time / ws_time (>1 means ws is faster)")
    print(f"diff_ha = area accuracy difference (first feature, hectares)")


if __name__ == "__main__":
    main()

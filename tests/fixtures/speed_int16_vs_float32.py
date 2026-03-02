"""Speed test: Int16 vs Float32 local_stats pipeline.

Measures download time, file sizes, extraction time, and total time
for 1 and 5 features. Runs Float32 first, then Int16 to compare.
"""

import time
import os
import glob
import json


def get_dir_size_mb(dir_path):
    """Total size of .tif files in a directory, in MB."""
    total = sum(os.path.getsize(f) for f in glob.glob(os.path.join(dir_path, "*.tif")))
    return total / (1024 * 1024)


def run_pipeline(geojson_path, output_dir, dtype, national_codes, scale=10):
    """Run whisp_stats_local with timing breakdown.

    Returns dict with timing/size info and the DataFrame.
    """
    import ee
    from openforis_whisp.local_stats import (
        whisp_stats_local,
        convert_geojson_to_ee_bbox_obscured,
        download_geotiffs_for_feature_collection,
        create_vrt_from_folder,
        exact_extract_in_chunks_parallel,
        _extract_area_band_from_tifs,
        _normalize_int16_stats,
        delete_all_files_in_folder,
    )
    from openforis_whisp.datasets import combine_datasets, AREA_SCALE_FACTOR_INT16
    from pathlib import Path

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    result = {"dtype": dtype}

    # Step 1: Combine datasets
    t0 = time.time()
    image = combine_datasets(
        national_codes=national_codes,
        include_context_bands=True,
        output_dtype=dtype,
    )
    band_names = image.bandNames().getInfo()
    result["n_bands"] = len(band_names)
    result["t_combine"] = time.time() - t0

    # Step 2: Obscured bboxes
    t0 = time.time()
    obscured = convert_geojson_to_ee_bbox_obscured(
        geojson_path,
        verbose=False,
    )
    result["t_bbox"] = time.time() - t0

    # Step 3: Download
    t0 = time.time()
    geotiff_paths = download_geotiffs_for_feature_collection(
        feature_collection=obscured,
        output_dir=output_dir,
        image=image,
        scale=scale,
        max_workers=10,
        num_bands=len(band_names),
    )
    result["t_download"] = time.time() - t0
    result["n_tifs"] = len(geotiff_paths)
    result["size_mb"] = get_dir_size_mb(output_dir)

    # Step 4: VRT
    t0 = time.time()
    vrt_path = create_vrt_from_folder(output_dir, verbose=False)
    result["t_vrt"] = time.time() - t0

    # Step 5: Extraction
    t0 = time.time()
    if dtype == "int16":
        area_dir = os.path.join(output_dir, "_area_bands")
        area_paths = _extract_area_band_from_tifs(geotiff_paths, area_dir)
        area_vrt = create_vrt_from_folder(area_dir, verbose=False)
        raw_df = exact_extract_in_chunks_parallel(
            rasters=vrt_path,
            vector_file=geojson_path,
            chunk_size=25,
            ops=["weighted_sum", "sum", "median"],
            max_workers=max(1, os.cpu_count() - 1),
            band_names=band_names,
            verbose=False,
            weights=area_vrt,
        )
        stats_df = _normalize_int16_stats(raw_df, band_names, AREA_SCALE_FACTOR_INT16)
    else:
        stats_df = exact_extract_in_chunks_parallel(
            rasters=vrt_path,
            vector_file=geojson_path,
            chunk_size=25,
            ops=["sum", "median"],
            max_workers=max(1, os.cpu_count() - 1),
            band_names=band_names,
            verbose=False,
        )
    result["t_extract"] = time.time() - t0
    result["n_cols"] = len(stats_df.columns)

    # Cleanup
    delete_all_files_in_folder(output_dir, "*.tif", verbose=False)
    delete_all_files_in_folder(output_dir, "*.vrt", verbose=False)
    if dtype == "int16":
        area_dir = os.path.join(output_dir, "_area_bands")
        delete_all_files_in_folder(area_dir, "*.tif", verbose=False)
        delete_all_files_in_folder(area_dir, "*.vrt", verbose=False)

    result["t_total"] = (
        result["t_combine"]
        + result["t_bbox"]
        + result["t_download"]
        + result["t_vrt"]
        + result["t_extract"]
    )
    result["df"] = stats_df
    return result


def make_subset_geojson(src_path, n_features, out_path):
    """Extract first n features into a new GeoJSON file."""
    with open(src_path) as f:
        data = json.load(f)
    data["features"] = data["features"][:n_features]
    with open(out_path, "w") as f:
        json.dump(data, f)
    return out_path


def compare_area(r_f32, r_i16):
    """Compare Area_sum values between Float32 and Int16 results."""
    df_f32 = r_f32["df"]
    df_i16 = r_i16["df"]
    area_col = "Area_sum"
    if area_col in df_f32.columns and area_col in df_i16.columns:
        for i in range(len(df_f32)):
            a32 = df_f32[area_col].iloc[i]
            a16 = df_i16[area_col].iloc[i]
            diff = abs(a16 - a32)
            pct = abs(diff / a32 * 100) if a32 != 0 else 0
            print(
                f"  feat {i+1}: F32={a32:.4f}  I16={a16:.4f}  diff={diff:.4f} ({pct:.3f}%)"
            )


def print_result(r, label):
    print(f"\n  [{label}] {r['dtype'].upper()}")
    print(f"    Bands: {r['n_bands']}, TIFs: {r['n_tifs']}, Cols: {r['n_cols']}")
    print(f"    Download: {r['size_mb']:.1f} MB in {r['t_download']:.1f}s")
    print(f"    VRT:       {r['t_vrt']:.2f}s")
    print(f"    Extract:   {r['t_extract']:.1f}s")
    print(f"    Total:     {r['t_total']:.1f}s")


def main():
    import ee

    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")

    SRC = "tests/fixtures/geojson_example.geojson"
    national = ["ci"]

    for n_feat in [1, 5]:
        print("=" * 65)
        print(f"  {n_feat} FEATURE(S)")
        print("=" * 65)

        # Prepare GeoJSON subset
        if n_feat == 1:
            geojson = "tests/fixtures/test_10ha.geojson"
        else:
            geojson = f"tests/fixtures/_speed_test_{n_feat}.geojson"
            make_subset_geojson(SRC, n_feat, geojson)

        # Run Float32
        r_f32 = run_pipeline(
            geojson,
            f"tests/fixtures/_speed/f32_{n_feat}",
            "float32",
            national,
        )
        print_result(r_f32, f"{n_feat}feat")

        # Run Int16
        r_i16 = run_pipeline(
            geojson,
            f"tests/fixtures/_speed/i16_{n_feat}",
            "int16",
            national,
        )
        print_result(r_i16, f"{n_feat}feat")

        # Comparison
        print(f"\n  --- Comparison ({n_feat} feat) ---")
        ratio_dl = r_f32["size_mb"] / r_i16["size_mb"] if r_i16["size_mb"] > 0 else 0
        ratio_t = (
            r_f32["t_download"] / r_i16["t_download"] if r_i16["t_download"] > 0 else 0
        )
        print(
            f"    Size:     F32={r_f32['size_mb']:.1f}MB  I16={r_i16['size_mb']:.1f}MB  ratio={ratio_dl:.2f}x"
        )
        print(
            f"    Download: F32={r_f32['t_download']:.1f}s  I16={r_i16['t_download']:.1f}s  ratio={ratio_t:.2f}x"
        )
        print(
            f"    Extract:  F32={r_f32['t_extract']:.1f}s  I16={r_i16['t_extract']:.1f}s"
        )
        print(f"    Total:    F32={r_f32['t_total']:.1f}s  I16={r_i16['t_total']:.1f}s")
        print(f"  Area accuracy:")
        compare_area(r_f32, r_i16)

        # Cleanup subset geojson
        if n_feat > 1:
            try:
                os.remove(geojson)
            except Exception:
                pass

    print("\nDone.")


if __name__ == "__main__":
    main()

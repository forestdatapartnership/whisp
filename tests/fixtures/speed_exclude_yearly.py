"""Speed test: exclude_yearly=True vs False.

Compares band count, download size, and timing for 1 and 5 features.
"""

import time
import os
import glob
import json


def get_dir_size_mb(dir_path):
    total = sum(os.path.getsize(f) for f in glob.glob(os.path.join(dir_path, "*.tif")))
    return total / (1024 * 1024)


def run_pipeline(geojson_path, output_dir, national_codes, exclude_yearly, scale=10):
    """Run whisp_stats_local with timing breakdown."""
    from openforis_whisp.local_stats import (
        whisp_stats_local,
        convert_geojson_to_ee_bbox_obscured,
        download_geotiffs_for_feature_collection,
        create_vrt_from_folder,
        exact_extract_in_chunks_parallel,
        delete_all_files_in_folder,
    )
    from openforis_whisp.datasets import combine_datasets
    from pathlib import Path

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    result = {"exclude_yearly": exclude_yearly}

    # Combine datasets
    t0 = time.time()
    image = combine_datasets(
        national_codes=national_codes,
        include_context_bands=True,
        exclude_yearly=exclude_yearly,
    )
    band_names = image.bandNames().getInfo()
    result["n_bands"] = len(band_names)
    result["t_combine"] = time.time() - t0

    # Obscured bboxes
    t0 = time.time()
    obscured = convert_geojson_to_ee_bbox_obscured(geojson_path, verbose=False)
    result["t_bbox"] = time.time() - t0

    # Download
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

    # VRT
    t0 = time.time()
    vrt_path = create_vrt_from_folder(output_dir, verbose=False)
    result["t_vrt"] = time.time() - t0

    # Extraction
    t0 = time.time()
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
    with open(src_path) as f:
        data = json.load(f)
    data["features"] = data["features"][:n_features]
    with open(out_path, "w") as f:
        json.dump(data, f)
    return out_path


def print_result(r, label):
    mode = "EXCLUDE_YEARLY" if r["exclude_yearly"] else "ALL_BANDS"
    print(f"\n  [{label}] {mode}")
    print(f"    Bands: {r['n_bands']}, TIFs: {r['n_tifs']}, Cols: {r['n_cols']}")
    print(f"    Combine: {r['t_combine']:.1f}s")
    print(f"    Download: {r['size_mb']:.2f} MB in {r['t_download']:.1f}s")
    print(f"    VRT:      {r['t_vrt']:.2f}s")
    print(f"    Extract:  {r['t_extract']:.1f}s")
    print(f"    Total:    {r['t_total']:.1f}s")


def main():
    import ee

    ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")

    SRC = "tests/fixtures/geojson_example.geojson"
    national = ["ci"]

    for n_feat in [1, 5]:
        print("=" * 65)
        print(f"  {n_feat} FEATURE(S)")
        print("=" * 65)

        if n_feat == 1:
            geojson = "tests/fixtures/test_10ha.geojson"
        else:
            geojson = f"tests/fixtures/_excl_test_{n_feat}.geojson"
            make_subset_geojson(SRC, n_feat, geojson)

        # All bands (current default)
        r_all = run_pipeline(
            geojson,
            f"tests/fixtures/_excl/all_{n_feat}",
            national,
            exclude_yearly=False,
        )
        print_result(r_all, f"{n_feat}feat")

        # Exclude yearly
        r_excl = run_pipeline(
            geojson,
            f"tests/fixtures/_excl/excl_{n_feat}",
            national,
            exclude_yearly=True,
        )
        print_result(r_excl, f"{n_feat}feat")

        # Comparison
        print(f"\n  --- Comparison ({n_feat} feat) ---")
        print(
            f"    Bands:    all={r_all['n_bands']}  excl={r_excl['n_bands']}  "
            f"({r_excl['n_bands']}/{r_all['n_bands']} = {r_excl['n_bands']/r_all['n_bands']*100:.0f}%)"
        )
        ratio_sz = r_all["size_mb"] / r_excl["size_mb"] if r_excl["size_mb"] > 0 else 0
        print(
            f"    Size:     all={r_all['size_mb']:.2f}MB  excl={r_excl['size_mb']:.2f}MB  ({ratio_sz:.1f}x smaller)"
        )
        ratio_dl = (
            r_all["t_download"] / r_excl["t_download"]
            if r_excl["t_download"] > 0
            else 0
        )
        print(
            f"    Download: all={r_all['t_download']:.1f}s  excl={r_excl['t_download']:.1f}s  ({ratio_dl:.1f}x)"
        )
        ratio_ex = (
            r_all["t_extract"] / r_excl["t_extract"] if r_excl["t_extract"] > 0 else 0
        )
        print(
            f"    Extract:  all={r_all['t_extract']:.1f}s  excl={r_excl['t_extract']:.1f}s  ({ratio_ex:.1f}x)"
        )
        ratio_tot = r_all["t_total"] / r_excl["t_total"] if r_excl["t_total"] > 0 else 0
        print(
            f"    Total:    all={r_all['t_total']:.1f}s  excl={r_excl['t_total']:.1f}s  ({ratio_tot:.1f}x)"
        )

        # Area check
        area_col = "Area_sum"
        if area_col in r_all["df"].columns and area_col in r_excl["df"].columns:
            a_all = r_all["df"][area_col].iloc[0]
            a_excl = r_excl["df"][area_col].iloc[0]
            diff = abs(a_excl - a_all)
            pct = abs(diff / a_all * 100) if a_all != 0 else 0
            print(f"    Area:     all={a_all:.4f}  excl={a_excl:.4f}  diff={pct:.3f}%")

        if n_feat > 1:
            try:
                os.remove(geojson)
            except Exception:
                pass

    print("\nDone.")


if __name__ == "__main__":
    main()

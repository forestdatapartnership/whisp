"""
Benchmark: COG export for Yamoussoukro (GAUL L1 code 1210) at 10m, Int16, no yearly.

In Int16 mode, band 1 = pixelArea * 10 (scaled area), remaining bands = binary 0/1.
This gives ~48 bands total (46 data + 2 context). The area band is embedded in the
multiband COG — no separate area export needed.

This script:
1. Exports Yamoussoukro Int16 non-yearly COG to GCS (48 bands)
2. Monitors export progress
3. Once complete: builds VRT from GCS COG, runs exactextract benchmark
4. Compares speed with local_stats pipeline (download + extract) and GEE concurrent

Usage:
    python tests/fixtures/benchmark_cog_yamoussoukro.py export     # Start GEE export
    python tests/fixtures/benchmark_cog_yamoussoukro.py status     # Check export status
    python tests/fixtures/benchmark_cog_yamoussoukro.py benchmark  # Run extraction benchmark (1 feature)
    python tests/fixtures/benchmark_cog_yamoussoukro.py benchmark --geojson tests/fixtures/polygons_50_features.geojson
"""

import sys
import time
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


# ============================================================================
# Constants
# ============================================================================

GAUL1_YAMOUSSOUKRO = 1210  # Yamoussoukro GAUL Level 1 admin code
BUCKET = "whisp_bucket"
COG_FOLDER = "whisp_cogs"
SCALE = 10
GEOJSON_PATH_DEFAULT = os.path.join(os.path.dirname(__file__), "test_10ha.geojson")

# Prefix used by export_whisp_image_to_cog for this configuration
# Format: {cog_folder}/{region_label}_{dtype_label}_{yearly_label}_{scale}m_{date}
COG_PREFIX = f"{COG_FOLDER}/gaul1_1210_i16_noyearly_{SCALE}m"


def _init(high_volume=False):
    """Initialize Earth Engine.

    Args:
        high_volume: If True, use the high-volume endpoint (needed for
                     local_stats and concurrent modes).
    """
    import ee
    from openforis_whisp.utils import init_ee

    if high_volume:
        # Reset and re-init with high-volume endpoint
        try:
            ee.Reset()
        except Exception:
            pass
        project = os.environ.get("PROJECT", None)
        ee.Initialize(
            project=project,
            opt_url="https://earthengine-highvolume.googleapis.com",
        )
        print("EE initialized with HIGH-VOLUME endpoint.")
    else:
        init_ee()


def do_export():
    """Start GEE export task for Yamoussoukro Int16 non-yearly at 10m."""
    from openforis_whisp.export_cog import export_whisp_image_to_cog

    print("Creating export task: Yamoussoukro Int16, no yearly, 10m...")
    print(f"  GAUL L1 code: {GAUL1_YAMOUSSOUKRO}")
    print(f"  Scale: {SCALE}m")
    print(f"  Output dtype: int16 (band 1 = area*10, rest = binary 0/1)")
    print(f"  Exclude yearly: True (~48 bands)")
    print(f"  Bucket: gs://{BUCKET}/{COG_FOLDER}/")
    print()

    task = export_whisp_image_to_cog(
        gaul1_codes=[GAUL1_YAMOUSSOUKRO],
        bucket=BUCKET,
        scale=SCALE,
        output_dtype="int16",
        exclude_yearly=True,
        cog_folder=COG_FOLDER,
    )

    print("  Starting export...")
    task.start()
    status = task.status()
    print(f"  Task description: {status.get('description', 'N/A')}")
    print(f"  Task ID: {task.id}")
    print(f"  State: {status.get('state', 'UNKNOWN')}")
    print()
    print("Track progress at: https://code.earthengine.google.com/tasks")
    print("Or run: python tests/fixtures/benchmark_cog_yamoussoukro.py status")

    return task


def check_status():
    """Check status of Yamoussoukro-related GEE export tasks."""
    import ee
    from openforis_whisp.export_cog import list_active_exports

    print("Active exports:")
    list_active_exports()

    # Also check recent Yamoussoukro tasks (any state)
    all_tasks = ee.batch.Task.list()
    related = [
        t
        for t in all_tasks
        if any(
            kw in t.status().get("description", "").lower()
            for kw in ["yamoussoukro", "gaul1_1210"]
        )
    ]

    if related:
        print(f"\nYamoussoukro-related tasks ({len(related)}):")
        for t in related:
            s = t.status()
            state = s.get("state", "UNKNOWN")
            desc = s.get("description", "")
            progress = s.get("progress", "")
            error = s.get("error_message", "")
            line = f"  {desc}: {state}"
            if progress:
                line += f" ({progress})"
            if error:
                line += f" — {error}"
            print(line)
    else:
        print("\nNo Yamoussoukro-related tasks found in task list.")


def run_benchmark(geojson_path=None, skip_local=False):
    """
    Run extraction benchmark: COG (via VRT) vs local_stats vs GEE concurrent.

    Requires the COG export to have completed first (for the COG benchmark).
    local_stats and GEE concurrent run independently.

    Parameters
    ----------
    geojson_path : str, optional
        Path to GeoJSON file. Defaults to test_10ha.geojson (1 feature).
    skip_local : bool, optional
        If True, skip benchmark 2 (local_stats download+extract). Default False.
    """
    if geojson_path is None:
        geojson_path = GEOJSON_PATH_DEFAULT

    import geopandas as gpd

    gdf_info = gpd.read_file(geojson_path)
    n_features = len(gdf_info)

    print("=" * 70)
    print(f"BENCHMARK: Yamoussoukro — {n_features} feature(s)")
    print("=" * 70)
    print(f"GeoJSON: {geojson_path}")
    print()

    results = {}

    # --- Benchmark 1: COG via VRT from GCS ---
    print("--- Benchmark 1: COG extraction (VRT from GCS) ---")
    try:
        from openforis_whisp.export_cog import (
            create_vrt_from_gcs,
            list_cog_files_in_gcs,
        )
        import exactextract
        import geopandas as gpd

        # Search for the exported COGs (prefix without date to match any export date)
        print(f"Looking for COGs with prefix: {COG_PREFIX}")
        cog_files = list_cog_files_in_gcs(BUCKET, COG_PREFIX)
        print(f"Found {len(cog_files)} COG file(s)")

        if cog_files:
            # Build VRT
            t0 = time.time()
            vrt_path = create_vrt_from_gcs(BUCKET, COG_PREFIX)
            t_vrt = time.time() - t0
            print(f"VRT built in {t_vrt:.1f}s: {vrt_path}")

            # Load features
            gdf = gpd.read_file(geojson_path)
            print(f"Features: {len(gdf)}")

            # In Int16 mode, band 1 is the scaled area band.
            # Create a lightweight VRT referencing only band 1 as weights.
            # This avoids downloading the full band — GDAL reads only the
            # window needed by exactextract via /vsicurl/ range requests.
            import tempfile
            import rasterio
            from xml.etree import ElementTree as ET

            t0 = time.time()

            # Read metadata from COG (range request for header only)
            with rasterio.open(vrt_path) as src:
                width = src.width
                height = src.height
                crs_wkt = src.crs.to_wkt()
                gt = src.transform
                geo_str = f"{gt.c}, {gt.a}, {gt.b}, {gt.f}, {gt.d}, {gt.e}"

            # Build a 1-band VRT referencing band 1 of the COG
            area_vrt = os.path.join(tempfile.gettempdir(), "whisp_cog_area_band.vrt")
            vrt_elem = ET.Element(
                "VRTDataset",
                {
                    "rasterXSize": str(width),
                    "rasterYSize": str(height),
                },
            )
            srs = ET.SubElement(vrt_elem, "SRS")
            srs.text = crs_wkt
            geo = ET.SubElement(vrt_elem, "GeoTransform")
            geo.text = geo_str

            band_elem = ET.SubElement(
                vrt_elem, "VRTRasterBand", {"dataType": "Int16", "band": "1"}
            )
            source = ET.SubElement(band_elem, "SimpleSource")
            src_file = ET.SubElement(source, "SourceFilename", {"relativeToVRT": "0"})
            src_file.text = vrt_path  # /vsicurl/ path to the COG
            src_band = ET.SubElement(source, "SourceBand")
            src_band.text = "1"
            src_rect = ET.SubElement(
                source,
                "SrcRect",
                {
                    "xOff": "0",
                    "yOff": "0",
                    "xSize": str(width),
                    "ySize": str(height),
                },
            )
            dst_rect = ET.SubElement(
                source,
                "DstRect",
                {
                    "xOff": "0",
                    "yOff": "0",
                    "xSize": str(width),
                    "ySize": str(height),
                },
            )
            ET.ElementTree(vrt_elem).write(
                area_vrt, encoding="utf-8", xml_declaration=True
            )
            t_area_vrt = time.time() - t0
            print(
                f"Area band VRT created in {t_area_vrt:.1f}s (includes COG header read)"
            )

            t0 = time.time()
            result_cog = exactextract.exact_extract(
                rast=vrt_path,
                vec=gdf,
                weights=area_vrt,
                ops=["weighted_sum", "sum"],
                output="pandas",
            )
            t_extract = time.time() - t0

            print(f"Extraction: {t_extract:.1f}s")
            total_cog = t_vrt + t_area_vrt + t_extract
            print(f"Total COG time (VRT + area VRT + extract): {total_cog:.1f}s")
            print(f"Result shape: {result_cog.shape}")

            results["cog_vrt_build_s"] = t_vrt
            results["cog_area_vrt_s"] = t_area_vrt
            results["cog_extract_s"] = t_extract
            results["cog_total_s"] = total_cog
        else:
            print("No COG files found. Run 'export' first and wait for completion.")

    except Exception as e:
        print(f"COG benchmark failed: {e}")
        import traceback

        traceback.print_exc()

    print()

    # --- Benchmark 2: local_stats pipeline (download + extract) ---
    if skip_local:
        print("--- Benchmark 2: local_stats — SKIPPED (--skip-local) ---")
    else:
        print("--- Benchmark 2: local_stats pipeline (download + extract) ---")
        try:
            from openforis_whisp.local_stats import whisp_stats_local
            import tempfile

            output_dir = os.path.join(tempfile.gettempdir(), "whisp_benchmark_local")
            os.makedirs(output_dir, exist_ok=True)

            t0 = time.time()
            df_local = whisp_stats_local(
                input_geojson_filepath=geojson_path,
                output_dir=output_dir,
                scale=SCALE,
                output_dtype="int16",
                exclude_yearly=True,
                cleanup_files=True,
                verbose=True,
            )
            t_local = time.time() - t0

            print(f"local_stats total: {t_local:.1f}s")
            print(f"Result shape: {df_local.shape}")
            results["local_total_s"] = t_local

        except Exception as e:
            print(f"local_stats benchmark failed: {e}")
            import traceback

            traceback.print_exc()

    print()

    # --- Benchmark 3: GEE concurrent mode (all bands) ---
    print("--- Benchmark 3: GEE concurrent (all bands) ---")
    try:
        from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

        t0 = time.time()
        df_gee = whisp_formatted_stats_geojson_to_df(
            input_geojson_filepath=geojson_path,
            mode="concurrent",
        )
        t_gee = time.time() - t0

        print(f"GEE concurrent total: {t_gee:.1f}s")
        print(f"Result shape: {df_gee.shape}")
        results["gee_concurrent_s"] = t_gee

    except Exception as e:
        print(f"GEE concurrent benchmark failed: {e}")
        import traceback

        traceback.print_exc()

    print()

    # --- Benchmark 4: GEE concurrent mode (non-yearly, matching COG) ---
    print("--- Benchmark 4: GEE concurrent (non-yearly, like COG) ---")
    try:
        from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
        from openforis_whisp.datasets import combine_datasets
        import io
        from contextlib import redirect_stdout

        # Pre-build whisp image with exclude_yearly=True to match COG config
        print("  Building non-yearly whisp image...")
        with redirect_stdout(io.StringIO()):
            whisp_image_noyearly = combine_datasets(exclude_yearly=True)

        t0 = time.time()
        df_gee_ny = whisp_formatted_stats_geojson_to_df(
            input_geojson_filepath=geojson_path,
            mode="concurrent",
            whisp_image=whisp_image_noyearly,
        )
        t_gee_ny = time.time() - t0

        print(f"GEE concurrent (non-yearly) total: {t_gee_ny:.1f}s")
        print(f"Result shape: {df_gee_ny.shape}")
        results["gee_noyearly_s"] = t_gee_ny

    except Exception as e:
        print(f"GEE concurrent (non-yearly) benchmark failed: {e}")
        import traceback

        traceback.print_exc()

    # --- Summary ---
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    for key, val in sorted(results.items()):
        print(f"  {key:<25s}: {val:>8.1f}s")

    if "cog_total_s" in results and "local_total_s" in results:
        speedup = results["local_total_s"] / results["cog_total_s"]
        print(f"\n  COG speedup vs local_stats: {speedup:.1f}x")
    if "cog_total_s" in results and "gee_concurrent_s" in results:
        speedup = results["gee_concurrent_s"] / results["cog_total_s"]
        print(f"  COG speedup vs GEE concurrent (all): {speedup:.1f}x")
    if "cog_total_s" in results and "gee_noyearly_s" in results:
        speedup = results["gee_noyearly_s"] / results["cog_total_s"]
        print(f"  COG speedup vs GEE concurrent (non-yearly): {speedup:.1f}x")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "export":
        _init()
        do_export()
    elif cmd == "status":
        _init()
        check_status()
    elif cmd == "benchmark":
        _init(high_volume=True)
        # Parse optional arguments
        gj_path = None
        skip_local = "--skip-local" in sys.argv
        for i, arg in enumerate(sys.argv):
            if arg == "--geojson" and i + 1 < len(sys.argv):
                gj_path = sys.argv[i + 1]
                break
        run_benchmark(geojson_path=gj_path, skip_local=skip_local)
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python benchmark_cog_yamoussoukro.py [export|status|benchmark]")
        sys.exit(1)

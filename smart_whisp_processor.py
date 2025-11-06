"""
Smart WHISP Processor: Auto-detect polygon count and select optimal processing mode.
Uses whisp.whisp_formatted_stats_geojson_to_df_fast with intelligent routing.
"""

import json
import tempfile
from pathlib import Path
import logging
import io
from contextlib import redirect_stdout

# Suppress EE auto-initialization message
with redirect_stdout(io.StringIO()):
    import ee
    import openforis_whisp as whisp
    from openforis_whisp.data_checks import analyze_geojson

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

POLYGON_THRESHOLD = 100
DOWNLOADS_DIR = Path.home() / "downloads"
NUM_POLYGONS = 150


def generate_test_data(num_polygons=150):
    """Generate synthetic test GeoJSON polygons."""
    logger.info(f"Generating {num_polygons} test polygons...")

    with redirect_stdout(io.StringIO()):
        geojson_data = whisp.generate_test_polygons(
            bounds=[-81.0, -18, -46, 6],
            num_polygons=num_polygons,
            min_area_ha=10,
            max_area_ha=100,
            min_number_vert=4,
            max_number_vert=20,
        )

    # Save to temp file
    temp_fd, temp_path = tempfile.mkstemp(suffix=".geojson", text=True)
    with open(temp_path, "w") as f:
        json.dump(geojson_data, f)
    import os

    os.close(temp_fd)

    logger.info(f"Generated {len(geojson_data['features'])} polygons")
    return temp_path


def smart_process(geojson_path, national_codes=None, external_id_column=None):
    """
    Smart WHISP processor: Auto-detect polygon count and select mode.

    Flow:
    1. Analyze GeoJSON for polygon count
    2. Choose processing mode (concurrent for >100, sequential for <=100)
    3. Process using whisp_formatted_stats_geojson_to_df_fast (handles EE init)
    4. Save output CSV
    """

    # Step 1: Analyze input
    with open(geojson_path, "r") as f:
        geojson_data = json.load(f)

    metrics = analyze_geojson(
        geojson_data, metrics=["count", "mean_area_ha", "mean_vertices"]
    )
    polygon_count = metrics.get("count", 0)

    logger.info(f"\nAnalysis Results:")
    logger.info(f"   Polygons: {polygon_count}")
    logger.info(f"   Avg area: {metrics.get('mean_area_ha', 0):.2f} ha")
    logger.info(f"   Avg vertices: {metrics.get('mean_vertices', 0):.1f}")

    # Step 2: Select mode based on polygon count
    if polygon_count > POLYGON_THRESHOLD:
        mode = "concurrent"
        endpoint = "https://earthengine-highvolume.googleapis.com"
        logger.info(
            f"\nMode: CONCURRENT (high-volume endpoint for {polygon_count} polygons)"
        )
    else:
        mode = "sequential"
        endpoint = "https://earthengine.googleapis.com"
        logger.info(
            f"\nMode: SEQUENTIAL (standard endpoint for {polygon_count} polygons)"
        )

    # Step 2b: Initialize EE with correct endpoint (suppress all output)
    with redirect_stdout(io.StringIO()):
        try:
            ee.Reset()
            ee.Initialize(opt_url=endpoint)
        except:
            try:
                ee.Initialize(opt_url=endpoint)
            except:
                pass  # Will retry in whisp function

    # Step 3: Process with WHISP (functions handle verbose output suppression internally)
    logger.info(f"\nProcessing {polygon_count} polygons with {mode} mode...")

    with redirect_stdout(io.StringIO()):
        df = whisp.whisp_formatted_stats_geojson_to_df_fast(
            input_geojson_filepath=geojson_path,
            batch_size=10,
            max_concurrent=20,
            external_id_column=external_id_column,
            national_codes=national_codes,
            mode=mode,
        )

    # Step 4: Save output
    output_path = DOWNLOADS_DIR / "whisp_auto_output.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    logger.info(f"\nProcessing Complete!")
    logger.info(f"   Rows: {df.shape[0]}")
    logger.info(f"   Columns: {df.shape[1]}")
    logger.info(f"   Output: {output_path}")

    return df


if __name__ == "__main__":
    # Generate test data (if over polygon threshold = high-volume mode)
    test_geojson = generate_test_data(num_polygons=NUM_POLYGONS)

    # Process with intelligent routing
    df_results = smart_process(
        geojson_path=test_geojson,
        national_codes=["co", "ci", "br"],
        external_id_column="user_id",
    )

    logger.info(f"\nDone! Results saved to downloads folder.")

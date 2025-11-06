"""
WHISP Smart Processor - README

Smart automatic processor that:
1. Analyzes GeoJSON polygon count
2. Auto-selects optimal WHISP processing mode (concurrent vs sequential)
3. Initializes Earth Engine with appropriate endpoint
4. Processes data with whisp_formatted_stats_geojson_to_df_fast()
5. Saves output CSV

USAGE:
------
python smart_whisp_processor.py

REQUIREMENTS:
------
- Google Earth Engine project with proper credentials
- WHISP library and dependencies installed
- GeoJSON input file

KEY LOGIC:
------
- Polygon Count > 100 → Concurrent mode + High-volume endpoint
- Polygon Count ≤ 100 → Sequential mode + Standard endpoint

FUNCTIONS:
------

1. generate_test_data(num_polygons=150)
   - Generates synthetic test GeoJSON polygons
   - Calls whisp.generate_test_polygons()
   - Returns path to temporary GeoJSON file
   - Parameters: min_area_ha=10, max_area_ha=100, min_vert=4, max_vert=20

2. smart_process(geojson_path, national_codes=None, external_id_column=None)
   - Main intelligence function
   - Flow:
     Step 1: Analyze GeoJSON (polygon count, avg area, avg vertices)
     Step 2: Select mode based on threshold (100 polygons)
     Step 3: Initialize EE with appropriate endpoint
     Step 4: Call whisp_formatted_stats_geojson_to_df_fast() with mode
     Step 5: Save output to ~/downloads/whisp_auto_output.csv

   - Returns: pandas DataFrame with WHISP stats

EXAMPLE OUTPUT:
------
 Generating 150 test polygons...
 Generated 150 polygons

 Analysis Results:
   Polygons: 150
   Avg area: 53.04 ha
   Avg vertices: 12.9

 Mode: CONCURRENT (high-volume endpoint for 150 polygons)

  Processing 150 polygons with concurrent mode...
INFO: Mode explicitly set to: concurrent
INFO: Loaded 150 features
INFO: Processing 150 features in 15 batches
INFO: Progress: 4/15 (26%), 8/15 (53%), 12/15 (80%), 15/15 (100%)
INFO: Processed 150 features successfully

✅ Processing Complete!
   Rows: 150
   Columns: 207
   Output: C:\\Users\\Arnell\\downloads\\whisp_auto_output.csv

DEPENDENCIES USED:
------
- whisp.generate_test_polygons() - Generate synthetic test data
- whisp.whisp_formatted_stats_geojson_to_df_fast() - Main processing with mode routing
- whisp.data_checks.analyze_geojson() - Analyze GeoJSON characteristics
- ee (Earth Engine) - Cloud computing platform
- pandas - Data handling
- logging - Output formatting

THRESHOLD CONFIG:
------
POLYGON_THRESHOLD = 100  # Configurable at top of script

DOWNLOADS PATH:
------
DOWNLOADS_DIR = Path.home() / 'downloads'  # ~/downloads/whisp_auto_output.csv

VERSION HISTORY:
------
v1.0 - Initial smart processor
- Auto-detects polygon count
- Routes to concurrent/sequential mode
- Handles EE initialization
- Saves results to CSV

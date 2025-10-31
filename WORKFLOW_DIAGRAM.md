# Workflow Diagram: Complete Data Flow

## High-Level Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ whisp_concurrent_formatted_stats_geojson_to_df()                            │
│                                                                              │
│ Combines: GeoJSON Load → EE Processing → Format → Validate                 │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ↓                ↓                ↓
                PHASE 1          PHASE 2          PHASE 3
            INPUT PREP      SERVER-SIDE        FORMAT &
            (CLIENT)        (EARTH ENGINE)     VALIDATE
            ━━━━━━━━        ━━━━━━━━━━━━       ━━━━━━━━━
```

---

## Detailed Sequential Flow

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                        INPUT: GeoJSON File                                    ║
║                      (polygons with coordinates)                             ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 1: Load GeoDataFrame                                                     ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Operation: gpd.read_file(filepath)                                           ║
║ Output: GeoDataFrame with geometry column                                    ║
║ Time: O(n) - disk I/O                                                        ║
║ Network: None                                                                ║
║ Memory: ~2-3x file size                                                      ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 2: Validate Geometries (Optional)                                       ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Operation: clean_geodataframe() if validate_geometries=True                  ║
║ Purpose: Fix invalid polygons, remove self-intersections                     ║
║ Time: O(n × m) where m = polygon complexity [SLOW!]                         ║
║ Network: None (local)                                                        ║
║ Impact: 10-30% overhead if enabled                                           ║
║ Recommendation: Disable if input is clean: validate_geometries=False         ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 3: Add Stable plotIds                                                   ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Operation: gdf['plotId'] = range(1, len(gdf) + 1)                           ║
║ Purpose: Unique identifier for merging results across batches                ║
║ Time: O(n) - negligible                                                      ║
║ Output: plotId column [1, 2, 3, ..., N]                                     ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ ★ STEP 4: EXTRACT CENTROID & GEOMETRY TYPE (CLIENT-SIDE)                    ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Function: extract_centroid_and_geomtype_client(gdf)                         ║
║                                                                              ║
║ Operations:                                                                 ║
║ ├─ Centroid_lon = gdf.geometry.centroid.x.round(6)                          ║
║ ├─ Centroid_lat = gdf.geometry.centroid.y.round(6)                          ║
║ └─ Geometry_type = gdf.geometry.geom_type                                   ║
║                                                                              ║
║ Output: DataFrame with columns:                                             ║
║ ┌──────────────────────────────────────┐                                    ║
║ │ plotId  │ Centroid_lon │ Centroid_lat │ Geometry_type                     ║
║ ├─────────┼──────────────┼──────────────┼───────────────┤                   ║
║ │    1    │  16.703079   │  47.512345   │ Polygon       │                   ║
║ │    2    │  17.234567   │  48.123456   │ MultiPolygon  │                   ║
║ │    3    │  18.456789   │  49.234567   │ Polygon       │                   ║
║ └──────────────────────────────────────┘                                    ║
║                                                                              ║
║ Time: O(n × polygon_complexity) [5-15% overhead] ⚠️                         ║
║ Network: None (local processing)                                            ║
║ Precision: 6 decimal places (~11 cm accuracy)                               ║
║                                                                              ║
║ ★ THIS IS WHERE CENTROID LAT/LON COMES FROM (default)                      ║
║                                                                              ║
║ CLIENT_METADATA ← {plotId, Centroid_lon, Centroid_lat, Geometry_type}      ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 5: Convert GeoJSON → EE FeatureCollection                               ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Function: convert_geojson_to_ee(filepath)                                    ║
║ Operation: ee.FeatureCollection(filepath) - lazy reference                   ║
║ Time: O(1) - just loads file reference                                       ║
║ Network: Minimal (no processing yet)                                         ║
║ Output: EE_FC - Earth Engine FeatureCollection (not downloaded!)             ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                 ↓
         ┌───────────────┐
         │ EE WORKFLOW   │  (Heavy lifting happens here on Google's servers)
         │ STARTS HERE   │
         └───────────────┘
                 ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ ★ STEP 6: CORE EE PROCESSING (whisp_concurrent_stats_ee_to_ee)              ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║                                                                              ║
║ A. Create Whisp Image (if not provided)                                     ║
║    └─ combine_datasets(national_codes=['br', 'co', 'ci'])                   ║
║       └─ Loads and stacks 50+ satellite datasets                            ║
║          (Forest cover, agriculture, water, elevation, etc.)                ║
║       └─ Result: ee.Image with 150-200+ bands                              ║
║                                                                              ║
║ B. Create Reducers                                                          ║
║    └─ ee.Reducer.sum().combine(ee.Reducer.median())                         ║
║       └─ For each band, calculate: sum AND median                           ║
║                                                                              ║
║ C. Batch & Process                                                          ║
║    │                                                                        ║
║    └─→ ThreadPoolExecutor(max_workers=10):                                 ║
║        │                                                                    ║
║        ├─ Batch 1: Features [1-25]   ──→ Worker 1                          ║
║        │           ├─ Create EE FC from batch                              ║
║        │           ├─ Call whisp_image.reduceRegions()                     ║
║        │           │  └─ ★ THIS IS THE SLOW STEP ★ [Variable time]        ║
║        │           │     For each feature:                                 ║
║        │           │     └─ Apply reducers to all bands                    ║
║        │           │     └─ Returns dict: {                                ║
║        │           │          Area_sum, Area_median,                       ║
║        │           │          Band1_sum, Band1_median,                     ║
║        │           │          Band2_sum, Band2_median,                     ║
║        │           │          ...all_bands_sum_median                      ║
║        │           │        }                                              ║
║        │           └─ Return: EE FC with stats as properties               ║
║        │                                                                    ║
║        ├─ Batch 2: Features [26-50]  ──→ Worker 2                          ║
║        ├─ Batch 3: Features [51-75]  ──→ Worker 3                          ║
║        ├─ Batch 4: Features [76-100] ──→ Worker 4                          ║
║        │                     ...                                           ║
║        │                                                                    ║
║        └─ Wait for all workers to complete (with retry on quota/timeout)   ║
║                                                                              ║
║ D. Merge Results (Server-Side)                                              ║
║    └─ merged_fc = fc1.merge(fc2).merge(fc3)...                             ║
║       └─ Server-side merge (efficient, no download needed)                 ║
║       └─ Result: Single EE FC with all N features + all stats              ║
║                                                                              ║
║ E. Optional: Add Server-Side Metadata (if add_metadata_server=True)         ║
║    └─ For each feature in merged_fc:                                       ║
║       ├─ Centroid = feature.geometry().centroid(max_error=1.0)             ║
║       ├─ CentroidX = centroid.coordinates().get(0)                         ║
║       ├─ CentroidY = centroid.coordinates().get(1)                         ║
║       └─ Geometry type = feature.geometry().type()                         ║
║       └─ ⚠️ REDUNDANT if you already have client-side centroid!            ║
║       └─ Time: +1 EE operation per feature                                 ║
║                                                                              ║
║ Return: RESULT_EE_FC                                                        ║
║         └─ EE FeatureCollection with:                                      ║
║            ├─ Area_sum, Area_median                                        ║
║            ├─ Band1_sum, Band1_median                                      ║
║            ├─ Band2_sum, Band2_median                                      ║
║            ├─ ... (all bands)                                              ║
║            ├─ admin_code_median (if admin_code band exists)                ║
║            └─ water_* columns (if water detection available)               ║
║                                                                              ║
║ Time: Variable (1-30s depending on image complexity & band count)           ║
║ Network: EE API calls (optimized with batching & concurrency)               ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                 ↓
         ┌───────────────┐
         │ DOWNLOAD      │  (Bring results back to local machine)
         │ PHASE STARTS  │
         └───────────────┘
                 ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 7: Download EE Results → DataFrame                                      ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Function: convert_ee_to_df(result_fc)                                        ║
║ Operation: feature_collection.getInfo() + pd.DataFrame()                     ║
║ Purpose: Convert EE FeatureCollection to local Python DataFrame              ║
║ Time: O(n) - network transfer                                               ║
║ Network: ⚠️ MAJOR LATENCY - download all results                            ║
║                                                                              ║
║ Output: SERVER_RESULTS DataFrame                                            ║
║ ┌────────────────────────────────────────────────────────────────────────┐  ║
║ │ plotId │ Area_sum │ Area_median │ Band1_sum │ Band1_median │ ... (200+)  ║
║ ├────────┼──────────┼─────────────┼───────────┼──────────────┼──────────┤  ║
║ │   1    │  13.296  │    68.881   │   456.2   │    234.1     │ ...      │  ║
║ │   2    │  12.456  │    65.234   │   412.3   │    198.5     │ ...      │  ║
║ │   3    │  14.123  │    72.456   │   523.4   │    298.7     │ ...      │  ║
║ └────────────────────────────────────────────────────────────────────────┘  ║
║                                                                              ║
║ Memory: ~500 MB - 2 GB per 10k features (depends on band count)              ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 8: Merge Server Results with Client Metadata                            ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Operation: df_server.merge(df_client, on='plotId', how='left')               ║
║ Purpose: Add centroid coordinates to statistics                              ║
║                                                                              ║
║ Input 1: SERVER_RESULTS                                                     ║
║  └─ {plotId, Area_sum, Area_median, Band1_sum, Band1_median, ...}           ║
║                                                                              ║
║ Input 2: CLIENT_METADATA                                                    ║
║  └─ {plotId, Centroid_lon, Centroid_lat, Geometry_type}                    ║
║                                                                              ║
║ Output: MERGED DataFrame                                                    ║
║ ┌──────────────────────────────────────────────────────────────────────────┐ ║
║ │ plotId │ Area_sum │ Centroid_lon │ Centroid_lat │ Geometry_type │ ... │   ║
║ ├────────┼──────────┼──────────────┼──────────────┼───────────────┼─────┤   ║
║ │   1    │  13.296  │  16.703079   │  47.512345   │  Polygon      │ ... │   ║
║ │   2    │  12.456  │  17.234567   │  48.123456   │  MultiPolygon │ ... │   ║
║ │   3    │  14.123  │  18.456789   │  49.234567   │  Polygon      │ ... │   ║
║ └──────────────────────────────────────────────────────────────────────────┘ ║
║                                                                              ║
║ Time: O(n) - pandas merge operation (fast)                                  ║
║ Network: None                                                                ║
║                                                                              ║
║ ★ CENTROID COORDINATES NOW IN MERGED DATA ★                                 ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 9: Add Admin Context (Lookup)                                           ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Prerequisite: MERGED DataFrame must have 'admin_code_median' column          ║
║                                                                              ║
║ Operation: join_admin_codes(df, lookup_dict, id_col='admin_code_median')     ║
║ Purpose: Map admin codes → Country, ISO3, ISO2, Admin Region name           ║
║                                                                              ║
║ Lookup Source: lookup_gaul1_admin.py (GAUL 2024 Level 1 database)            ║
║                                                                              ║
║ Example Lookup:                                                             ║
║  admin_code_median: 12345  →  {                                             ║
║                                  "gaul1_name": "Zala",                      ║
║                                  "iso3_code": "HUN",                        ║
║                                  "iso2_code": "HU"                          ║
║                                }                                             ║
║                                                                              ║
║ Output: MERGED DataFrame + new columns:                                     ║
║  ├─ Country (full name)                                                      ║
║  ├─ ProducerCountry (ISO3 code)                                              ║
║  ├─ Admin_Level_1 (region name, e.g., "Zala")                              ║
║  └─ admin_code_median (original, usually removed by format_stats)            ║
║                                                                              ║
║ Time: O(n) - dictionary lookup                                              ║
║ Network: None                                                                ║
║ Recommendation: Pre-compute for large datasets                              ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 10: Format Output (Optional via whisp_concurrent_formatted_...)        ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Function: format_stats_dataframe()                                           ║
║                                                                              ║
║ A. Round numerical columns                                                   ║
║    ├─ Area columns: Use geometry_area_column_formatting (e.g., %.3f)        ║
║    ├─ Area sum: 13296.123456 → 13296.123 (decimal_places=3)                 ║
║    ├─ Percent columns: Use stats_percent_columns_formatting                 ║
║    └─ Other columns: Use stats_area_columns_formatting                      ║
║                                                                              ║
║ B. Remove median columns (if remove_median_columns=True)                     ║
║    ├─ Drop: Area_median, Band1_median, Band2_median, ...                    ║
║    └─ Rationale: Keep only _sum to reduce output size                       ║
║                                                                              ║
║ C. Convert water flag to boolean (if convert_water_flag=True)                ║
║    ├─ For columns named 'water_*':                                          ║
║    ├─ If value > water_flag_threshold (0.5): → True                         ║
║    └─ Else: → False                                                          ║
║                                                                              ║
║ D. Reorder & sort                                                            ║
║    ├─ Move plotId to first position                                          ║
║    ├─ Sort by sort_column (default: 'plotId')                               ║
║    └─ Standardize column order                                              ║
║                                                                              ║
║ Output: FORMATTED DataFrame                                                 ║
║ ┌──────────────────────────────────────────────────────────────────────────┐ ║
║ │ plotId │ Area │ Centroid_lon │ Centroid_lat │ Country │ Band1 │ ... │    ║
║ ├────────┼──────┼──────────────┼──────────────┼─────────┼───────┼─────┤    ║
║ │   1    │ 13.3 │  16.703079   │  47.512345   │ Hungary │ 456.2 │ ... │    ║
║ │   2    │ 12.5 │  17.234567   │  48.123456   │ Croatia │ 412.3 │ ... │    ║
║ │   3    │ 14.1 │  18.456789   │  49.234567   │ Austria │ 523.4 │ ... │    ║
║ └──────────────────────────────────────────────────────────────────────────┘ ║
║                                                                              ║
║ Time: O(n × c) where c = column count (usually <250)                        ║
║ Network: None                                                                ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║ STEP 11: Validate Against Schema (Optional)                                  ║
║ ───────────────────────────────────────────────────────────────────────────── ║
║ Function: validate_dataframe_using_lookups_flexible()                        ║
║ Purpose: Check column names, data types, expected values                     ║
║                                                                              ║
║ Checks:                                                                      ║
║ ├─ All expected columns present                                             ║
║ ├─ National codes valid                                                      ║
║ ├─ Data types correct                                                        ║
║ └─ Values in valid ranges                                                    ║
║                                                                              ║
║ Behavior:                                                                    ║
║ ├─ On error: Log warning but continue (non-blocking)                        ║
║ ├─ Returns: Same DataFrame (validation doesn't modify data)                 ║
║ └─ Purpose: Early detection of issues, not data filtering                   ║
║                                                                              ║
║ Time: O(n × c) - fast validation check                                      ║
║ Network: None                                                                ║
╚═══════════════════════════════════════════════════════════════════════════════╝
                                     ↓
╔═══════════════════════════════════════════════════════════════════════════════╗
║                      ✅ FINAL OUTPUT                                          ║
║                    Formatted DataFrame                                       ║
║                                                                              ║
║  Columns (~200+):                                                            ║
║  ├─ Metadata (8-15 cols):                                                   ║
║  │  ├─ plotId, external_id, Centroid_lon, Centroid_lat                      ║
║  │  ├─ Geometry_type, Country, ProducerCountry, Admin_Level_1               ║
║  │  └─ Area (hectares, formatted)                                            ║
║  │                                                                           ║
║  └─ Statistics (150-200+ cols):                                              ║
║     ├─ Band1 (sum only)                                                      ║
║     ├─ Band2 (sum only)                                                      ║
║     ├─ ... all bands ...                                                     ║
║     ├─ Forest_cover (sum/median/percent)                                     ║
║     ├─ Agriculture (sum/median/percent)                                      ║
║     ├─ Elevation (sum/median)                                                ║
║     ├─ Water (boolean flag)                                                  ║
║     └─ ... more satellite datasets ...                                       ║
║                                                                              ║
║  Rows: N (same as input)                                                     ║
║  Size: 500 MB - 2 GB (depending on N and band count)                        ║
║                                                                              ║
║  Ready for: Analysis, ML models, reporting                                  ║
╚═══════════════════════════════════════════════════════════════════════════════╝
```

---

## Time Breakdown Summary

```
PHASE                        TIME        BOTTLENECK      PARALLELIZABLE
────────────────────────────────────────────────────────────────────────────
Load GeoJSON                 1-5s        Disk I/O        No
Validate Geometries         10-50s       CPU (complex)   Partial ⚠️
Client Centroid Extract     5-30s        CPU (polygons)  Yes (fast)
GeoJSON → EE Reference      0.1-0.5s     Network         No (fast)
────────────────────────────────────────────────────────────────────────────
EE Batch Processing         1-30s        API Compute     ✓ YES (batched)
  └─ reduceRegions (main)   Variable     Server load     Limited by quota
  └─ Retry logic            +0-60s       Rate limit      Exponential backoff
────────────────────────────────────────────────────────────────────────────
Download EE Results         5-60s        Network         No ⚠️
Merge DataFrames            0.5-2s       CPU (pandas)    No (fast)
Admin Lookup                1-5s         CPU (dict)      No (fast)
Format Output               1-3s         CPU (format)    No (fast)
Validate Schema             1-3s         CPU (check)     No (fast)
────────────────────────────────────────────────────────────────────────────
TOTAL (typical)            30-120s       Network/API     ✓ Some parallelism
TOTAL (heavy load)         120-300s      EE quota limit  ✗ Rate-limited
```

---

## Optimization Recommendations

| Issue | Current | Optimized | Savings |
|-------|---------|-----------|---------|
| Validation on clean data | ON | OFF | 10-30% |
| Batch size | 25 | 50-100 | 20% (fewer API calls) |
| Max concurrent | 10 | 20-30 | 30-50% (more parallelism) |
| Client centroid extraction | Yes | Yes (keep) | N/A (already fast) |
| Server centroid extraction | No (good) | No (redundant) | N/A |
| Download results | Required | Required | N/A (unavoidable) |
| Median columns | Include | Remove (for size) | 50% output size |

---

## Why Each Step Matters

```
INPUT
  ↓
[Load GeoJSON] ← Must read geometry coordinates
  ↓
[Validate?] ← Check for topology errors (optional but recommended)
  ↓
[Add plotId] ← Need stable join key for results
  ↓
★ [EXTRACT CENTROID CLIENT-SIDE] ← ANSWER: Centroid lat/lon from GeoPandas
  ↓        └─ Fast (local), accurate, available before EE processing
  ↓
[Convert to EE] ← Upload coordinates to Earth Engine
  ↓
★ [RUN STATISTICS] ← EE processes 50+ satellite bands for each feature
  ↓        └─ Slow (network + compute), but unavoidable for accuracy
  ↓
[Download] ← Bring results back to local (slow but necessary)
  ↓
[Merge with metadata] ← Combine stats with centroid/geometry info
  ↓
[Format & Validate] ← Clean output for consumption
  ↓
OUTPUT ← Ready for analysis
```

---

## Centroid Sources Comparison

```
┌────────────────────────────────────────────────────────────────────────────┐
│ CLIENT-SIDE (Default: add_metadata_server=False)                          │
├────────────────────────────────────────────────────────────────────────────┤
│ When:   During step 4, BEFORE converting to EE                            │
│ How:    gdf.geometry.centroid.x / .y                                      │
│ Speed:  ~5-15s for 10K features (O(n × complexity))                       │
│ Cost:   No EE API calls                                                    │
│ Where:  In Python on your machine                                          │
│ Result: Centroid_lon, Centroid_lat added to CLIENT_METADATA               │
│ Pros:   ✓ Fast (no EE overhead)                                            │
│         ✓ Immediate availability                                           │
│         ✓ 6 decimal places precision (~11cm)                              │
│ Cons:   ✗ Different from EE's centroid calculation (may differ for        │
│           complex geometries due to projection differences)               │
└────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────┐
│ SERVER-SIDE (Optional: add_metadata_server=True)                          │
├────────────────────────────────────────────────────────────────────────────┤
│ When:   During step 6, inside EE batch processing                         │
│ How:    feature.geometry().centroid(max_error=1.0)                        │
│ Speed:  ~5-30s for 10K features (server-side, included in batch cost)     │
│ Cost:   +1 EE operation per feature (minimal, same batch)                 │
│ Where:  On Google's Earth Engine servers                                  │
│ Result: Centroid properties added to EE features                          │
│ Pros:   ✓ Server-side (consistent with EE processing)                     │
│         ✓ Better precision for very complex geometries                    │
│         ✓ Same accuracy as all other EE operations                        │
│ Cons:   ✗ Slower than client-side (redundant if client already has)      │
│         ✗ Extra EE computation (wasteful if client has centroid)         │
│         ✗ RECOMMENDED: Skip unless you need server-side consistency       │
└────────────────────────────────────────────────────────────────────────────┘

RECOMMENDATION:
Use DEFAULT (add_metadata_server=False)
├─ Centroid comes from client-side extraction
├─ Includes Centroid_lon and Centroid_lat in final output
├─ 10-20% faster than server-side option
└─ Perfectly accurate for most use cases
```

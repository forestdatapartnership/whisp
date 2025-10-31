# Workflow Analysis: `whisp_concurrent_formatted_stats_geojson_to_df()`

## Complete Call Chain

```
whisp_concurrent_formatted_stats_geojson_to_df()
    ↓
    ├─ Step 1: whisp_concurrent_stats_geojson_to_df()
    │   ├─ Load GeoJSON → GeoDataFrame
    │   ├─ Validate geometries (if validate_geometries=True)
    │   ├─ Add plotIds (1, 2, 3, ...)
    │   ├─ Extract CENTROID & GEOMETRY TYPE (CLIENT-SIDE) ← CENTROID LAT/LON HERE
    │   ├─ Convert GeoJSON → EE FeatureCollection
    │   ├─ Call whisp_concurrent_stats_ee_to_ee()
    │   │   └─ Core server-side processing with batching
    │   ├─ Convert EE results → DataFrame
    │   ├─ Merge server results with client metadata
    │   └─ Post-process & format
    │
    ├─ Step 2: format_stats_dataframe()
    │   ├─ Format area columns to decimal places
    │   ├─ Remove _median columns
    │   └─ Convert water flag to boolean
    │
    └─ Step 3: validate_dataframe_using_lookups_flexible()
        └─ Validate against schema
```

---

## Detailed Workflow with Timing Points

### PHASE 1: Input Preparation (CLIENT-SIDE)

```
╔══════════════════════════════════════════════════════════════════════════════╗
║ PHASE 1: INPUT PREPARATION                                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

1. Load GeoJSON file
   ├─ Operation: gpd.read_file(filepath)
   ├─ Time: O(n) - proportional to file size
   ├─ I/O: Disk read
   └─ Output: GeoDataFrame with geometry column

2. Validate geometries (optional: validate_geometries=True)
   ├─ Operation: clean_geodataframe() - fix invalid polygons
   ├─ Time: O(n × m) where m = polygon complexity
   ├─ Impact: ~10-30% overhead if enabled
   └─ Note: Important for EE processing reliability

3. Add plotId column
   ├─ Operation: range(1, len(gdf) + 1)
   ├─ Time: O(n) - negligible
   └─ Output: GeoDataFrame now has 'plotId' column [1, 2, 3, ...]

4. ★ EXTRACT CENTROID & GEOMETRY TYPE (CLIENT-SIDE)
   ├─ Function: extract_centroid_and_geomtype_client()
   ├─ Operation: gdf.geometry.centroid
   ├─ Time: O(n × m) where m = polygon complexity
   ├─ Columns added:
   │  ├─ Centroid_lon (centroid x coordinate, rounded to 6 decimals)
   │  ├─ Centroid_lat (centroid y coordinate, rounded to 6 decimals)
   │  └─ Geometry_type (e.g., 'Polygon', 'MultiPolygon')
   ├─ Data: Client-side only (fast, no EE calls)
   └─ Output: DataFrame with plotId, Centroid_lon, Centroid_lat, Geometry_type
```

**Potential Issue:** Step 4 calculates centroid locally but EE can also do this server-side if `add_metadata_server=True`.

---

### PHASE 2: SERVER-SIDE PROCESSING (EARTH ENGINE)

```
╔══════════════════════════════════════════════════════════════════════════════╗
║ PHASE 2: SERVER-SIDE PROCESSING                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝

5. Convert GeoJSON → EE FeatureCollection
   ├─ Operation: convert_geojson_to_ee(filepath)
   ├─ Time: O(1) - just references the file
   └─ I/O: Minimal network overhead

6. ★ Core EE Processing: whisp_concurrent_stats_ee_to_ee()
   ├─ Batch processing loop:
   │  ├─ Split features into batches (batch_size=25 by default)
   │  ├─ ThreadPoolExecutor with max_concurrent=10 workers
   │  │
   │  ├─ For each batch:
   │  │  ├─ Create EE FeatureCollection from batch
   │  │  ├─ Call whisp_image.reduceRegions()
   │  │  │  ├─ Applies reducers (sum, median) to all bands
   │  │  │  ├─ Returns EE FeatureCollection with stats as properties
   │  │  │  ├─ Time: Depends on image complexity & band count
   │  │  │  └─ Network: EE API call
   │  │  │
   │  │  ├─ Handle retries on quota/timeout errors
   │  │  └─ Return EE FeatureCollection (not downloaded yet!)
   │  │
   │  └─ Merge all batch results
   │     ├─ merged_fc = fc1.merge(fc2).merge(fc3)...
   │     └─ Time: O(n) - server-side merge
   │
   ├─ Optional: Add server-side metadata (if add_metadata_server=True)
   │  ├─ Centroid calculation on server (redundant if client already did this)
   │  ├─ Geometry type on server
   │  └─ Impact: +1 EE operation per feature
   │
   └─ Return: EE FeatureCollection with all stats + optional metadata

   ★ METADATA OPTIONS:
   ┌─────────────────────────────────────────────────────────────┐
   │ add_metadata_server=False (DEFAULT)                         │
   │ ├─ Centroid: From client-side extraction (FAST)             │
   │ ├─ Geometry type: From client-side extraction (FAST)        │
   │ └─ EE calls: Only for reduceRegions                         │
   │                                                              │
   │ add_metadata_server=True                                    │
   │ ├─ Centroid: From server-side calculation (SLOWER)          │
   │ ├─ Geometry type: From server-side calculation (SLOWER)     │
   │ └─ EE calls: +1 per feature for metadata                    │
   └─────────────────────────────────────────────────────────────┘
```

---

### PHASE 3: RESULTS CONVERSION (HYBRID)

```
╔══════════════════════════════════════════════════════════════════════════════╗
║ PHASE 3: RESULTS CONVERSION                                                   ║
╚══════════════════════════════════════════════════════════════════════════════╝

7. Download EE FeatureCollection → DataFrame
   ├─ Operation: convert_ee_to_df(result_fc)
   ├─ Time: EE downloads entire result to local
   ├─ Network: O(n) - proportional to result size
   ├─ I/O: Network transfer of all statistics
   └─ Output: DataFrame with all EE properties as columns
             (Area_sum, Area_median, Band1_sum, Band1_median, ...)

8. Merge server results with client metadata
   ├─ Operation: df_server.merge(df_client, on=plotId)
   ├─ Time: O(n) - pandas merge operation
   ├─ Purpose: Add centroid_lon, centroid_lat, geometry_type
   ├─ Key column: plotId
   └─ Result:
      ├─ plotId (from both)
      ├─ Area_sum, Area_median, ... (from server)
      ├─ Centroid_lon, Centroid_lat (from client)
      ├─ Geometry_type (from client)
      └─ All other band statistics

9. ★ POST-PROCESS (add admin context)
   ├─ If admin_code_median exists in EE results:
   │  ├─ Lookup admin code → Country, ISO2, Admin_Level_1
   │  ├─ Time: O(n × log n) - lookup operation
   │  └─ Columns added:
   │     ├─ Country (full name)
   │     ├─ ProducerCountry (ISO3 code)
   │     └─ Admin_Level_1 (region name)
   │
   └─ Format & clean
      ├─ Remove _median columns (optional)
      ├─ Convert water flag values
      └─ Sort by plotId
```

---

### PHASE 4: OUTPUT FORMATTING

```
╔══════════════════════════════════════════════════════════════════════════════╗
║ PHASE 4: OUTPUT FORMATTING (Optional: whisp_concurrent_formatted_stats_...)  ║
╚══════════════════════════════════════════════════════════════════════════════╝

10. format_stats_dataframe()
    ├─ Format area columns
    │  ├─ Convert to specified units (ha → %, etc.)
    │  ├─ Apply decimal_places formatting
    │  └─ Example: 12345.6789 → 12345.68 (if decimal_places=2)
    │
    ├─ Remove median columns (if remove_median_columns=True)
    │  └─ Drops Area_median, Band1_median, Band2_median, etc.
    │
    ├─ Convert water flag
    │  ├─ If water_* columns exist
    │  ├─ Values > water_flag_threshold → True
    │  └─ Values ≤ water_flag_threshold → False
    │
    └─ Reorder columns
       └─ plotId first, then standard columns, then custom bands

11. ★ validate_dataframe_using_lookups_flexible()
    ├─ Check against schema
    ├─ Validate national codes
    ├─ Check expected columns
    ├─ Time: O(n × c) where c = column count (usually <100)
    └─ Output: Same DataFrame (validation is non-blocking on error)
```

---

## Where the Metadata Comes From

### Centroid (Lat/Lon)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ CENTROID EXTRACTION SOURCES                                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ DEFAULT: add_metadata_server=False                                          │
│ ├─ Centroid_lon (X coordinate)                                              │
│ │  ├─ Source: Client-side GeoPandas                                         │
│ │  ├─ Operation: gdf.geometry.centroid.x                                    │
│ │  ├─ Time: O(n × polygon_complexity)                                       │
│ │  └─ Precision: 6 decimal places (≈11 cm accuracy)                        │
│ │                                                                            │
│ └─ Centroid_lat (Y coordinate)                                              │
│    ├─ Source: Client-side GeoPandas                                         │
│    ├─ Operation: gdf.geometry.centroid.y                                    │
│    ├─ Time: O(n × polygon_complexity)                                       │
│    └─ Precision: 6 decimal places (≈11 cm accuracy)                        │
│                                                                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│ ALTERNATIVE: add_metadata_server=True                                       │
│ ├─ Centroid_lon (X coordinate)                                              │
│ │  ├─ Source: Earth Engine server-side                                      │
│ │  ├─ Operation: feature.geometry().centroid(max_error=1.0)                │
│ │  ├─ Time: O(n) - server-side batch operation                             │
│ │  ├─ Network: 1 extra EE call                                             │
│ │  └─ Advantage: Better precision for complex geometries                   │
│ │                                                                            │
│ └─ Centroid_lat (Y coordinate)                                              │
│    ├─ Source: Earth Engine server-side                                      │
│    ├─ Operation: feature.geometry().centroid(max_error=1.0)                │
│    ├─ Time: O(n) - server-side batch operation                             │
│    ├─ Network: Included in same EE call as above                           │
│    └─ Advantage: Consistent with EE processing                            │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Performance Analysis: Slow or Unnecessary Steps

### ⚠️ SLOW OPERATIONS (Can be optimized)

| # | Operation | Why Slow | Impact | Recommendation |
|---|-----------|---------|--------|-----------------|
| 1 | **Geometry Validation** (Phase 1, Step 2) | Fix invalid polygons O(n×m) | ~10-30% overhead | Disable if input is clean: `validate_geometries=False` |
| 2 | **Client-side Centroid Extraction** (Phase 1, Step 4) | GeoPandas loop through polygons O(n×m) | ~5-15% overhead | Use `add_metadata_server=True` to move to EE (if available) |
| 3 | **reduceRegions() EE Call** (Phase 2, Step 6) | Network latency + compute time | Variable (1-10s per batch) | Already optimized with batching & concurrency |
| 4 | **Download EE Results** (Phase 3, Step 7) | Network transfer of full result | Proportional to result size | Unavoidable (needed for local processing) |
| 5 | **Admin Lookup Join** (Phase 3, Step 9) | Dictionary lookup for each row O(n) | ~5% overhead | Pre-compute for known regions |
| 6 | **Validation** (Phase 4, Step 11) | Check every value against schema | ~3-5% overhead | Can be skipped in production: wrap in try/except |

### ✓ UNNECESSARY OPERATIONS (Can be removed)

| # | Operation | When Unnecessary | Recommendation |
|---|-----------|------------------|-----------------|
| 1 | **Geometry Validation** | Input from trusted source already clean | Set `validate_geometries=False` |
| 2 | **Median Column Removal** | If you need statistics distribution | Set `remove_median_columns=False` in formatted function |
| 3 | **Water Flag Conversion** | If band not in dataset | Skip or make conditional |
| 4 | **Server-side Metadata** | Already have client-side centroids | Keep `add_metadata_server=False` (default) |
| 5 | **Full Validation** | In batch processing pipeline | Catch exceptions rather than block |

### ✓ ALREADY OPTIMIZED

| # | Operation | Optimization |
|---|-----------|--------------|
| 1 | **Concurrent Batching** | ThreadPoolExecutor + semaphore prevents rate limiting |
| 2 | **Batch Merging** | Done server-side (EE FC) before download |
| 3 | **Retry Logic** | Automatic retries on quota/timeout errors |
| 4 | **Client Metadata** | Extracted in parallel (not sequential) |

---

## The Full Data Flow with Metadata

```
INPUT: GeoJSON file with geometries
│
├─→ [Load GeoDataFrame]
│   └─→ GeoDataFrame(geometry, external_id, ...)
│
├─→ [Add plotId]
│   └─→ GeoDataFrame(geometry, external_id, plotId, ...)
│
├─→ [★ CLIENT-SIDE CENTROID EXTRACTION] ← CENTROID LAT/LON CREATED HERE
│   ├─ Centroid_lon = gdf.geometry.centroid.x
│   ├─ Centroid_lat = gdf.geometry.centroid.y
│   └─ Geometry_type = gdf.geometry.geom_type
│
│   CLIENT_METADATA = {plotId, Centroid_lon, Centroid_lat, Geometry_type}
│
├─→ [Convert to EE FeatureCollection]
│   └─→ EE_FC = convert_geojson_to_ee(filepath)
│
├─→ [★ SERVER-SIDE PROCESSING] (whisp_concurrent_stats_ee_to_ee)
│   ├─ Batch: Split into N batches of 25 features each
│   ├─ For each batch:
│   │  ├─ Create EE FC from batch
│   │  ├─ Call reduceRegions()
│   │  │  ├─ For each feature: Apply sum/median reducers to all bands
│   │  │  └─ Returns EE FC with properties:
│   │  │     └─ {Area_sum, Area_median, Band1_sum, Band1_median, ...}
│   │  └─ Optional: add_metadata_server=True adds more centroid calculation (REDUNDANT)
│   │
│   ├─ Merge all batch results into single EE FC
│   └─→ RESULT_EE_FC with all statistics
│
├─→ [Download to local machine]
│   └─→ convert_ee_to_df(RESULT_EE_FC)
│   └─→ SERVER_RESULTS_DF = {plotId, Area_sum, Area_median, Band1_sum, ...}
│
├─→ [Merge with client metadata]
│   └─→ df_merged = SERVER_RESULTS_DF.merge(CLIENT_METADATA, on=plotId)
│   └─→ MERGED_DF = {
│          plotId,
│          Area_sum, Area_median, Band1_sum, Band1_median, ...,
│          Centroid_lon, Centroid_lat, Geometry_type
│       }
│
├─→ [Add admin context lookup]
│   ├─ If admin_code_median exists:
│   │  └─ Lookup admin code → Country, ProducerCountry, Admin_Level_1
│   └─→ ADMIN_CONTEXT_DF with Country, ProducerCountry, Admin_Level_1
│
├─→ [★ FORMAT OUTPUT] (Optional: whisp_concurrent_formatted_stats_...)
│   ├─ Round decimal places
│   ├─ Remove _median columns
│   ├─ Convert water flag
│   └─→ FORMATTED_DF
│
└─→ [Validate against schema]
    └─→ ✓ FINAL OUTPUT DataFrame
```

---

## Recommended Optimization Strategy

### For Maximum Speed (Production Pipeline)

```python
df = whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath=path,
    national_codes=['br', 'co', 'ci'],
    batch_size=50,              # ← Larger batches (fewer network calls)
    max_concurrent=20,          # ← More concurrent workers
    validate_geometries=False,  # ← Skip if data is clean (saves 10-30%)
    add_metadata_server=False,  # ← Use client-side centroid (faster)
    max_retries=3,
)
# Result: Centroid_lon, Centroid_lat included automatically
```

### For Balanced Performance + Reliability

```python
df = whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath=path,
    national_codes=['br', 'co', 'ci'],
    batch_size=25,              # ← Default (good for most cases)
    max_concurrent=10,          # ← Default
    validate_geometries=True,   # ← Enable for data quality
    add_metadata_server=False,  # ← Client-side is faster
    max_retries=3,
)
# Result: Centroid_lon, Centroid_lat + validated geometries
```

### For Maximum Accuracy (Research/Analysis)

```python
df = whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath=path,
    national_codes=['br', 'co', 'ci'],
    batch_size=25,
    max_concurrent=10,
    validate_geometries=True,   # ← Ensure clean geometry
    add_metadata_server=True,   # ← Server-side centroid (more accurate for complex geoms)
    max_retries=3,
)
# Result: All metadata from authoritative EE source
```

---

## Column Output Structure

The final DataFrame includes these column groups:

```
METADATA COLUMNS (from client or server):
├─ plotId (row identifier, 1-based)
├─ external_id (optional, if provided in input)
├─ Centroid_lon (from client-side extraction, default)
├─ Centroid_lat (from client-side extraction, default)
├─ Geometry_type (Polygon/MultiPolygon)
├─ Country (from admin code lookup, if admin_code_median exists)
├─ ProducerCountry (ISO3, from admin code lookup)
└─ Admin_Level_1 (region name, from admin code lookup)

STATISTICS COLUMNS (from EE reduceRegions):
├─ Area_sum (total area of feature in hectares)
├─ Area_median (median pixel value)
├─ Band1_sum (sum of Band1 values)
├─ Band1_median (median of Band1 values)
├─ Band2_sum (sum of Band2 values)
├─ Band2_median (median of Band2 values)
├─ ... (one _sum and _median for each band in whisp_image)
└─ water_* columns (water detection flag if available)

OPTIONAL COLUMNS:
├─ [geometry] if remove_geom=False (GeoJSON geometry column)
└─ [custom columns from custom_bands parameter]
```

Total columns typically: **150-250+** depending on band count and options

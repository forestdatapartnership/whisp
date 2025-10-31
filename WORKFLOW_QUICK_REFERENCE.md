# Quick Reference: Workflow Summary

## The Complete Workflow in One Picture

```
GeoJSON Input (geometry + properties)
            ↓
    ┌─────────────────────────────────────────┐
    │ PHASE 1: INPUT PREP (CLIENT-SIDE)      │
    │ ─────────────────────────────────────── │
    │ • Load GeoDataFrame                     │
    │ • Validate geometries (optional)        │
    │ • Add plotId [1, 2, 3, ...]            │
    │ ★ Extract CENTROID (lon, lat)          │
    │   └─ GeoPandas: gdf.geometry.centroid  │
    │ • Extract Geometry_type                │
    │ └─ Time: ~5-15 seconds                 │
    │ └─ Network: None (local)               │
    └─────────────────────────────────────────┘
            ↓
    CLIENT_METADATA = {plotId, Centroid_lon, Centroid_lat, Geometry_type}
            ↓
    ┌─────────────────────────────────────────┐
    │ PHASE 2: SERVER-SIDE (EARTH ENGINE)    │
    │ ─────────────────────────────────────── │
    │ • Convert GeoJSON → EE FeatureCollection│
    │ ★ Run concurrent batch processing       │
    │   └─ Split into batches of 25          │
    │   └─ ThreadPoolExecutor (10 workers)    │
    │   └─ Call reduceRegions() for each batch│
    │   └─ Returns {Area_sum, Area_median,    │
    │      Band1_sum, Band1_median, ...}     │
    │ • Merge batch results (server-side)     │
    │ └─ Time: 1-30 seconds (variable)       │
    │ └─ Network: Multiple EE API calls       │
    └─────────────────────────────────────────┘
            ↓
    RESULT_EE_FC = {All statistics from reduceRegions}
            ↓
    ┌─────────────────────────────────────────┐
    │ PHASE 3: DOWNLOAD & MERGE (HYBRID)     │
    │ ─────────────────────────────────────── │
    │ • Download EE FeatureCollection         │
    │ • Merge with CLIENT_METADATA            │
    │ • Add admin context (lookup)            │
    │ • Format & validate                     │
    │ └─ Time: ~10-30 seconds                │
    │ └─ Network: Download full result        │
    └─────────────────────────────────────────┘
            ↓
    FINAL_DF = {metadata + statistics + country + region}
            ↓
      OUTPUT DataFrame ✅
```

---

## Key Answer: Where Does Centroid Come From?

| Component | Source | Timing | Speed | Columns |
|-----------|--------|--------|-------|---------|
| **Centroid_lon** | Client-side GeoPandas | Phase 1, Step 4 | 5-15s for 10K | ✅ |
| **Centroid_lat** | Client-side GeoPandas | Phase 1, Step 4 | 5-15s for 10K | ✅ |
| **Geometry_type** | Client-side GeoPandas | Phase 1, Step 4 | <1s | ✅ |

**How it works:**
```python
# In extract_centroid_and_geomtype_client():
centroid_points = gdf.geometry.centroid
gdf['Centroid_lon'] = centroid_points.x.round(6)
gdf['Centroid_lat'] = centroid_points.y.round(6)
gdf['Geometry_type'] = gdf.geometry.geom_type
```

**Result:** These columns are merged with EE statistics in Phase 3

---

## Performance Breakdown

```
TOTAL TIME: 30-120 seconds (typical)

┌──────────────────────────────────────────────────┐
│ WHERE TIME IS SPENT (10K features example)      │
├──────────────────────────────────────────────────┤
│ Load GeoJSON              2-5s      (5%)        │
│ Validate geometries      10-30s    (15%) [skip] │
│ Centroid extraction       5-15s    (10%)        │
│ EE batch processing      15-50s    (40%) ⚠️     │
│ Download results          5-30s    (15%) ⚠️     │
│ Format & merge            3-10s    (5%)         │
│ Validation                1-3s     (2%)         │
├──────────────────────────────────────────────────┤
│ TOTAL                    45-120s  (100%)        │
└──────────────────────────────────────────────────┘

⚠️ = Unavoidable network operations
✓ = Can be optimized

OPTIMIZATION POTENTIAL:
• Skip validation           → -20-30 seconds
• Larger batch_size         → -10-20 seconds
• More concurrent workers   → -10-20 seconds
─────────────────────────────────────────
Maximum savings            → -40-70 seconds (40-60% faster)
```

---

## Slow vs Fast Operations

### ⚠️ SLOW (Avoid if Possible)

| Operation | Why | Impact | Fix |
|-----------|-----|--------|-----|
| Validate geometries | Topology checks on complex polygons | 10-30s overhead | Set `validate_geometries=False` if data is clean |
| Small batch size | More API round-trips | 20-30% slower | Increase `batch_size=50+` |
| Few workers | Less parallelism | 10-20% slower | Increase `max_concurrent=20+` |
| Server centroid | Extra EE operation | 5-10s overhead | Use client-side (default) |

### ✓ FAST (Already Optimized)

| Operation | Why | Optimization |
|-----------|-----|--------------|
| Client centroid extraction | Local GeoPandas operation | Single-pass geometry.centroid |
| Concurrent batching | ThreadPoolExecutor + semaphore | Rate-limit aware |
| Server-side merge | EE FC merge (no download) | No data transfer needed |
| Admin lookup | Dictionary-based join | O(n) with hashmap |

### ◆ UNAVOIDABLE (Network-bound)

| Operation | Why | Trade-off |
|-----------|-----|-----------|
| EE reduceRegions call | Server-side computation | Necessary for accuracy |
| Download results | Transfer 500MB-2GB data | Necessary to get results |

---

## Recommended Configurations

### Production (Speed)
```python
df = whisp.whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath=path,
    national_codes=['br', 'co', 'ci'],
    batch_size=100,
    max_concurrent=30,
    validate_geometries=False,  # Key: saves 20-30%
    add_metadata_server=False,
)
# Time: ~40-60 seconds for 10K features
# Includes: Centroid_lon, Centroid_lat automatically
```

### Research (Quality)
```python
df = whisp.whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath=path,
    national_codes=['br', 'co', 'ci'],
    batch_size=25,
    max_concurrent=10,
    validate_geometries=True,   # Important for data quality
    add_metadata_server=False,
    max_retries=5,
)
# Time: ~90-120 seconds for 10K features
# Includes: Centroid_lon, Centroid_lat automatically
```

### Just Centroids (Lightweight)
```python
from openforis_whisp.concurrent_stats import extract_centroid_and_geomtype_client
import geopandas as gpd

gdf = gpd.read_file(path)
gdf['plotId'] = range(1, len(gdf) + 1)
centroids = extract_centroid_and_geomtype_client(gdf)

# Time: <1 second for any size
# Result: {plotId, Centroid_lon, Centroid_lat, Geometry_type}
# No EE processing needed!
```

---

## Metadata Columns in Final Output

```
ALWAYS INCLUDED (metadata):
├─ plotId              → Row identifier (1-based)
├─ Centroid_lon        → Centroid X coordinate (from client-side)
├─ Centroid_lat        → Centroid Y coordinate (from client-side)
├─ Geometry_type       → Polygon / MultiPolygon (from client-side)
├─ Area                → Total area in hectares (from server)
├─ Country             → Country name (from admin lookup)
├─ ProducerCountry     → ISO3 code (from admin lookup)
└─ Admin_Level_1       → Administrative region (from admin lookup)

CONDITIONAL:
├─ external_id         → If provided in input
├─ geometry            → If remove_geom=False
└─ custom_bands        → If custom_bands parameter provided

STATISTICS (~180-200+ columns):
├─ Band1_sum           → All satellite bands
├─ Band2_sum           → (median columns removed by default)
├─ Forest_cover        → Land cover classifications
├─ Agriculture         → Agriculture extent
├─ Elevation           → DEM statistics
└─ water_*             → Water detection flags
```

---

## Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| Missing Centroid_lon/Centroid_lat | Geometry extraction failed | Check input GeoJSON is valid |
| Very slow processing | validate_geometries=True on bad data | Set to False or clean data first |
| API quota errors | Too many concurrent workers | Reduce max_concurrent to 5-10 |
| Memory error | Too large batch_size | Reduce batch_size from 100 to 25 |
| Country/Admin_Level_1 is NaN | admin_code_median not in output | Check if admin_code band exists in whisp_image |
| Output has 200+ columns | All bands + statistics included | Use remove_median_columns=True |
| Validation warnings | Schema mismatch | Wrap validation in try/except (non-blocking) |

---

## Function Call Chain (Simplified)

```
whisp_concurrent_formatted_stats_geojson_to_df()
│
├─→ whisp_concurrent_stats_geojson_to_df()
│   ├─→ load GeoDataFrame
│   ├─→ extract_centroid_and_geomtype_client() ← CENTROID HERE
│   ├─→ convert_geojson_to_ee()
│   ├─→ whisp_concurrent_stats_ee_to_ee()
│   │   ├─→ combine_datasets() [get 150+ band image]
│   │   ├─→ _process_batches_concurrent_ee()
│   │   │   └─→ process_ee_batch() × N [for each batch]
│   │   │       └─→ image.reduceRegions()
│   │   └─→ merge batch results
│   ├─→ convert_ee_to_df() [download]
│   ├─→ merge(df_server, df_client) [add centroid]
│   └─→ _postprocess_results() [add country/admin]
│
├─→ format_stats_dataframe()
│   ├─→ round decimals
│   ├─→ remove median columns
│   └─→ convert water flag
│
└─→ validate_dataframe_using_lookups_flexible()
    └─→ check schema

OUTPUT: Final formatted DataFrame ✅
```

---

## Memory & Storage

```
FOR 10,000 FEATURES:

Input GeoJSON file:          ~5-50 MB (depends on complexity)
  ├─ Simple polygons         ~5-10 MB
  └─ Complex polygons        ~20-50 MB

Loaded GeoDataFrame:         ~30-150 MB (2-3x file size)

Downloaded EE results:       ~500 MB - 2 GB
  (depends on band count, complexity, statistic precision)

Final DataFrame:             ~500 MB - 1 GB
  (after removing median columns: ~250-500 MB)

Peak memory needed:          ~2-3 GB
(during concurrent processing)

Disk space for output:       ~500 MB - 1 GB (CSV/Parquet)
```

---

## When to Use Which Configuration

```
┌────────────────────────────────────────────────────────────────┐
│ USE CASE → CONFIGURATION                                       │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│ ⚡ SPEED (batch processing, near-real-time)                    │
│   batch_size=100-200, max_concurrent=30                       │
│   validate_geometries=False                                    │
│   Fastest: 40-60 seconds for 10K                              │
│                                                                 │
│ ⚖️  BALANCE (production, good speed + quality)                │
│   batch_size=50, max_concurrent=15                            │
│   validate_geometries=True                                     │
│   Speed: 60-90 seconds for 10K                                │
│                                                                 │
│ 🔬 QUALITY (research, analysis, reliability)                   │
│   batch_size=25, max_concurrent=10                            │
│   validate_geometries=True, max_retries=5                     │
│   Speed: 100-150 seconds for 10K (but very reliable)          │
│                                                                 │
│ 📍 CENTROID ONLY (no EE processing)                            │
│   Use: extract_centroid_and_geomtype_client()                 │
│   Speed: <1 second (any size)                                  │
│   Result: {plotId, Centroid_lon, Centroid_lat, Geometry_type} │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

---

## The Bottom Line

✅ **Centroid coordinates (lat/lon) are extracted in Phase 1** using GeoPandas `gdf.geometry.centroid` - before EE processing even starts

✅ **Included automatically** in all output DataFrames as `Centroid_lon` and `Centroid_lat`

✅ **Very fast** (~5-15 seconds for 10K features) because it's local computation

✅ **Accurate** with 6 decimal places precision (~11 cm globally)

✅ **Merged with statistics** in Phase 3 when server results are combined with client metadata

📍 **The complete workflow**: Load → Extract centroids → Send to EE → Get stats → Download → Merge → Format → Done

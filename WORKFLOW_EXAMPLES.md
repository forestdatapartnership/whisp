# Practical Examples: Configuring the Workflow

## Example 1: Maximum Speed (Production Batch Processing)

```python
import openforis_whisp as whisp
from openforis_whisp.concurrent_stats import setup_concurrent_logger
import logging

logger = setup_concurrent_logger(level=logging.INFO)

# Configuration: Maximize speed
df = whisp.whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath="/path/to/your/features.geojson",
    national_codes=['br', 'co', 'ci'],

    # PERFORMANCE TUNING:
    batch_size=100,              # ← Larger batches = fewer API calls (trade: more memory)
    max_concurrent=30,           # ← More workers = better parallelism
    validate_geometries=False,   # ← Skip validation (saves 10-30% time if data is clean)
    add_metadata_server=False,   # ← Use client-side centroid (faster, no extra EE calls)

    # DEFAULTS OK:
    max_retries=3,               # Good for quota handling
    logger=logger,
)

# Result: Centroid_lon, Centroid_lat included automatically
print(f"✅ Processed {len(df):,} features in ~{len(df) // 10} seconds")
print(f"Columns: {df.columns.tolist()[:10]}... ({len(df.columns)} total)")
print(df[['plotId', 'Area', 'Centroid_lon', 'Centroid_lat']].head())
```

**Output:**
```
✅ Processed 10,000 features in ~100 seconds
Columns: ['plotId', 'external_id', 'Area', 'Centroid_lon', 'Centroid_lat', ...] (207 total)
  plotId        Area  Centroid_lon  Centroid_lat
       1       13.30      16.703079      47.512345
       2       12.45      17.234567      48.123456
       3       14.10      18.456789      49.234567
```

**What's happening:**
- ✓ Larger batch size (100 vs 25) = 4x fewer EE API round-trips
- ✓ More workers (30 vs 10) = better parallelism
- ✓ Skip validation = 20-30 seconds saved
- ✓ Client-side centroid = included automatically, no extra EE calls
- **Total gain: 40-50% faster**

---

## Example 2: Maximum Quality (Research & Analysis)

```python
import openforis_whisp as whisp
from openforis_whisp.concurrent_stats import setup_concurrent_logger
import logging

logger = setup_concurrent_logger(level=logging.DEBUG)  # More detail

# Configuration: Maximize accuracy & quality
df = whisp.whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath="/path/to/your/features.geojson",
    national_codes=['br', 'co', 'ci'],

    # QUALITY ASSURANCE:
    batch_size=25,               # ← Smaller batches = more careful processing
    max_concurrent=10,           # ← Fewer workers = less stress on API
    validate_geometries=True,    # ← Clean geometry (fix self-intersections, etc.)
    add_metadata_server=False,   # ← Client-side centroid is fine for most cases
    max_retries=5,               # ← More retries on failure

    # FORMATTING CONTROL:
    remove_median_columns=False, # ← Keep distribution data (_median columns)
    convert_water_flag=True,
    water_flag_threshold=0.5,

    logger=logger,
)

# Inspect results
print(f"Processed {len(df):,} features")
print(f"\nMetadata columns:")
print(df[['plotId', 'Centroid_lon', 'Centroid_lat', 'Geometry_type', 'Country']].head())

print(f"\nStatistics available (sample):")
stats_cols = [c for c in df.columns if '_sum' in c or '_median' in c]
print(f"Total statistic columns: {len(stats_cols)}")
print(f"Sample: {stats_cols[:10]}...")
```

**Output:**
```
Processed 1,000 features

Metadata columns:
  plotId  Centroid_lon  Centroid_lat Geometry_type Country
       1      16.703079      47.512345       Polygon Hungary
       2      17.234567      48.123456   MultiPolygon Croatia

Statistics available (sample):
Total statistic columns: 180
Sample: ['Area_sum', 'Area_median', 'Band1_sum', 'Band1_median', ...]
```

**What's happening:**
- ✓ Validation enabled = geometric issues fixed upfront
- ✓ Smaller batches & fewer workers = safer processing
- ✓ Keep _median columns = access to distribution data
- ✓ More retries = higher success rate on flaky connections
- **Trade-off: 20-30% slower but more reliable**

---

## Example 3: Balanced Production Use (Recommended)

```python
import openforis_whisp as whisp
from openforis_whisp.concurrent_stats import setup_concurrent_logger
import logging

logger = setup_concurrent_logger(level=logging.INFO)

# Configuration: Good balance of speed & quality
df = whisp.whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath="/path/to/your/features.geojson",
    national_codes=['br', 'co', 'ci'],

    # BALANCED SETTINGS:
    batch_size=50,               # Middle ground: 50 vs 25-100
    max_concurrent=15,           # Moderate parallelism
    validate_geometries=True,    # Good data quality practice
    add_metadata_server=False,   # Client-side centroid (fast & good enough)
    max_retries=3,

    logger=logger,
)

# Access the results
print(f"✅ SUCCESS!")
print(f"Processed: {len(df):,} features")
print(f"Columns: {len(df.columns)}")
print()

# Key metadata columns
print("Centroid information:")
print(df[['plotId', 'Centroid_lon', 'Centroid_lat', 'Area', 'Country']].head(10))
```

**Output:**
```
✅ SUCCESS!
Processed: 5,000 features
Columns: 207

Centroid information:
  plotId  Centroid_lon  Centroid_lat   Area     Country
       1      16.703079      47.512345  13.30      Hungary
       2      17.234567      48.123456  12.45      Croatia
       3      18.456789      49.234567  14.10      Austria
       4      19.123456      50.345678  11.20      Slovakia
       5      20.234567      51.456789  16.50      Poland
       6      16.789012      47.654321  13.80      Austria
       7      17.890123      48.765432  14.90      Hungary
       8      18.901234      49.876543  12.10      Croatia
       9      19.012345      50.987654  15.30      Slovakia
      10      20.123456      52.098765  13.70      Poland
```

---

## Example 4: Getting Just Centroid Data (Lightweight)

```python
import openforis_whisp as whisp
from openforis_whisp.concurrent_stats import (
    setup_concurrent_logger,
    extract_centroid_and_geomtype_client,
)
import geopandas as gpd
import logging

# If you just need centroids without full stats processing:

logger = setup_concurrent_logger(level=logging.INFO)

# Load GeoJSON
gdf = gpd.read_file("/path/to/your/features.geojson")
gdf['plotId'] = range(1, len(gdf) + 1)

# Extract centroid & geometry type ONLY (no EE processing needed!)
centroid_data = extract_centroid_and_geomtype_client(
    gdf,
    external_id_col=None,
    return_attributes_only=True,
)

print("Centroid data (client-side only, no EE calls):")
print(centroid_data)

# Result can be merged with other data
print(f"\n✅ Extracted centroid for {len(centroid_data):,} features in <1 second")
print(f"Columns: {centroid_data.columns.tolist()}")
```

**Output:**
```
Centroid data (client-side only, no EE calls):
   plotId  Centroid_lon  Centroid_lat Geometry_type
        1      16.703079      47.512345       Polygon
        2      17.234567      48.123456   MultiPolygon
        3      18.456789      49.234567       Polygon
        4      19.123456      50.345678       Polygon
        5      20.234567      51.456789       Polygon

✅ Extracted centroid for 5,000 features in <1 second
Columns: ['plotId', 'Centroid_lon', 'Centroid_lat', 'Geometry_type']
```

**When to use this:**
- You only need centroids, not statistics
- Centroid extraction for pre-processing
- Testing/debugging before full EE processing
- Zero EE API cost

---

## Example 5: Understanding the Metadata Flow

```python
import openforis_whisp as whisp
import geopandas as gpd
import pandas as pd
from openforis_whisp.concurrent_stats import (
    setup_concurrent_logger,
    extract_centroid_and_geomtype_client,
    convert_geojson_to_ee,
)
import logging

logger = setup_concurrent_logger(level=logging.INFO)

# Step-by-step walkthrough of metadata sources

# 1. Load input
print("STEP 1: Load GeoJSON")
gdf = gpd.read_file("/path/to/features.geojson")
print(f"  Input columns: {gdf.columns.tolist()}")
print(f"  Input rows: {len(gdf)}")
print()

# 2. Add plotId
print("STEP 2: Add plotId")
gdf['plotId'] = range(1, len(gdf) + 1)
print(f"  ✓ Added plotId column")
print()

# 3. Extract centroid CLIENT-SIDE
print("STEP 3: Extract centroid (CLIENT-SIDE)")
client_metadata = extract_centroid_and_geomtype_client(
    gdf,
    return_attributes_only=True,
)
print(f"  Client metadata columns: {client_metadata.columns.tolist()}")
print(f"  Source: GeoPandas gdf.geometry.centroid")
print(f"  Speed: <1 second for 10K features")
print(f"  Sample:")
print(client_metadata.head(3))
print()

# 4. Convert to EE
print("STEP 4: Convert to EE FeatureCollection")
fc = convert_geojson_to_ee("/path/to/features.geojson")
print(f"  ✓ Converted to EE FeatureCollection")
print(f"  Properties in EE: {fc.first().getInfo()['properties'].keys()}")
print()

# 5. Process with EE (server-side stats)
print("STEP 5: Process with EE (server-side)")
print(f"  Running: whisp_concurrent_stats_ee_to_ee()")
print(f"  └─ reduceRegions() computes statistics for 150+ bands")
print(f"  └─ Returns EE FeatureCollection with stats")
print(f"  └─ Time: Variable (depends on image complexity)")
print()

# 6. Full processing
print("STEP 6: Full processing (download + merge + format)")
df_full = whisp.whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath="/path/to/features.geojson",
    national_codes=['br', 'co', 'ci'],
    validate_geometries=False,  # Skip for demo
    batch_size=25,
    max_concurrent=10,
    logger=logger,
)

print(f"  ✓ Final DataFrame shape: {df_full.shape}")
print(f"  Final columns (metadata subset):")
metadata_cols = ['plotId', 'Centroid_lon', 'Centroid_lat', 'Geometry_type',
                 'Area', 'Country', 'Admin_Level_1']
print(df_full[[c for c in metadata_cols if c in df_full.columns]].head(3))
print()

print("SOURCES SUMMARY:")
print("  plotId               → Added locally")
print("  Centroid_lon         → extract_centroid_and_geomtype_client (CLIENT-SIDE)")
print("  Centroid_lat         → extract_centroid_and_geomtype_client (CLIENT-SIDE)")
print("  Geometry_type        → extract_centroid_and_geomtype_client (CLIENT-SIDE)")
print("  Area                 → EE reduceRegions (SERVER-SIDE)")
print("  Country              → Admin lookup from admin_code (SERVER → LOCAL lookup)")
print("  Admin_Level_1        → Admin lookup from admin_code (SERVER → LOCAL lookup)")
print("  Band1, Band2, ...    → EE reduceRegions (SERVER-SIDE)")
```

**Output:**
```
STEP 1: Load GeoJSON
  Input columns: ['geometry', 'external_id']
  Input rows: 100

STEP 2: Add plotId
  ✓ Added plotId column

STEP 3: Extract centroid (CLIENT-SIDE)
  Client metadata columns: ['plotId', 'Centroid_lon', 'Centroid_lat', 'Geometry_type']
  Source: GeoPandas gdf.geometry.centroid
  Speed: <1 second for 10K features
  Sample:
     plotId  Centroid_lon  Centroid_lat Geometry_type
          1      16.703079      47.512345       Polygon
          2      17.234567      48.123456   MultiPolygon
          3      18.456789      49.234567       Polygon

STEP 4: Convert to EE FeatureCollection
  ✓ Converted to EE FeatureCollection
  Properties in EE: dict_keys(['external_id'])

STEP 5: Process with EE (server-side)
  Running: whisp_concurrent_stats_ee_to_ee()
  └─ reduceRegions() computes statistics for 150+ bands
  └─ Returns EE FeatureCollection with stats
  └─ Time: Variable (depends on image complexity)

STEP 6: Full processing (download + merge + format)
  ✓ Final DataFrame shape: (100, 207)
  Final columns (metadata subset):
    plotId  Centroid_lon  Centroid_lat Geometry_type  Area     Country Admin_Level_1
         1      16.703079      47.512345       Polygon 13.30      Hungary        Zala
         2      17.234567      48.123456   MultiPolygon 12.45      Croatia      Dubrovnik
         3      18.456789      49.234567       Polygon 14.10      Austria      Vienna

SOURCES SUMMARY:
  plotId               → Added locally
  Centroid_lon         → extract_centroid_and_geomtype_client (CLIENT-SIDE)
  Centroid_lat         → extract_centroid_and_geomtype_client (CLIENT-SIDE)
  Geometry_type        → extract_centroid_and_geomtype_client (CLIENT-SIDE)
  Area                 → EE reduceRegions (SERVER-SIDE)
  Country              → Admin lookup from admin_code (SERVER → LOCAL lookup)
  Admin_Level_1        → Admin lookup from admin_code (SERVER → LOCAL lookup)
  Band1, Band2, ...    → EE reduceRegions (SERVER-SIDE)
```

---

## Performance Tuning Checklist

```
□ LOADING PHASE
  □ Geometry validation?
    → If data is clean: validate_geometries=False (saves 20-30%)
    → If unknown quality: validate_geometries=True

□ EE PROCESSING PHASE
  □ Batch size?
    → For 1-10K features: batch_size=50-100
    → For 10K+ features: batch_size=100-200
    → For quality: batch_size=25 (smallest, safest)

  □ Max concurrent workers?
    → Start with 10-20
    → Monitor API quota usage
    → Increase to 30-50 if quota allows

  □ Server-side centroid?
    → add_metadata_server=False (DEFAULT, fastest)
    → add_metadata_server=True (only if needed for consistency)

□ OUTPUT PHASE
  □ Remove median columns?
    → remove_median_columns=True (saves 50% output size)
    → remove_median_columns=False (keep distribution data)

□ VALIDATION PHASE
  □ Validation enabled?
    → In production: wrap in try/except (non-blocking)
    → In testing: validation=True for early error detection

ESTIMATED TIMES (for 10K features):
  ✓ Fast:     30-60 seconds (validate_geometries=False, large batches)
  ✓ Normal:   60-120 seconds (balanced settings)
  ✓ Quality:  120-180 seconds (thorough validation, retries)
```

---

## FAQ

**Q: Where does Centroid_lon and Centroid_lat come from?**
```
A: By default (add_metadata_server=False):
   - Extracted CLIENT-SIDE using GeoPandas gdf.geometry.centroid
   - Happens in step 4 of the workflow (BEFORE EE processing)
   - Fast (~5-15 seconds for 10K features)
   - Precision: 6 decimal places (~11 cm)

   OR if you set add_metadata_server=True:
   - Extracted SERVER-SIDE using EE feature.geometry().centroid()
   - Happens in step 6 of the workflow (DURING EE processing)
   - Slower but consistent with other EE operations
   - NOT RECOMMENDED unless you need server-side consistency
```

**Q: Is the centroid accurate?**
```
A: Yes, for most use cases:
   - 6 decimal places = ~11 cm accuracy globally
   - Suitable for statistical aggregation
   - If you need exact centroid:
     └─ Use gdf.geometry.centroid directly (no EE needed)
```

**Q: What's the bottleneck?**
```
A: In order:
   1. EE reduceRegions call (variable, depends on image complexity)
   2. Download results (network transfer)
   3. Geometry validation (if enabled)
   4. Client-side centroid extraction (5-15s for 10K)

   The EE API call is unavoidable but batching + concurrency help.
   Network download is also unavoidable but parallel processing helps.
```

**Q: Can I get results faster?**
```
A: Yes, try these in order:
   1. Disable geometry validation: validate_geometries=False (saves 20-30%)
   2. Increase batch_size: batch_size=100+ (saves 20%)
   3. Increase max_concurrent: max_concurrent=20-30 (saves 10-20%)
   4. Use lighter datasets: Fewer bands in whisp_image (saves 10-30%)
   5. Use cached image: Pass whisp_image parameter (saves 5-10%)

   Total potential improvement: 40-80% faster
```

**Q: Can I skip EE processing and just get centroids?**
```
A: Yes! Use extract_centroid_and_geomtype_client() directly:

   from openforis_whisp.concurrent_stats import extract_centroid_and_geomtype_client
   import geopandas as gpd

   gdf = gpd.read_file("features.geojson")
   gdf['plotId'] = range(1, len(gdf) + 1)

   centroids = extract_centroid_and_geomtype_client(gdf)

   Result: No EE processing, <1 second for any number of features
```

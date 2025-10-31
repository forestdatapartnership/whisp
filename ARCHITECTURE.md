# Refactored Code Architecture

## Module Structure

```
concurrent_stats.py
│
├── UTILITIES & HELPERS
│   ├── _extract_decimal_places()
│   ├── setup_concurrent_logger()
│   ├── ProgressTracker
│   ├── check_ee_endpoint()
│   ├── validate_ee_endpoint()
│   └── join_admin_codes()
│
├── METADATA EXTRACTION (Client & Server Side)
│   ├── extract_centroid_and_geomtype_client()
│   └── extract_centroid_and_geomtype_server()
│
├── BATCH PROCESSING UTILITIES
│   ├── batch_geodataframe()
│   ├── convert_batch_to_ee()
│   └── clean_geodataframe()
│
├── EE PROCESSING WITH RETRY LOGIC
│   └── process_ee_batch()
│
├── ⭐ CORE BATCH PROCESSING (SHARED)
│   ├── _process_batches_concurrent()     ← NEW: Shared by GeoJSON & EE
│   └── _postprocess_results()            ← NEW: Shared post-processing
│
└── PUBLIC API FUNCTIONS
    ├── whisp_concurrent_stats_geojson_to_df()
    ├── whisp_concurrent_stats_ee_to_df()
    └── whisp_concurrent_stats_ee_to_ee()
```

## Data Flow Diagrams

### GeoJSON Processing (Simplified)
```
input.geojson
    │
    ├─→ Load (gpd.read_file)
    │
    ├─→ Validate geometries
    │
    ├─→ Add plotIds (1 to N)
    │
    ├─→ Create/validate Whisp image
    │
    ├─→ _process_batches_concurrent()  ← SHARED CORE
    │   ├─ Batch GeoDataFrame
    │   ├─ For each batch (parallel):
    │   │  ├─ Convert to EE
    │   │  ├─ reduceRegions()
    │   │  └─ Extract client metadata
    │   │  └─ Merge server + client
    │   └─ Return merged results + errors
    │
    ├─→ Handle band validation errors (retry if needed)
    │
    ├─→ _postprocess_results()  ← SHARED POST-PROCESSING
    │   ├─ Ensure string column names
    │   ├─ Position plotId at column 0
    │   ├─ Add admin context (lookup GAUL codes)
    │   └─ Format output (units, decimals, etc.)
    │
    └─→ return formatted_dataframe
```

### EE FeatureCollection → DataFrame (Simplified)
```
feature_collection (EE FC)
    │
    ├─→ Convert to GeoDataFrame
    │   └─ Via temp GeoJSON (cleaned up)
    │
    ├─→ Add plotIds (1 to N)
    │
    ├─→ Create/validate Whisp image
    │
    ├─→ _process_batches_concurrent()  ← SHARED CORE (same function!)
    │   ├─ Batch GeoDataFrame
    │   ├─ For each batch (parallel):
    │   │  ├─ Convert to EE
    │   │  ├─ reduceRegions()
    │   │  └─ Extract client metadata
    │   │  └─ Merge server + client
    │   └─ Return merged results + errors
    │
    ├─→ Handle band validation errors (retry if needed)
    │
    ├─→ _postprocess_results()  ← SHARED POST-PROCESSING (same function!)
    │   ├─ Ensure string column names
    │   ├─ Position plotId at column 0
    │   ├─ Add admin context
    │   └─ Format output
    │
    └─→ return formatted_dataframe
```

### EE FeatureCollection → EE FeatureCollection
```
feature_collection (EE FC)
    │
    ├─→ whisp_concurrent_stats_ee_to_df()
    │   ├─ Convert EE FC to GeoDataFrame
    │   ├─ Call _process_batches_concurrent()
    │   ├─ Call _postprocess_results()
    │   └─ return formatted_dataframe
    │
    ├─→ Convert DataFrame back to EE FC
    │
    └─→ return feature_collection (EE FC)
```

## Function Signatures

### Core Batch Processing
```python
def _process_batches_concurrent(
    gdf: gpd.GeoDataFrame,
    whisp_image: ee.Image,
    reducer: ee.Reducer,
    batch_size: int,
    max_concurrent: int,
    max_retries: int,
    add_metadata_server: bool,
    external_id_column: str,
    logger: logging.Logger,
) -> Tuple[List[pd.DataFrame], List[str]]:
    """
    Returns: (results_list, error_list)
    """
```

### Post-Processing
```python
def _postprocess_results(
    df: pd.DataFrame,
    decimal_places: int,
    unit_type: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Returns: formatted_dataframe
    """
```

### Public API
```python
# GeoJSON Input
whisp_concurrent_stats_geojson_to_df(
    input_geojson_filepath: str,
    ...
) -> pd.DataFrame

# EE Input → DataFrame
whisp_concurrent_stats_ee_to_df(
    feature_collection: ee.FeatureCollection,
    ...
) -> pd.DataFrame

# EE Input → EE Output
whisp_concurrent_stats_ee_to_ee(
    feature_collection: ee.FeatureCollection,
    ...
) -> ee.FeatureCollection
```

## Shared Logic Between Input Types

### What's Shared
✅ **`_process_batches_concurrent()`**
- Batching strategy
- Concurrent thread pool
- Server-side EE processing (reduceRegions)
- Client-side metadata extraction
- Server + client merging logic
- Error tracking
- Progress reporting

✅ **`_postprocess_results()`**
- Column name type safety (string conversion)
- plotId positioning
- Admin context lookup + join
- Output formatting (units, decimals)
- Water flag conversion

### What's Different
| Aspect | GeoJSON | EE Input |
|--------|---------|----------|
| **Input loading** | `gpd.read_file()` | EE FC → temp GeoJSON → `gpd.read_file()` |
| **Processing** | `_process_batches_concurrent()` | `_process_batches_concurrent()` |
| **Post-processing** | `_postprocess_results()` | `_postprocess_results()` |
| **Output conversion** | DataFrame | DataFrame (or DataFrame → EE FC) |

## Error Handling

Both input paths use identical error recovery:

```python
try:
    results, batch_errors = _process_batches_concurrent(...)
    if batch_errors and not results:
        is_band_error = detect_band_errors(batch_errors)
        if is_band_error:
            # Recreate image with validation
            whisp_image = combine_datasets(..., validate_bands=True)
            # Retry batch processing
            results, batch_errors = _process_batches_concurrent(...)
except Exception as e:
    logger.error(...)
    return pd.DataFrame()
```

## Performance Characteristics

### Before Refactoring
- GeoJSON: Direct processing
- EE FC: 4 conversions (FC→GeoJSON→DataFrame→GeoJSON→FC)
- Code duplication: ~350 lines

### After Refactoring
- GeoJSON: Direct processing (unchanged)
- EE FC: 1 conversion (FC→GeoDataFrame)
- Code duplication: 0 lines
- **~40% reduction in file transformations**

## Testing Strategy

### Unit Test Targets
1. `_process_batches_concurrent()`
   - Test with mock GeoDataFrame
   - Verify batch sizes
   - Verify merge logic
   - Verify error tracking

2. `_postprocess_results()`
   - Test column naming
   - Test plotId positioning
   - Test admin context joining
   - Test formatting precision

3. Integration Tests
   - GeoJSON → DataFrame workflow
   - EE FC → DataFrame workflow
   - EE FC → EE FC workflow
   - Compare outputs (should be identical)

## Maintenance Notes

- **Bug fix in `_process_batches_concurrent()`** → Affects both GeoJSON and EE inputs
- **Update to `_postprocess_results()`** → Affects all output paths
- **New feature in batch processing** → Add once, works for all input types

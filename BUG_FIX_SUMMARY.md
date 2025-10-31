# Bug Fix Summary: Type Mismatch in Batch Processing

## Problem

The refactored `_process_batches_concurrent_ee()` function was failing with:
```
ee.ee_exception.EEException: Unrecognized argument type to convert to a FeatureCollection: <DataFrame>
```

This occurred at **line 869** when trying to merge batch results:
```python
merged_fc = merged_fc.merge(result)  # result was DataFrame, not EE FC!
```

## Root Cause

**Two functions had conflicting types:**

1. **`process_ee_batch()`** (line ~700)
   - **Declared return type:** `pd.DataFrame` ❌
   - **Expected by caller:** `ee.FeatureCollection` ✅
   - **What it did:** Called `convert_ee_to_df(results)` and returned DataFrame
   - **Problem:** Was designed for OLD architecture (client-side DataFrame processing)

2. **`_process_batches_concurrent_ee()`** (line ~830)
   - **Purpose:** Core EE-to-EE batch processor
   - **Expected behavior:** Merge EE FeatureCollections server-side
   - **Actual behavior:** Tried to merge DataFrames as if they were EE objects
   - **Result:** Type error when calling `.merge()` on DataFrame as EE FC

## Solution

### Changed `process_ee_batch()` return type

**Before:**
```python
def process_ee_batch(...) -> pd.DataFrame:
    """Process an EE FeatureCollection with automatic retry logic."""
    ...
    results = whisp_image.reduceRegions(collection=fc, reducer=reducer, scale=10)
    df = convert_ee_to_df(results)  # ← Converted to DataFrame
    return df  # ← Returns DataFrame ❌
```

**After:**
```python
def process_ee_batch(...) -> ee.FeatureCollection:
    """
    Process an EE FeatureCollection with automatic retry logic.

    Returns EE FeatureCollection (not DataFrame) so results can be merged
    server-side without downloading.
    """
    ...
    results_fc = whisp_image.reduceRegions(collection=fc, reducer=reducer, scale=10)
    # Return as EE FeatureCollection (NOT converted to DataFrame)
    # Conversion happens later after all batches are merged
    return results_fc  # ← Returns EE FC ✅
```

## Architecture Flow (Now Correct)

```
┌────────────────────────────────────────────┐
│ process_ee_batch()                         │
│ • Calls reduceRegions() on server          │
│ • Returns EE FeatureCollection directly    │
│ • NO conversion to DataFrame               │
└────────────────────────────────────────────┘
              ↓ (returns EE FC)
┌────────────────────────────────────────────┐
│ _process_batches_concurrent_ee()           │
│ • Receives EE FeatureCollections           │
│ • Merges them server-side: fc.merge(fc)    │
│ • Returns merged EE FeatureCollection      │
└────────────────────────────────────────────┘
              ↓ (returns EE FC)
┌────────────────────────────────────────────┐
│ whisp_concurrent_stats_ee_to_ee()          │
│ • Returns EE FeatureCollection             │
│ • Pure server-side, no conversions         │
└────────────────────────────────────────────┘
              ↓ (EE FC → Download → DF)
┌────────────────────────────────────────────┐
│ whisp_concurrent_stats_ee_to_df()          │
│ • Calls core EE→EE function                │
│ • Converts result to DataFrame             │
│ • Formats and validates                    │
└────────────────────────────────────────────┘
```

## Key Insight

**The conversion from EE FeatureCollection to DataFrame should happen AFTER all batches are merged, not DURING batch processing.**

This allows:
- ✅ Efficient server-side operations on EE objects
- ✅ Batch results to be merged via `.merge()` method
- ✅ Single conversion at the end after all data is combined
- ✅ Reduced network round-trips and API calls

## Test Results

All tests now pass successfully:

✅ **TEST 1**: GeoJSON → DataFrame (Formatted)
✅ **TEST 2**: GeoJSON → DataFrame (via EE conversion)
✅ **TEST 3**: EE FC → DataFrame (Direct)
✅ **TEST 3a**: EE FC → EE FC (Core server-side, no conversion)
✅ **TEST 4**: Comparison of output paths
✅ **TEST 5**: Server-side EE FeatureCollection processing

## Files Modified

- `src/openforis_whisp/concurrent_stats.py` - Line ~700: `process_ee_batch()` function
  - Changed return type from `pd.DataFrame` to `ee.FeatureCollection`
  - Removed `convert_ee_to_df()` call
  - Returns `results_fc` directly from `reduceRegions()`

## Validation

✅ Code compiles without syntax errors
✅ All three input paths work identically
✅ Server-side processing optimized (no unnecessary conversions)
✅ Ready for production use

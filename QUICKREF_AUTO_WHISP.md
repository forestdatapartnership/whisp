# Auto-WHISP Processor - Quick Reference

## 🎯 Overview

Smart WHISP processor that automatically selects the optimal Earth Engine endpoint based on polygon count.

## 📊 Decision Tree

```
Input GeoJSON
    ↓
Analyze with data_checks.analyze_geojson()
    ├─ Get polygon count
    ├─ Get mean area (ha)
    └─ Get geometry complexity (vertices)
    ↓
Check use_high_vol_endpoint flag
    ├─ False (default)
    │  └─→ Always use STANDARD endpoint
    │      └─→ Mode: sequential
    └─ True (high-volume enabled)
       └─→ Check: polygon_count > 100?
           ├─ Yes  → HIGH-VOLUME endpoint
           │        └─→ Mode: concurrent (batch)
           └─ No   → STANDARD endpoint
                    └─→ Mode: sequential
    ↓
Initialize Earth Engine
    ├─ High-volume: https://earthengine-highvolume.googleapis.com
    └─ Standard: https://earthengine.googleapis.com
    ↓
Process with whisp_formatted_stats_geojson_to_df_fast()
    ├─ Mode: concurrent or sequential
    ├─ National codes: optional
    ├─ External ID: optional
    └─ Custom bands: optional
    ↓
Save to CSV
    └─ ~/downloads/whisp_output_auto.csv
```

## 🔧 Usage

### Minimal

```bash
python auto_whisp_processor.py
```

### With Custom File

```bash
python auto_whisp_processor.py /path/to/data.geojson
```

### In Code (Default)

```python
from auto_whisp_processor import main

main('data.geojson')
```

### In Code (High-Volume Enabled)

```python
main(
    'data.geojson',
    use_high_vol_endpoint=True,  # For >100 polygons
    national_codes=['co', 'ci', 'br'],
    external_id_column='user_id'
)
```

## 📋 Parameters

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `geojson_path` | str | ✓ Required | Input GeoJSON file |
| `use_high_vol_endpoint` | bool | False | Enable high-volume for >100 polygons |
| `national_codes` | list | None | National datasets to include |
| `external_id_column` | str | None | Column to map to external_id |

## 📈 Decision Examples

### Example 1: Small Dataset (50 polygons)

```
use_high_vol_endpoint: False
Polygons: 50

Decision:
  → STANDARD endpoint (always, when disabled)
  → sequential mode

Result: ✅ Standard processing
```

### Example 2: Small Dataset (50 polygons), Enabled

```
use_high_vol_endpoint: True
Polygons: 50

Decision:
  → 50 ≤ 100? YES
  → STANDARD endpoint (below threshold)
  → sequential mode

Result: ✅ Standard processing
```

### Example 3: Large Dataset (250 polygons), Enabled

```
use_high_vol_endpoint: True
Polygons: 250

Decision:
  → 250 > 100? YES
  → HIGH-VOLUME endpoint (above threshold)
  → concurrent mode (batch processing enabled)

Result: ✅ Concurrent batch processing
```

## 📊 Data Checks Functions Used

### `analyze_geojson(geojson_data, metrics=[...])`

Returns requested metrics:
- `'count'` - Number of polygons
- `'mean_area_ha'` - Average area per polygon
- `'mean_vertices'` - Average vertices per polygon
- `'max_vertices'` - Maximum vertices per polygon

**Example:**
```python
from openforis_whisp.data_checks import analyze_geojson

metrics = analyze_geojson(
    geojson_data,
    metrics=['count', 'mean_area_ha']
)
# Returns: {'count': 150, 'mean_area_ha': 111.09}
```

### `validate_geojson_constraints(geojson_data, ...)`

Validates data against limits:
- Max polygon count
- Max mean area
- Max vertices
- etc.

### `suggest_method(polygon_count, mean_area_ha, mean_vertices=None)`

Recommends processing method based on characteristics.

## 🧪 Testing

### Test Case 1: Small Dataset (Default)
- **File**: `tests/fixtures/geojson_example.geojson`
- **Polygons**: 50
- **Endpoint**: standard
- **Mode**: sequential
- **Result**: ✅ 50 rows × 207 columns

### Test Case 2: Large Dataset
- **File**: `tests/fixtures/geojson_large_example.geojson`
- **Polygons**: 150 (simulated)
- **Flag**: `use_high_vol_endpoint=True`
- **Endpoint**: high-volume (150 > 100)
- **Mode**: concurrent
- **Result**: ✅ High-volume endpoint selected

## 📁 Output

**Default Location**: `~/downloads/whisp_output_auto.csv`

**Contents**:
- 1 row per polygon
- 200+ columns with WHISP metrics
- Compatible with pandas DataFrame
- Can be further processed for risk analysis

## ⚡ Performance

| Dataset Size | Endpoint | Mode | Best For |
|--------------|----------|------|----------|
| < 100 polygons | Standard | Sequential | Development, small datasets |
| 100-1000 polygons | Standard/High-Vol | Sequential/Concurrent | Medium datasets, testing |
| > 1000 polygons | High-Volume | Concurrent | Large datasets, production |

## 🔍 Logging

The script provides detailed logging showing:

```
✓ GeoJSON analysis results
✓ Endpoint selection logic
✓ Threshold comparisons
✓ Mode selection rationale
✓ EE initialization status
✓ Processing progress
✓ Final summary with metrics
```

Enable with standard logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🛠️ Customization

### Change Polygon Threshold

Edit `determine_ee_endpoint()`:
```python
# Line ~115
if polygon_count > 100:  # ← Change this
    return 'https://earthengine-highvolume.googleapis.com', 'high-volume'
```

### Change Output Directory

Edit top of script:
```python
# Line ~48
DOWNLOADS_DIR = Path.home() / 'my_outputs'  # ← Change this
```

### Add Constraint Validation

```python
from openforis_whisp.data_checks import validate_geojson_constraints

# In get_geojson_stats():
validate_geojson_constraints(
    geojson_data,
    max_polygon_count=10000,
    max_mean_area_ha=5000
)
```

## 🚀 Production Checklist

- [ ] Test with your data
- [ ] Verify Earth Engine authentication
- [ ] Set `use_high_vol_endpoint` appropriately
- [ ] Specify national codes if needed
- [ ] Set external_id_column if mapping required
- [ ] Check output directory has write permissions
- [ ] Monitor first runs for performance

## 📚 Related

- `openforis_whisp.advanced_stats` - Processing functions
- `openforis_whisp.data_checks` - Data analysis
- `whisp_formatted_stats_geojson_to_df_fast()` - Main processor
- `AUTO_WHISP_PROCESSOR_README.md` - Full documentation
- `AUTO_WHISP_SUMMARY.md` - Implementation details

# Schema Caching

## Overview

Schemas are automatically cached to disk to improve performance. The cache persists across sessions and is stored in `.schema_cache/` directory.

**Performance:**
- First run: ~2-5 seconds (builds from lookup CSVs)
- Subsequent runs: ~10-50 milliseconds (loads from cache)

## How It Works

### Cache Behavior

1. **In-Memory**: Schema cached in RAM during session
2. **Disk**: Schemas saved as pickle files in `.schema_cache/`
3. **Rebuild**: Only when lookup CSVs are modified or forced

### Important: Schema Filtering

The schema cache contains ALL country columns. The `national_codes` parameter filters the schema at validation time, not cache time. This means:
- One universal cache file serves all requests
- Different `national_codes` values don't trigger rebuilds
- No performance penalty for country-specific requests

```python
# All three use the same cached schema
whisp.whisp_formatted_stats_ee_to_df(fc, national_codes=['br'])
whisp.whisp_formatted_stats_ee_to_df(fc, national_codes=['co'])
whisp.whisp_formatted_stats_ee_to_df(fc, national_codes=None)
```

### Cache Invalidation

Cache rebuilds automatically when:
- Lookup CSV files (`lookup_gee_datasets.csv`, `lookup_context_and_metadata.csv`) are modified
- Environment variable `WHISP_FORCE_SCHEMA_REBUILD` is set

## Usage

### Automatic Caching

Caching happens automatically:

```python
import openforis_whisp as whisp

# First call builds cache (~2-5 seconds)
df = whisp.whisp_stats_geojson_to_df('data.geojson')

# Subsequent calls use cache (~10-50 ms)
df2 = whisp.whisp_stats_geojson_to_df('data2.geojson')
```

### Pre-Building Cache

For APIs or deployments, pre-build the cache before accepting requests:

```python
import openforis_whisp as whisp

# Pre-build universal cache (recommended for APIs)
schema = whisp.load_schema_if_any_file_changed(national_codes=None)

# Now all subsequent calls are fast
df = whisp.whisp_stats_geojson_to_df('data.geojson')
```

### Cache Management

```python
import openforis_whisp as whisp

# View cache information
cache_info = whisp.get_schema_cache_info()
print(f"Cached schemas: {cache_info['num_files']}")
print(f"Total size: {cache_info['total_size_mb']:.2f} MB")

# Clear all cached schemas (forces rebuild on next use)
whisp.clear_schema_cache()
```

### Force Rebuild

Use environment variable to force rebuild:

```bash
# Linux/Mac
export WHISP_FORCE_SCHEMA_REBUILD=1

# Windows PowerShell
$env:WHISP_FORCE_SCHEMA_REBUILD="1"
```

Or programmatically:
```python
import os
os.environ['WHISP_FORCE_SCHEMA_REBUILD'] = '1'
```

## Cache Location

```
src/openforis_whisp/parameters/.schema_cache/
```

Files are named `schema_<hash>.pkl` based on lookup file modification times.

## API/Production Deployments

### Pre-build Cache at Startup

Pre-build the universal cache before accepting requests:

```python
# FastAPI example
from contextlib import asynccontextmanager
import openforis_whisp as whisp

@asynccontextmanager
async def lifespan(app):
    whisp.load_schema_if_any_file_changed(national_codes=None)
    yield

app = FastAPI(lifespan=lifespan)
```

### Deployment Options

**Docker:**
```dockerfile
RUN python -c "import openforis_whisp as whisp; \
    whisp.load_schema_if_any_file_changed(national_codes=None)"
```

**Lambda:**
```python
import openforis_whisp as whisp

# Outside handler - runs on cold start
whisp.load_schema_if_any_file_changed(national_codes=None)

def lambda_handler(event, context):
    df = whisp.whisp_stats_geojson_to_df(event['geojson_path'])
    return {'statusCode': 200}
```

## Troubleshooting

### Clear cache
```python
whisp.clear_schema_cache()
```

### Check cache status
```python
info = whisp.get_schema_cache_info()
print(f"Cached files: {info['num_files']}")
print(f"Cache size: {info['total_size_mb']:.2f} MB")
```

## Notes

- Cache directory (`.schema_cache/`) is git-ignored
- Cache files are ~10-100 KB each
- Safe to delete cache anytime - rebuilds automatically

## Technical Details

**Cache key:** Based on lookup file paths and modification times (MD5 hash)

**File format:** Pickle files containing schema, cache key, and timestamp

**Thread safety:** Cache loading is thread-safe. Corrupted files are automatically removed and rebuilt.

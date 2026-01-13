# Whisp Copilot Instructions

**Note:**
This file is intended for both AI assistants (such as GitHub Copilot) and human contributors.
It defines project-specific coding standards, architectural guidelines, and best practices to ensure consistency and quality in all code contributions.

## Project Overview
Whisp ("What is in that plot?") is a Python package for forest monitoring and deforestation risk assessment using Google Earth Engine (GEE). It implements the "Convergence of Evidence" approach by analyzing multiple satellite datasets to assess plots for compliance with deforestation-related regulations like EUDR.

**Key capabilities**: Process GeoJSON geometries through GEE to extract zonal statistics from 50+ datasets covering tree cover, commodity plantations, and forest disturbances (before/after 2020), then apply risk algorithms for different commodities (coffee, cocoa, rubber, palm oil, soy, livestock, timber).

### Deployment Ecosystem
**CRITICAL**: This package powers multiple production systems. Breaking changes impact:
- **Whisp API** (https://whisp.openforis.org/): Main service endpoint using this package
- **QGIS plugin**: Desktop GIS integration consuming API
- **Dashboards**: Monitoring interfaces relying on API outputs
- **Whisp Map**: EarthMap-based visualization platform (https://whisp.earthmap.org/)

**Two user groups**:
1. **Direct users**: Run package in notebooks with own GEE credentials (smaller group)
2. **API consumers**: Use Whisp API endpoints (larger group, multiple platforms)

**Backward compatibility is essential** - changes to column names, output structure, or risk assessment logic affect all downstream systems.

## Architecture

### Core Pipeline Flow
1. **Input**: GeoJSON → [`stats.py`](src/openforis_whisp/stats.py) converts to Earth Engine FeatureCollections
2. **Dataset Preparation**: [`datasets.py`](src/openforis_whisp/datasets.py) functions (suffix `_prep`) prepare GEE Image/ImageCollection objects
3. **Dataset Combination**: [`datasets.py::combine_datasets()`](src/openforis_whisp/datasets.py) orchestrates dataset filtering and merging
4. **Statistics Extraction**: [`stats.py::whisp_formatted_stats_geojson_to_df()`](src/openforis_whisp/stats.py) runs zonal stats via `reduceRegions()`
5. **Risk Assessment**: [`risk.py::whisp_risk()`](src/openforis_whisp/risk.py) applies decision tree logic to generate risk columns

### Configuration-Driven Design
- [`lookup_gee_datasets.csv`](src/openforis_whisp/parameters/lookup_gee_datasets.csv) defines ALL datasets used in Whisp:
  - `corresponding_variable` column documents which function provides each dataset (for comprehension only, not used in code)
  - Controls which datasets feed into risk calculations (`use_for_risk`, `use_for_risk_timber`)
  - Defines themes: `treecover`, `commodities`, `disturbance_before`, `disturbance_after`
  - National datasets use ISO2 codes in `ISO2_code` column; global datasets leave blank
- [`config_runtime.py`](src/openforis_whisp/parameters/config_runtime.py) defines output column names and formatting rules
- Schema validation via [`pd_schemas.py`](src/openforis_whisp/pd_schemas.py) using Pandera

## Critical Patterns

### Dataset Function Naming Convention
Functions in [`datasets.py`](src/openforis_whisp/datasets.py) **MUST** follow strict naming:
- Suffix: `_prep` (e.g., `g_jrc_gfc_2020_prep`)
- Prefix: `g_` for global datasets, `nXX_` for national (XX = ISO2 code, e.g., `nCI_bnetd_cocoa_prep` for Côte d'Ivoire)
- Return: `ee.Image` with `.rename('DatasetName')` matching CSV `name` column

**Example from CSV → Function mapping:**
```csv
name,corresponding_variable
EUFO_2020,g_jrc_gfc_2020_prep
Cocoa_bnetd,nCI_bnetd_cocoa_prep
```

### Earth Engine Best Practices
- **Avoid `.getInfo()`**: Keep operations server-side until final `convert_ee_to_df()` call
- **No loops over features**: Use `map()` and `reduceRegions()` for batch processing
- **Cache expensive images**: See `get_water_flag_image()` and `get_admin_boundaries_fc()` in [`stats.py`](src/openforis_whisp/stats.py) - reuses global datasets across all features instead of recreating per-feature
- **Date filtering**: Use module-level `CURRENT_YEAR` constant (from `datetime.now().year`) to avoid repeated calls

### Unit Handling System
Whisp supports both **hectares** and **percent** units:
- Unit type stored in column defined by `stats_unit_type_column` (default: `"Unit"`)
- [`risk.py::detect_unit_type()`](src/openforis_whisp/risk.py) auto-detects or accepts `explicit_unit_type` override
- All rows in a DataFrame **must** use same unit type (no mixing)
- Risk thresholds (e.g., `ind_1_pcent_threshold`) are percentage thresholds regardless of unit type

### Risk Assessment Logic
[`risk.py::whisp_risk()`](src/openforis_whisp/risk.py) implements commodity-specific decision trees:
- **Perennial crops** (coffee, cocoa, rubber, palm): Uses `Risk_PCrop` output
- **Annual crops** (soy): Uses `Risk_ACrop`
- **Livestock**: Uses `Risk_Livestock` (NB still not integrated in Whisp main as of Jan 2026)
- **Timber**: Uses `Risk_Timber` (includes additional categories like primary forests, logging concessions)

Decision tree checks in order:
1. Treecover in 2020? (Indicator 1)
2. Commodity presence in 2020? (Indicator 2)
3. Disturbance before 2020-12-31? (Indicator 3)
4. Disturbance after 2020-12-31? (Indicator 4)

Output values: `"High"`, `"Low"`, `"More info needed"`

## Development Workflows

### Code Style Principles
**CRITICAL - Maintain existing code patterns**:
- **Keep it simple**: Avoid unnecessary complexity or "clever" solutions
- **Match existing style**: Don't introduce decorators, classes, or patterns not already used in the codebase
- **Functional over OOP**: Whisp uses simple functions, not class hierarchies - maintain this approach (until a future refactor)
- **Limit AI fingerprints**: Code should be indistinguishable from existing codebase in style and complexity

**Examples of what NOT to do**:
- ❌ Adding decorators when rest of codebase uses plain functions
- ❌ Creating abstract base classes when existing code uses simple functions
- ❌ Refactoring simple `if/else` into complex patterns
- ❌ Adding type classes (dataclasses, Pydantic models) where dict/DataFrame suffices

**What TO do**:
- ✅ Use same function signature patterns as existing code
- ✅ Keep logic in simple, readable functions like [`datasets.py`](src/openforis_whisp/datasets.py)
- ✅ Match commenting style and verbosity level
- ✅ Preserve existing code organization (no unnecessary restructuring)

**Note on future refactoring**: While the current functional style works well and should be maintained for consistency, comprehensive refactoring to cleaner patterns is possible as a future deliberate effort. However, any such refactoring should be:
- Discussed and planned (not done piecemeal by AI)
- Applied consistently across the entire codebase
- Not mixed with feature development

### Making Changes Safely
**Before modifying core functionality**:
1. Check if change affects API contract (column names, output structure, risk values)
2. Consider impact on downstream systems (QGIS plugin, dashboards, Whisp Map)
3. Maintain backward compatibility or version changes appropriately
4. Document breaking changes clearly for API consumers

### Testing with GEE
```bash
# GEE authentication happens automatically via conftest.py fixture
pytest  # Runs basic tests for Whisp stats)

# Key test: tests/helpers/test_assess_risk.py
# - Uses fixtures/geojson_example.geojson (~50 test geometries)
# - Tests full pipeline: GeoJSON → stats → risk assessment
```

### Adding New Datasets
1. **Add function to [`datasets.py`](src/openforis_whisp/datasets.py)**: Follow naming convention (`g_*_prep` or `nXX_*_prep`)
2. **Add row to [`lookup_gee_datasets.csv`](src/openforis_whisp/parameters/lookup_gee_datasets.csv)**:
   - Set `corresponding_variable` to function name
   - Set `theme` (treecover/commodities/disturbance_before/disturbance_after)
   - Set `use_for_risk=1` or ('use_for_risk_timber' = 1) if dataset should feed into main risk calculations
   - Set `ISO2_code` if national dataset
3. No code changes needed for dataset to appear in output - CSV drives everything!
4. Update documentation files as needed (see Documentation References below)

### Code Quality Tools
```bash
# Poetry for dependency management
poetry install

# Ruff for linting/formatting (configured in pyproject.toml)
# Pre-commit hooks handle automatic formatting

pre-commit install  # Set up hooks
```

**Ruff rules**: Max complexity 10, line length 120, enforces type hints (ANN), imports sorted (I), etc. See [`pyproject.toml`](pyproject.toml) `[tool.ruff]` section.

### Running Locally vs. Colab
- **Local**: Use virtual environment (`.venv`), install with `pip install --pre openforis-whisp`
- **Colab**: See [`notebooks/Colab_whisp_geojson_to_csv.ipynb`](notebooks/Colab_whisp_geojson_to_csv.ipynb) for authentication flow
- **SEPAL**: Special virtual environment setup (see [SEPAL docs](https://docs.sepal.io/en/latest/cli/python.html#virtual-environment))

## Key Gotchas

### Column Name Dependencies
Many column names are hardcoded in various parts of the codebase and external systems. Changing them can break functionality.
Some critical columns:
Indicator (e.g., `ind_1_treecover_2020`) and risk columns (e.g., `risk_pcrop`) are both hardcoded in [src/openforis_whisp/risk.py](src/openforis_whisp/risk.py).

Further standard column names are defined in [src/openforis_whisp/parameters/lookup_context_and_metadata.csv](src/openforis_whisp/parameters/lookup_context_and_metadata.csv). Two key columns are specifically named to help ensure compatibility with external systems (such as the EU TRACES platform for EUDR):
- `Area` (geometry area in hectares), defined in [src/openforis_whisp/datasets.py](src/openforis_whisp/datasets.py#L23)
- `ProducerCountry` (ISO2 country code), defined in [src/openforis_whisp/advanced_stats.py](src/openforis_whisp/advanced_stats.py)


**When adding datasets/columns:**

- For schema validation (drives output structure prior to risk assessment) the following files must be updated:
  - [src/openforis_whisp/parameters/lookup_gee_datasets.csv](src/openforis_whisp/parameters/lookup_gee_datasets.csv)
  - [src/openforis_whisp/parameters/lookup_context_and_metadata.csv](src/openforis_whisp/parameters/lookup_context_and_metadata.csv)
  - [src/openforis_whisp/pd_schemas.py](src/openforis_whisp/pd_schemas.py) (validation logic uses the above lookups)

- For documentation (ensures users and downstream systems are aware of new columns):
  - [layers_description.md](layers_description.md) (dataset and column descriptions)
  - whisp_columns.xlsx (output column documentation for external systems)
In future, updating these may be automated to reduce manual updates to multiple similar files.

### Legacy vs. Modern Functions
[stats.py](src/openforis_whisp/stats.py):

- **Modern**: `whisp_formatted_stats_geojson_to_df()` — uses faster processing from [advanced_stats.py](src/openforis_whisp/advanced_stats.py) when "mode" is "sequential" or "concurrent".
  If "mode" is "legacy", it calls the old function below.
- **Legacy**: `whisp_formatted_stats_geojson_to_df_legacy()` — kept for backward compatibility, but will be removed in the future.

Always use modern functions for new code. Legacy function works correctly but lacks newer optimizations.

### GeoJSON Geometry Handling
- Input: GeoJSON features with Polygon/MultiPolygon geometries
- Internal: Stored as string in `geometry_column` (default `"geo"`) during processing
- Conversion: [`data_conversion.py`](src/openforis_whisp/data_conversion.py) handles GeoJSON ↔ EE ↔  DataFrame transformations
- Tracking geometry changes from conversions: as the conversion process to EE can change the geometries, the optional 'geometry_audit_trail' parameter in `whisp_formatted_stats_geojson_to_df()`, allows the user to retain the original geometry column  (prior to conversion to EE) in the output for comparison.

## Documentation References
- [Full dataset list](layers_description.md): Detailed provenance for all 50+ datasets
- [Example notebooks](notebooks/): End-to-end workflows for different use cases
- [Output columns](whisp_columns.xlsx): Describes all statistics and risk assessment columns (external file)
- [API documentation](https://whisp.openforis.org/documentation/api-guide): For Whisp App integration

## Performance Considerations
- **Batch processing**: Process features in conccurently in multiple batches via `reduceRegions()` and using the GEE high volume endpoint. This is implemented in [`advanced_stats.py::whisp_ee_stats_fc_to_df_concurrent()`](src/openforis_whisp/advanced_stats.py)
- **Asset caching**: Water mask and admin boundaries cached at module level in [`stats.py`](src/openforis_whisp/stats.py)
- **Filtering**: Country-based filtering in [`reformat.py::filter_lookup_by_country_codes()`](src/openforis_whisp/reformat.py) reduces the additional national datasets processed per request. Aim to automate this in future to be driven by the input GeoJSON plot locations.
- **Avoid temporal filters in loops**: Use module constants like `CURRENT_YEAR` instead of `datetime.now()` calls

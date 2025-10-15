# !pip install pandera[io] # special version used
import pandera as pa
import pandas as pd
import os
import logging
import pickle
import hashlib
from pathlib import Path  # Add this import

from openforis_whisp.logger import StdoutLogger, FileLogger

from openforis_whisp.pd_schemas import data_lookup_type


from openforis_whisp.parameters.config_runtime import (
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
    DEFAULT_CONTEXT_LOOKUP_TABLE_PATH,
)

logger = StdoutLogger(__name__)


# Directory for cached schemas
SCHEMA_CACHE_DIR = Path(__file__).parent / "parameters" / ".schema_cache"

# Dictionary to cache schema and modification times for multiple files
cached_schema = None
cached_file_mtimes = {}


def _get_cache_key(file_paths, national_codes=None):
    """
    Generate a stable cache key from file paths and mtimes ONLY.

    National codes are NOT included in the cache key anymore!
    This means we build ONE universal schema cache with all columns,
    and filter at validation time instead.
    """
    key_parts = []

    for file_path in file_paths:
        if Path(file_path).exists():
            mtime = Path(file_path).stat().st_mtime
            key_parts.append(f"{Path(file_path).name}:{mtime}")
        else:
            key_parts.append(f"{Path(file_path).name}:missing")

    # DO NOT include national_codes in cache key
    # This ensures a single universal cache regardless of filtering
    key_parts.append("schema:universal")

    # Create hash for filesystem-safe filename
    cache_string = "|".join(key_parts)
    cache_hash = hashlib.md5(cache_string.encode()).hexdigest()

    return cache_hash, cache_string


def _get_cached_schema_path(cache_key_hash):
    """Get the path to cached schema file"""
    SCHEMA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return SCHEMA_CACHE_DIR / f"schema_{cache_key_hash}.pkl"


def _save_schema_to_disk(schema, cache_key_hash, cache_key_string):
    """Save schema to disk as pickle file with metadata"""
    cache_path = _get_cached_schema_path(cache_key_hash)

    cache_data = {
        "schema": schema,
        "cache_key": cache_key_string,
        "created_at": pd.Timestamp.now().isoformat(),
    }

    try:
        with open(cache_path, "wb") as f:
            pickle.dump(cache_data, f, protocol=pickle.HIGHEST_PROTOCOL)
        logger.info(f"✅ Schema cached to disk: {cache_path.name}")
    except Exception as e:
        logger.warning(f"⚠️  Could not save schema cache to disk: {e}")


def _load_schema_from_disk(cache_key_hash):
    """Load schema from disk cache if it exists"""
    cache_path = _get_cached_schema_path(cache_key_hash)

    if not cache_path.exists():
        return None

    try:
        with open(cache_path, "rb") as f:
            cache_data = pickle.load(f)

        logger.info(
            f"✅ Loaded schema from disk cache (created: {cache_data.get('created_at', 'unknown')})"
        )
        return cache_data["schema"]
    except Exception as e:
        logger.warning(f"⚠️  Could not load schema cache from disk: {e}")
        # If cache is corrupted, remove it
        try:
            cache_path.unlink()
        except:
            pass
        return None


def _should_force_rebuild():
    """Check if schema rebuild should be forced (for development)"""
    return os.environ.get("WHISP_FORCE_SCHEMA_REBUILD", "").lower() in (
        "1",
        "true",
        "yes",
    )


def validate_dataframe_using_lookups(
    df_stats: pd.DataFrame, file_paths: list = None, national_codes: list = None
) -> pd.DataFrame:
    """
    Load the schema and validate the DataFrame against it.

    IMPORTANT: Schema loading and filtering are now separate!
    - Schema is always loaded as universal (all countries)
    - If national_codes is provided, we filter the schema AFTER loading
    - This ensures the same cached schema works for all API requests

    Args:
        df_stats (pd.DataFrame): The DataFrame to validate.
        file_paths (list): List of paths to schema files.
        national_codes (list, optional): List of ISO2 country codes to include.
                                         If provided, filters schema to only these countries.

    Returns:
        pd.DataFrame: The validated DataFrame.
    """

    # Load the UNIVERSAL schema (always includes all countries)
    schema = load_schema_if_any_file_changed(file_paths, national_codes=None)

    # If national_codes filtering is requested, filter the schema NOW
    if national_codes is not None and len(national_codes) > 0:
        schema = _filter_schema_by_country_codes(schema, national_codes)

    # Validate the DataFrame
    validated_df = validate_dataframe(df_stats, schema)

    return validated_df


def _filter_schema_by_country_codes(
    schema: pa.DataFrameSchema, national_codes: list
) -> pa.DataFrameSchema:
    """
    Filter a Pandera schema to only include columns for specified countries.

    Keeps:
    - Global columns (prefixed with 'g_')
    - General columns (not country-specific)
    - Country-specific columns matching the provided ISO2 codes (nXX_)

    Args:
        schema: Full Pandera schema with all countries
        national_codes: List of ISO2 country codes to include

    Returns:
        Filtered schema with only relevant columns
    """
    if not national_codes:
        return schema

    # Normalize codes to lowercase
    normalized_codes = [
        code.lower() for code in national_codes if isinstance(code, str)
    ]

    # Filter schema columns
    filtered_columns = {}

    for col_name, col_spec in schema.columns.items():
        # Always keep global columns
        if col_name.startswith("g_"):
            filtered_columns[col_name] = col_spec
            continue

        # Check if this is a country-specific column
        is_country_column = False
        matched_country = False

        # Country-specific columns follow pattern: nXX_* (e.g., nbr_forest_2020)
        if len(col_name) >= 4 and col_name[0] == "n" and col_name[2] == "_":
            country_code = col_name[1:3].lower()
            if country_code.isalpha():
                is_country_column = True
                if country_code in normalized_codes:
                    matched_country = True

        # Include if it's not country-specific OR if it matches a requested country
        if not is_country_column or matched_country:
            filtered_columns[col_name] = col_spec

    # Create new schema with filtered columns
    filtered_schema = pa.DataFrameSchema(
        filtered_columns,
        strict=schema.strict,
        unique_column_names=schema.unique_column_names,
        add_missing_columns=schema.add_missing_columns,
        coerce=schema.coerce,
    )

    logger.info(
        f"🔍 Filtered schema: {len(schema.columns)} → {len(filtered_schema.columns)} columns for countries: {national_codes}"
    )

    return filtered_schema


def load_schema_if_any_file_changed(file_paths=None, national_codes=None):
    """
    Load schema with intelligent caching:
    1. Check in-memory cache (fastest)
    2. Check disk cache (fast - milliseconds)
    3. Build from lookup files (slow - only when needed)

    IMPORTANT: Schema caching is now INDEPENDENT of national_codes!
    - Schema is always built with ALL country columns (universal)
    - Filtering by national_codes happens at validation time
    - This ensures ONE cache file regardless of API requests

    Cache is invalidated ONLY when:
    - Lookup CSV files are modified
    - WHISP_FORCE_SCHEMA_REBUILD env var is set

    Note: national_codes parameter is kept for backwards compatibility
    but is IGNORED for caching purposes.
    """

    if file_paths is None:
        file_paths = [
            DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
            DEFAULT_CONTEXT_LOOKUP_TABLE_PATH,
        ]

    # Generate cache key based ONLY on file mtimes (not national_codes!)
    cache_key_hash, cache_key_string = _get_cache_key(file_paths)

    # Check if force rebuild is requested
    force_rebuild = _should_force_rebuild()
    if force_rebuild:
        logger.info("🔧 Force rebuild enabled (WHISP_FORCE_SCHEMA_REBUILD=true)")

    # STEP 1: Check in-memory cache (fastest - microseconds)
    if not force_rebuild and (
        hasattr(load_schema_if_any_file_changed, "_cached_schema")
        and hasattr(load_schema_if_any_file_changed, "_last_cache_key")
        and load_schema_if_any_file_changed._last_cache_key == cache_key_hash
    ):
        logger.info(f"⚡ Using in-memory cached universal schema")
        return load_schema_if_any_file_changed._cached_schema

    # STEP 2: Check disk cache (fast - milliseconds)
    if not force_rebuild:
        schema = _load_schema_from_disk(cache_key_hash)
        if schema is not None:
            # Cache in memory for even faster subsequent access
            load_schema_if_any_file_changed._cached_schema = schema
            load_schema_if_any_file_changed._last_cache_key = cache_key_hash
            return schema

    # STEP 3: Build schema from lookup files (slow - seconds)
    logger.info(
        f"🔨 Building universal schema from lookup files (includes ALL countries)"
    )

    # Load and combine lookup files
    combined_lookup_df = append_csvs_to_dataframe(file_paths)

    # DO NOT filter by national codes - keep ALL columns
    # Filtering will happen at validation time in validate_dataframe_using_lookups()

    # Create schema from FULL lookup (all countries)
    schema = create_schema_from_dataframe(combined_lookup_df)

    # Cache in memory
    load_schema_if_any_file_changed._cached_schema = schema
    load_schema_if_any_file_changed._last_cache_key = cache_key_hash

    # Save to disk for future sessions
    _save_schema_to_disk(schema, cache_key_hash, cache_key_string)

    logger.info(f"✅ Universal schema built and cached ({len(schema.columns)} columns)")

    return schema


def validate_dataframe(
    df_stats: pd.DataFrame, schema: pa.DataFrameSchema
) -> pd.DataFrame:
    """Validate the DataFrame against the given schema, reorder columns to match schema order, and list missing columns.

    Args:
        schema (pa.DataFrameSchema): The schema to validate against.
        df_stats (pd.DataFrame): The DataFrame to validate.
        required_false (bool): If True, sets all columns in the schema as optional (required=False).

    Returns:
        pd.DataFrame: The validated DataFrame with columns ordered according to the schema, or None if validation fails.
    """
    log_missing_columns(df_stats, schema)

    # df_stats = df_stats.reindex(schema.columns.keys(), axis=1)

    # Try to automatically coerce the DataFrame to match the schema types
    try:
        validated_df = schema(df_stats)
    except pa.errors.SchemaError as e:
        print("Error during validation:", e)
        # Return None or raise the error if validation fails
        return None  # or raise e

    # Reorder the validated DataFrame to match the schema's column order
    validated_df = validated_df.reindex(schema.columns.keys(), axis=1)

    return validated_df


def append_csvs_to_dataframe(csv_paths):
    """
    Appends multiple CSV files into a single Pandas DataFrame.

    Args:
    - csv_paths (list of str): List of paths to CSV files to append.

    Returns:
    - pd.DataFrame: Combined DataFrame containing data from all provided CSV files.

    Raises:
    - ValueError: If any CSV file cannot be read.
    """

    combined_df = pd.DataFrame()  # Initialize an empty DataFrame

    for path in csv_paths:
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(path)
            # Append to the combined DataFrame
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        except Exception as e:
            raise ValueError(f"Error reading {path}: {e}")

    return combined_df


def create_schema_from_dataframe(schema_df: pd.DataFrame) -> pa.DataFrameSchema:
    """Create a Pandera schema from a DataFrame containing schema information."""

    if schema_df.empty:
        raise ValueError("The input DataFrame is empty.")

    required_columns = ["name", "col_type", "is_nullable", "is_required"]
    missing_columns = [col for col in required_columns if col not in schema_df.columns]
    if missing_columns:
        raise ValueError(f"Missing columns in schema DataFrame: {missing_columns}")

    # print("Schema DataFrame columns:", schema_df.columns)

    # Sort DataFrame by 'order' if it exists
    if "order" in schema_df.columns:
        schema_df = schema_df.sort_values(by="order")

    # Remove rows where 'exclude_from_output' equals 1, if that column exists
    if "exclude_from_output" in schema_df.columns:
        schema_df = schema_df[schema_df["exclude_from_output"] != 1]

    # Create a dictionary to hold the column schema
    schema_dict = {}
    for _, row in schema_df.iterrows():
        col_name = row["name"]
        col_type = row["col_type"]
        is_nullable = row["is_nullable"] in (1, "1", True, "True")
        is_required = row["is_required"] in (1, "1", True, "True")

        # print(
        #     f"Processing column: {col_name}, Type: {col_type}, Nullable: {is_nullable}, Required: {is_required}"
        # )

        # Map DataFrame types to Pandera types
        if col_type == "int64":
            schema_dict[col_name] = pa.Column(
                pa.Int64, nullable=is_nullable, required=is_required
            )
        elif col_type == "int":
            schema_dict[col_name] = pa.Column(
                pa.Int, nullable=is_nullable, required=is_required
            )
        elif col_type == "string":
            schema_dict[col_name] = pa.Column(
                pa.String, nullable=is_nullable, required=is_required
            )
        elif col_type == "float32":
            schema_dict[col_name] = pa.Column(
                pa.Float32, nullable=is_nullable, required=is_required
            )
        elif col_type == "float64":
            schema_dict[col_name] = pa.Column(
                pa.Float64, nullable=is_nullable, required=is_required
            )
        elif col_type == "bool":
            schema_dict[col_name] = pa.Column(
                pa.Bool, nullable=is_nullable, required=is_required
            )
        else:
            raise ValueError(f"Unsupported type: {col_type}")

    # Create and return the DataFrame schema with coercion enabled
    schema = pa.DataFrameSchema(
        schema_dict,
        strict=False,
        unique_column_names=True,
        add_missing_columns=True,
        coerce=True,
    )

    return schema


# def setup_logger(name):
#     # Create and configure logger
#     logging.basicConfig(level=logging.INFO)
#     logger = logging.getLogger(name)
#     return logger


def log_missing_columns(df_stats: pd.DataFrame, template_schema: pa.DataFrameSchema):
    # Initialize the logger
    logger = setup_logger(__name__)

    # Extract the expected columns from the DataFrameSchema
    template_columns = template_schema.columns.keys()
    df_stats_columns = df_stats.columns

    # Find missing columns
    missing_in_template = [
        col for col in df_stats_columns if col not in template_columns
    ]
    missing_in_stats = [col for col in template_columns if col not in df_stats_columns]

    # Log results for missing columns in df_stats
    if missing_in_template:
        logger.warning(
            f"The following columns from the results dataframe did not match any columns in the schema: \n{', '.join(missing_in_template)}"
        )
    else:
        logger.info("All columns from dataframe found in the schema.")

    # Log results for missing columns in template_df
    if missing_in_stats:
        logger.warning(
            f"The following columns in the schema did not match any columns from the results dataframe: \n{', '.join(missing_in_stats)}"
        )
    else:
        logger.info("All columns from the schema found in the results dataframe.")


def setup_logger(name):
    """
    Set up a logger with a specific name to avoid duplicate logs.
    """
    logger = logging.getLogger(name)
    if not logger.hasHandlers():
        # Create handlers only if there are none
        stdout_handler = logging.StreamHandler()
        file_handler = logging.FileHandler("missing_columns.log")

        # Set levels
        stdout_handler.setLevel(logging.WARNING)
        file_handler.setLevel(logging.WARNING)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        stdout_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # Add handlers to the logger
        logger.addHandler(stdout_handler)
        logger.addHandler(file_handler)

    return logger


def filter_lookup_by_country_codes(
    lookup_df: pd.DataFrame, national_codes: list
) -> pd.DataFrame:
    """
    Filter lookup DataFrame to include only:
    1. Global columns (prefixed with 'g_')
    2. General columns (not country-specific)
    3. Country-specific columns matching the provided ISO2 codes

    Args:
        lookup_df (pd.DataFrame): The lookup DataFrame used to create the schema
        national_codes (list): List of ISO2 country codes to include

    Returns:
        pd.DataFrame: Filtered lookup DataFrame
    """
    if not national_codes:
        return lookup_df

    # Normalize national_codes to lowercase for case-insensitive comparison
    normalized_codes = [
        code.lower() for code in national_codes if isinstance(code, str)
    ]

    # Keep track of rows to filter out
    rows_to_remove = []

    # Process each row in the lookup DataFrame
    for idx, row in lookup_df.iterrows():
        col_name = row["name"]

        # Skip if not a column name entry
        if pd.isna(col_name):
            continue

        # Always keep global columns (g_) and columns that aren't country-specific
        if col_name.startswith("g_"):
            continue

        # Check if this is a country-specific column (nXX_)
        is_country_column = False
        matched_country = False

        # Look for pattern nXX_ which would indicate a country-specific column
        for i in range(len(col_name) - 3):
            if (
                col_name[i : i + 1].lower() == "n"
                and len(col_name) > i + 3
                and col_name[i + 3 : i + 4] == "_"
            ):
                country_code = col_name[i + 1 : i + 3].lower()
                is_country_column = True
                if country_code in normalized_codes:
                    matched_country = True
                break

        # If it's a country column but doesn't match our list, flag for removal
        if is_country_column and not matched_country:
            rows_to_remove.append(idx)

    # Filter out rows for countries not in our list
    if rows_to_remove:
        return lookup_df.drop(rows_to_remove)


#     return lookup_df
def filter_lookup_by_country_codes(
    lookup_df: pd.DataFrame, national_codes: list = None
) -> pd.DataFrame:
    """
    Filter lookup DataFrame to include only:
    1. Global columns (prefixed with 'g_')
    2. General columns (not country-specific)
    3. Country-specific columns matching the provided ISO2 codes (if national_codes provided)

    If no national_codes are provided, ALL country-specific columns are filtered out.

    Args:
        lookup_df (pd.DataFrame): The lookup DataFrame used to create the schema
        national_codes (list, optional): List of ISO2 country codes to include.
                                        If None, all country-specific columns are removed.

    Returns:
        pd.DataFrame: Filtered lookup DataFrame
    """

    # Normalize national_codes to lowercase for case-insensitive comparison
    if national_codes:
        normalized_codes = [
            code.lower() for code in national_codes if isinstance(code, str)
        ]
    else:
        normalized_codes = []

    # Keep track of rows to remove
    rows_to_remove = []

    # Process each row in the lookup DataFrame
    for idx, row in lookup_df.iterrows():
        col_name = row["name"]

        # Skip if not a column name entry
        if pd.isna(col_name):
            continue

        # Always keep global columns (g_) and general columns
        if col_name.startswith("g_"):
            continue

        # Check if this is a country-specific column (nXX_)
        is_country_column = False
        matched_country = False

        # Look for pattern nXX_ which indicates a country-specific column
        for i in range(len(col_name) - 3):
            if (
                col_name[i : i + 1].lower() == "n"
                and len(col_name) > i + 3
                and col_name[i + 3 : i + 4] == "_"
            ):
                country_code = col_name[i + 1 : i + 3].lower()
                is_country_column = True

                # Only match if we have national_codes AND this country is in the list
                if national_codes and country_code in normalized_codes:
                    matched_country = True
                break

        # Remove country-specific columns that don't match our criteria:
        # - If no national_codes provided: remove ALL country columns
        # - If national_codes provided: remove country columns NOT in the list
        if is_country_column and not matched_country:
            rows_to_remove.append(idx)

    # Filter out flagged rows
    if rows_to_remove:
        print(
            f"Filtering out {(rows_to_remove)} country-specific row(s) not matching criteria"
        )
        filtered_df = lookup_df.drop(rows_to_remove)

        # Filter out flagged rows
    #     if rows_to_remove:
    #         # Create detailed debug info
    #         removed_rows_info = []
    #         for idx in rows_to_remove:
    #             row_name = lookup_df.loc[idx, "name"]
    #             removed_rows_info.append({
    #                 'index': idx,
    #                 'name': row_name
    #             })

    #         # Extract just the column names for easy viewing
    #         removed_column_names = [info['name'] for info in removed_rows_info]

    #         print(f"Filtered out {len(rows_to_remove)} country-specific row(s) not matching criteria")
    #         print(f"Removed column names: {removed_column_names}")
    #         return filtered_df

    return lookup_df


def filter_lookup_by_country_codes(
    lookup_df: pd.DataFrame, filter_col, national_codes: list = None
):
    """Filter by actual ISO2 column values instead of column name patterns"""

    if not national_codes:
        # Remove all rows with country codes
        rows_with_country_codes = ~lookup_df[filter_col].isna()
        removed_names = lookup_df[rows_with_country_codes]["name"].tolist()
        logger.debug(
            f"No national codes provided - removing {len(removed_names)} rows with country codes"
        )
        logger.debug(f"Removed column names: {removed_names}")
        return lookup_df[lookup_df[filter_col].isna()]

    logger.debug(f"Filtering for national codes: {national_codes}")
    logger.debug(f"Total rows before filtering: {len(lookup_df)}")

    # Keep rows with no country code (global) OR matching country codes
    normalized_codes = [code.lower() for code in national_codes]

    mask = lookup_df[filter_col].isna() | lookup_df[  # Global datasets
        filter_col
    ].str.lower().isin(
        normalized_codes
    )  # Matching countries

    logger.debug(
        f"Filtering lookup by country codes: {national_codes}, keeping {mask.sum()} rows"
    )

    return lookup_df[mask]


def validate_dataframe_using_lookups_flexible(
    df_stats: pd.DataFrame,
    file_paths: list = None,
    national_codes: list = None,
    custom_bands=None,
) -> pd.DataFrame:
    """
    Load schema and validate DataFrame while handling custom bands properly.

    IMPORTANT: Schema loading and filtering are now separate!
    - Schema is always loaded as universal (all countries)
    - If national_codes is provided, we filter the schema AFTER loading
    - This ensures the same cached schema works for all API requests

    Parameters
    ----------
    df_stats : pd.DataFrame
        DataFrame to validate
    file_paths : list, optional
        Schema file paths
    national_codes : list, optional
        Country codes for filtering - filters schema AFTER loading
    custom_bands : list or dict or None, optional
        Custom band information:
        - List: ['band1', 'band2'] - only preserves these specific bands
        - Dict: {'band1': 'float64', 'band2': 'int64'} - validates these specific bands with types
        - None: excludes ALL custom bands (strict mode)

    Returns
    -------
    pd.DataFrame
        Validated DataFrame with custom bands handled according to specification
    """
    # Load the UNIVERSAL schema (always includes all countries)
    schema = load_schema_if_any_file_changed(file_paths, national_codes=None)

    # If national_codes filtering is requested, filter the schema NOW
    if national_codes is not None and len(national_codes) > 0:
        schema = _filter_schema_by_country_codes(schema, national_codes)

    schema_columns = list(schema.columns.keys())

    # Identify extra columns
    df_columns = df_stats.columns.tolist()
    extra_columns = [col for col in df_columns if col not in schema_columns]
    schema_only_columns = [col for col in df_columns if col in schema_columns]

    if extra_columns:
        logger.info(f"Found {len(extra_columns)} extra columns: {extra_columns}")

        # Split DataFrame
        df_schema_part = (
            df_stats[schema_only_columns].copy()
            if schema_only_columns
            else pd.DataFrame()
        )
        df_extra_part = df_stats[extra_columns].copy()

        # Validate schema columns if any exist
        if not df_schema_part.empty:
            try:
                validated_schema_part = validate_dataframe(df_schema_part, schema)
            except Exception as e:
                logger.error(f"Schema validation failed: {e}")
                validated_schema_part = (
                    df_schema_part  # Keep original if validation fails
                )
        else:
            validated_schema_part = pd.DataFrame()

        # ========== KEY FIX: Handle custom_bands=None properly ==========
        if custom_bands is None:
            # STRICT MODE: Exclude all custom bands when None
            logger.info("custom_bands=None: Excluding all custom bands (strict mode)")
            # Return only the schema columns, no extra columns
            return (
                validated_schema_part
                if not validated_schema_part.empty
                else pd.DataFrame()
            )

        elif custom_bands is not None:
            # Process custom bands as specified
            df_extra_part = _process_custom_bands(df_extra_part, custom_bands)

            # Combine results
            if not validated_schema_part.empty and not df_extra_part.empty:
                result = pd.concat([validated_schema_part, df_extra_part], axis=1)
            elif not validated_schema_part.empty:
                result = validated_schema_part
            else:
                result = df_extra_part

            # Reorder: schema columns first, then extra columns
            if not validated_schema_part.empty:
                ordered_columns = [
                    col for col in schema_columns if col in result.columns
                ] + [col for col in df_extra_part.columns]
                result = result[ordered_columns]

            return result

    else:
        # No extra columns - use normal validation
        return validate_dataframe(df_stats, schema)


def _process_custom_bands(df_extra: pd.DataFrame, custom_bands) -> pd.DataFrame:
    """
    Process custom bands according to user specifications.

    Parameters
    ----------
    df_extra : pd.DataFrame
        DataFrame with extra columns
    custom_bands : list or dict
        Custom band specifications

    Returns
    -------
    pd.DataFrame
        Processed DataFrame with custom bands
    """
    if isinstance(custom_bands, list):
        # Just preserve specified columns as-is
        custom_band_cols = [col for col in custom_bands if col in df_extra.columns]
        if custom_band_cols:
            logger.info(f"Preserving custom bands as-is: {custom_band_cols}")
            return df_extra[custom_band_cols]
        else:
            logger.warning(
                f"None of the specified custom bands {custom_bands} found in DataFrame"
            )
            return df_extra

    elif isinstance(custom_bands, dict):
        # Apply type conversions
        result_df = df_extra.copy()

        for band_name, target_type in custom_bands.items():
            if band_name in result_df.columns:
                try:
                    if target_type == "float64":
                        result_df[band_name] = pd.to_numeric(
                            result_df[band_name], errors="coerce"
                        ).astype("float64")
                    elif target_type == "float32":
                        result_df[band_name] = pd.to_numeric(
                            result_df[band_name], errors="coerce"
                        ).astype("float32")
                    elif target_type == "int64":
                        result_df[band_name] = pd.to_numeric(
                            result_df[band_name], errors="coerce"
                        ).astype(
                            "Int64"
                        )  # Nullable int
                    elif target_type == "string":
                        result_df[band_name] = result_df[band_name].astype("string")
                    elif target_type == "bool":
                        result_df[band_name] = result_df[band_name].astype("bool")

                    logger.info(f"Converted {band_name} to {target_type}")

                except Exception as e:
                    logger.warning(
                        f"Failed to convert {band_name} to {target_type}: {e}"
                    )
            else:
                logger.warning(f"Custom band {band_name} not found in DataFrame")

        return result_df

    else:
        # Unknown format, just return as-is
        logger.warning(
            f"Unknown custom_bands format: {type(custom_bands)}. Preserving all extra columns as-is."
        )
        return df_extra


# Fix the duplicate logging issue
def log_missing_columns(df_stats: pd.DataFrame, template_schema: pa.DataFrameSchema):
    # Remove the duplicate logger creation line
    # logger = setup_logger(__name__)  # DELETE THIS LINE

    # Use the existing module-level logger (line 18: logger = StdoutLogger(__name__))

    # Extract the expected columns from the DataFrameSchema
    template_columns = list(template_schema.columns.keys())
    df_stats_columns = df_stats.columns.tolist()

    # Find missing and extra columns
    missing_in_df = [col for col in template_columns if col not in df_stats_columns]
    extra_in_df = [col for col in df_stats_columns if col not in template_columns]

    # Log missing schema columns
    if missing_in_df:
        logger.warning(f"Missing expected schema columns: {missing_in_df}")
    else:
        logger.info("All expected schema columns found in DataFrame.")

    # Log extra columns (will be preserved)
    if extra_in_df:
        logger.info(f"Extra columns found (will be preserved): {extra_in_df}")
    else:
        logger.info("No extra columns found in DataFrame.")


def clear_schema_cache():
    """
    Clear all cached schema files from disk.

    Useful for:
    - Troubleshooting schema issues
    - After updating lookup files manually
    - Development/testing

    Returns:
        int: Number of cache files removed
    """
    if not SCHEMA_CACHE_DIR.exists():
        logger.info("No schema cache directory found")
        return 0

    cache_files = list(SCHEMA_CACHE_DIR.glob("schema_*.pkl"))
    removed_count = 0

    for cache_file in cache_files:
        try:
            cache_file.unlink()
            removed_count += 1
        except Exception as e:
            logger.warning(f"Could not remove {cache_file.name}: {e}")

    logger.info(f"✅ Removed {removed_count} cached schema file(s)")
    return removed_count


def get_schema_cache_info():
    """
    Get information about cached schemas.

    Returns:
        dict: Dictionary with cache statistics and file info
    """
    if not SCHEMA_CACHE_DIR.exists():
        return {
            "cache_dir": str(SCHEMA_CACHE_DIR),
            "exists": False,
            "num_files": 0,
            "total_size_mb": 0,
            "files": [],
        }

    cache_files = list(SCHEMA_CACHE_DIR.glob("schema_*.pkl"))
    total_size = sum(f.stat().st_size for f in cache_files)

    file_info = []
    for cache_file in cache_files:
        try:
            with open(cache_file, "rb") as f:
                cache_data = pickle.load(f)

            # Parse cache key to show what it's for
            cache_key = cache_data.get("cache_key", "unknown")
            is_universal = "nc:universal" in cache_key

            file_info.append(
                {
                    "filename": cache_file.name,
                    "size_kb": cache_file.stat().st_size / 1024,
                    "created_at": cache_data.get("created_at", "unknown"),
                    "cache_key": cache_key[:100] + "..."
                    if len(cache_key) > 100
                    else cache_key,
                    "is_universal": is_universal,  # Works for all countries
                    "description": "Universal (all countries)"
                    if is_universal
                    else "Filtered by country",
                }
            )
        except:
            file_info.append(
                {
                    "filename": cache_file.name,
                    "size_kb": cache_file.stat().st_size / 1024,
                    "created_at": "corrupted",
                    "cache_key": "corrupted",
                    "is_universal": False,
                    "description": "Corrupted cache file",
                }
            )

    return {
        "cache_dir": str(SCHEMA_CACHE_DIR),
        "exists": True,
        "num_files": len(cache_files),
        "total_size_mb": total_size / (1024 * 1024),
        "files": file_info,
    }

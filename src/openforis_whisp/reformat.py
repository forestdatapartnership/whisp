# !pip install pandera[io] # special version used
# Support both old and new pandera import paths
try:
    import pandera.pandas as pa
except (ImportError, ModuleNotFoundError):
    import pandera as pa

import pandas as pd
import os
import logging
from pathlib import Path  # Add this import

from openforis_whisp.logger import StdoutLogger, FileLogger

from openforis_whisp.pd_schemas import data_lookup_type


from openforis_whisp.parameters.config_runtime import (
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
    DEFAULT_CONTEXT_LOOKUP_TABLE_PATH,
)

logger = StdoutLogger(__name__)


# Dictionary to cache schema and modification times for multiple files
cached_schema = None
cached_file_mtimes = {}


def validate_dataframe_using_lookups(
    df_stats: pd.DataFrame, file_paths: list = None, national_codes: list = None
) -> pd.DataFrame:
    """
    Load the schema if any file in the list has changed and validate the DataFrame against the loaded schema.
    Optionally filter columns by country code.

    Args:
        df_stats (pd.DataFrame): The DataFrame to validate.
        file_paths (list): List of paths to schema files.
        national_codes (list, optional): List of ISO2 country codes to include.

    Returns:
        pd.DataFrame: The validated DataFrame.
    """

    # Load the schema
    schema = load_schema_if_any_file_changed(file_paths, national_codes=national_codes)

    # Validate the DataFrame
    validated_df = validate_dataframe(df_stats, schema)

    return validated_df


def load_schema_if_any_file_changed(file_paths=None, national_codes=None):
    """Load schema if files changed OR if national_codes changed"""

    if file_paths is None:
        file_paths = [
            DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
            DEFAULT_CONTEXT_LOOKUP_TABLE_PATH,
        ]

    # Include national_codes in cache key (including None case)
    cache_key_parts = []
    for file_path in file_paths:
        if Path(file_path).exists():
            mtime = Path(file_path).stat().st_mtime
            cache_key_parts.append(f"{file_path}:{mtime}")
        else:
            cache_key_parts.append(f"{file_path}:missing")

    # Always include national_codes in cache key (even if None)
    national_codes_key = (
        str(sorted(national_codes)) if national_codes else "no_countries"
    )
    cache_key_parts.append(f"national_codes:{national_codes_key}")

    current_cache_key = "|".join(cache_key_parts)

    # Check cache
    if (
        not hasattr(load_schema_if_any_file_changed, "_cached_schema")
        or not hasattr(load_schema_if_any_file_changed, "_last_cache_key")
        or load_schema_if_any_file_changed._last_cache_key != current_cache_key
    ):

        # Suppress verbose output
        if logger.level <= logging.INFO:
            logger.debug(f"Creating schema for national_codes: {national_codes}")
        # else: suppress entirely

        # Load and combine lookup files
        combined_lookup_df = append_csvs_to_dataframe(file_paths)

        # ALWAYS filter by national codes (even if None - this removes all country columns)
        filtered_lookup_df = filter_lookup_by_country_codes(
            lookup_df=combined_lookup_df,
            filter_col="ISO2_code",
            national_codes=national_codes,
        )

        # Create schema from filtered lookup
        schema = create_schema_from_dataframe(filtered_lookup_df)

        # Cache the results
        load_schema_if_any_file_changed._cached_schema = schema
        load_schema_if_any_file_changed._last_cache_key = current_cache_key

        return schema
    else:
        # Suppress verbose output
        if logger.level <= logging.INFO:
            logger.debug(f"Using cached schema for national_codes: {national_codes}")
        # else: suppress entirely
        return load_schema_if_any_file_changed._cached_schema


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
    _log_missing_columns(df_stats, schema)

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


def _log_missing_columns(df_stats: pd.DataFrame, template_schema: pa.DataFrameSchema):
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

    Parameters
    ----------
    df_stats : pd.DataFrame
        DataFrame to validate
    file_paths : list, optional
        Schema file paths
    national_codes : list, optional
        Country codes for filtering
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
    # Load default schema
    schema = load_schema_if_any_file_changed(file_paths, national_codes=national_codes)
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
def format_stats_dataframe(
    df,
    area_col="Area_sum",
    decimal_places=2,
    unit_type="ha",
    stats_unit_type_column="Unit",
    strip_suffix="_sum",
    remove_columns=True,
    remove_columns_suffix="_median",
    convert_water_flag=True,
    water_flag_column="In_waterbody_sum",
    water_flag_threshold=0.5,
    sort_column="plotId",
):
    """Flexible stats formatting for DataFrame columns.

    - Converts columns ending with `strip_suffix` (default '_sum') to hectares or percent.
    - Removes columns ending with `remove_columns_suffix` (default '_median') if `remove_columns` is True.
    - Optionally converts a water-flag stat into a boolean column based on the threshold compared to `area_col`.
    - Strips the `strip_suffix` from produced stat column names (so 'Cocoa_sum' -> 'Cocoa').
    - Fills `stats_unit_type_column` with `unit_type` for every row.

    Returns a new DataFrame (copy) with conversions applied. Helper sub-functions are used for clarity
    and to avoid fragmenting the original DataFrame (we build new columns and concat once).

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with stats columns
    area_col : str
        Name of area column (default 'Area_sum')
    decimal_places : int
        Decimal places for rounding (default 2)
    unit_type : str
        'ha' or 'percent' (default 'ha')
    stats_unit_type_column : str
        Column name for unit type (default 'Unit')
    strip_suffix : str
        Suffix to strip from stat column names (default '_sum')
    remove_columns : bool
        Whether to remove columns with remove_columns_suffix (default True)
    remove_columns_suffix : str
        Suffix for columns to remove (default '_median')
    convert_water_flag : bool
        Whether to convert water flag to boolean (default True)
    water_flag_column : str
        Name of water flag column (default 'In_waterbody_sum')
    water_flag_threshold : float
        Threshold for water flag ratio (default 0.5)
    sort_column : str
        Column to sort by, or None to skip sorting (default "plotId")

    Returns
    -------
    pd.DataFrame
        Formatted DataFrame with converted values and updated column names
    """
    # Helper: find stat columns that end with the strip_suffix (and are not the area_col)
    def _collect_stat_columns(columns, strip_suffix, area_col):
        cols = [c for c in columns if c.endswith(strip_suffix) and c != area_col]
        return cols

    # Helper: drop columns with a given suffix
    def _drop_suffix_columns(df, suffix):
        if suffix is None or suffix == "":
            logger.debug(f"Suffix is None or empty, returning df as-is")
            return df
        cols_to_drop = df.columns[df.columns.str.endswith(suffix)].tolist()
        logger.debug(f"Columns ending with '{suffix}': {cols_to_drop}")
        result = df.loc[:, ~df.columns.str.endswith(suffix)]
        logger.debug(f"After dropping '{suffix}': {result.columns.tolist()}")
        return result

    # Helper: build converted stats (returns DataFrame of new columns indexed same as df)
    def _build_converted_stats(
        df, stat_cols, area_col, unit_type, decimal_places, strip_suffix
    ):
        area = df[area_col].replace(0, float("nan"))
        new = {}
        for col in stat_cols:
            base = (
                col[: -len(strip_suffix)]
                if strip_suffix and col.endswith(strip_suffix)
                else col
            )
            if unit_type == "ha":
                # value is in whatever units the sum uses (ee outputs square meters) -> convert to hectares
                # (user earlier used divide by 10000 pattern)
                new[base] = (df[col] / 10000).round(decimal_places)
            elif unit_type == "percent":
                new[base] = ((df[col] / area) * 100).round(decimal_places)
            else:
                # unknown unit type: just copy the raw sums
                new[base] = df[col].round(decimal_places)
        df[area_col] = (df[area_col] / 10000).round(decimal_places)
        return pd.DataFrame(new, index=df.index)

    # Helper: convert water flag stat (if present) into bool by thresholding water_area / total_area
    def _apply_water_flag(df, water_flag_column, strip_suffix, area_col, threshold):
        # possible names for water stat: exact provided name, name+suffix
        candidates = []
        if water_flag_column in df.columns:
            candidates.append(water_flag_column)
        suffixed = water_flag_column + strip_suffix if strip_suffix else None
        if suffixed and suffixed in df.columns:
            candidates.append(suffixed)
        # also check generic 'water' candidates
        if "water" + strip_suffix in df.columns:
            candidates.append("water" + strip_suffix)
        if not candidates:
            # nothing to do
            return df
        # pick first available candidate
        water_col = candidates[0]
        total_area = df[area_col].replace(0, float("nan"))
        # compute ratio
        ratio = df[water_col] / total_area
        df[water_flag_column] = (ratio > threshold).astype(bool)
        return df

    # 1) Work on a shallow copy to avoid mutating caller inplace accidentally
    df = df.copy()

    # 2) Optionally drop median (or other) columns
    if remove_columns and remove_columns_suffix:
        logger.debug(f"Dropping columns ending with '{remove_columns_suffix}'")
        logger.debug(
            f"Columns before drop: {[c for c in df.columns if c.endswith(remove_columns_suffix)]}"
        )
        df = _drop_suffix_columns(df, remove_columns_suffix)
        logger.debug(
            f"Columns after drop: {[c for c in df.columns if c.endswith(remove_columns_suffix)]}"
        )

    # 3) Collect stat columns to convert (those ending with strip_suffix and not equal to area_col)
    # EXCLUDE intermediate admin/context columns that should be completely dropped, not stripped
    columns_to_exclude_from_stripping = ["admin_code", "water_flag"]
    stat_cols = _collect_stat_columns(df.columns, strip_suffix, area_col)
    stat_cols = [
        c
        for c in stat_cols
        if not any(exc in c for exc in columns_to_exclude_from_stripping)
    ]
    logger.debug(
        f"Stat columns after excluding intermediate admin/context columns: {stat_cols}"
    )

    # 4) Build converted stats DataFrame (these will have suffix removed as column names)
    if stat_cols:
        converted_stats_df = _build_converted_stats(
            df, stat_cols, area_col, unit_type, decimal_places, strip_suffix
        )
    else:
        converted_stats_df = pd.DataFrame(index=df.index)

    # 5) Remove original stat columns (the ones with strip_suffix) from df (but keep area_col)
    df = df.loc[
        :, [c for c in df.columns if not (c.endswith(strip_suffix) and c != area_col)]
    ]

    # 6) Concatenate converted stats into df in one go to avoid fragmentation
    if not converted_stats_df.empty:
        df = pd.concat([df, converted_stats_df], axis=1)

    # 7) Fill stats unit type column
    df[stats_unit_type_column] = unit_type

    # 8) Optionally convert water flag to boolean
    if convert_water_flag:
        df = _apply_water_flag(
            df, water_flag_column, strip_suffix, area_col, water_flag_threshold
        )

    # 9) rename area_col by stripping suffix from area_col
    area_col_stripped = (
        area_col[: -len(strip_suffix)] if area_col.endswith(strip_suffix) else area_col
    )
    df.rename(columns={area_col: area_col_stripped}, inplace=True)

    # 10) reorder by plotId column if present
    df = (
        df.sort_values(sort_column).reset_index(drop=True)
        if sort_column in df.columns
        else df
    )

    # 11) Defragment final DataFrame and return
    return df.copy()

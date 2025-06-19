# !pip install pandera[io] # special version used
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

        print(f"Creating schema for national_codes: {national_codes}")

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
        print(f"Using cached schema for national_codes: {national_codes}")
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


def setup_logger(name):
    # Create and configure logger
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(name)
    return logger


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


# def filter_lookup_by_country_codes(
#     lookup_df: pd.DataFrame, national_codes: list
# ) -> pd.DataFrame:
#     """
#     Filter lookup DataFrame to include only:
#     1. Global columns (prefixed with 'g_')
#     2. General columns (not country-specific)
#     3. Country-specific columns matching the provided ISO2 codes

#     Args:
#         lookup_df (pd.DataFrame): The lookup DataFrame used to create the schema
#         national_codes (list): List of ISO2 country codes to include

#     Returns:
#         pd.DataFrame: Filtered lookup DataFrame
#     """
#     if not national_codes:
#         return lookup_df

#     # Normalize national_codes to lowercase for case-insensitive comparison
#     normalized_codes = [
#         code.lower() for code in national_codes if isinstance(code, str)
#     ]

#     # Keep track of rows to filter out
#     rows_to_remove = []

#     # Process each row in the lookup DataFrame
#     for idx, row in lookup_df.iterrows():
#         col_name = row["name"]

#         # Skip if not a column name entry
#         if pd.isna(col_name):
#             continue

#         # Always keep global columns (g_) and columns that aren't country-specific
#         if col_name.startswith("g_"):
#             continue

#         # Check if this is a country-specific column (nXX_)
#         is_country_column = False
#         matched_country = False

#         # Look for pattern nXX_ which would indicate a country-specific column
#         for i in range(len(col_name) - 3):
#             if (
#                 col_name[i : i + 1].lower() == "n"
#                 and len(col_name) > i + 3
#                 and col_name[i + 3 : i + 4] == "_"
#             ):
#                 country_code = col_name[i + 1 : i + 3].lower()
#                 is_country_column = True
#                 if country_code in normalized_codes:
#                     matched_country = True
#                 break

#         # If it's a country column but doesn't match our list, flag for removal
#         if is_country_column and not matched_country:
#             rows_to_remove.append(idx)

#     # Filter out rows for countries not in our list
#     if rows_to_remove:
#         return lookup_df.drop(rows_to_remove)

# #     return lookup_df
# def filter_lookup_by_country_codes(
#     lookup_df: pd.DataFrame, national_codes: list = None
# ) -> pd.DataFrame:
#     """
#     Filter lookup DataFrame to include only:
#     1. Global columns (prefixed with 'g_')
#     2. General columns (not country-specific)
#     3. Country-specific columns matching the provided ISO2 codes (if national_codes provided)

#     If no national_codes are provided, ALL country-specific columns are filtered out.

#     Args:
#         lookup_df (pd.DataFrame): The lookup DataFrame used to create the schema
#         national_codes (list, optional): List of ISO2 country codes to include.
#                                        If None, all country-specific columns are removed.

#     Returns:
#         pd.DataFrame: Filtered lookup DataFrame
#     """

#     # Normalize national_codes to lowercase for case-insensitive comparison
#     if national_codes:
#         normalized_codes = [
#             code.lower() for code in national_codes if isinstance(code, str)
#         ]
#     else:
#         normalized_codes = []

#     # Keep track of rows to remove
#     rows_to_remove = []

#     # Process each row in the lookup DataFrame
#     for idx, row in lookup_df.iterrows():
#         col_name = row["name"]

#         # Skip if not a column name entry
#         if pd.isna(col_name):
#             continue

#         # Always keep global columns (g_) and general columns
#         if col_name.startswith("g_"):
#             continue

#         # Check if this is a country-specific column (nXX_)
#         is_country_column = False
#         matched_country = False

#         # Look for pattern nXX_ which indicates a country-specific column
#         for i in range(len(col_name) - 3):
#             if (
#                 col_name[i : i + 1].lower() == "n"
#                 and len(col_name) > i + 3
#                 and col_name[i + 3 : i + 4] == "_"
#             ):
#                 country_code = col_name[i + 1 : i + 3].lower()
#                 is_country_column = True

#                 # Only match if we have national_codes AND this country is in the list
#                 if national_codes and country_code in normalized_codes:
#                     matched_country = True
#                 break

#         # Remove country-specific columns that don't match our criteria:
#         # - If no national_codes provided: remove ALL country columns
#         # - If national_codes provided: remove country columns NOT in the list
#         if is_country_column and not matched_country:
#             rows_to_remove.append(idx)

#     # Filter out flagged rows
#     if rows_to_remove:
#         print(f"Filtering out {(rows_to_remove)} country-specific row(s) not matching criteria")
#         filtered_df = lookup_df.drop(rows_to_remove)

#         # Filter out flagged rows
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

#     return lookup_df


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

# !pip install pandera[io] # special version used
import pandera as pa
import pandas as pd
import os
import logging


from whisp.logger import StdoutLogger, FileLogger

from whisp.pd_schemas import data_lookup_type


from whisp.parameters.config_runtime import (
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
    DEFAULT_CONTEXT_LOOKUP_TABLE_PATH,
)

logger = StdoutLogger(__name__)


# Dictionary to cache schema and modification times for multiple files
cached_schema = None
cached_file_mtimes = {}


def validate_dataframe_using_lookups(
    df_stats: pd.DataFrame, file_paths: list = None
) -> pd.DataFrame:
    """
    Load the schema if any file in the list has changed and validate the DataFrame against the loaded schema.

    Args:
        df_stats (pd.DataFrame): The DataFrame to validate.
        file_paths (list): List of paths to schema files.

    Returns:
        pd.DataFrame: The validated DataFrame.
    """
    # Load the schema
    schema = load_schema_if_any_file_changed(file_paths)

    # Validate the DataFrame
    validated_df = validate_dataframe(df_stats, schema)

    return validated_df


# NB uses default inputs. If you want to use custom inputs, you can pass them as arguments
def load_schema_if_any_file_changed(file_paths):
    """Load schema only if any file in the list has changed."""
    global cached_schema, cached_file_mtimes

    if file_paths is None:
        file_paths = [
            DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
            DEFAULT_CONTEXT_LOOKUP_TABLE_PATH,
        ]

    # Flag to indicate if any file has changed
    schema_needs_update = False

    # Check each file's modification time
    for file_path in file_paths:
        current_mtime = os.path.getmtime(file_path)

        # If the file is new or has been modified, mark schema for update
        if (
            file_path not in cached_file_mtimes
            or current_mtime != cached_file_mtimes[file_path]
        ):
            print(f"File {file_path} changed, updating schema...")
            schema_needs_update = True
            cached_file_mtimes[
                file_path
            ] = current_mtime  # Update the modification time

    # If any file has changed, update the schema
    if schema_needs_update or cached_schema is None:
        print("Creating or updating schema based on changed files...")
        # You can combine the files as needed; here we assume one schema file
        # If you want to handle multiple schema files differently, adjust this

        # add checks on lookup inputs (i.e. a dataframe in type format: data_lookup_type)
        combined_lookup_df: data_lookup_type = append_csvs_to_dataframe(
            file_paths
        )  # concatonates input lookup files

        cached_schema = create_schema_from_dataframe(
            combined_lookup_df
        )  # create cached schema

    else:
        print("Using cached schema.")

    return cached_schema


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


def load_schema_if_any_file_changed(file_paths):
    """Load schema only if any file in the list has changed."""
    global cached_schema, cached_file_mtimes

    if file_paths is None:
        file_paths = [
            DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
            DEFAULT_CONTEXT_LOOKUP_TABLE_PATH,
        ]

    # Flag to indicate if any file has changed
    schema_needs_update = False

    # Check each file's modification time
    for file_path in file_paths:
        current_mtime = os.path.getmtime(file_path)

        # If the file is new or has been modified, mark schema for update
        if (
            file_path not in cached_file_mtimes
            or current_mtime != cached_file_mtimes[file_path]
        ):
            print(f"File {file_path} changed, updating schema...")
            schema_needs_update = True
            cached_file_mtimes[
                file_path
            ] = current_mtime  # Update the modification time

    # If any file has changed, update the schema
    if schema_needs_update or cached_schema is None:
        print("Creating or updating schema based on changed files...")
        # You can combine the files as needed; here we assume one schema file
        # If you want to handle multiple schema files differently, adjust this

        # add checks on lookup inputs (i.e. a dataframe in type format: data_lookup_type)
        combined_lookup_df: data_lookup_type = append_csvs_to_dataframe(
            file_paths
        )  # concatonates input lookup files

        cached_schema = create_schema_from_dataframe(
            combined_lookup_df
        )  # create cached schema

    else:
        print("Using cached schema.")

    return cached_schema


# example code to convert schema to JSON format if want to export (note pandera[io] required)
# cached_schema.to_yaml(output_file_path)

# loaded_schema = io.from_yaml(output_file_path)


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

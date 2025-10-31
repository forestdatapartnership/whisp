"""
Concurrent and non-concurrent statistics processing for WHISP.

This module provides functions for processing GeoJSON/EE FeatureCollections
with Whisp datasets using concurrent batching (for high-volume processing)
and standard sequential processing.

Key features:
  - whisp_concurrent_stats_geojson_to_df/ee_to_df/geojson_to_geojson/ee_to_ee
  - whisp_stats_geojson_to_df_non_concurrent (standard endpoint, sequential)
  - Proper logging at different levels (WARNING, INFO, DEBUG)
  - Progress tracking without external dependencies
  - Client-side and server-side metadata extraction options
  - Endpoint validation and warnings
"""

import ee
import pandas as pd
import geopandas as gpd
import logging
import sys
import threading
import time
import warnings
import json
import io
from contextlib import redirect_stdout
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import os

from openforis_whisp.parameters.config_runtime import (
    plot_id_column,
    external_id_column,
    geometry_type_column,
    geometry_area_column,
    centroid_x_coord_column,
    centroid_y_coord_column,
    iso3_country_column,
    iso2_country_column,
    admin_1_column,
    water_flag,
    geometry_area_column_formatting,
    stats_area_columns_formatting,
    stats_percent_columns_formatting,
)
from openforis_whisp.data_conversion import (
    convert_geojson_to_ee,
    convert_ee_to_df,
    convert_ee_to_geojson,
)
from openforis_whisp.datasets import combine_datasets
from openforis_whisp.reformat import validate_dataframe_using_lookups_flexible


# ============================================================================
# LOGGING & PROGRESS UTILITIES
# ============================================================================


def _extract_decimal_places(format_string: str) -> int:
    """
    Extract decimal places from a format string like '%.3f'.

    Parameters
    ----------
    format_string : str
        Format string (e.g., '%.3f' → 3)

    Returns
    -------
    int
        Number of decimal places
    """
    import re

    match = re.search(r"\.(\d+)f", format_string)
    if match:
        return int(match.group(1))
    return 2  # Default to 2 decimal places


def _add_admin_context(
    df: pd.DataFrame, admin_code_col: str = "admin_code_median", debug: bool = False
) -> pd.DataFrame:
    """
    Join admin codes to get Country, ProducerCountry, and Admin_Level_1 information.

    Uses GAUL 2024 Level 1 administrative lookup to map admin codes to country and
    administrative region names.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with admin_code_median column from reduceRegions
    admin_code_col : str
        Name of the admin code column (default: "admin_code_median")
    debug : bool
        If True, print detailed debugging information (default: False)

    Returns
    -------
    pd.DataFrame
        DataFrame with added Country, ProducerCountry, Admin_Level_1 columns
    """
    logger = logging.getLogger("whisp-concurrent")

    # Return early if admin code column doesn't exist
    if admin_code_col not in df.columns:
        logger.debug(f"Admin code column '{admin_code_col}' not found in dataframe")
        if debug:
            print(f"DEBUG: Admin code column '{admin_code_col}' not found")
            print(f"DEBUG: Available columns: {df.columns.tolist()}")
        return df

    try:
        from openforis_whisp.parameters.lookup_gaul1_admin import lookup_dict

        if debug:
            print(f"DEBUG: Found admin_code_col '{admin_code_col}'")
            print(f"DEBUG: Sample values: {df[admin_code_col].head()}")
            print(f"DEBUG: Value types: {df[admin_code_col].dtype}")
            print(f"DEBUG: Null count: {df[admin_code_col].isna().sum()}")

        # Create lookup dataframe
        lookup_data = []
        for gaul_code, info in lookup_dict.items():
            lookup_data.append(
                {
                    "gaul1_code": gaul_code,
                    "gaul1_name": info.get("gaul1_name"),
                    "iso3_code": info.get("iso3_code"),
                    "iso2_code": info.get("iso2_code"),
                }
            )

        lookup_df = pd.DataFrame(lookup_data)

        if debug:
            print(f"DEBUG: Lookup dictionary has {len(lookup_df)} entries")
            print(f"DEBUG: Sample lookup codes: {lookup_df['gaul1_code'].head()}")

        # Prepare data for join
        df = df.copy()
        df["admin_code_for_join"] = df[admin_code_col].fillna(-9999).astype("int32")
        lookup_df["gaul1_code"] = lookup_df["gaul1_code"].astype("int32")

        if debug:
            print(
                f"DEBUG: Codes to join (first 10): {df['admin_code_for_join'].unique()[:10]}"
            )

        # Perform join
        df_joined = df.merge(
            lookup_df, left_on="admin_code_for_join", right_on="gaul1_code", how="left"
        )

        if debug:
            matched = df_joined["iso3_code"].notna().sum()
            print(f"DEBUG: Merge result - {matched}/{len(df_joined)} rows matched")
            print(f"DEBUG: Sample matched rows:")
            print(
                df_joined[
                    ["admin_code_for_join", "iso3_code", "iso2_code", "gaul1_name"]
                ].head()
            )

        # Rename columns to match output schema
        df_joined = df_joined.rename(
            columns={
                "iso3_code": iso3_country_column,  # 'Country'
                "iso2_code": iso2_country_column,  # 'ProducerCountry'
                "gaul1_name": admin_1_column,  # 'Admin_Level_1'
            }
        )

        # Drop temporary columns
        df_joined = df_joined.drop(
            columns=["admin_code_for_join", "gaul1_code"], errors="ignore"
        )

        logger.debug(
            f"Admin context added: {iso3_country_column}, {iso2_country_column}, {admin_1_column}"
        )
        return df_joined

    except ImportError:
        logger.warning(
            "Could not import GAUL lookup dictionary - admin context not added"
        )
        if debug:
            print("DEBUG: ImportError - could not load lookup dictionary")
        return df
    except Exception as e:
        logger.warning(f"Error adding admin context: {e}")
        if debug:
            print(f"DEBUG: Exception in _add_admin_context: {e}")
            import traceback

            traceback.print_exc()
        return df


def join_admin_codes(
    df: pd.DataFrame, lookup_dict: Dict, id_col: str = "admin_code_median"
) -> pd.DataFrame:
    """
    Join admin codes to DataFrame using a lookup dictionary.

    Converts the admin code column to integer and performs a left join with
    the lookup dictionary to add Country, ProducerCountry, and Admin_Level_1.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with admin code column
    lookup_dict : dict
        Dictionary mapping GAUL codes to admin info (iso3_code, iso2_code, gaul1_name)
    id_col : str
        Name of the admin code column (default: "admin_code_median")

    Returns
    -------
    pd.DataFrame
        DataFrame with added Country, ProducerCountry, Admin_Level_1 columns
    """
    logger = logging.getLogger("whisp-concurrent")

    # Return early if admin code column doesn't exist
    if id_col not in df.columns:
        logger.debug(f"Admin code column '{id_col}' not found in dataframe")
        return df

    try:
        # Create lookup dataframe
        lookup_data = []
        for gaul_code, info in lookup_dict.items():
            lookup_data.append(
                {
                    "gaul1_code": gaul_code,
                    "gaul1_name": info.get("gaul1_name"),
                    "iso3_code": info.get("iso3_code"),
                    "iso2_code": info.get("iso2_code"),
                }
            )

        lookup_df = pd.DataFrame(lookup_data)

        # Prepare data for join
        df = df.copy()
        # Round to nearest integer (handles float values from EE reducers)
        df["admin_code_for_join"] = df[id_col].fillna(-9999).astype("int32")
        lookup_df["gaul1_code"] = lookup_df["gaul1_code"].astype("int32")

        # Perform join
        df_joined = df.merge(
            lookup_df, left_on="admin_code_for_join", right_on="gaul1_code", how="left"
        )

        # Rename columns to match output schema
        df_joined = df_joined.rename(
            columns={
                "iso3_code": iso3_country_column,  # 'Country'
                "iso2_code": iso2_country_column,  # 'ProducerCountry'
                "gaul1_name": admin_1_column,  # 'Admin_Level_1'
            }
        )

        # Drop temporary columns
        df_joined = df_joined.drop(
            columns=["admin_code_for_join", "gaul1_code"], errors="ignore"
        )

        logger.debug(
            f"Admin codes joined: {iso3_country_column}, {iso2_country_column}, {admin_1_column}"
        )
        return df_joined

    except Exception as e:
        logger.warning(f"Error joining admin codes: {e}")
        return df


def setup_concurrent_logger(name: str = "whisp-concurrent", level: int = logging.INFO):
    """
    Configure logging for concurrent operations with minimal duplication.

    Parameters
    ----------
    name : str
        Logger name
    level : int
        Logging level (logging.INFO, logging.DEBUG, etc.)

    Returns
    -------
    logging.Logger
        Configured logger
    """
    logger = logging.getLogger(name)

    # Remove ALL existing handlers to ensure no duplicates
    logger.handlers.clear()

    # Disable propagation to prevent duplicate output from parent loggers
    logger.propagate = False

    # Create and add a single handler
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)
    handler.setLevel(level)

    logger.addHandler(handler)
    logger.setLevel(level)

    return logger


class ProgressTracker:
    """
    Track batch processing progress with minimal output.

    Shows cumulative progress at key milestones (25%, 50%, 75%, 100%)
    instead of per-batch updates.
    """

    def __init__(self, total: int, logger: logging.Logger = None):
        """
        Initialize progress tracker.

        Parameters
        ----------
        total : int
            Total number of items to process
        logger : logging.Logger, optional
            Logger for output
        """
        self.total = total
        self.completed = 0
        self.lock = threading.Lock()
        self.logger = logger or logging.getLogger("whisp-concurrent")
        self.milestones = {25, 50, 75, 100}
        self.shown_milestones = set()

    def update(self, n: int = 1) -> None:
        """
        Update progress count.

        Parameters
        ----------
        n : int
            Number of items completed
        """
        with self.lock:
            self.completed += n
            percent = int((self.completed / self.total) * 100)

            # Show milestone messages (25%, 50%, 75%, 100%)
            for milestone in sorted(self.milestones):
                if percent >= milestone and milestone not in self.shown_milestones:
                    self.shown_milestones.add(milestone)
                    self.logger.info(
                        f"Progress: {self.completed}/{self.total} "
                        f"({percent}% complete)"
                    )

    def finish(self) -> None:
        """Log completion."""
        with self.lock:
            self.logger.info(
                f"✅ Processing complete: {self.completed}/{self.total} batches"
            )


# ============================================================================
# ENDPOINT VALIDATION
# ============================================================================


def check_ee_endpoint(endpoint_type: str = "high-volume") -> bool:
    """
    Check if Earth Engine is using the correct endpoint.

    Parameters
    ----------
    endpoint_type : str
        Expected endpoint type: "high-volume" or "standard"

    Returns
    -------
    bool
        True if using expected endpoint, False otherwise
    """
    api_url = str(ee.data._cloud_api_base_url)

    if endpoint_type == "high-volume":
        return "highvolume" in api_url.lower()
    elif endpoint_type == "standard":
        return "highvolume" not in api_url.lower()
    else:
        return False


def validate_ee_endpoint(endpoint_type: str = "high-volume", raise_error: bool = True):
    """
    Validate Earth Engine endpoint and warn/error if incorrect.

    Parameters
    ----------
    endpoint_type : str
        Expected endpoint type
    raise_error : bool
        If True, raise error if incorrect endpoint; if False, warn

    Raises
    ------
    RuntimeError
        If incorrect endpoint and raise_error=True
    """
    if not check_ee_endpoint(endpoint_type):
        msg = (
            f"❌ Not using {endpoint_type.upper()} endpoint.\n"
            f"Current URL: {ee.data._cloud_api_base_url}\n"
            f"\nTo use {endpoint_type} endpoint, run:\n"
        )

        if endpoint_type == "high-volume":
            msg += "  ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')"
        else:
            msg += "  ee.Initialize()  # Uses standard endpoint by default"

        if raise_error:
            raise RuntimeError(msg)
        else:
            logging.warning(msg)


# ============================================================================
# METADATA EXTRACTION (CLIENT & SERVER SIDE)
# ============================================================================


def extract_centroid_and_geomtype_client(
    gdf: gpd.GeoDataFrame,
    x_col: str = None,
    y_col: str = None,
    type_col: str = None,
    external_id_col: str = None,
    return_attributes_only: bool = True,
) -> pd.DataFrame:
    """
    Extract centroid coordinates and geometry type using GeoPandas (client-side).

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Input GeoDataFrame
    x_col : str, optional
        Column name for centroid x. Defaults to config value
    y_col : str, optional
        Column name for centroid y. Defaults to config value
    type_col : str, optional
        Column name for geometry type. Defaults to config value
    external_id_col : str, optional
        Name of external ID column to preserve
    return_attributes_only : bool
        If True, return only attribute columns (no geometry)

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        DataFrame/GeoDataFrame with centroid and geometry type columns
    """
    x_col = x_col or centroid_x_coord_column
    y_col = y_col or centroid_y_coord_column
    type_col = type_col or geometry_type_column

    gdf = gdf.copy()

    # Extract centroid coordinates (suppressing geographic CRS warning)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        centroid_points = gdf.geometry.centroid

    gdf[x_col] = centroid_points.x.round(6)
    gdf[y_col] = centroid_points.y.round(6)
    gdf[type_col] = gdf.geometry.geom_type

    if return_attributes_only:
        cols = [x_col, y_col, type_col]
        if external_id_col and external_id_col in gdf.columns:
            cols = [external_id_col] + cols

        # Always include plot_id_column if present (needed for merging batches)
        if plot_id_column in gdf.columns and plot_id_column not in cols:
            cols = [plot_id_column] + cols

        # Always include __row_id__ if present (needed for merging)
        if "__row_id__" in gdf.columns and "__row_id__" not in cols:
            cols = ["__row_id__"] + cols

        return gdf[cols].reset_index(drop=True)

    return gdf


def extract_centroid_and_geomtype_server(
    fc: ee.FeatureCollection,
    x_col: str = None,
    y_col: str = None,
    type_col: str = None,
    max_error: float = 1.0,
) -> ee.FeatureCollection:
    """
    Extract centroid coordinates and geometry type using Earth Engine (server-side).

    Parameters
    ----------
    fc : ee.FeatureCollection
        Input FeatureCollection
    x_col : str, optional
        Column name for centroid x
    y_col : str, optional
        Column name for centroid y
    type_col : str, optional
        Column name for geometry type
    max_error : float
        Maximum error for centroid calculation (meters)

    Returns
    -------
    ee.FeatureCollection
        FeatureCollection with centroid and geometry type properties
    """
    x_col = x_col or centroid_x_coord_column
    y_col = y_col or centroid_y_coord_column
    type_col = type_col or geometry_type_column

    def add_metadata(feature):
        centroid = feature.geometry().centroid(max_error)
        coords = centroid.coordinates()
        x = ee.Number(coords.get(0)).multiply(1e6).round().divide(1e6)
        y = ee.Number(coords.get(1)).multiply(1e6).round().divide(1e6)
        return feature.set({x_col: x, y_col: y, type_col: feature.geometry().type()})

    return fc.map(add_metadata)


# ============================================================================
# BATCH PROCESSING UTILITIES
# ============================================================================


def batch_geodataframe(
    gdf: gpd.GeoDataFrame,
    batch_size: int,
) -> List[gpd.GeoDataFrame]:
    """
    Split a GeoDataFrame into batches.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Input GeoDataFrame
    batch_size : int
        Size of each batch

    Returns
    -------
    List[gpd.GeoDataFrame]
        List of batch GeoDataFrames
    """
    batches = []
    for i in range(0, len(gdf), batch_size):
        batches.append(gdf.iloc[i : i + batch_size].copy())
    return batches


def convert_batch_to_ee(batch_gdf: gpd.GeoDataFrame) -> ee.FeatureCollection:
    """
    Convert a batch GeoDataFrame to EE FeatureCollection using temporary GeoJSON.

    Preserves the __row_id__ column if present so it can be retrieved after processing.

    Parameters
    ----------
    batch_gdf : gpd.GeoDataFrame
        Input batch (should have __row_id__ column)

    Returns
    -------
    ee.FeatureCollection
        EE FeatureCollection with __row_id__ as a feature property
    """
    temp_fd, temp_path = tempfile.mkstemp(suffix=".geojson", text=True)
    try:
        os.close(temp_fd)
        batch_gdf.to_file(temp_path, driver="GeoJSON")
        fc = convert_geojson_to_ee(temp_path)

        # If __row_id__ is in the original GeoDataFrame, it will be preserved
        # as a feature property in the GeoJSON and thus in the EE FeatureCollection
        return fc
    finally:
        time.sleep(0.1)
        if os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass


def clean_geodataframe(
    gdf: gpd.GeoDataFrame,
    remove_nulls: bool = True,
    fix_invalid: bool = True,
    logger: logging.Logger = None,
) -> gpd.GeoDataFrame:
    """
    Validate and clean GeoDataFrame geometries.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Input GeoDataFrame
    remove_nulls : bool
        Remove null geometries
    fix_invalid : bool
        Fix invalid geometries
    logger : logging.Logger, optional
        Logger for output

    Returns
    -------
    gpd.GeoDataFrame
        Cleaned GeoDataFrame
    """
    logger = logger or logging.getLogger("whisp-concurrent")

    if remove_nulls:
        null_count = gdf.geometry.isna().sum()
        if null_count > 0:
            logger.warning(f"Removing {null_count} null geometries")
            gdf = gdf[~gdf.geometry.isna()].copy()

    if fix_invalid:
        valid_count = gdf.geometry.is_valid.sum()
        invalid_count = len(gdf) - valid_count
        if invalid_count > 0:
            logger.warning(f"Fixing {invalid_count} invalid geometries")
            from shapely.validation import make_valid

            gdf = gdf.copy()
            gdf["geometry"] = gdf["geometry"].apply(
                lambda g: make_valid(g) if g and not g.is_valid else g
            )

    logger.debug(f"Validation complete: {len(gdf):,} geometries ready")
    return gdf


# ============================================================================
# EE PROCESSING WITH RETRY LOGIC
# ============================================================================


def process_ee_batch(
    fc: ee.FeatureCollection,
    whisp_image: ee.Image,
    reducer: ee.Reducer,
    batch_idx: int,
    max_retries: int = 3,
    logger: logging.Logger = None,
) -> ee.FeatureCollection:
    """
    Process an EE FeatureCollection with automatic retry logic.

    Returns EE FeatureCollection (not DataFrame) so results can be merged
    server-side without downloading.

    Parameters
    ----------
    fc : ee.FeatureCollection
        Input FeatureCollection
    whisp_image : ee.Image
        Image containing bands to reduce
    reducer : ee.Reducer
        Reducer to apply
    batch_idx : int
        Batch index (for logging)
    max_retries : int
        Maximum retry attempts
    logger : logging.Logger, optional
        Logger for output

    Returns
    -------
    ee.FeatureCollection
        Results as EE FeatureCollection (NOT converted to DataFrame)

    Raises
    ------
    RuntimeError
        If processing fails after all retries
    """
    logger = logger or logging.getLogger("whisp-concurrent")

    for attempt in range(max_retries):
        try:
            results_fc = whisp_image.reduceRegions(
                collection=fc,
                reducer=reducer,
                scale=10,
            )

            # Return as EE FeatureCollection (NOT converted to DataFrame)
            # Conversion happens later after all batches are merged
            return results_fc

        except ee.EEException as e:
            error_msg = str(e)

            if "Quota" in error_msg or "limit" in error_msg.lower():
                if attempt < max_retries - 1:
                    wait_time = min(30, 2**attempt)
                    logger.warning(
                        f"Batch {batch_idx + 1}: Rate limited, waiting {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise RuntimeError(f"Batch {batch_idx + 1}: Quota exhausted")

            elif "timeout" in error_msg.lower():
                if attempt < max_retries - 1:
                    wait_time = min(15, 2**attempt)
                    logger.warning(
                        f"Batch {batch_idx + 1}: Timeout, retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)
                else:
                    raise

            else:
                if attempt < max_retries - 1:
                    wait_time = min(5, 2**attempt)
                    time.sleep(wait_time)
                else:
                    raise

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(min(5, 2**attempt))
            else:
                raise RuntimeError(f"Batch {batch_idx + 1}: {str(e)}")

    raise RuntimeError(f"Batch {batch_idx + 1}: Failed after {max_retries} attempts")


# ============================================================================
# CORE BATCH PROCESSING (SHARED BY ALL INPUT TYPES)
# ============================================================================


def _process_batches_concurrent_ee(
    fc: ee.FeatureCollection,
    whisp_image: ee.Image,
    reducer: ee.Reducer,
    batch_size: int,
    max_concurrent: int,
    max_retries: int,
    add_metadata_server: bool,
    logger: logging.Logger,
) -> ee.FeatureCollection:
    """
    Core concurrent batch processing (EE-to-EE).

    This is the true core: takes EE FC, processes in batches on server,
    returns EE FC with statistics as properties. No conversions.

    Parameters
    ----------
    fc : ee.FeatureCollection
        Input FeatureCollection with plotId property
    whisp_image : ee.Image
        Combined Whisp image
    reducer : ee.Reducer
        EE reducer for band statistics
    batch_size : int
        Features per batch
    max_concurrent : int
        Max concurrent EE calls
    max_retries : int
        Retry attempts per batch
    add_metadata_server : bool
        Add centroid/geomtype server-side
    logger : logging.Logger
        Logger instance

    Returns
    -------
    ee.FeatureCollection
        Results FeatureCollection with statistics as properties
    """
    # Get feature list size
    count = fc.size().getInfo()
    logger.info(
        f"Processing {count:,} features in {(count + batch_size - 1) // batch_size} batches"
    )

    # Add metadata server-side if requested
    if add_metadata_server:
        fc = extract_centroid_and_geomtype_server(fc)

    # Setup semaphore for EE concurrency control
    ee_semaphore = threading.BoundedSemaphore(max_concurrent)
    progress = ProgressTracker((count + batch_size - 1) // batch_size, logger=logger)
    results = []
    batch_errors = []

    def process_ee_batch_with_metadata(
        batch_idx: int, batch_list: list
    ) -> ee.FeatureCollection:
        """Process one EE batch."""
        with ee_semaphore:
            # Create FC from batch
            batch_fc = ee.FeatureCollection(batch_list)
            # Process on server
            return process_ee_batch(
                batch_fc, whisp_image, reducer, batch_idx, max_retries, logger
            )

    # Convert FC to list of features and batch them
    features = fc.toList(count).getInfo()
    num_batches = (count + batch_size - 1) // batch_size

    # Process batches with thread pool
    pool_workers = max(2 * max_concurrent, max_concurrent + 2)

    with ThreadPoolExecutor(max_workers=pool_workers) as executor:
        futures = {}
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = min(start_idx + batch_size, count)
            batch_list = features[start_idx:end_idx]
            futures[executor.submit(process_ee_batch_with_metadata, i, batch_list)] = i

        for future in as_completed(futures):
            try:
                batch_result = future.result()
                results.append(batch_result)
                progress.update()
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Batch processing error: {error_msg[:100]}")
                logger.debug(f"Full error: {str(e)}")
                batch_errors.append(error_msg)

    progress.finish()

    if results:
        # Merge all batch results back into single FC
        merged_fc = ee.FeatureCollection([]).flatten()
        for result in results:
            merged_fc = merged_fc.merge(result)
        return merged_fc
    elif batch_errors:
        raise RuntimeError(f"Batch processing failed: {batch_errors[0]}")
    else:
        raise RuntimeError("No results produced")


def _process_with_client_metadata(
    fc_results: ee.FeatureCollection,
    gdf: gpd.GeoDataFrame,
    external_id_column: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Convert EE results to DataFrame and merge with client-side metadata.

    Parameters
    ----------
    fc_results : ee.FeatureCollection
        Results from EE processing (with statistics as properties)
    gdf : gpd.GeoDataFrame
        Original GeoDataFrame (for client-side metadata extraction)
    external_id_column : str
        External ID column name
    logger : logging.Logger
        Logger instance

    Returns
    -------
    pd.DataFrame
        Merged results with server stats + client metadata
    """
    # Convert EE results to DataFrame
    logger.debug("Converting EE results to DataFrame...")
    df_server = convert_ee_to_df(fc_results)

    # Ensure plot_id_column is present
    if plot_id_column not in df_server.columns:
        df_server[plot_id_column] = range(len(df_server))

    # Extract client-side metadata
    logger.debug("Extracting client-side metadata...")
    df_client = extract_centroid_and_geomtype_client(
        gdf,
        external_id_col=external_id_column,
        return_attributes_only=True,
    )

    # Merge server results with client metadata
    merged = df_server.merge(
        df_client, on=plot_id_column, how="left", suffixes=("", "_client")
    )

    # Ensure all column names are strings
    merged.columns = merged.columns.astype(str)

    # Ensure plotId is at position 0
    if plot_id_column in merged.columns:
        merged = merged[
            [plot_id_column] + [col for col in merged.columns if col != plot_id_column]
        ]

    return merged


def _postprocess_results(
    df: pd.DataFrame,
    decimal_places: int,
    unit_type: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Post-process results: add admin context, format output.

    Parameters
    ----------
    df : pd.DataFrame
        Results DataFrame (server stats + client metadata)
    decimal_places : int
        Decimal places for formatting
    unit_type : str
        "ha" or "percent"
    logger : logging.Logger
        Logger instance

    Returns
    -------
    pd.DataFrame
        Formatted results with plotId at position 0
    """
    from openforis_whisp.reformat import format_stats_dataframe

    # Add admin context BEFORE formatting (which removes _median columns)
    logger.debug("Adding administrative context...")
    try:
        from openforis_whisp.parameters.lookup_gaul1_admin import lookup_dict

        df = join_admin_codes(
            df=df, lookup_dict=lookup_dict, id_col="admin_code_median"
        )
    except ImportError:
        logger.warning("Could not import lookup dictionary - admin context not added")

    # Format the output
    logger.debug("Formatting output...")
    formatted = format_stats_dataframe(
        df=df,
        area_col=f"{geometry_area_column}_sum",
        decimal_places=decimal_places,
        unit_type=unit_type,
        remove_columns=True,
        convert_water_flag=True,
    )

    return formatted


# ============================================================================
# CONCURRENT PROCESSING FUNCTIONS
# ============================================================================


def whisp_concurrent_stats_geojson_to_df(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    batch_size: int = 25,
    max_concurrent: int = 10,
    validate_geometries: bool = True,
    max_retries: int = 3,
    add_metadata_server: bool = False,
    logger: logging.Logger = None,
    decimal_places: int = None,
) -> pd.DataFrame:
    """
    Process GeoJSON concurrently to compute Whisp statistics with automatic formatting.

    Converts GeoJSON → EE FeatureCollection, then calls core EE processing.
    Client-side metadata extraction is applied; optionally add server-side metadata too.

    Parameters
    ----------
    input_geojson_filepath : str
        Path to input GeoJSON file
    external_id_column : str, optional
        Column name for external IDs
    remove_geom : bool
        Remove geometry column from output
    national_codes : List[str], optional
        ISO2 codes for national datasets
    unit_type : str
        "ha" or "percent"
    whisp_image : ee.Image, optional
        Pre-combined image (created with combine_datasets if None)
    custom_bands : Dict[str, Any], optional
        Custom band information
    batch_size : int
        Features per batch
    max_concurrent : int
        Maximum concurrent EE calls
    validate_geometries : bool
        Validate and clean geometries
    max_retries : int
        Retry attempts per batch
    add_metadata_server : bool
        Add metadata server-side (in addition to client-side)
    logger : logging.Logger, optional
        Logger for output
    decimal_places : int, optional
        Decimal places for formatting. If None, auto-detects from config.

    Returns
    -------
    pd.DataFrame
        Formatted results DataFrame with Whisp statistics
    """
    logger = logger or setup_concurrent_logger()

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Validate endpoint
    validate_ee_endpoint("high-volume", raise_error=True)

    logger.debug("Step 1/4: Loading GeoJSON...")
    gdf = gpd.read_file(input_geojson_filepath)
    logger.info(f"Loaded {len(gdf):,} features from {input_geojson_filepath}")

    if validate_geometries:
        logger.debug("Step 2/4: Validating geometries...")
        gdf = clean_geodataframe(gdf, logger=logger)

    # Add stable plotIds for merging (starting from 1, not 0)
    gdf[plot_id_column] = range(1, len(gdf) + 1)

    # Extract client-side metadata BEFORE converting to EE
    logger.debug("Step 3/4: Extracting client-side metadata...")
    df_client = extract_centroid_and_geomtype_client(
        gdf,
        external_id_col=external_id_column,
        return_attributes_only=True,
    )

    # Convert GeoJSON to EE FC
    logger.debug("Converting to EE FeatureCollection...")
    fc = convert_geojson_to_ee(input_geojson_filepath)

    # Run core EE-to-EE processing
    logger.debug("Step 4/4: Core EE processing (server-side)...")
    result_fc = whisp_concurrent_stats_ee_to_ee(
        feature_collection=fc,
        external_id_column=external_id_column,
        remove_geom=remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,
        custom_bands=custom_bands,
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        max_retries=max_retries,
        add_metadata_server=add_metadata_server,
        logger=logger,
    )

    # Convert results to DataFrame
    logger.debug("Converting results to DataFrame...")
    df_server = convert_ee_to_df(result_fc)

    # Ensure plot_id_column is present
    if plot_id_column not in df_server.columns:
        df_server[plot_id_column] = range(1, len(df_server) + 1)

    # Merge server results with client metadata
    logger.debug("Merging server results with client metadata...")
    merged = df_server.merge(
        df_client, on=plot_id_column, how="left", suffixes=("", "_client")
    )

    # Post-process (format, add admin context)
    formatted = _postprocess_results(merged, decimal_places, unit_type, logger)

    logger.info(f"✅ Processed {len(formatted):,} features successfully")
    return formatted


def whisp_concurrent_stats_ee_to_ee(
    feature_collection: ee.FeatureCollection,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    batch_size: int = 25,
    max_concurrent: int = 10,
    max_retries: int = 3,
    add_metadata_server: bool = True,
    logger: logging.Logger = None,
    decimal_places: int = None,
) -> ee.FeatureCollection:
    """
    Core EE-to-EE concurrent processing: pure server-side reduceRegions.

    This is the foundational function - takes EE FC, reduces statistics server-side,
    returns EE FC. No conversions, no downloads.

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        Input FeatureCollection (must have plotId property)
    external_id_column : str, optional
        Column name for external IDs (informational, not used in EE-only path)
    remove_geom : bool
        Remove geometry from output
    national_codes : List[str], optional
        ISO2 codes for national datasets
    unit_type : str
        "ha" or "percent" (informational only, EE returns raw values)
    whisp_image : ee.Image, optional
        Pre-combined image
    custom_bands : Dict[str, Any], optional
        Custom band information
    batch_size : int
        Features per batch (default 25)
    max_concurrent : int
        Maximum concurrent EE calls (default 10)
    max_retries : int
        Retry attempts per batch (default 3)
    add_metadata_server : bool
        Add centroid & geometry type server-side (default True)
    logger : logging.Logger, optional
        Logger for output
    decimal_places : int, optional
        Not used in EE-only path (EE returns raw numeric values)

    Returns
    -------
    ee.FeatureCollection
        Results as EE FeatureCollection with stats as properties
    """
    logger = logger or setup_concurrent_logger()

    # Validate endpoint
    validate_ee_endpoint("high-volume", raise_error=True)

    logger.info("Processing EE FeatureCollection (server-side, no conversions)")

    # Create image if not provided
    if whisp_image is None:
        logger.debug("Creating Whisp image...")
        with redirect_stdout(io.StringIO()):
            try:
                whisp_image = combine_datasets(
                    national_codes=national_codes, validate_bands=False
                )
            except Exception as e:
                logger.warning(
                    f"First attempt failed: {str(e)[:100]}. Retrying with validate_bands=True..."
                )
                whisp_image = combine_datasets(
                    national_codes=national_codes, validate_bands=True
                )

    # Create reducer
    reducer = ee.Reducer.sum().combine(ee.Reducer.median(), sharedInputs=True)

    # Run core EE-to-EE batch processing (no local conversions)
    try:
        result_fc = _process_batches_concurrent_ee(
            fc=feature_collection,
            whisp_image=whisp_image,
            reducer=reducer,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            max_retries=max_retries,
            add_metadata_server=add_metadata_server,
            logger=logger,
        )
        logger.info("✅ Concurrent EE→EE processing complete (server-side)")
        return result_fc

    except Exception as e:
        is_band_error = any(
            keyword in str(e)
            for keyword in ["Image.load", "asset", "not found", "does not exist"]
        )
        if is_band_error:
            logger.warning("Detected band error. Retrying with validate_bands=True...")
            try:
                with redirect_stdout(io.StringIO()):
                    whisp_image = combine_datasets(
                        national_codes=national_codes, validate_bands=True
                    )
                logger.info("Image recreated. Retrying...")
                result_fc = _process_batches_concurrent_ee(
                    fc=feature_collection,
                    whisp_image=whisp_image,
                    reducer=reducer,
                    batch_size=batch_size,
                    max_concurrent=max_concurrent,
                    max_retries=max_retries,
                    add_metadata_server=add_metadata_server,
                    logger=logger,
                )
                return result_fc
            except Exception as e2:
                logger.error(f"Failed to recover: {str(e2)[:100]}")
                raise
        else:
            raise


def whisp_concurrent_stats_ee_to_df(
    feature_collection: ee.FeatureCollection,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    batch_size: int = 25,
    max_concurrent: int = 10,
    max_retries: int = 3,
    add_metadata_server: bool = True,
    logger: logging.Logger = None,
    decimal_places: int = None,
) -> pd.DataFrame:
    """
    Process EE FeatureCollection concurrently, return formatted DataFrame.

    Wraps whisp_concurrent_stats_ee_to_ee: runs core EE processing, then
    converts results to local DataFrame and applies formatting.

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        Input FeatureCollection
    external_id_column : str, optional
        Column name for external IDs
    remove_geom : bool
        Remove geometry from output
    national_codes : List[str], optional
        ISO2 codes for national datasets
    unit_type : str
        "ha" or "percent"
    whisp_image : ee.Image, optional
        Pre-combined image
    custom_bands : Dict[str, Any], optional
        Custom band information
    batch_size : int
        Features per batch (default 25)
    max_concurrent : int
        Maximum concurrent EE calls (default 10)
    max_retries : int
        Retry attempts per batch (default 3)
    add_metadata_server : bool
        Add metadata server-side (default True)
    logger : logging.Logger, optional
        Logger for output
    decimal_places : int, optional
        Decimal places for formatting. If None, auto-detects from config.

    Returns
    -------
    pd.DataFrame
        Formatted results DataFrame
    """
    logger = logger or setup_concurrent_logger()

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    logger.debug("Step 1/2: EE→EE processing (server-side)...")

    # Run core EE-to-EE processing (no local data yet)
    result_fc = whisp_concurrent_stats_ee_to_ee(
        feature_collection=feature_collection,
        external_id_column=external_id_column,
        remove_geom=remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,
        custom_bands=custom_bands,
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        max_retries=max_retries,
        add_metadata_server=add_metadata_server,
        logger=logger,
    )

    # Convert results to DataFrame
    logger.debug("Step 2/2: Converting to DataFrame and formatting...")
    df_results = convert_ee_to_df(result_fc)

    # Post-process (format, add admin context)
    formatted = _postprocess_results(df_results, decimal_places, unit_type, logger)

    logger.info(f"✅ Processed {len(formatted):,} features successfully")
    return formatted


# ============================================================================
# NON-CONCURRENT PROCESSING (STANDARD ENDPOINT)
# ============================================================================


def whisp_stats_geojson_to_df_non_concurrent(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    add_metadata_client: bool = True,
    logger: logging.Logger = None,
    # Format parameters (auto-detect from config if not provided)
    decimal_places: int = None,
) -> pd.DataFrame:
    """
    Process GeoJSON non-concurrently using standard EE endpoint with automatic formatting.

    Uses reduceRegions for server-side processing and client-side metadata
    extraction via GeoPandas. Suitable for smaller datasets or when high-volume
    endpoint is not available. Automatically formats output.

    Requires: standard EE endpoint (default)

    Parameters
    ----------
    input_geojson_filepath : str
        Path to input GeoJSON
    external_id_column : str, optional
        Column name for external IDs
    remove_geom : bool
        Remove geometry from output
    national_codes : List[str], optional
        ISO2 codes for national datasets
    unit_type : str
        "ha" or "percent"
    whisp_image : ee.Image, optional
        Pre-combined image
    custom_bands : Dict[str, Any], optional
        Custom band information
    add_metadata_client : bool
        Add client-side metadata (recommended)
    logger : logging.Logger, optional
        Logger for output
    decimal_places : int, optional
        Decimal places for formatting. If None, auto-detects from config.

    Returns
    -------
    pd.DataFrame
        Formatted results DataFrame
    """
    from openforis_whisp.reformat import format_stats_dataframe

    logger = logger or setup_concurrent_logger()

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Validate endpoint
    validate_ee_endpoint("standard", raise_error=True)

    logger.info(f"Loading GeoJSON: {input_geojson_filepath}")
    gdf = gpd.read_file(input_geojson_filepath)
    logger.info(f"Loaded {len(gdf):,} features")

    # Clean geometries
    gdf = clean_geodataframe(gdf, logger=logger)

    # Add stable row IDs
    row_id_col = "__row_id__"
    gdf[row_id_col] = range(len(gdf))

    # Create image if not provided
    if whisp_image is None:
        logger.debug("Creating Whisp image...")
        # Suppress print statements from combine_datasets
        with redirect_stdout(io.StringIO()):
            whisp_image = combine_datasets(national_codes=national_codes)

    # Convert to EE (suppress print statements from convert_geojson_to_ee)
    logger.debug("Converting to EE FeatureCollection...")
    with redirect_stdout(io.StringIO()):
        fc = convert_geojson_to_ee(input_geojson_filepath)

    # Create reducer
    reducer = ee.Reducer.sum().combine(ee.Reducer.median(), sharedInputs=True)

    # Process server-side
    logger.info("Processing with Earth Engine...")
    results_fc = whisp_image.reduceRegions(collection=fc, reducer=reducer, scale=10)
    df_server = convert_ee_to_df(results_fc)

    logger.debug("Server-side processing complete")

    # Add row_id if missing
    if row_id_col not in df_server.columns:
        df_server[row_id_col] = range(len(df_server))

    # Add client-side metadata if requested
    if add_metadata_client:
        logger.debug("Extracting client-side metadata...")
        df_client = extract_centroid_and_geomtype_client(
            gdf,
            external_id_col=external_id_column,
            return_attributes_only=True,
        )

        # Merge
        result = df_server.merge(
            df_client, on=row_id_col, how="left", suffixes=("", "_client")
        )
    else:
        result = df_server

    # Remove internal __row_id__ column if present
    if row_id_col in result.columns:
        result = result.drop(columns=[row_id_col])

    # Format the output
    # Add admin context (Country, ProducerCountry, Admin_Level_1) from admin_code
    # MUST be done BEFORE formatting (which removes _median columns)
    logger.debug("Adding administrative context...")
    try:
        from openforis_whisp.parameters.lookup_gaul1_admin import lookup_dict

        result = join_admin_codes(
            df=result, lookup_dict=lookup_dict, id_col="admin_code_median"
        )
    except ImportError:
        logger.warning("Could not import lookup dictionary - admin context not added")

    # Format the output
    logger.debug("Formatting output...")
    formatted = format_stats_dataframe(
        df=result,
        area_col=f"{geometry_area_column}_sum",
        decimal_places=decimal_places,
        unit_type=unit_type,
        remove_columns=True,
        convert_water_flag=True,
    )

    logger.info(f"✅ Processed {len(formatted):,} features")
    return formatted


# ============================================================================
# FORMATTED WRAPPER FUNCTIONS (STATS + FORMAT)
# ============================================================================


def whisp_concurrent_formatted_stats_geojson_to_df(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    batch_size: int = 25,
    max_concurrent: int = 10,
    validate_geometries: bool = True,
    max_retries: int = 3,
    add_metadata_server: bool = False,
    logger: logging.Logger = None,
    # Format parameters (auto-detect from config if not provided)
    decimal_places: int = None,
    remove_median_columns: bool = True,
    convert_water_flag: bool = True,
    water_flag_threshold: float = 0.5,
    sort_column: str = "plotId",
) -> pd.DataFrame:
    """
    Process GeoJSON concurrently with automatic formatting and validation.

    Combines whisp_concurrent_stats_geojson_to_df + format_stats_dataframe + validation
    for a complete pipeline: extract stats → convert units → format output → validate schema.

    Uses high-volume endpoint and concurrent batching.

    Parameters
    ----------
    input_geojson_filepath : str
        Path to input GeoJSON file
    external_id_column : str, optional
        Column name for external IDs
    remove_geom : bool
        Remove geometry column from output
    national_codes : List[str], optional
        ISO2 codes for national datasets
    unit_type : str
        "ha" or "percent"
    whisp_image : ee.Image, optional
        Pre-combined image
    custom_bands : Dict[str, Any], optional
        Custom band information
    batch_size : int
        Features per batch (default 25)
    max_concurrent : int
        Maximum concurrent EE calls (default 10)
    validate_geometries : bool
        Validate and clean geometries (default True)
    max_retries : int
        Retry attempts per batch (default 3)
    add_metadata_server : bool
        Add metadata server-side (default False)
    logger : logging.Logger, optional
        Logger for output
    decimal_places : int, optional
        Decimal places for rounding. If None, auto-detects from config:
        - Area columns: geometry_area_column_formatting
        - Percent columns: stats_percent_columns_formatting
        - Other columns: stats_area_columns_formatting
    remove_median_columns : bool
        Remove '_median' columns (default True)
    convert_water_flag : bool
        Convert water flag to boolean (default True)
    water_flag_threshold : float
        Water flag ratio threshold (default 0.5)
    sort_column : str
        Column to sort by (default "plotId", None to skip)

    Returns
    -------
    pd.DataFrame
        Validated, formatted results DataFrame
    """
    from openforis_whisp.reformat import format_stats_dataframe

    logger = logger or setup_concurrent_logger()

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        # Use stats_area_columns_formatting as default for most columns
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Step 1: Get raw stats
    logger.debug("Step 1/2: Extracting statistics (concurrent)...")
    df_raw = whisp_concurrent_stats_geojson_to_df(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
        remove_geom=remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,
        custom_bands=custom_bands,
        batch_size=batch_size,
        max_concurrent=max_concurrent,
        validate_geometries=validate_geometries,
        max_retries=max_retries,
        add_metadata_server=add_metadata_server,
        logger=logger,
    )

    # Step 2: Format the output
    logger.debug("Step 2/2: Formatting output...")
    df_formatted = format_stats_dataframe(
        df=df_raw,
        area_col=f"{geometry_area_column}_sum",
        decimal_places=decimal_places,
        unit_type=unit_type,
        remove_columns=remove_median_columns,
        convert_water_flag=convert_water_flag,
        water_flag_threshold=water_flag_threshold,
        sort_column=sort_column,
    )

    # Step 3: Validate against schema
    logger.debug("Step 3/3: Validating against schema...")
    try:
        from openforis_whisp.reformat import validate_dataframe_using_lookups_flexible

        df_validated = validate_dataframe_using_lookups_flexible(
            df_stats=df_formatted,
            national_codes=national_codes,
            custom_bands=custom_bands,
        )
    except Exception as e:
        logger.warning(f"Validation failed (non-critical): {str(e)[:100]}")
        logger.info("Proceeding without validation")
        df_validated = df_formatted

    logger.info("✅ Concurrent processing + formatting + validation complete")
    return df_validated


def whisp_formatted_stats_geojson_to_df_non_concurrent(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    add_metadata_client: bool = True,
    logger: logging.Logger = None,
    # Format parameters (auto-detect from config if not provided)
    decimal_places: int = None,
    remove_median_columns: bool = True,
    convert_water_flag: bool = True,
    water_flag_threshold: float = 0.5,
    sort_column: str = "plotId",
) -> pd.DataFrame:
    """
    Process GeoJSON non-concurrently with automatic formatting and validation.

    Combines whisp_stats_geojson_to_df_non_concurrent + format_stats_dataframe + validation
    for a complete pipeline: extract stats → convert units → format output → validate schema.

    Uses standard endpoint for sequential processing.

    Parameters
    ----------
    input_geojson_filepath : str
        Path to input GeoJSON file
    external_id_column : str, optional
        Column name for external IDs
    remove_geom : bool
        Remove geometry from output
    national_codes : List[str], optional
        ISO2 codes for national datasets
    unit_type : str
        "ha" or "percent"
    whisp_image : ee.Image, optional
        Pre-combined image
    custom_bands : Dict[str, Any], optional
        Custom band information
    add_metadata_client : bool
        Add client-side metadata (default True)
    logger : logging.Logger, optional
        Logger for output
    decimal_places : int, optional
        Decimal places for rounding. If None, auto-detects from config:
        - Area columns: geometry_area_column_formatting
        - Percent columns: stats_percent_columns_formatting
        - Other columns: stats_area_columns_formatting
    remove_median_columns : bool
        Remove '_median' columns (default True)
    convert_water_flag : bool
        Convert water flag to boolean (default True)
    water_flag_threshold : float
        Water flag ratio threshold (default 0.5)
    sort_column : str
        Column to sort by (default "plotId", None to skip)

    Returns
    -------
    pd.DataFrame
        Validated, formatted results DataFrame
    """
    from openforis_whisp.reformat import format_stats_dataframe

    logger = logger or setup_concurrent_logger()

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        # Use stats_area_columns_formatting as default for most columns
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Step 1: Get raw stats
    logger.debug("Step 1/2: Extracting statistics (non-concurrent)...")
    df_raw = whisp_stats_geojson_to_df_non_concurrent(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
        remove_geom=remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,
        custom_bands=custom_bands,
        add_metadata_client=add_metadata_client,
        logger=logger,
    )

    # Step 2: Format the output
    logger.debug("Step 2/2: Formatting output...")
    df_formatted = format_stats_dataframe(
        df=df_raw,
        area_col=f"{geometry_area_column}_sum",
        decimal_places=decimal_places,
        unit_type=unit_type,
        remove_columns=remove_median_columns,
        convert_water_flag=convert_water_flag,
        water_flag_threshold=water_flag_threshold,
        sort_column=sort_column,
    )

    # Step 3: Validate against schema
    logger.debug("Step 3/3: Validating against schema...")
    try:
        from openforis_whisp.reformat import validate_dataframe_using_lookups_flexible

        df_validated = validate_dataframe_using_lookups_flexible(
            df_stats=df_formatted,
            national_codes=national_codes,
            custom_bands=custom_bands,
        )
    except Exception as e:
        logger.warning(f"Validation failed (non-critical): {str(e)[:100]}")
        logger.info("Proceeding without validation")
        df_validated = df_formatted

    logger.info("✅ Non-concurrent processing + formatting + validation complete")
    return df_validated


# ============================================================================
# PLACEHOLDER FOR EXPORTS (to be added to __init__.py)
# ============================================================================

__all__ = [
    "whisp_concurrent_stats_geojson_to_df",
    "whisp_concurrent_stats_ee_to_ee",
    "whisp_concurrent_stats_ee_to_df",
    "whisp_stats_geojson_to_df_non_concurrent",
    "whisp_concurrent_formatted_stats_geojson_to_df",
    "whisp_formatted_stats_geojson_to_df_non_concurrent",
    "setup_concurrent_logger",
    "check_ee_endpoint",
    "validate_ee_endpoint",
    "ProgressTracker",
]

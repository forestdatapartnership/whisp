"""
Advanced statistics processing for WHISP - concurrent and sequential endpoints.

This module provides optimized functions for processing GeoJSON FeatureCollections
with Whisp datasets using concurrent batching (for high-volume processing)
and standard sequential processing.

NOTE: This module is a transition state. The plan is to eventually merge these
functions into stats.py and replace the standard functions there as the primary
implementation, deprecating the legacy versions.

Key features:
  - whisp_stats_geojson_to_df_concurrent
  - whisp_stats_geojson_to_df_sequential (standard endpoint, sequential)
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
import os
import subprocess
from contextlib import redirect_stdout, contextmanager
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile

# ============================================================================
# STDOUT/STDERR SUPPRESSION CONTEXT MANAGER (for C-level output)
# ============================================================================


@contextmanager
def suppress_c_level_output():
    """Suppress C-level stdout/stderr writes from libraries like Fiona."""
    if sys.platform == "win32":
        # Windows doesn't support dup2() reliably for STDOUT/STDERR
        # Fall back to Python-level suppression
        with redirect_stdout(io.StringIO()):
            yield
    else:
        # Unix-like systems: use file descriptor redirection
        saved_stdout = os.dup(1)
        saved_stderr = os.dup(2)
        try:
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, 1)
            os.dup2(devnull, 2)
            yield
        finally:
            os.dup2(saved_stdout, 1)
            os.dup2(saved_stderr, 2)
            os.close(devnull)
            os.close(saved_stdout)
            os.close(saved_stderr)


# Suppress verbose warnings globally for this module
# Note: FutureWarnings are kept (they signal important API changes)
warnings.filterwarnings("ignore", category=UserWarning, message=".*geographic CRS.*")
warnings.simplefilter("ignore", UserWarning)
warnings.filterwarnings("ignore", category=DeprecationWarning)

# Suppress verbose logging from GeoPandas/Fiona/pyogrio
logging.getLogger("fiona").setLevel(logging.WARNING)
logging.getLogger("fiona.ogrext").setLevel(logging.WARNING)
logging.getLogger("pyogrio").setLevel(logging.WARNING)
logging.getLogger("pyogrio._io").setLevel(logging.WARNING)

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
from openforis_whisp.stats import (
    reformat_geometry_type,
    set_point_geometry_area_to_zero,
)


# ============================================================================
# LOGGING & PROGRESS UTILITIES
# ============================================================================


def _suppress_verbose_output(max_concurrent: int = None):
    """
    Suppress verbose warnings and logging from dependencies.

    Dynamically adjusts urllib3 logger level based on max_concurrent to prevent
    "Connection pool is full" warnings during high-concurrency scenarios.

    Parameters
    ----------
    max_concurrent : int, optional
        Maximum concurrent workers. Adjusts urllib3 logging level:
        - max_concurrent <= 20: WARNING (pool rarely full)
        - max_concurrent 21-35: CRITICAL (suppress pool warnings)
        - max_concurrent >= 36: CRITICAL (maximum suppression)
    """
    warnings.filterwarnings("ignore", category=UserWarning)
    warnings.filterwarnings("ignore", category=DeprecationWarning)

    # Suppress urllib3 connection pool warnings via filters
    warnings.filterwarnings("ignore", message=".*Connection pool is full.*")
    warnings.filterwarnings("ignore", message=".*discarding connection.*")

    # Set logger levels to WARNING to suppress INFO messages
    for mod_name in [
        "openforis_whisp.reformat",
        "openforis_whisp.data_conversion",
        "geopandas",
        "fiona",
        "pyogrio._io",
        "urllib3",
    ]:
        logging.getLogger(mod_name).setLevel(logging.WARNING)

    # ALL urllib3 loggers: use CRITICAL to suppress ALL connection pool warnings
    # (these appear at WARNING level during high concurrency)
    urllib3_loggers = [
        "urllib3.connectionpool",
        "urllib3.poolmanager",
        "urllib3",
        "requests.packages.urllib3.connectionpool",
        "requests.packages.urllib3.poolmanager",
        "requests.packages.urllib3",
    ]

    for logger_name in urllib3_loggers:
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)

    # Suppress warning logs specifically from reformat module during validation
    reformat_logger = logging.getLogger("openforis_whisp.reformat")
    reformat_logger.setLevel(logging.ERROR)


def _load_geojson_silently(filepath: str) -> gpd.GeoDataFrame:
    """Load GeoJSON file with all output suppressed."""
    fiona_logger = logging.getLogger("fiona")
    pyogrio_logger = logging.getLogger("pyogrio._io")
    old_fiona_level = fiona_logger.level
    old_pyogrio_level = pyogrio_logger.level
    fiona_logger.setLevel(logging.CRITICAL)
    pyogrio_logger.setLevel(logging.CRITICAL)

    try:
        with redirect_stdout(io.StringIO()):
            gdf = gpd.read_file(filepath)
        return gdf
    finally:
        fiona_logger.setLevel(old_fiona_level)
        pyogrio_logger.setLevel(old_pyogrio_level)


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


class ProgressTracker:
    """
    Track batch processing progress with time estimation.

    Shows progress at key milestones (25%, 50%, 75%, 100%) with estimated
    time remaining based on processing speed.
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
        self.start_time = time.time()
        self.last_update_time = self.start_time

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

                    # Calculate time metrics
                    elapsed = time.time() - self.start_time
                    rate = self.completed / elapsed if elapsed > 0 else 0
                    remaining_items = self.total - self.completed
                    eta_seconds = remaining_items / rate if rate > 0 else 0

                    # Format time strings
                    eta_str = self._format_time(eta_seconds)
                    elapsed_str = self._format_time(elapsed)

                    # Build progress message
                    msg = f"Progress: {self.completed}/{self.total} ({percent}%)"
                    if percent < 100:
                        msg += f" | Elapsed: {elapsed_str} | ETA: {eta_str}"
                    else:
                        msg += f" | Total time: {elapsed_str}"

                    self.logger.info(msg)

    @staticmethod
    def _format_time(seconds: float) -> str:
        """Format seconds as human-readable string."""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            mins = seconds / 60
            return f"{mins:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"

    def finish(self) -> None:
        """Log completion."""
        with self.lock:
            total_time = time.time() - self.start_time
            time_str = self._format_time(total_time)
            self.logger.info(
                f"Processing complete: {self.completed}/{self.total} batches in {time_str}"
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
            f"Not using {endpoint_type.upper()} endpoint.\n"
            f"Current URL: {ee.data._cloud_api_base_url}\n"
            f"\nTo use {endpoint_type} endpoint, run:\n"
        )
        msg += "ee.Reset()\n"
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
    external_id_column: str = None,
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
    external_id_column: : str, optional
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

    # Extract centroid coordinates (suppressing geographic CRS warning from Shapely)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        warnings.simplefilter("ignore", UserWarning)  # Additional suppression
        centroid_points = gdf.geometry.centroid

    gdf[x_col] = centroid_points.x.round(6)
    gdf[y_col] = centroid_points.y.round(6)
    gdf[type_col] = gdf.geometry.geom_type

    if return_attributes_only:
        # Build column list starting with merge keys
        cols = []

        # Always include __row_id__ first if present (needed for row-level merging)
        if "__row_id__" in gdf.columns:
            cols.append("__row_id__")

        # Always include plot_id_column if present (needed for merging batches)
        if plot_id_column in gdf.columns:
            cols.append(plot_id_column)

        # Include external_id_column if provided and exists
        if (
            external_id_column
            and external_id_column in gdf.columns
            and external_id_column not in cols
        ):
            cols.append(external_id_column)

        # Always include metadata columns (centroid, geometry type)
        cols.extend([x_col, y_col, type_col])

        # Remove any duplicates while preserving order
        cols = list(dict.fromkeys(cols))

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
) -> pd.DataFrame:
    """
    Process an EE FeatureCollection with automatic retry logic.

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
    pd.DataFrame
        Results as DataFrame

    Raises
    ------
    RuntimeError
        If processing fails after all retries
    """
    logger = logger or logging.getLogger("whisp-concurrent")

    for attempt in range(max_retries):
        try:
            results = whisp_image.reduceRegions(
                collection=fc,
                reducer=reducer,
                scale=10,
            )
            df = convert_ee_to_df(results)

            # Ensure plot_id_column is present for merging
            # It should come from the feature properties (added before EE processing)
            if plot_id_column not in df.columns:
                df[plot_id_column] = range(len(df))

            # Ensure all column names are strings (fixes pandas .str accessor issues)
            df.columns = df.columns.astype(str)

            return df

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
# CONCURRENT PROCESSING FUNCTIONS
# ============================================================================


def whisp_stats_geojson_to_df_concurrent(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    batch_size: int = 10,
    max_concurrent: int = 20,
    validate_geometries: bool = True,
    max_retries: int = 3,
    add_metadata_server: bool = False,
    logger: logging.Logger = None,
    # Format parameters (auto-detect from config if not provided)
    decimal_places: int = None,
) -> pd.DataFrame:
    """
    Process GeoJSON concurrently to compute Whisp statistics with automatic formatting.

    Uses high-volume endpoint and concurrent batching. Client-side metadata
    extraction is always applied; optionally add server-side metadata too.
    Automatically formats output (converts units, removes noise columns, etc.).

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
    from openforis_whisp.reformat import format_stats_dataframe

    logger = logger or logging.getLogger("whisp-concurrent")

    # Suppress verbose output from dependencies (dynamically adjust based on max_concurrent)
    _suppress_verbose_output(max_concurrent=max_concurrent)

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Validate endpoint
    validate_ee_endpoint("high-volume", raise_error=True)

    # Load GeoJSON with output suppressed
    gdf = _load_geojson_silently(input_geojson_filepath)
    logger.info(f"Loaded {len(gdf):,} features")

    if validate_geometries:
        gdf = clean_geodataframe(gdf, logger=logger)

    # Add stable plotIds for merging (starting from 1, not 0)
    gdf[plot_id_column] = range(1, len(gdf) + 1)

    # Create image if not provided
    if whisp_image is None:
        logger.debug("Creating Whisp image...")
        # Suppress print statements from combine_datasets
        with redirect_stdout(io.StringIO()):
            try:
                # First try without validation
                whisp_image = combine_datasets(
                    national_codes=national_codes, validate_bands=False
                )
            except Exception as e:
                logger.warning(
                    f"First attempt failed: {str(e)[:100]}. Retrying with validate_bands=True..."
                )
                # Retry with validation to catch and fix bad bands
                whisp_image = combine_datasets(
                    national_codes=national_codes, validate_bands=True
                )

    # Create reducer
    reducer = ee.Reducer.sum().combine(ee.Reducer.median(), sharedInputs=True)

    # Batch the data
    batches = batch_geodataframe(gdf, batch_size)
    logger.info(f"Processing {len(gdf):,} features in {len(batches)} batches")

    # Setup semaphore for EE concurrency control
    ee_semaphore = threading.BoundedSemaphore(max_concurrent)

    # Progress tracker
    progress = ProgressTracker(len(batches), logger=logger)

    results = []

    def process_batch(
        batch_idx: int, batch: gpd.GeoDataFrame
    ) -> Tuple[int, pd.DataFrame, pd.DataFrame]:
        """Process one batch: server EE work + client metadata."""
        with ee_semaphore:
            # Server-side: convert to EE, optionally add metadata, reduce
            fc = convert_batch_to_ee(batch)
            if add_metadata_server:
                fc = extract_centroid_and_geomtype_server(fc)
            df_server = process_ee_batch(
                fc, whisp_image, reducer, batch_idx, max_retries, logger
            )

            # Client-side: extract metadata using GeoPandas
            df_client = extract_centroid_and_geomtype_client(
                batch,
                external_id_column=external_id_column,
                return_attributes_only=True,
            )

        return batch_idx, df_server, df_client

    # Process batches with thread pool
    pool_workers = max(2 * max_concurrent, max_concurrent + 2)

    # Track if we had errors that suggest bad bands
    batch_errors = []

    # Suppress fiona logging during batch processing (threads create new loggers)
    fiona_logger = logging.getLogger("fiona")
    pyogrio_logger = logging.getLogger("pyogrio._io")
    old_fiona_level = fiona_logger.level
    old_pyogrio_level = pyogrio_logger.level
    fiona_logger.setLevel(logging.CRITICAL)
    pyogrio_logger.setLevel(logging.CRITICAL)

    try:
        with redirect_stdout(io.StringIO()):
            with ThreadPoolExecutor(max_workers=pool_workers) as executor:
                futures = {
                    executor.submit(process_batch, i, batch): i
                    for i, batch in enumerate(batches)
                }

                for future in as_completed(futures):
                    try:
                        batch_idx, df_server, df_client = future.result()

                        # Merge server and client results
                        if plot_id_column not in df_server.columns:
                            df_server[plot_id_column] = range(len(df_server))

                        merged = df_server.merge(
                            df_client,
                            on=plot_id_column,
                            how="left",
                            suffixes=("_ee", "_client"),
                        )
                        results.append(merged)
                        progress.update()

                    except Exception as e:
                        error_msg = str(e)
                        logger.error(f"Batch processing error: {error_msg[:100]}")
                        import traceback

                        logger.debug(traceback.format_exc())
                        batch_errors.append(error_msg)
    finally:
        # Restore logger levels
        fiona_logger.setLevel(old_fiona_level)
        pyogrio_logger.setLevel(old_pyogrio_level)

    progress.finish()

    # Check if we should retry with validation due to band errors
    if batch_errors and not results:
        # All batches failed - likely a bad band issue
        is_band_error = any(
            keyword in str(batch_errors)
            for keyword in ["Image.load", "asset", "not found", "does not exist"]
        )

        if is_band_error:
            logger.warning(
                "Detected potential bad band error. Retrying with validate_bands=True..."
            )
            try:
                with redirect_stdout(io.StringIO()):
                    whisp_image = combine_datasets(
                        national_codes=national_codes, validate_bands=True
                    )
                logger.info(
                    "Image recreated with validation. Retrying batch processing..."
                )

                # Retry batch processing with validated image
                results = []
                progress = ProgressTracker(len(batches), logger=logger)

                # Suppress fiona logging during batch processing (threads create new loggers)
                fiona_logger = logging.getLogger("fiona")
                pyogrio_logger = logging.getLogger("pyogrio._io")
                old_fiona_level = fiona_logger.level
                old_pyogrio_level = pyogrio_logger.level
                fiona_logger.setLevel(logging.CRITICAL)
                pyogrio_logger.setLevel(logging.CRITICAL)

                try:
                    with ThreadPoolExecutor(max_workers=pool_workers) as executor:
                        futures = {
                            executor.submit(process_batch, i, batch): i
                            for i, batch in enumerate(batches)
                        }

                        for future in as_completed(futures):
                            try:
                                batch_idx, df_server, df_client = future.result()
                                if plot_id_column not in df_server.columns:
                                    df_server[plot_id_column] = range(len(df_server))
                                merged = df_server.merge(
                                    df_client,
                                    on=plot_id_column,
                                    how="left",
                                    suffixes=("", "_client"),
                                )
                                results.append(merged)
                                progress.update()
                            except Exception as e:
                                logger.error(
                                    f"Batch processing error (retry): {str(e)[:100]}"
                                )

                    progress.finish()
                finally:
                    # Restore logger levels
                    fiona_logger.setLevel(old_fiona_level)
                    pyogrio_logger.setLevel(old_pyogrio_level)
            except Exception as validation_e:
                logger.error(
                    f"Failed to recover with validation: {str(validation_e)[:100]}"
                )
                return pd.DataFrame()

    if results:
        # Filter out empty DataFrames and all-NA columns to avoid FutureWarning in pd.concat
        results_filtered = []
        for df in results:
            if not df.empty:
                # Drop columns that are entirely NA
                df_clean = df.dropna(axis=1, how="all")
                if not df_clean.empty:
                    results_filtered.append(df_clean)
        results = results_filtered

        if results:
            # Concatenate with explicit dtype handling to suppress FutureWarning
            combined = pd.concat(results, ignore_index=True, sort=False)
            # Ensure all column names are strings (fixes pandas .str accessor issues later)
            combined.columns = combined.columns.astype(str)
        else:
            return pd.DataFrame()

        # Clean up duplicate external_id columns created by merges
        # Rename external_id_column to standardized 'external_id' for schema validation
        if external_id_column:
            # Find all columns related to external_id
            external_id_variants = [
                col
                for col in combined.columns
                if external_id_column.lower() in col.lower()
            ]

            if external_id_variants:
                # Use the base column name if it exists, otherwise use first variant
                base_col = (
                    external_id_column
                    if external_id_column in combined.columns
                    else external_id_variants[0]
                )

                # Rename to standardized 'external_id'
                if base_col != "external_id":
                    combined = combined.rename(columns={base_col: "external_id"})

                # Drop all other variants
                cols_to_drop = [c for c in external_id_variants if c != base_col]
                combined = combined.drop(columns=cols_to_drop, errors="ignore")

        # plotId column is already present from batch processing
        # Just ensure it's at position 0
        if plot_id_column in combined.columns:
            combined = combined[
                [plot_id_column]
                + [col for col in combined.columns if col != plot_id_column]
            ]

        # Add admin context (Country, ProducerCountry, Admin_Level_1) from admin_code
        # MUST be done BEFORE formatting (which removes _median columns)
        logger.debug("Adding administrative context...")
        try:
            from openforis_whisp.parameters.lookup_gaul1_admin import lookup_dict

            combined = join_admin_codes(
                df=combined, lookup_dict=lookup_dict, id_col="admin_code_median"
            )
        except ImportError:
            logger.warning(
                "Could not import lookup dictionary - admin context not added"
            )

        # Format the output with error handling for bad bands
        logger.debug("Formatting output...")
        try:
            formatted = format_stats_dataframe(
                df=combined,
                area_col=f"{geometry_area_column}_sum",
                decimal_places=decimal_places,
                unit_type=unit_type,
                remove_columns=True,
                convert_water_flag=True,
            )
        except Exception as e:
            # If formatting fails, try recreating the image with validation
            logger.warning(
                f"Formatting failed: {str(e)[:100]}. Attempting to recreate image with band validation..."
            )
            try:
                with redirect_stdout(io.StringIO()):
                    whisp_image_validated = combine_datasets(
                        national_codes=national_codes, validate_bands=True
                    )

                # Reprocess batches with validated image - create a local process function
                logger.info("Reprocessing batches with validated image...")
                results_validated = []

                def process_batch_validated(
                    batch_idx: int, batch: gpd.GeoDataFrame
                ) -> Tuple[int, pd.DataFrame, pd.DataFrame]:
                    """Process one batch with validated image."""
                    with ee_semaphore:
                        fc = convert_batch_to_ee(batch)
                        if add_metadata_server:
                            fc = extract_centroid_and_geomtype_server(fc)
                        df_server = process_ee_batch(
                            fc,
                            whisp_image_validated,
                            reducer,
                            batch_idx,
                            max_retries,
                            logger,
                        )
                        df_client = extract_centroid_and_geomtype_client(
                            batch,
                            external_id_column=external_id_column,
                            return_attributes_only=True,
                        )
                    return batch_idx, df_server, df_client

                with ThreadPoolExecutor(max_workers=pool_workers) as executor:
                    futures = {
                        executor.submit(process_batch_validated, i, batch): i
                        for i, batch in enumerate(batches)
                    }

                    for future in as_completed(futures):
                        try:
                            batch_idx, df_server, df_client = future.result()
                            if plot_id_column not in df_server.columns:
                                df_server[plot_id_column] = range(len(df_server))

                            # Drop external_id_column from df_client if it exists (already in df_server)
                            if (
                                external_id_column
                                and external_id_column in df_client.columns
                            ):
                                df_client = df_client.drop(columns=[external_id_column])

                            merged = df_server.merge(
                                df_client,
                                on=plot_id_column,
                                how="left",
                                suffixes=("", "_client"),
                            )
                            results_validated.append(merged)
                        except Exception as batch_e:
                            logger.error(
                                f"Batch reprocessing error: {str(batch_e)[:100]}"
                            )

                if results_validated:
                    # Concatenate with explicit dtype handling to suppress FutureWarning
                    combined = pd.concat(
                        results_validated, ignore_index=True, sort=False
                    )
                    # Ensure all column names are strings (fixes pandas .str accessor issues later)
                    combined.columns = combined.columns.astype(str)

                    # Clean up duplicate external_id columns created by merges
                    if external_id_column:
                        external_id_variants = [
                            col
                            for col in combined.columns
                            if external_id_column.lower() in col.lower()
                        ]

                        if external_id_variants:
                            base_col = external_id_column
                            if (
                                base_col not in combined.columns
                                and external_id_variants
                            ):
                                base_col = external_id_variants[0]
                                combined = combined.rename(
                                    columns={base_col: "external_id"}
                                )

                            cols_to_drop = [
                                c for c in external_id_variants if c != base_col
                            ]
                            combined = combined.drop(
                                columns=cols_to_drop, errors="ignore"
                            )

                    # plotId column is already present, just ensure it's at position 0
                    if plot_id_column in combined.columns:
                        combined = combined[
                            [plot_id_column]
                            + [col for col in combined.columns if col != plot_id_column]
                        ]

                    # Add admin context again
                    try:
                        from openforis_whisp.parameters.lookup_gaul1_admin import (
                            lookup_dict,
                        )

                        combined = join_admin_codes(
                            df=combined,
                            lookup_dict=lookup_dict,
                            id_col="admin_code_median",
                        )
                    except ImportError:
                        logger.warning(
                            "Could not import lookup dictionary - admin context not added"
                        )

                    # Try formatting again with validated data
                    formatted = format_stats_dataframe(
                        df=combined,
                        area_col=f"{geometry_area_column}_sum",
                        decimal_places=decimal_places,
                        unit_type=unit_type,
                        remove_columns=True,
                        convert_water_flag=True,
                    )
                else:
                    logger.error(" Reprocessing with validation produced no results")
                    return pd.DataFrame()
            except Exception as retry_e:
                logger.error(
                    f"Failed to recover from formatting error: {str(retry_e)[:100]}"
                )
                raise retry_e

        logger.info(f"Processed {len(formatted):,} features successfully")
        return formatted
    else:
        logger.error(" No results produced")
        return pd.DataFrame()


# ============================================================================
# SEQUENTIAL PROCESSING (STANDARD ENDPOINT)
# ============================================================================


def whisp_stats_geojson_to_df_sequential(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    add_metadata_client_side: bool = True,
    logger: logging.Logger = None,
    # Format parameters (auto-detect from config if not provided)
    decimal_places: int = None,
) -> pd.DataFrame:
    """
    Process GeoJSON sequentially using standard EE endpoint with automatic formatting.

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
    add_metadata_client_side : bool
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

    logger = logger or logging.getLogger("whisp-concurrent")

    # Suppress verbose output from dependencies (sequential has lower concurrency, use default)
    _suppress_verbose_output(max_concurrent=1)

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Validate endpoint
    validate_ee_endpoint("standard", raise_error=True)

    # Load GeoJSON with output suppressed
    gdf = _load_geojson_silently(input_geojson_filepath)
    logger.info(f"Loaded {len(gdf):,} features")

    # Clean geometries
    gdf = clean_geodataframe(gdf, logger=logger)

    # Add stable plotIds for merging (starting from 1, not 0)
    gdf[plot_id_column] = range(1, len(gdf) + 1)

    # Add stable row IDs
    row_id_col = "__row_id__"
    gdf[row_id_col] = range(len(gdf))

    # Create image if not provided
    if whisp_image is None:
        logger.debug("Creating Whisp image...")
        # Suppress print statements from combine_datasets
        with redirect_stdout(io.StringIO()):
            try:
                # First try without validation
                whisp_image = combine_datasets(
                    national_codes=national_codes, validate_bands=False
                )
            except Exception as e:
                logger.warning(
                    f"First attempt failed: {str(e)[:100]}. Retrying with validate_bands=True..."
                )
                # Retry with validation to catch and fix bad bands
                whisp_image = combine_datasets(
                    national_codes=national_codes, validate_bands=True
                )

    # Convert to EE (suppress print statements from convert_geojson_to_ee)
    logger.debug("Converting to EE FeatureCollection...")
    with redirect_stdout(io.StringIO()):
        fc = convert_geojson_to_ee(input_geojson_filepath)

    # Create reducer
    reducer = ee.Reducer.sum().combine(ee.Reducer.median(), sharedInputs=True)

    # Process server-side with error handling for bad bands
    logger.info("Processing with Earth Engine...")
    try:
        results_fc = whisp_image.reduceRegions(collection=fc, reducer=reducer, scale=10)
        df_server = convert_ee_to_df(results_fc)
    except Exception as e:
        # Check if this is a band error
        error_msg = str(e)
        is_band_error = any(
            keyword in error_msg
            for keyword in ["Image.load", "asset", "not found", "does not exist"]
        )

        if is_band_error and whisp_image is not None:
            logger.warning(
                f"Detected bad band error: {error_msg[:100]}. Retrying with validate_bands=True..."
            )
            try:
                with redirect_stdout(io.StringIO()):
                    whisp_image = combine_datasets(
                        national_codes=national_codes, validate_bands=True
                    )
                logger.info("Image recreated with validation. Retrying processing...")
                results_fc = whisp_image.reduceRegions(
                    collection=fc, reducer=reducer, scale=10
                )
                df_server = convert_ee_to_df(results_fc)
            except Exception as retry_e:
                logger.error(f"Retry failed: {str(retry_e)[:100]}")
                raise
        else:
            raise

    logger.debug("Server-side processing complete")

    # Add row_id if missing
    if row_id_col not in df_server.columns:
        df_server[row_id_col] = range(len(df_server))

    # Add client-side metadata if requested
    if add_metadata_client_side:
        logger.debug("Extracting client-side metadata...")
        df_client = extract_centroid_and_geomtype_client(
            gdf,
            external_id_column=external_id_column,
            return_attributes_only=True,
        )

        # Drop external_id_column from df_client if it exists (already in df_server)
        if external_id_column and external_id_column in df_client.columns:
            df_client = df_client.drop(columns=[external_id_column])

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

    logger.info(f"Processed {len(formatted):,} features")

    # Consolidate external_id_column to standardized 'external_id'
    if external_id_column:
        variants = [
            col
            for col in formatted.columns
            if external_id_column.lower() in col.lower()
        ]
        if variants:
            base_col = (
                external_id_column
                if external_id_column in formatted.columns
                else variants[0]
            )
            if base_col != "external_id":
                formatted = formatted.rename(columns={base_col: "external_id"})
            # Drop other variants
            formatted = formatted.drop(
                columns=[c for c in variants if c != base_col], errors="ignore"
            )

    return formatted


# ============================================================================
# FORMATTED WRAPPER FUNCTIONS (STATS + FORMAT)
# ============================================================================


def whisp_formatted_stats_geojson_to_df_concurrent(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    batch_size: int = 10,
    max_concurrent: int = 20,
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

    Combines whisp_stats_geojson_to_df_concurrent + format_stats_dataframe + validation
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

    logger = logger or logging.getLogger("whisp-concurrent")

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        # Use stats_area_columns_formatting as default for most columns
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Step 1: Get raw stats
    logger.debug("Step 1/2: Extracting statistics (concurrent)...")
    df_raw = whisp_stats_geojson_to_df_concurrent(
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
    median_cols_before = [c for c in df_raw.columns if c.endswith("_median")]
    logger.debug(
        f"Columns ending with '_median' BEFORE formatting: {median_cols_before}"
    )

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

    median_cols_after = [c for c in df_formatted.columns if c.endswith("_median")]
    logger.debug(f"Columns ending with '_median' AFTER formatting: {median_cols_after}")

    # Step 2b: Reformat geometry and handle point areas
    try:
        df_formatted = reformat_geometry_type(df_formatted)
    except Exception as e:
        logger.warning(f"Error reformatting geometry type: {e}")

    try:
        df_formatted = set_point_geometry_area_to_zero(df_formatted)
    except Exception as e:
        logger.warning(f"Error setting point geometry area to zero: {e}")

    # Step 3: Validate against schema
    logger.debug("Step 3/3: Validating against schema...")
    from openforis_whisp.reformat import validate_dataframe_using_lookups_flexible

    df_validated = validate_dataframe_using_lookups_flexible(
        df_stats=df_formatted,
        national_codes=national_codes,
        custom_bands=custom_bands,
    )

    logger.info("Concurrent processing + formatting + validation complete")
    return df_validated


def whisp_formatted_stats_geojson_to_df_sequential(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    add_metadata_client_side: bool = True,
    logger: logging.Logger = None,
    # Format parameters (auto-detect from config if not provided)
    decimal_places: int = None,
    remove_median_columns: bool = True,
    convert_water_flag: bool = True,
    water_flag_threshold: float = 0.5,
    sort_column: str = "plotId",
) -> pd.DataFrame:
    """
    Process GeoJSON sequentially with automatic formatting and validation.

    Combines whisp_stats_geojson_to_df_sequential + format_stats_dataframe + validation
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
    add_metadata_client_side : bool
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

    logger = logger or logging.getLogger("whisp-concurrent")

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        # Use stats_area_columns_formatting as default for most columns
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Step 1: Get raw stats
    logger.debug("Step 1/2: Extracting statistics (sequential)...")
    df_raw = whisp_stats_geojson_to_df_sequential(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
        remove_geom=remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,
        custom_bands=custom_bands,
        add_metadata_client_side=add_metadata_client_side,
        logger=logger,
    )

    # Step 2: Format the output
    logger.debug("Step 2/2: Formatting output...")
    median_cols_before = [c for c in df_raw.columns if c.endswith("_median")]
    logger.debug(
        f"Columns ending with '_median' BEFORE formatting: {median_cols_before}"
    )

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

    median_cols_after = [c for c in df_formatted.columns if c.endswith("_median")]
    logger.debug(f"Columns ending with '_median' AFTER formatting: {median_cols_after}")

    # Step 2b: Reformat geometry and handle point areas
    try:
        df_formatted = reformat_geometry_type(df_formatted)
    except Exception as e:
        logger.warning(f"Error reformatting geometry type: {e}")

    try:
        df_formatted = set_point_geometry_area_to_zero(df_formatted)
    except Exception as e:
        logger.warning(f"Error setting point geometry area to zero: {e}")

    # Step 3: Validate against schema
    logger.debug("Step 3/3: Validating against schema...")
    from openforis_whisp.reformat import validate_dataframe_using_lookups_flexible

    df_validated = validate_dataframe_using_lookups_flexible(
        df_stats=df_formatted,
        national_codes=national_codes,
        custom_bands=custom_bands,
    )

    logger.info("Sequential processing + formatting + validation complete")
    return df_validated


# ============================================================================
# FAST PROCESSING WITH AUTO-ROUTING
# ============================================================================


def whisp_formatted_stats_geojson_to_df_fast(
    input_geojson_filepath: str,
    external_id_column: str = None,
    remove_geom: bool = False,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    mode: str = "auto",
    # Concurrent-specific parameters
    batch_size: int = 10,
    max_concurrent: int = 20,
    validate_geometries: bool = True,
    max_retries: int = 3,
    add_metadata_server: bool = False,
    # Format parameters (auto-detect from config if not provided)
    decimal_places: int = None,
    remove_median_columns: bool = True,
    convert_water_flag: bool = True,
    water_flag_threshold: float = 0.5,
    sort_column: str = "plotId",
) -> pd.DataFrame:
    """
    Process GeoJSON to Whisp statistics with optimized fast processing.

    Automatically selects between concurrent (high-volume endpoint) and sequential
    (standard endpoint) based on file size, or allows explicit mode selection.

    This is the recommended entry point for most users who want automatic optimization.

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
    mode : str
        Processing mode:
        - "auto": Choose based on file size (default)
          * <1MB: sequential
          * 1-5MB: sequential
          * >5MB: concurrent
        - "concurrent": Force high-volume endpoint (batch processing)
        - "sequential": Force standard endpoint (single-threaded)
    batch_size : int
        Features per batch (only for concurrent mode)
    max_concurrent : int
        Maximum concurrent EE calls (only for concurrent mode)
    validate_geometries : bool
        Validate and clean geometries
    max_retries : int
        Retry attempts per batch (only for concurrent mode)
    add_metadata_server : bool
        Add metadata server-side (only for concurrent mode)
    decimal_places : int, optional
        Decimal places for rounding. If None, auto-detects from config.
    remove_median_columns : bool
        Remove '_median' columns
    convert_water_flag : bool
        Convert water flag to boolean
    water_flag_threshold : float
        Water flag ratio threshold
    sort_column : str
        Column to sort by

    Returns
    -------
    pd.DataFrame
        Validated, formatted results DataFrame

    Examples
    --------
    >>> # Auto-detect best method based on file size
    >>> df = whisp_formatted_stats_geojson_to_df_fast("data.geojson")

    >>> # Force concurrent processing for large datasets
    >>> df = whisp_formatted_stats_geojson_to_df_fast(
    ...     "large_data.geojson",
    ...     mode="concurrent"
    ... )

    >>> # Use sequential for guaranteed completion
    >>> df = whisp_formatted_stats_geojson_to_df_fast(
    ...     "data.geojson",
    ...     mode="sequential"
    ... )
    """
    logger = logging.getLogger("whisp-concurrent")

    # Determine processing mode
    if mode == "auto":
        try:
            file_size = Path(input_geojson_filepath).stat().st_size
            if file_size > 5_000_000:  # >5MB
                chosen_mode = "concurrent"
                logger.info(
                    f"File size {file_size/1e6:.1f}MB → Using concurrent (high-volume endpoint)"
                )
            else:  # <=5MB
                chosen_mode = "sequential"
                logger.info(
                    f"File size {file_size/1e6:.1f}MB → Using sequential (standard endpoint)"
                )
        except Exception as e:
            logger.warning(
                f"Could not determine file size: {e}. Defaulting to sequential."
            )
            chosen_mode = "sequential"
    elif mode in ("concurrent", "sequential"):
        chosen_mode = mode
        logger.info(f"Mode explicitly set to: {mode}")
    else:
        raise ValueError(
            f"Invalid mode '{mode}'. Must be 'auto', 'concurrent', or 'sequential'."
        )

    # Route to appropriate function
    if chosen_mode == "concurrent":
        logger.debug("Routing to concurrent processing...")
        return whisp_formatted_stats_geojson_to_df_concurrent(
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
            decimal_places=decimal_places,
            remove_median_columns=remove_median_columns,
            convert_water_flag=convert_water_flag,
            water_flag_threshold=water_flag_threshold,
            sort_column=sort_column,
        )
    else:  # sequential
        logger.debug("Routing to sequential processing...")
        return whisp_formatted_stats_geojson_to_df_sequential(
            input_geojson_filepath=input_geojson_filepath,
            external_id_column=external_id_column,
            remove_geom=remove_geom,
            national_codes=national_codes,
            unit_type=unit_type,
            whisp_image=whisp_image,
            custom_bands=custom_bands,
            logger=logger,
            decimal_places=decimal_places,
            remove_median_columns=remove_median_columns,
            convert_water_flag=convert_water_flag,
            water_flag_threshold=water_flag_threshold,
            sort_column=sort_column,
        )

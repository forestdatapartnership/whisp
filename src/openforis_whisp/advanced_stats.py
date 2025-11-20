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
from typing import Optional, List, Dict, Any, Tuple, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile

# Configure the "whisp" logger with auto-flush handler for Colab visibility
_whisp_logger = logging.getLogger("whisp")
if not _whisp_logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setLevel(logging.DEBUG)
    _handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    # Override emit to force flush after each message for Colab
    _original_emit = _handler.emit

    def _emit_with_flush(record):
        _original_emit(record)
        sys.stdout.flush()

    _handler.emit = _emit_with_flush
    _whisp_logger.addHandler(_handler)
    _whisp_logger.setLevel(logging.INFO)
    _whisp_logger.propagate = False  # Don't propagate to root to avoid duplicates

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


def _normalize_keep_external_columns(
    keep_external_columns: Union[bool, List[str]],
    all_columns: List[str],
    plot_id_column: str = "plotId",
) -> List[str]:
    """
    Normalize keep_external_columns parameter to a list of column names.

    Converts flexible user input (bool or list) to a concrete list of columns to keep.

    Parameters
    ----------
    keep_external_columns : bool or List[str]
        - False: keep nothing (return empty list)
        - True: keep all columns except geometry and plot_id
        - List[str]: keep specific columns (return as-is)
    all_columns : List[str]
        All available columns to choose from
    plot_id_column : str
        Name of plot ID column to exclude

    Returns
    -------
    List[str]
        Columns to keep from external (GeoJSON) data

    Examples
    --------
    >>> cols = _normalize_keep_external_columns(False, ["id", "Country", "geom"], "id")
    >>> cols
    []

    >>> cols = _normalize_keep_external_columns(True, ["id", "Country", "geom"], "id")
    >>> cols
    ['Country']

    >>> cols = _normalize_keep_external_columns(["Country"], ["id", "Country", "geom"], "id")
    >>> cols
    ['Country']
    """
    if keep_external_columns is True:
        # Keep all columns except geometry and plot_id
        return [c for c in all_columns if c not in [plot_id_column, "geometry"]]
    elif keep_external_columns is False:
        # Keep nothing
        return []
    else:
        # Use provided list (handle None case)
        return keep_external_columns or []


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
    logger = logging.getLogger("whisp")

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
    logger = logging.getLogger("whisp")

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

        # Fill NaN values with "Unknown" and "not found" for features outside admin boundaries
        # (e.g., points in the ocean or international waters)
        df_joined[iso3_country_column] = df_joined[iso3_country_column].fillna(
            "Unknown"
        )
        df_joined[iso2_country_column] = df_joined[iso2_country_column].fillna(
            "not found"
        )
        df_joined[admin_1_column] = df_joined[admin_1_column].fillna("Unknown")

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

    Shows progress at adaptive milestones (more frequent for small datasets,
    less frequent for large datasets) with estimated time remaining based on
    processing speed. Includes time-based heartbeat to prevent long silences.
    """

    def __init__(
        self,
        total: int,
        logger: logging.Logger = None,
        heartbeat_interval: int = 180,
        status_file: str = None,
    ):
        """
        Initialize progress tracker.

        Parameters
        ----------
        total : int
            Total number of items to process
        logger : logging.Logger, optional
            Logger for output
        heartbeat_interval : int, optional
            Seconds between heartbeat messages (default: 180 = 3 minutes)
        status_file : str, optional
            Path to JSON status file for API/web app consumption.
            Checkpoints auto-save to same directory as status_file.
        """
        self.total = total
        self.completed = 0
        self.lock = threading.Lock()
        self.logger = logger or logging.getLogger("whisp")
        self.heartbeat_interval = heartbeat_interval

        # Handle status_file: if directory passed, auto-generate filename
        if status_file:
            import os

            if os.path.isdir(status_file):
                self.status_file = os.path.join(
                    status_file, "whisp_processing_status.json"
                )
            else:
                # Validate that parent directory exists
                parent_dir = os.path.dirname(status_file)
                if parent_dir and not os.path.isdir(parent_dir):
                    self.logger.warning(
                        f"Status file directory does not exist: {parent_dir}"
                    )
                    self.status_file = None
                else:
                    self.status_file = status_file
        else:
            self.status_file = None

        # Adaptive milestones based on dataset size
        # Small datasets (< 50): show every 25% (not too spammy)
        # Medium (50-500): show every 20%
        # Large (500-1000): show every 10%
        # Very large (1000+): show every 5% (cleaner for long jobs)
        if total < 50:
            self.milestones = {25, 50, 75, 100}
        elif total < 500:
            self.milestones = {20, 40, 60, 80, 100}
        elif total < 1000:
            self.milestones = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
        else:
            self.milestones = {
                5,
                10,
                15,
                20,
                25,
                30,
                35,
                40,
                45,
                50,
                55,
                60,
                65,
                70,
                75,
                80,
                85,
                90,
                95,
                100,
            }

        self.shown_milestones = set()
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.heartbeat_stop = threading.Event()
        self.heartbeat_thread = None

    def _write_status_file(self, status: str = "processing") -> None:
        """Write current progress to JSON status file using atomic write."""
        if not self.status_file:
            return

        try:
            import json
            import os

            elapsed = time.time() - self.start_time
            percent = (self.completed / self.total * 100) if self.total > 0 else 0
            rate = self.completed / elapsed if elapsed > 0 else 0
            eta = (
                (self.total - self.completed) / rate * 1.15
                if rate > 0 and percent >= 5
                else None
            )

            # Write to temp file then atomic rename to prevent partial reads
            from datetime import datetime

            temp_file = self.status_file + ".tmp"
            with open(temp_file, "w") as f:
                json.dump(
                    {
                        "status": status,
                        "progress": f"{self.completed}/{self.total}",
                        "percent": round(percent, 1),
                        "elapsed_sec": round(elapsed),
                        "eta_sec": round(eta) if eta else None,
                        "updated_at": datetime.now().isoformat(),
                    },
                    f,
                )
            os.replace(temp_file, self.status_file)
        except Exception:
            pass

    def start_heartbeat(self) -> None:
        """Start background heartbeat thread for time-based progress updates."""
        if self.heartbeat_thread is None or not self.heartbeat_thread.is_alive():
            self.heartbeat_stop.clear()
            self.heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop, daemon=True
            )
            self.heartbeat_thread.start()
            # Write initial status
            self._write_status_file(status="processing")

    def _heartbeat_loop(self) -> None:
        """Background loop that logs progress at time intervals."""
        while not self.heartbeat_stop.wait(self.heartbeat_interval):
            with self.lock:
                # Only log if we haven't shown a milestone recently
                time_since_update = time.time() - self.last_update_time
                if (
                    time_since_update >= self.heartbeat_interval
                    and self.completed < self.total
                ):
                    elapsed = time.time() - self.start_time
                    percent = int((self.completed / self.total) * 100)
                    elapsed_str = self._format_time(elapsed)
                    self.logger.info(
                        f"[Processing] {self.completed:,}/{self.total:,} batches ({percent}%) | "
                        f"Elapsed: {elapsed_str}"
                    )
                    self.last_update_time = time.time()

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

            # Show milestone messages (5%, 10%, 15%... for large datasets)
            for milestone in sorted(self.milestones):
                if percent >= milestone and milestone not in self.shown_milestones:
                    self.shown_milestones.add(milestone)

                    # Calculate time metrics
                    elapsed = time.time() - self.start_time
                    rate = self.completed / elapsed if elapsed > 0 else 0
                    remaining_items = self.total - self.completed

                    # Calculate ETA with padding for overhead (loading, joins, etc.)
                    # Don't show ETA until we have some samples (at least 5% complete)
                    if rate > 0 and self.completed >= max(5, self.total * 0.05):
                        eta_seconds = (
                            remaining_items / rate
                        ) * 1.15  # Add 15% padding for overhead
                    else:
                        eta_seconds = 0

                    # Format time strings
                    eta_str = (
                        self._format_time(eta_seconds)
                        if eta_seconds > 0
                        else "calculating..."
                    )
                    elapsed_str = self._format_time(elapsed)

                    # Build progress message
                    msg = f"Progress: {self.completed:,}/{self.total:,} batches ({percent}%)"
                    if percent < 100:
                        msg += f" | Elapsed: {elapsed_str} | ETA: {eta_str}"
                    else:
                        msg += f" | Total time: {elapsed_str}"

                    self.logger.info(msg)
                    self.last_update_time = time.time()

        # Update status file for API consumption
        self._write_status_file()

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

    def finish(self, output_file: str = None) -> None:
        """Stop heartbeat and log completion."""
        # Stop heartbeat thread
        self.heartbeat_stop.set()
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=1)

        with self.lock:
            total_time = time.time() - self.start_time
            time_str = self._format_time(total_time)
            msg = f"Processing complete: {self.completed:,}/{self.total:,} batches in {time_str}"
            self.logger.info(msg)

        # Write final status
        self._write_status_file(status="completed")


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
        if endpoint_type == "high-volume":
            msg = (
                "Concurrent mode requires the HIGH-VOLUME endpoint. To change endpoint run:\n"
                "ee.Reset()\n"
                "ee.Initialize(opt_url='https://earthengine-highvolume.googleapis.com')\n"
                "Or with project specified (e.g. when in Colab):\n"
                "ee.Initialize(project='your_cloud_project_name', opt_url='https://earthengine-highvolume.googleapis.com')"
            )
        else:  # standard endpoint
            msg = (
                "Sequential mode requires the STANDARD endpoint. To change endpoint run:\n"
                "ee.Reset()\n"
                "ee.Initialize()\n"
                "Or with project specified (e.g. when in Colab):\n"
                "ee.Initialize(project='your_cloud_project_name')"
            )

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
    Convert a batch GeoDataFrame to EE FeatureCollection efficiently.

    OPTIMIZATION: Passes GeoDataFrame directly to convert_geojson_to_ee to preserve CRS.
    This ensures proper coordinate system handling and reprojection to WGS84 if needed.

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
    # Pass GeoDataFrame directly to preserve CRS metadata
    # convert_geojson_to_ee will handle:
    # - CRS detection and conversion to WGS84 if needed
    # - Data type sanitization (datetime, object columns)
    # - Geometry validation and Z-coordinate stripping

    fc = convert_geojson_to_ee(batch_gdf, enforce_wgs84=True, strip_z_coords=True)

    # If __row_id__ is in the original GeoDataFrame, it will be preserved
    # as a feature property in the GeoJSON and thus in the EE FeatureCollection
    return fc


def clean_geodataframe(
    gdf: gpd.GeoDataFrame,
    remove_nulls: bool = False,
    repair_geometries: bool = False,
    logger: logging.Logger = None,
) -> gpd.GeoDataFrame:
    """
    Validate and clean GeoDataFrame geometries.

    Parameters
    ----------
    gdf : gpd.GeoDataFrame
        Input GeoDataFrame
    remove_nulls : bool
        Remove null geometries. Defaults to False to preserve data integrity.
        Set to True only if you explicitly want to drop rows with null geometries.
    repair_geometries : bool
        Repair invalid geometries using Shapely's make_valid(). Defaults to False to preserve
        original geometries. Set to True only if you want to automatically repair invalid geometries.
    logger : logging.Logger, optional
        Logger for output

    Returns
    -------
    gpd.GeoDataFrame
        Cleaned GeoDataFrame
    """
    logger = logger or logging.getLogger("whisp")

    if remove_nulls:
        null_count = gdf.geometry.isna().sum()
        if null_count > 0:
            logger.warning(f"Removing {null_count} null geometries")
            gdf = gdf[~gdf.geometry.isna()].copy()

    if repair_geometries:
        valid_count = gdf.geometry.is_valid.sum()
        invalid_count = len(gdf) - valid_count
        if invalid_count > 0:
            logger.warning(f"Repairing {invalid_count} invalid geometries")
            from shapely.validation import make_valid

            gdf = gdf.copy()
            gdf["geometry"] = gdf["geometry"].apply(
                lambda g: make_valid(g) if g and not g.is_valid else g
            )

    logger.debug(f"Validation complete: {len(gdf):,} geometries ready")
    return gdf


# ============================================================================
# BATCH RETRY HELPER
# ============================================================================


# ============================================================================
# BATCH RETRY HELPER - DEPRECATED (removed due to semaphore deadlock issues)
# ============================================================================
# Note: Retry logic via sub-batching has been removed. Instead, use fail-fast
# approach: when a batch fails, reduce batch_size parameter and retry manually.
# This avoids semaphore deadlocks and provides clearer error messages.


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
    logger = logger or logging.getLogger("whisp")

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
    status_file: str = None,
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

    logger = logger or logging.getLogger("whisp")

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

    # Validate external_id_column if provided (lightweight client-side check)
    if external_id_column and external_id_column not in gdf.columns:
        # Exclude geometry column from available columns list
        available_cols = [c for c in gdf.columns if c != gdf.geometry.name]
        raise ValueError(
            f"Column '{external_id_column}' not found in GeoJSON properties. "
            f"Available columns: {available_cols}"
        )

    # Check completeness of external_id_column (warn if nulls exist)
    if external_id_column and external_id_column in gdf.columns:
        null_count = gdf[external_id_column].isna().sum()
        if null_count > 0:
            null_pct = (null_count / len(gdf)) * 100
            logger.warning(
                f"Column '{external_id_column}' has {null_count:,} null values ({null_pct:.1f}% of {len(gdf):,} features). "
                f"These features may have missing external IDs in output."
            )

    if validate_geometries:
        gdf = clean_geodataframe(
            gdf, remove_nulls=False, repair_geometries=False, logger=logger
        )

    # Add stable plotIds for merging (starting from 1, not 0)
    gdf[plot_id_column] = range(1, len(gdf) + 1)

    # Strip unnecessary properties before sending to EE
    # Keep only: geometry, plot_id_column, and external_id_column
    # This prevents duplication of GeoJSON properties in EE results
    keep_cols = ["geometry", plot_id_column]
    if external_id_column and external_id_column in gdf.columns:
        keep_cols.append(external_id_column)

    gdf_for_ee = gdf[keep_cols].copy()
    logger.debug(f"Stripped GeoJSON to essential columns: {keep_cols}")

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
    batches = batch_geodataframe(gdf_for_ee, batch_size)
    logger.info(
        f"Processing {len(gdf_for_ee):,} features in {len(batches)} batches (concurrent mode)..."
    )

    # Setup semaphore for EE concurrency control
    ee_semaphore = threading.BoundedSemaphore(max_concurrent)

    # Progress tracker with heartbeat for long-running jobs
    progress = ProgressTracker(
        len(batches), logger=logger, heartbeat_interval=180, status_file=status_file
    )
    progress.start_heartbeat()

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
        # Don't suppress stdout here - we want progress messages to show in Colab
        with ThreadPoolExecutor(max_workers=pool_workers) as executor:
            futures = {
                executor.submit(process_batch, i, batch): i
                for i, batch in enumerate(batches)
            }

            # Track which batches failed for retry
            batch_map = {i: batch for i, batch in enumerate(batches)}
            batch_futures = {future: i for future, i in futures.items()}

            for future in as_completed(futures):
                batch_idx = batch_futures[future]
                try:
                    batch_idx, df_server, df_client = future.result()

                    # Merge server and client results
                    if plot_id_column not in df_server.columns:
                        df_server[plot_id_column] = range(len(df_server))

                    # Keep all EE statistics from server (all columns with _sum and _median suffixes)
                    # These are the actual EE processing results
                    df_server_clean = df_server.copy()

                    # Keep external metadata: plot_id, external_id, geometry, geometry type, and centroids from client
                    # (formatted wrapper handles keep_external_columns parameter)
                    keep_external_columns = [plot_id_column]
                    if external_id_column and external_id_column in df_client.columns:
                        keep_external_columns.append(external_id_column)
                    if "geometry" in df_client.columns:
                        keep_external_columns.append("geometry")
                    # Keep geometry type column (Geometry_type)
                    if geometry_type_column in df_client.columns:
                        keep_external_columns.append(geometry_type_column)
                    # Also keep centroid columns (Centroid_lon, Centroid_lat)
                    centroid_cols = [
                        c for c in df_client.columns if c.startswith("Centroid_")
                    ]
                    keep_external_columns.extend(centroid_cols)

                    df_client_clean = df_client[
                        [c for c in keep_external_columns if c in df_client.columns]
                    ]
                    # Don't drop duplicates - we need one row per feature (one per plot_id)
                    # Each plot_id should have exactly one row with its metadata

                    merged = df_server_clean.merge(
                        df_client_clean,
                        on=plot_id_column,
                        how="left",
                        suffixes=("_ee", "_client"),
                    )
                    results.append(merged)
                    progress.update()

                except Exception as e:
                    # Batch failed - fail fast with clear guidance
                    error_msg = str(e)
                    logger.error(f"Batch {batch_idx} failed: {error_msg[:100]}")
                    logger.debug(f"Full error: {error_msg}")

                    # Get original batch for error reporting
                    original_batch = batch_map[batch_idx]

                    # Add to batch errors for final reporting
                    batch_errors.append((batch_idx, original_batch, error_msg))
    except (KeyboardInterrupt, SystemExit) as interrupt:
        logger.warning("Processing interrupted by user")
        # Update status file with interrupted state
        progress._write_status_file(status="interrupted")
        raise interrupt
    finally:
        # Restore logger levels
        fiona_logger.setLevel(old_fiona_level)
        pyogrio_logger.setLevel(old_pyogrio_level)

    progress.finish()

    # If we have batch errors after retry attempts, fail the entire process
    if batch_errors:
        total_failed_rows = sum(len(batch) for _, batch, _ in batch_errors)
        failed_batch_indices = [str(idx) for idx, _, _ in batch_errors]

        # Format detailed error information for debugging
        error_details_list = []
        for idx, batch, msg in batch_errors:
            error_details_list.append(f"  Batch {idx} ({len(batch)} features): {msg}")
        error_details = "\n".join(error_details_list)

        # Analyze error patterns for debugging hints
        error_patterns = {
            "memory": any("memory" in msg.lower() for _, _, msg in batch_errors),
            "request_size": any(
                keyword in msg.lower()
                for _, _, msg in batch_errors
                for keyword in ["too large", "10mb", "payload", "size limit"]
            ),
            "quota": any("quota" in msg.lower() for _, _, msg in batch_errors),
            "timeout": any("timeout" in msg.lower() for _, _, msg in batch_errors),
        }

        # Build helpful suggestions based on error patterns
        suggestions = []
        if error_patterns["memory"]:
            suggestions.append(
                f"  • Reduce batch_size parameter (currently: {batch_size}). Try: batch_size=5 or lower"
            )
        if error_patterns["request_size"]:
            suggestions.append(
                "  • Request payload too large: reduce batch_size or simplify input geometries"
            )
        if error_patterns["quota"]:
            suggestions.append("  • Earth Engine quota exceeded: wait and retry later")
        if error_patterns["timeout"]:
            suggestions.append(
                "  • Processing timeout: reduce batch_size or simplify input geometries"
            )

        suggestions_text = (
            "\nDebugging hints:\n" + "\n".join(suggestions) if suggestions else ""
        )

        raise RuntimeError(
            f"Failed to process {len(batch_errors)} batch(es):\n"
            f"\n{error_details}\n"
            f"\nTotal rows affected: {total_failed_rows}\n"
            f"{suggestions_text}\n"
            f"Please reduce batch_size and try again."
        )

    # Check if we should retry with validation due to band errors (legacy band error handling)
    if not results:
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

        logger.info(f"Processing complete: {len(formatted):,} features")
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

    logger = logger or logging.getLogger("whisp")

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

    # Validate external_id_column if provided (lightweight client-side check)
    if external_id_column and external_id_column not in gdf.columns:
        # Exclude geometry column from available columns list
        available_cols = [c for c in gdf.columns if c != gdf.geometry.name]
        raise ValueError(
            f"Column '{external_id_column}' not found in GeoJSON properties. "
            f"Available columns: {available_cols}"
        )

    # Check completeness of external_id_column (warn if nulls exist)
    if external_id_column and external_id_column in gdf.columns:
        null_count = gdf[external_id_column].isna().sum()
        if null_count > 0:
            null_pct = (null_count / len(gdf)) * 100
            logger.warning(
                f"Column '{external_id_column}' has {null_count:,} null values ({null_pct:.1f}% of {len(gdf):,} features). "
                f"These features may have missing external IDs in output."
            )

    # Clean geometries (preserve both null and invalid geometries by default)
    gdf = clean_geodataframe(
        gdf, remove_nulls=False, repair_geometries=False, logger=logger
    )

    # Add stable plotIds for merging (starting from 1, not 0)
    gdf[plot_id_column] = range(1, len(gdf) + 1)

    # Add stable row IDs
    row_id_col = "__row_id__"
    gdf[row_id_col] = range(len(gdf))

    # Strip unnecessary properties before sending to EE
    # Keep only: geometry, plot_id_column, and external_id_column
    # This prevents duplication of GeoJSON properties in EE results
    keep_cols = ["geometry", plot_id_column, row_id_col]
    if external_id_column and external_id_column in gdf.columns:
        keep_cols.append(external_id_column)

    gdf_for_ee = gdf[keep_cols].copy()
    logger.debug(f"Stripped GeoJSON to essential columns: {keep_cols}")

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
        fc = convert_geojson_to_ee(gdf_for_ee, enforce_wgs84=True, strip_z_coords=True)

    # Create reducer
    reducer = ee.Reducer.sum().combine(ee.Reducer.median(), sharedInputs=True)

    # Process server-side with error handling for bad bands
    logger.info(
        f"Processing {len(gdf):,} features with Earth Engine (sequential mode)..."
    )
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

    logger.info(f"Processing complete: {len(formatted):,} features")

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
    geometry_audit_trail: bool = False,
    status_file: str = None,
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
    geometry_audit_trail : bool, default False
        If True, includes original input geometry column:
        - geo_original: Original input geometry (before EE processing), stored as GeoJSON
        Enables geometry traceability for compliance and audit purposes.

    Returns
    -------
    pd.DataFrame
        Validated, formatted results DataFrame with optional audit trail
    """
    from openforis_whisp.reformat import format_stats_dataframe
    from datetime import datetime, timezone
    import json
    from shapely.geometry import mapping

    logger = logger or logging.getLogger("whisp")

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        # Use stats_area_columns_formatting as default for most columns
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Load original geometries once here if needed for audit trail (avoid reloading later)
    gdf_original_geoms = None
    if geometry_audit_trail:
        logger.debug("Pre-loading GeoJSON for geometry audit trail...")
        gdf_original_geoms = _load_geojson_silently(input_geojson_filepath)

    # Step 1: Get raw stats
    logger.debug("Step 1/2: Extracting statistics (concurrent)...")
    df_raw = whisp_stats_geojson_to_df_concurrent(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
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
        status_file=status_file,
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

    # Step 2c: Add audit trail columns (AFTER validation to preserve columns)
    if geometry_audit_trail:
        logger.debug("Adding audit trail columns...")
        try:
            # Use pre-loaded original geometries (loaded at wrapper start to avoid reloading)
            if gdf_original_geoms is None:
                logger.warning("Original geometries not pre-loaded, loading now...")
                gdf_original_geoms = _load_geojson_silently(input_geojson_filepath)

            # Use plotId from df_validated to maintain mapping
            df_original_geom = pd.DataFrame(
                {
                    "plotId": df_validated["plotId"].values[: len(gdf_original_geoms)],
                    "geo_original": gdf_original_geoms["geometry"].apply(
                        lambda g: json.dumps(mapping(g)) if g is not None else None
                    ),
                }
            )

            # Merge original geometries back
            df_validated = df_validated.merge(df_original_geom, on="plotId", how="left")

            # Store processing metadata
            df_validated.attrs["processing_metadata"] = {
                "whisp_version": "3.0.0a1",
                "processing_date": datetime.now().isoformat(),
                "processing_mode": "concurrent",
                "ee_endpoint": "high_volume",
                "validate_geometries": validate_geometries,
                "datasets_used": national_codes or [],
                "geometry_audit_trail": True,
            }

            logger.info(f"Audit trail added: geo_original column")

        except Exception as e:
            logger.warning(f"Error adding audit trail: {e}")
            # Continue without audit trail if something fails

    # Add processing metadata column using pd.concat to avoid fragmentation warning
    metadata_dict = {
        "whisp_version": "3.0.0a1",
        "processing_timestamp_utc": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        ),
    }
    metadata_series = pd.Series(
        [metadata_dict] * len(df_validated), name="whisp_processing_metadata"
    )
    df_validated = pd.concat([df_validated, metadata_series], axis=1)

    logger.info("Concurrent processing + formatting + validation complete")
    return df_validated


def whisp_formatted_stats_geojson_to_df_sequential(
    input_geojson_filepath: str,
    external_id_column: str = None,
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
    geometry_audit_trail: bool = False,
    status_file: str = None,
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
    geometry_audit_trail : bool, default True
        If True, includes original input geometry column:
        - geo_original: Original input geometry (before EE processing), stored as GeoJSON
        Enables geometry traceability for compliance and audit purposes.

    Returns
    -------
    pd.DataFrame
        Validated, formatted results DataFrame with optional audit trail
    """
    from openforis_whisp.reformat import format_stats_dataframe
    from datetime import datetime, timezone
    import json
    from shapely.geometry import mapping

    logger = logger or logging.getLogger("whisp")

    # Auto-detect decimal places from config if not provided
    if decimal_places is None:
        # Use stats_area_columns_formatting as default for most columns
        decimal_places = _extract_decimal_places(stats_area_columns_formatting)
        logger.debug(f"Using decimal_places={decimal_places} from config")

    # Load original geometries once here if needed for audit trail (avoid reloading later)
    gdf_original_geoms = None
    if geometry_audit_trail:
        logger.debug("Pre-loading GeoJSON for geometry audit trail...")
        gdf_original_geoms = _load_geojson_silently(input_geojson_filepath)

    # Step 1: Get raw stats
    logger.debug("Step 1/2: Extracting statistics (sequential)...")
    df_raw = whisp_stats_geojson_to_df_sequential(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
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

    # Step 2c: Add audit trail columns (AFTER validation to preserve columns)
    if geometry_audit_trail:
        logger.debug("Adding audit trail columns...")
        try:
            # Use pre-loaded original geometries (loaded at wrapper start to avoid reloading)
            if gdf_original_geoms is None:
                logger.warning("Original geometries not pre-loaded, loading now...")
                gdf_original_geoms = _load_geojson_silently(input_geojson_filepath)

            # Use plotId from df_validated to maintain mapping
            df_original_geom = pd.DataFrame(
                {
                    "plotId": df_validated["plotId"].values[: len(gdf_original_geoms)],
                    "geo_original": gdf_original_geoms["geometry"].apply(
                        lambda g: json.dumps(mapping(g)) if g is not None else None
                    ),
                }
            )

            # Merge original geometries back
            df_validated = df_validated.merge(df_original_geom, on="plotId", how="left")

            # Store processing metadata
            df_validated.attrs["processing_metadata"] = {
                "whisp_version": "3.0.0a1",
                "processing_date": datetime.now().isoformat(),
                "processing_mode": "sequential",
                "ee_endpoint": "standard",
                "datasets_used": national_codes or [],
                "geometry_audit_trail": True,
            }

            logger.info(f"Audit trail added: geo_original column")

        except Exception as e:
            logger.warning(f"Error adding audit trail: {e}")
            # Continue without audit trail if something fails

    # Add processing metadata column using pd.concat to avoid fragmentation warning
    metadata_dict = {
        "whisp_version": "3.0.0a1",
        "processing_timestamp_utc": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S UTC"
        ),
    }
    metadata_series = pd.Series(
        [metadata_dict] * len(df_validated), name="whisp_processing_metadata"
    )
    df_validated = pd.concat([df_validated, metadata_series], axis=1)

    logger.info("Sequential processing + formatting + validation complete")
    return df_validated


# ============================================================================
# FAST PROCESSING WITH AUTO-ROUTING
# ============================================================================


def whisp_formatted_stats_geojson_to_df_fast(
    input_geojson_filepath: str,
    external_id_column: str = None,
    national_codes: List[str] = None,
    unit_type: str = "ha",
    whisp_image: ee.Image = None,
    custom_bands: Dict[str, Any] = None,
    mode: str = "sequential",
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
    geometry_audit_trail: bool = False,
    status_file: str = None,
) -> pd.DataFrame:
    """
    Process GeoJSON to Whisp statistics with optimized fast processing.

    Routes to concurrent (high-volume endpoint) or sequential (standard endpoint)
    based on explicit mode selection.

    This is the recommended entry point for most users.

    Parameters
    ----------
    input_geojson_filepath : str
        Path to input GeoJSON file
    external_id_column : str, optional
        Column name for external IDs
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
        - "concurrent": Uses high-volume endpoint with batch processing
        - "sequential": Uses standard endpoint for sequential processing
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
    geometry_audit_trail : bool
        Include geometry modification audit trail columns

    Returns
    -------
    pd.DataFrame
        Validated, formatted results DataFrame

    Examples
    --------
    >>> # Use concurrent processing (recommended for most datasets)
    >>> df = whisp_formatted_stats_geojson_to_df_fast(
    ...     "data.geojson",
    ...     mode="concurrent"
    ... )

    >>> # Use sequential processing for more stable results
    >>> df = whisp_formatted_stats_geojson_to_df_fast(
    ...     "data.geojson",
    ...     mode="sequential"
    ... )
    """
    logger = logging.getLogger("whisp")

    # Validate mode parameter
    if mode not in ("concurrent", "sequential"):
        raise ValueError(
            f"Invalid mode '{mode}'. Must be 'concurrent' or 'sequential'."
        )

    logger.info(f"Mode: {mode}")

    # Route to appropriate function
    if mode == "concurrent":
        logger.debug("Routing to concurrent processing...")
        return whisp_formatted_stats_geojson_to_df_concurrent(
            input_geojson_filepath=input_geojson_filepath,
            external_id_column=external_id_column,
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
            geometry_audit_trail=geometry_audit_trail,
            status_file=status_file,
        )
    else:  # sequential
        logger.debug("Routing to sequential processing...")
        return whisp_formatted_stats_geojson_to_df_sequential(
            input_geojson_filepath=input_geojson_filepath,
            external_id_column=external_id_column,
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
            geometry_audit_trail=geometry_audit_trail,
            status_file=status_file,
        )

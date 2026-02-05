"""
Local statistics processing module for Whisp.

This module provides functions for downloading Earth Engine data locally
and processing it with exactextract for privacy-preserving analysis.
The workflow allows users to obscure exact plot locations before sending
requests to Earth Engine, then perform zonal statistics locally.

Key functions:
- download_geotiff_for_feature: Download GeoTIFF for a single EE feature
- download_geotiffs_for_feature_collection: Parallel download for multiple features
- convert_geojson_to_ee_bbox_obscured: Create obscured bounding boxes with decoys
- create_vrt_from_folder: Create virtual raster mosaic from downloaded TIFFs
- exact_extract_in_chunks_parallel: Parallel local zonal statistics
- create_geojson: Generate random test polygons
- reformat_geojson_properties: Add IDs to GeoJSON features
"""

import os
import ee
import json
import pandas as pd
import geopandas as gpd
import numpy as np
import logging
import time
import requests
import random
import math
import gc
import uuid
import concurrent.futures
from pathlib import Path
from datetime import datetime, timezone
from importlib.metadata import version as get_version

from shapely.geometry import Polygon, mapping
from shapely.validation import make_valid
from rio_vrt import build_vrt

# Import formatting and validation functions for post-processing
from openforis_whisp.reformat import (
    format_stats_dataframe,
    validate_dataframe_using_lookups_flexible,
)
from openforis_whisp.advanced_stats import (
    extract_centroid_and_geomtype_client,
    join_admin_codes,
)
from openforis_whisp.stats import (
    reformat_geometry_type,
    set_point_geometry_area_to_zero,
)
from openforis_whisp.parameters.lookup_gaul1_admin import (
    lookup_dict as gaul_lookup_dict,
)
from openforis_whisp.parameters.config_runtime import (
    plot_id_column,
    geometry_column,
    geometry_area_column,
)
import sys
import warnings


# Set up module logger (consistent with advanced_stats.py)
_whisp_logger = logging.getLogger("whisp")
if not _whisp_logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setLevel(logging.DEBUG)
    _handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    _whisp_logger.addHandler(_handler)
    _whisp_logger.setLevel(logging.INFO)
    _whisp_logger.propagate = False


def _suppress_gdal_warnings():
    """
    Suppress verbose GDAL/rasterio warnings that occur during GeoTIFF processing.

    These warnings are benign - they occur because Earth Engine exports GeoTIFFs
    with many bands, which triggers TIFF metadata warnings about color channels.
    """
    # Suppress GDAL warnings via environment variable
    os.environ["CPL_LOG"] = "/dev/null"  # Unix
    os.environ["CPL_LOG"] = "NUL"  # Windows
    os.environ["GDAL_PAM_ENABLED"] = "NO"  # Disable aux.xml creation

    # Try to set GDAL error handler directly (most effective method)
    try:
        from osgeo import gdal

        gdal.SetConfigOption("CPL_LOG", "OFF")
        gdal.SetConfigOption("CPL_DEBUG", "OFF")
        gdal.PushErrorHandler("CPLQuietErrorHandler")
    except ImportError:
        pass

    # Suppress via logging (catches Python-level warnings from bindings)
    for logger_name in [
        "rasterio",
        "rasterio._io",
        "rasterio.env",
        "rasterio._filepath",
        "fiona",
        "fiona.ogrext",
        "pyogrio",
        "pyogrio._io",
        "GDAL",
        "osgeo",
        "urllib3",
        "urllib3.connectionpool",
    ]:
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)

    # Also suppress the root logger for GDAL-related messages
    # (exactextract may log through root logger)
    root_logger = logging.getLogger()

    class GDALFilter(logging.Filter):
        def filter(self, record):
            msg = str(record.getMessage())
            # Filter out GDAL/TIFF warnings
            if "CPLE_AppDefined" in msg or "TIFFReadDirectory" in msg:
                return False
            if "Photometric" in msg or "ExtraSamples" in msg:
                return False
            return True

    # Add filter to root logger and all its handlers
    root_logger.addFilter(GDALFilter())
    for handler in root_logger.handlers:
        handler.addFilter(GDALFilter())

    # Suppress Python warnings from these modules
    warnings.filterwarnings("ignore", category=UserWarning, module="rasterio")
    warnings.filterwarnings("ignore", category=UserWarning, module="fiona")
    warnings.filterwarnings("ignore", message=".*CPLE_AppDefined.*")
    warnings.filterwarnings("ignore", message=".*TIFFReadDirectory.*")
    warnings.filterwarnings("ignore", message=".*Photometric.*")
    warnings.filterwarnings("ignore", message=".*ExtraSamples.*")
    warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)


# ============================================================================
# Progress Reporting Utilities
# ============================================================================


def _format_time(seconds):
    """Format seconds into human-readable time string."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        secs = int(seconds % 60)
        return f"{minutes}m {secs}s"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def _log_progress(completed, total, start_time, step_name="items", verbose=True):
    """
    Log progress in a consistent format matching the concurrent mode.

    Args:
        completed: Number of items completed
        total: Total number of items
        start_time: Start time from time.time()
        step_name: Name for items being processed (e.g., "features", "batches")
        verbose: If True, print progress messages (default: True)
    """
    if not verbose:
        return

    logger = _whisp_logger
    percent = int((completed / total) * 100) if total > 0 else 0
    elapsed = time.time() - start_time

    # Calculate rate and ETA
    rate = completed / elapsed if elapsed > 0 else 0
    remaining = total - completed

    if rate > 0 and completed >= max(2, total * 0.05):
        eta_seconds = (remaining / rate) * 1.1  # Add 10% padding
        eta_str = _format_time(eta_seconds)
    else:
        eta_str = "calculating..."

    elapsed_str = _format_time(elapsed)

    # Build progress message (matching concurrent mode format)
    if percent < 100:
        msg = f"Progress: {completed:,}/{total:,} {step_name} ({percent}%) | Elapsed: {elapsed_str} | ETA: {eta_str}"
    else:
        msg = f"Progress: {completed:,}/{total:,} {step_name} ({percent}%) | Total time: {elapsed_str}"

    _whisp_logger.info(msg)


# ============================================================================
# GeoTIFF Download Functions
# ============================================================================


# Earth Engine download limit (empirically determined)
# EE uses ~5 bytes per band (not 4) due to GeoTIFF structure/metadata overhead
# Limit is exactly 50,331,648 bytes per the error message
EE_DOWNLOAD_LIMIT_BYTES = 50_331_648  # ~48MB exact limit from EE error messages
EE_BYTES_PER_BAND = 5.02  # Empirically measured (Float32 + GeoTIFF overhead)
EE_SAFETY_MARGIN = 0.95  # 5% margin (empirical formula is accurate)
DEFAULT_NUM_BANDS = 240  # Conservative default - band count may grow


def calculate_max_tile_size_degrees(
    num_bands: int,
    scale_meters: int = 10,
    bytes_per_band: float = None,
    latitude: float = 0.0,
) -> tuple:
    """
    Calculate the maximum tile size in degrees that stays within EE download limits.

    Accounts for latitude: 1° longitude shrinks at higher latitudes, so we can
    use larger degree values for longitude at higher latitudes.

    Args:
        num_bands: Number of bands in the image
        scale_meters: Resolution in meters (default 10m)
        bytes_per_band: Bytes per pixel per band. If None, uses empirically determined
            value (~5.02 bytes, accounts for GeoTIFF overhead).
        latitude: Latitude in degrees for longitude adjustment (default 0 = equator)

    Returns:
        Tuple of (max_lon_degrees, max_lat_degrees) for tile dimensions
    """
    if bytes_per_band is None:
        bytes_per_band = EE_BYTES_PER_BAND

    # Calculate max pixels that fit in the download limit
    max_bytes = EE_DOWNLOAD_LIMIT_BYTES * EE_SAFETY_MARGIN
    max_pixels_total = max_bytes / (num_bands * bytes_per_band)
    max_pixels_per_side = int(math.sqrt(max_pixels_total))

    # Convert pixels to meters
    max_meters = max_pixels_per_side * scale_meters

    # Convert meters to degrees
    # 1° latitude = ~111km everywhere
    # 1° longitude = ~111km * cos(latitude)
    meters_per_degree_lat = 111_000
    cos_lat = math.cos(math.radians(abs(latitude)))
    # Clamp to avoid division by zero near poles
    cos_lat = max(cos_lat, 0.1)
    meters_per_degree_lon = 111_000 * cos_lat

    max_lat_degrees = max_meters / meters_per_degree_lat
    max_lon_degrees = max_meters / meters_per_degree_lon

    return (max_lon_degrees, max_lat_degrees)


def split_bbox_into_tiles(bbox: dict, max_tile_degrees: tuple) -> list:
    """
    Split a bounding box into tiles that fit within the max size.

    Args:
        bbox: Dictionary with 'coordinates' key containing [[minx, miny], [maxx, maxy], ...] or
              a GeoJSON geometry dict
        max_tile_degrees: Tuple of (max_lon_degrees, max_lat_degrees) for tile dimensions

    Returns:
        List of tile bounding boxes as [[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]
    """
    # Handle both tuple and single float for backwards compatibility
    if isinstance(max_tile_degrees, (int, float)):
        max_lon_deg = max_lat_deg = max_tile_degrees
    else:
        max_lon_deg, max_lat_deg = max_tile_degrees

    # Extract bounds from various formats
    if isinstance(bbox, dict):
        if "coordinates" in bbox:
            coords = bbox["coordinates"][0]  # First ring of polygon
            min_x = min(c[0] for c in coords)
            max_x = max(c[0] for c in coords)
            min_y = min(c[1] for c in coords)
            max_y = max(c[1] for c in coords)
        else:
            raise ValueError(f"Unexpected bbox format: {bbox}")
    else:
        raise ValueError(f"Unexpected bbox type: {type(bbox)}")

    # Calculate bbox dimensions
    width = max_x - min_x
    height = max_y - min_y

    # Check if splitting is needed
    if width <= max_lon_deg and height <= max_lat_deg:
        # No splitting needed, return original bbox as single tile
        return [
            [
                [min_x, min_y],
                [max_x, min_y],
                [max_x, max_y],
                [min_x, max_y],
                [min_x, min_y],
            ]
        ]

    # Calculate number of tiles needed (use appropriate max for each dimension)
    num_cols = max(1, math.ceil(width / max_lon_deg))
    num_rows = max(1, math.ceil(height / max_lat_deg))

    # Calculate actual tile sizes (may be smaller than max to fit evenly)
    tile_width = width / num_cols
    tile_height = height / num_rows

    tiles = []
    for row in range(num_rows):
        for col in range(num_cols):
            tile_min_x = min_x + col * tile_width
            tile_max_x = min_x + (col + 1) * tile_width
            tile_min_y = min_y + row * tile_height
            tile_max_y = min_y + (row + 1) * tile_height

            # Create tile bbox as closed polygon coordinates
            tile_coords = [
                [tile_min_x, tile_min_y],
                [tile_max_x, tile_min_y],
                [tile_max_x, tile_max_y],
                [tile_min_x, tile_max_y],
                [tile_min_x, tile_min_y],
            ]
            tiles.append(tile_coords)

    return tiles


def _estimate_bbox_size_degrees(geometry) -> tuple:
    """
    Estimate the bounding box size of a geometry in degrees.

    Returns:
        Tuple of (width_degrees, height_degrees, center_latitude)
    """
    try:
        bbox = geometry.bounds().getInfo()
        coords = bbox["coordinates"][0]
        min_x = min(c[0] for c in coords)
        max_x = max(c[0] for c in coords)
        min_y = min(c[1] for c in coords)
        max_y = max(c[1] for c in coords)
        center_lat = (min_y + max_y) / 2
        return (max_x - min_x, max_y - min_y, center_lat)
    except Exception:
        return (0, 0, 0)


def _estimate_tiles_needed(geometry, num_bands: int = None, scale: int = 10) -> int:
    """
    Estimate number of tiles needed for a geometry.

    Returns:
        Estimated number of tiles (1 if no tiling needed)
    """
    if num_bands is None:
        num_bands = DEFAULT_NUM_BANDS
    width, height, center_lat = _estimate_bbox_size_degrees(geometry)
    max_lon_deg, max_lat_deg = calculate_max_tile_size_degrees(
        num_bands, scale, latitude=center_lat
    )

    if width <= max_lon_deg and height <= max_lat_deg:
        return 1

    num_cols = max(1, math.ceil(width / max_lon_deg))
    num_rows = max(1, math.ceil(height / max_lat_deg))
    return num_cols * num_rows


def download_geotiff_for_feature(
    ee_feature,
    image,
    output_dir,
    scale=10,
    max_retries=3,
    retry_delay=5,
    num_bands=None,
):
    """
    Download a GeoTIFF for a specific Earth Engine feature by clipping the image.

    For large bounding boxes that exceed EE's download limit, automatically splits
    into tiles and downloads each separately.

    Args:
        ee_feature: Earth Engine feature to clip the image to
        image: Earth Engine image to download (e.g., whisp.combine_datasets())
        output_dir: Directory to save the GeoTIFF
        scale: Resolution in meters (default 10m)
        max_retries: Maximum number of retry attempts for download
        retry_delay: Seconds to wait between retries
        num_bands: Number of bands in the image (for calculating max tile size).
            If None, uses conservative default (240 bands).

    Returns:
        output_path: Path to the downloaded GeoTIFF file (or list of paths if tiled)
    """
    # Get the feature ID
    try:
        internal_id = ee_feature.get("internal_id").getInfo()
        _whisp_logger.debug(f"Downloading GeoTIFF for feature {internal_id}")
    except Exception as e:
        _whisp_logger.error(f"Error getting internal_id from feature: {str(e)}")
        internal_id = f"unknown_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Create a unique filename
    filename = f"feature_{internal_id}.tif"
    output_path = output_dir / filename

    # If file already exists, don't re-download
    if output_path.exists():
        _whisp_logger.debug(f"File {filename} already exists, skipping download")
        return output_path

    # Get geometry for download
    geometry = ee_feature.geometry()

    # Set default num_bands for Whisp (conservative - band count may grow)
    if num_bands is None:
        num_bands = DEFAULT_NUM_BANDS

    # Check upfront if tiling will be needed
    estimated_tiles = _estimate_tiles_needed(geometry, num_bands, scale)

    if estimated_tiles == 1:
        # Small enough for single download - try direct
        result = _download_single_tile(
            image=image,
            geometry=geometry,
            output_path=output_path,
            scale=scale,
            max_retries=max_retries,
            retry_delay=retry_delay,
            internal_id=internal_id,
        )

        if result is not None:
            return result

        # If it failed unexpectedly, fall through to tiling
        _whisp_logger.debug(
            f"Direct download failed for feature {internal_id}, trying tiling..."
        )

    # Tiling needed (either upfront or as fallback)
    _whisp_logger.debug(
        f"Feature {internal_id} requires tiling (~{estimated_tiles} tiles estimated)"
    )

    # Get bbox geometry as GeoJSON and extract center latitude
    bbox_geojson = geometry.bounds().getInfo()
    coords = bbox_geojson["coordinates"][0]
    min_y = min(c[1] for c in coords)
    max_y = max(c[1] for c in coords)
    center_lat = (min_y + max_y) / 2

    # Calculate max tile size (latitude-aware for optimal tile count)
    max_lon_deg, max_lat_deg = calculate_max_tile_size_degrees(
        num_bands, scale, latitude=center_lat
    )
    _whisp_logger.debug(
        f"Max tile size at lat {center_lat:.1f}°: {max_lon_deg:.4f}° lon × {max_lat_deg:.4f}° lat (~{max_lon_deg * 111 * math.cos(math.radians(center_lat)):.1f}km × {max_lat_deg * 111:.1f}km)"
    )

    # Split into tiles
    tiles = split_bbox_into_tiles(bbox_geojson, (max_lon_deg, max_lat_deg))
    _whisp_logger.debug(f"Split into {len(tiles)} tiles for feature {internal_id}")

    # Download each tile
    tile_paths = []
    for tile_idx, tile_coords in enumerate(tiles):
        tile_filename = f"feature_{internal_id}_tile_{tile_idx}.tif"
        tile_path = output_dir / tile_filename

        if tile_path.exists():
            tile_paths.append(tile_path)
            continue

        # Create EE geometry for tile
        tile_geometry = ee.Geometry.Polygon([tile_coords])

        result = _download_single_tile(
            image=image,
            geometry=tile_geometry,
            output_path=tile_path,
            scale=scale,
            max_retries=max_retries,
            retry_delay=retry_delay,
            internal_id=f"{internal_id}_tile_{tile_idx}",
        )

        if result is not None:
            tile_paths.append(result)
        else:
            _whisp_logger.warning(
                f"Failed to download tile {tile_idx} for feature {internal_id}"
            )

    if not tile_paths:
        _whisp_logger.error(f"Failed to download any tiles for feature {internal_id}")
        return None

    # Return list of tile paths (VRT will mosaic them)
    return tile_paths


def _download_single_tile(
    image, geometry, output_path, scale, max_retries, retry_delay, internal_id
):
    """
    Download a single tile/region. Returns output_path on success, None on failure.

    If the download fails due to size limits, returns None (caller should try tiling).
    """
    retries = 0

    while retries < max_retries:
        try:
            # Clip the image to the geometry
            clipped_image = image.clip(geometry)

            # Generate the download URL
            _whisp_logger.debug(f"Generating download URL for {internal_id}")
            start_time = time.time()
            download_url = clipped_image.getDownloadURL(
                {
                    "format": "GeoTIFF",
                    "region": geometry,
                    "scale": scale,
                    "crs": "EPSG:4326",
                }
            )
            url_time = time.time() - start_time
            _whisp_logger.debug(f"URL generated in {url_time:.2f}s")

            # Download the image with timeout
            _whisp_logger.debug(f"Downloading to {output_path}")
            response = requests.get(download_url, timeout=300)  # 5-minute timeout

            if response.status_code == 200:
                # Check if the response is actually a GeoTIFF
                content_type = response.headers.get("Content-Type", "")
                if (
                    "tiff" in content_type.lower()
                    or "octet-stream" in content_type.lower()
                ):
                    with open(output_path, "wb") as f:
                        f.write(response.content)
                    _whisp_logger.debug(f"Successfully downloaded {output_path.name}")
                    return output_path
                else:
                    _whisp_logger.error(
                        f"Download returned non-TIFF content: {content_type}"
                    )
                    retries += 1
            else:
                _whisp_logger.error(
                    f"Failed to download (status {response.status_code}): {response.text[:200]}"
                )
                retries += 1

            # Wait before retrying
            if retries < max_retries:
                sleep_time = retry_delay * (2**retries)
                _whisp_logger.debug(
                    f"Retrying in {sleep_time}s (attempt {retries+1}/{max_retries})"
                )
                time.sleep(sleep_time)

        except ee.ee_exception.EEException as e:
            error_msg = str(e)
            # Check if this is a size limit error - don't retry, need to tile
            if (
                "request size" in error_msg.lower()
                or "must be less than" in error_msg.lower()
            ):
                _whisp_logger.debug(
                    f"Size limit exceeded for {internal_id}: {error_msg[:100]}"
                )
                return None  # Signal to caller to try tiling

            _whisp_logger.error(
                f"EE error downloading {internal_id}: {error_msg[:100]}"
            )
            retries += 1
            if retries < max_retries:
                time.sleep(retry_delay)

        except Exception as e:
            _whisp_logger.error(f"Error downloading {internal_id}: {str(e)[:100]}")
            retries += 1
            if retries < max_retries:
                time.sleep(retry_delay)

    _whisp_logger.error(f"Maximum retries reached for {internal_id}")
    return None


def download_geotiffs_for_feature_collection(
    feature_collection,
    image,
    output_dir=None,
    scale=10,
    max_features=None,
    max_workers=None,
    max_retries=3,
    retry_delay=5,
    num_bands=None,
):
    """
    Download GeoTIFFs for an entire Earth Engine FeatureCollection, with parallel processing.

    Uses ThreadPoolExecutor for parallel downloads - optimal for I/O-bound network operations.
    (ProcessPoolExecutor doesn't help here since downloads are network-bound, not CPU-bound,
    and would require slow EE re-initialization in each worker process.)

    Args:
        feature_collection: Earth Engine FeatureCollection to process
        image: Earth Engine image to clip and download
        output_dir: Directory to save the GeoTIFFs (default: ~/Downloads/whisp_features)
        scale: Resolution in meters (default 10m)
        max_features: Maximum number of features to process (default: all)
        max_workers: Maximum number of parallel workers (default: None, sequential processing)
        max_retries: Maximum number of retry attempts for each download
        retry_delay: Base delay in seconds between retries (uses exponential backoff)
        num_bands: Number of bands in the image (for tile size calculation). If None,
            uses conservative default (240) - may create extra tiles but never fails.

    Returns:
        List of paths to successfully downloaded GeoTIFF files
    """
    # Set default output directory
    if output_dir is None:
        output_dir = Path.home() / "Downloads" / "whisp_features"

    # Create directory if it doesn't exist
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Get collection size and limit if needed
    collection_size = feature_collection.size().getInfo()
    _whisp_logger.debug(
        f"Processing Earth Engine FeatureCollection with {collection_size} features"
    )

    if max_features and max_features < collection_size:
        feature_collection = feature_collection.limit(max_features)
        collection_size = max_features
        _whisp_logger.debug(f"Limited to processing first {max_features} features")

    # Get features as a list
    features = feature_collection.toList(collection_size)

    # Create a function to download a single feature given its index
    # num_bands is passed through if available, otherwise download_geotiff_for_feature uses default
    def download_feature(index):
        try:
            ee_feature = ee.Feature(features.get(index))
            return download_geotiff_for_feature(
                ee_feature=ee_feature,
                image=image,
                output_dir=output_dir,
                scale=scale,
                max_retries=max_retries,
                retry_delay=retry_delay,
                num_bands=num_bands,
            )
        except Exception as e:
            _whisp_logger.error(
                f"Error processing feature at index {index}: {str(e)}", exc_info=True
            )
            return None

    results = []
    start_time = time.time()
    completed = 0

    # Progress milestones (log at 10%, 20%, ..., 100%)
    milestones = set(range(10, 101, 10))
    shown_milestones = set()

    # Parallel processing if max_workers is specified and > 1
    # Uses ThreadPoolExecutor - threads share EE session, no re-init needed
    if max_workers and max_workers > 1:
        _whisp_logger.debug(
            f"Downloading {collection_size} features using {max_workers} parallel workers..."
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_index = {
                executor.submit(download_feature, i): i for i in range(collection_size)
            }

            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    path = future.result()
                    if path:
                        # Handle both single path and list of paths (for tiled downloads)
                        if isinstance(path, list):
                            results.extend(path)
                        else:
                            results.append(path)
                    completed += 1

                    # Log progress at milestones
                    percent = int((completed / collection_size) * 100)
                    for milestone in sorted(milestones):
                        if percent >= milestone and milestone not in shown_milestones:
                            shown_milestones.add(milestone)
                            if percent >= 100:
                                shown_milestones.add(100)  # Prevent final duplicate
                            _log_progress(
                                completed, collection_size, start_time, "downloads"
                            )
                            break

                except Exception as e:
                    completed += 1
                    _whisp_logger.error(
                        f"Exception occurred while processing feature {index+1}: {str(e)}"
                    )
    else:
        # Sequential processing
        _whisp_logger.debug(f"Downloading {collection_size} features sequentially...")
        for i in range(collection_size):
            path = download_feature(i)
            if path:
                # Handle both single path and list of paths (for tiled downloads)
                if isinstance(path, list):
                    results.extend(path)
                else:
                    results.append(path)
            completed += 1

            # Log progress at milestones
            percent = int((completed / collection_size) * 100)
            for milestone in sorted(milestones):
                if percent >= milestone and milestone not in shown_milestones:
                    shown_milestones.add(milestone)
                    if percent >= 100:
                        shown_milestones.add(100)  # Prevent final duplicate
                    _log_progress(completed, collection_size, start_time, "downloads")
                    break

    # Final progress report - use completed count (features) not results length (may include tiles)
    if 100 not in shown_milestones:
        _log_progress(completed, collection_size, start_time, "downloads")
    return results


# ============================================================================
# Bounding Box Manipulation Functions
# ============================================================================


def extend_bbox(minx, miny, maxx, maxy, extension_distance=None, extension_range=None):
    """
    Extends a bounding box by a fixed distance or a random distance within a range.

    Args:
        minx, miny, maxx, maxy: The original bounding box coordinates
        extension_distance: Fixed distance to extend in all directions
        extension_range: List [min_dist, max_dist] for random extension

    Returns:
        Tuple of (minx, miny, maxx, maxy) for the extended bounding box
    """
    if extension_distance is None and extension_range is None:
        return minx, miny, maxx, maxy

    # Determine the extension distance
    if extension_range is not None:
        min_dist, max_dist = extension_range
        dist = random.uniform(min_dist, max_dist)
    else:
        dist = extension_distance

    # Extend the bounding box
    extended_minx = minx - dist
    extended_miny = miny - dist
    extended_maxx = maxx + dist
    extended_maxy = maxy + dist

    return extended_minx, extended_miny, extended_maxx, extended_maxy


def shift_bbox(minx, miny, maxx, maxy, max_shift_distance, pixel_length=0.0001):
    """
    Shifts a bounding box in a random direction within max_shift_distance.

    Args:
        minx, miny, maxx, maxy: The bounding box coordinates
        max_shift_distance: Maximum distance to shift
        pixel_length: Length of a pixel to avoid accuracy loss

    Returns:
        Tuple of (minx, miny, maxx, maxy) for the shifted bounding box
    """
    if max_shift_distance <= 0:
        return minx, miny, maxx, maxy

    # Calculate the effective max shift (max_shift - pixel_length)
    effective_max_shift = max(0, max_shift_distance - pixel_length)

    # Random shift distance (less than effective_max_shift)
    shift_distance = random.uniform(0, effective_max_shift)

    # Random angle in radians
    angle = random.uniform(0, 2 * math.pi)

    # Calculate shift components
    dx = shift_distance * math.cos(angle)
    dy = shift_distance * math.sin(angle)

    # Apply shift
    shifted_minx = minx + dx
    shifted_miny = miny + dy
    shifted_maxx = maxx + dx
    shifted_maxy = maxy + dy

    return shifted_minx, shifted_miny, shifted_maxx, shifted_maxy


# ============================================================================
# Obscuration Functions
# ============================================================================


def ee_featurecollection_to_gdf(fc):
    """
    Convert an Earth Engine FeatureCollection to a GeoPandas GeoDataFrame.
    """
    geojson = fc.getInfo()
    gdf = gpd.GeoDataFrame.from_features(geojson["features"])
    return gdf


def generate_random_box_geometries(gdf, max_distance, proportion=0.5):
    """
    Generates random geometries near the original features in a GeoDataFrame.
    Each random geometry is placed within the specified distance of a randomly selected
    existing feature, rather than anywhere within the overall extent.

    Args:
        gdf: GeoDataFrame with the original features
        max_distance: Maximum distance from original features
        proportion: Proportion of extra geometries to create (relative to original count)

    Returns:
        List of Earth Engine features with random geometries
    """
    if proportion <= 0 or max_distance <= 0 or len(gdf) == 0:
        return []

    random_features = []

    # Get the dimensions and centroids from original features
    feature_info = []

    for idx, row in gdf.iterrows():
        minx, miny, maxx, maxy = row.geometry.bounds
        width = maxx - minx
        height = maxy - miny
        centroid_x = (minx + maxx) / 2
        centroid_y = (miny + maxy) / 2
        feature_info.append(
            {
                "width": width,
                "height": height,
                "center_x": centroid_x,
                "center_y": centroid_y,
                "bounds": (minx, miny, maxx, maxy),
            }
        )

    # Calculate number of random features to create
    num_random_features = max(1, int(len(gdf) * proportion))

    # Generate random features
    for i in range(num_random_features):
        # Select a random original feature to be near
        random_feature_idx = random.randint(0, len(feature_info) - 1)
        selected_feature = feature_info[random_feature_idx]

        # Get the original feature's dimensions
        width = selected_feature["width"]
        height = selected_feature["height"]
        orig_x = selected_feature["center_x"]
        orig_y = selected_feature["center_y"]

        # Add some variation to dimensions (± 20%)
        width_variation = random.uniform(1, 5)
        height_variation = random.uniform(1, 5)
        width *= width_variation
        height *= height_variation

        # Generate a random position within max_distance of the selected feature
        angle = random.uniform(0, 2 * math.pi)
        distance = random.uniform(0, max_distance)

        # Calculate the new center point
        center_x = orig_x + (distance * math.cos(angle))
        center_y = orig_y + (distance * math.sin(angle))

        # Calculate corners for the random rectangle
        r_minx = center_x - (width / 2)
        r_miny = center_y - (height / 2)
        r_maxx = center_x + (width / 2)
        r_maxy = center_y + (height / 2)

        # Create Earth Engine Rectangle geometry
        ee_geometry = ee.Geometry.Rectangle([r_minx, r_miny, r_maxx, r_maxy])

        # Create random properties
        properties = {
            "random_feature": True,
            "internal_id": f"random_{i + 1000}",
            "obscured": True,
            "near_feature_id": random_feature_idx + 1,
        }

        # Create an Earth Engine feature
        ee_feature = ee.Feature(ee_geometry, properties)
        random_features.append(ee_feature)

    return random_features


def convert_geojson_to_ee_bbox_obscured(
    geojson_filepath,
    extension_distance=None,
    extension_range=None,
    shift_geometries=False,
    shift_proportion=0.5,
    pixel_length=0.0001,
    add_random_features=False,
    max_distance=0.1,
    random_proportion=0.5,
    verbose=True,
) -> ee.FeatureCollection:
    """
    Reads a GeoJSON file, creates bounding boxes for each feature,
    and converts to Earth Engine FeatureCollection with options to obscure locations.

    This function provides privacy-preserving transformations:
    - Extension: Expand bounding boxes by fixed or random amounts
    - Shifting: Move bounding boxes in random directions
    - Decoy features: Add random fake features to hide true locations

    Args:
        geojson_filepath (str or Path): The filepath to the GeoJSON file
        extension_distance (float): Fixed distance to extend bounding boxes
        extension_range (list): [min_dist, max_dist] for random extension
        shift_geometries (bool): Whether to shift bounding boxes randomly
        shift_proportion (float): How much of extension can be used for shifting (0-1)
        pixel_length (float): Length of a pixel to avoid accuracy loss
        add_random_features (bool): Whether to add random decoy features
        max_distance (float): Maximum distance for random features
        random_proportion (float): Proportion of random features to add
        verbose (bool): If True, print progress messages (default: True)

    Returns:
        ee.FeatureCollection: Earth Engine FeatureCollection of bounding boxes
    """
    logger = _whisp_logger

    # Read the GeoJSON file using geopandas
    if isinstance(geojson_filepath, (str, Path)):
        file_path = os.path.abspath(geojson_filepath)
        if verbose:
            _whisp_logger.debug(f"Reading GeoJSON file from: {file_path}")

        try:
            gdf = gpd.read_file(file_path)
        except Exception as e:
            raise ValueError(f"Error reading GeoJSON file: {str(e)}")
    else:
        raise ValueError("Input must be a file path (str or Path)")

    # Check if GeoDataFrame is empty
    if len(gdf) == 0:
        raise ValueError("GeoJSON contains no features")

    # Add internal_id if not present
    if "internal_id" not in gdf.columns:
        gdf["internal_id"] = range(1, len(gdf) + 1)

    # Create a new list with bounding boxes
    bbox_features = []

    # Validate shift_proportion to be between 0 and 1
    shift_proportion = max(0, min(1, shift_proportion))

    for idx, row in gdf.iterrows():
        try:
            # Get the bounds of the geometry (minx, miny, maxx, maxy)
            minx, miny, maxx, maxy = row.geometry.bounds

            # Apply bounding box extension if requested
            if extension_distance is not None or extension_range is not None:
                minx, miny, maxx, maxy = extend_bbox(
                    minx,
                    miny,
                    maxx,
                    maxy,
                    extension_distance=extension_distance,
                    extension_range=extension_range,
                )

            # Apply random shift if requested
            if shift_geometries:
                max_shift = 0
                if extension_distance is not None:
                    max_shift = extension_distance * shift_proportion
                elif extension_range is not None:
                    max_shift = extension_range[1] * shift_proportion

                if max_shift > 0:
                    minx, miny, maxx, maxy = shift_bbox(
                        minx, miny, maxx, maxy, max_shift, pixel_length
                    )
                else:
                    if verbose:
                        _whisp_logger.debug(
                            f"No shifting applied to feature {idx} due to missing extension parameters"
                        )

            # Create an Earth Engine Rectangle geometry
            ee_geometry = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

            # Copy properties from the original feature
            properties = {col: row[col] for col in gdf.columns if col != "geometry"}

            # Convert numpy types to native Python types for proper serialization
            for key, value in properties.items():
                if hasattr(value, "item"):
                    properties[key] = value.item()
                elif pd.isna(value):
                    properties[key] = None

            # Create an Earth Engine feature with the bbox geometry
            ee_feature = ee.Feature(ee_geometry, properties)
            bbox_features.append(ee_feature)

        except Exception as e:
            _whisp_logger.error(f"Error processing feature {idx}: {str(e)}")

    # Check if any features were created
    if not bbox_features:
        raise ValueError("No valid features found in GeoJSON")

    # Add random decoy features if requested
    if add_random_features:
        random_features = generate_random_box_geometries(
            gdf, max_distance, random_proportion
        )

        if random_features:
            bbox_features.extend(random_features)
            if verbose:
                _whisp_logger.debug(
                    f"Added {len(random_features)} random decoy features to obscure real locations"
                )

    # Create the Earth Engine FeatureCollection
    feature_collection = ee.FeatureCollection(bbox_features)
    if verbose:
        _whisp_logger.debug(
            f"Created Earth Engine FeatureCollection with {len(bbox_features)} bounding box features"
        )

    return feature_collection


# ============================================================================
# VRT and Local Processing Functions
# ============================================================================


def create_vrt_from_folder(folder_path, exclude_pattern="random", verbose=True):
    """
    Create a virtual raster (VRT) file from all TIF files in a folder,
    excluding any files with the specified pattern in the filename.

    Args:
        folder_path (str or Path): Path to the folder containing TIF files
        exclude_pattern (str): String pattern to exclude from filenames (default: "random")
        verbose (bool): If True, print progress messages (default: True)

    Returns:
        str: Path to the created VRT file
    """
    logger = _whisp_logger
    folder = Path(folder_path)

    # Get list of TIFF files, excluding those with the exclude_pattern in the filename
    tif_files = []
    for ext in ["*.tif", "*.tiff"]:
        for file_path in folder.glob(ext):
            if exclude_pattern.lower() not in file_path.name.lower():
                tif_files.append(str(file_path))

    # Check if any files were found
    if not tif_files:
        _whisp_logger.warning(f"No suitable TIF files found in {folder_path}")
        return None

    if verbose:
        _whisp_logger.debug(f"Found {len(tif_files)} TIF files to include in the VRT")

    # Output VRT path
    output_vrt = str(folder / "combined_rasters.vrt")

    # Handle single file case - rio_vrt.build_vrt has issues with single file
    if len(tif_files) == 1:
        # For single file, just return the path directly (no VRT needed)
        if verbose:
            _whisp_logger.debug(f"Single TIF file, skipping VRT creation")
        return tif_files[0]

    # Create the VRT file for multiple files
    build_vrt(output_vrt, tif_files)

    if verbose:
        _whisp_logger.debug(f"VRT file created at: {output_vrt}")
    return output_vrt


# ============================================================================
# Parallel Zonal Statistics Functions
# ============================================================================


def get_band_names_from_raster(raster_path):
    """
    Get band names/descriptions from a raster file.

    Args:
        raster_path: Path to raster file (GeoTIFF or VRT)

    Returns:
        list: Band names/descriptions, or None if not available
    """
    try:
        import rasterio

        with rasterio.open(raster_path) as src:
            # Try to get band descriptions
            descriptions = list(src.descriptions)
            # Filter out None values and check if we have meaningful names
            if descriptions and any(d is not None for d in descriptions):
                return [d if d else f"band_{i+1}" for i, d in enumerate(descriptions)]
            return None
    except Exception as e:
        _whisp_logger.debug(f"Could not read band names from raster: {e}")
        return None


def rename_exactextract_columns(df, band_names, ops=None):
    """
    Rename exactextract output columns to use band names instead of band numbers.

    exactextract outputs columns like "band_1_sum", "band_2_sum", etc.
    This function renames them to "{band_name}_sum" format.

    Args:
        df: DataFrame with exactextract output
        band_names: List of band names in order
        ops: List of operations used (default: ['sum'])

    Returns:
        DataFrame with renamed columns
    """
    if ops is None:
        ops = ["sum"]

    if band_names is None or len(band_names) == 0:
        _whisp_logger.debug("band_names is None or empty, skipping rename")
        return df

    # Build rename mapping
    rename_map = {}
    for i, band_name in enumerate(band_names):
        band_num = i + 1
        for op in ops:
            old_col = f"band_{band_num}_{op}"
            new_col = f"{band_name}_{op}"
            if old_col in df.columns:
                rename_map[old_col] = new_col

    if rename_map:
        _whisp_logger.debug(f"Renamed {len(rename_map)} columns to use band names")
        df = df.rename(columns=rename_map)
    else:
        # Debug: show what columns exist vs what we expected
        band_cols = [c for c in df.columns if c.startswith("band_")]
        _whisp_logger.debug(
            f"No columns matched for renaming. Found {len(band_cols)} band columns: {band_cols[:5]}..."
        )

    return df


def _process_chunk_df(chunk_gdf, rasters, ops, chunk_idx, num_chunks):
    """
    Process a single chunk of features for exact_extract.
    This is a helper function for parallel processing.
    """
    from exactextract import exact_extract

    # Suppress GDAL warnings in this worker process
    _suppress_gdal_warnings()

    chunk_start_time = time.time()
    try:
        chunk_results = exact_extract(
            progress=False, rast=rasters, vec=chunk_gdf, ops=ops, output="pandas"
        )
        gc.collect()
        return chunk_results
    except Exception as e:
        _whisp_logger.error(
            f"Error processing chunk {chunk_idx+1}/{num_chunks}: {str(e)}"
        )
        return None
    finally:
        gc.collect()


def exact_extract_in_chunks_parallel(
    rasters,
    vector_file,
    chunk_size=25,
    ops=None,
    max_workers=4,
    band_names=None,
    verbose=True,
):
    """
    Process exactextract in parallel chunks of features.

    This function splits a vector file into chunks and processes them
    in parallel using multiple processes for improved performance.

    Args:
        rasters: List of raster files or single raster path (e.g., VRT file)
        vector_file: Path to vector file (GeoJSON, shapefile, etc.)
        chunk_size: Number of features to process in each chunk
        ops: List of operations to perform (default: ["sum"])
        max_workers: Maximum number of parallel processes to use
        band_names: List of band names for column renaming (default: auto-detect from raster)
        verbose: If True, print progress messages (default: True)

    Returns:
        pd.DataFrame: Combined results from all chunks
    """
    logger = _whisp_logger

    if ops is None:
        ops = ["sum"]

    # Suppress GDAL warnings (TIFF metadata issues are common but harmless)
    _suppress_gdal_warnings()

    # Configure GDAL for multithreaded environment
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif,.vrt"
    os.environ["VSI_CACHE"] = "TRUE"
    os.environ["VSI_CACHE_SIZE"] = "50000000"  # 50MB cache

    # Try to get band names from raster if not provided
    if band_names is None:
        raster_path = rasters if isinstance(rasters, str) else rasters[0]
        band_names = get_band_names_from_raster(raster_path)
        if band_names and verbose:
            _whisp_logger.debug(
                f"Auto-detected {len(band_names)} band names from raster"
            )
    else:
        if verbose:
            _whisp_logger.debug(f"Using {len(band_names)} provided band names")

    start_time = time.time()
    gdf = gpd.read_file(vector_file)
    total_features = len(gdf)
    num_chunks = (total_features + chunk_size - 1) // chunk_size

    # Log processing info
    _whisp_logger.info(
        f"Processing {total_features:,} features in {num_chunks} batches (local mode)..."
    )

    # Split the GeoDataFrame into chunks to avoid pickling the whole gdf
    chunks = [
        gdf.iloc[i * chunk_size : min((i + 1) * chunk_size, total_features)].copy()
        for i in range(num_chunks)
    ]

    # Progress tracking
    milestones = set(range(10, 101, 10))
    shown_milestones = set()
    completed = 0

    all_results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_process_chunk_df, chunk, rasters, ops, i, num_chunks)
            for i, chunk in enumerate(chunks)
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result is not None and not result.empty:
                    all_results.append(result)
                completed += 1

                # Log progress at milestones
                percent = int((completed / num_chunks) * 100)
                for milestone in sorted(milestones):
                    if percent >= milestone and milestone not in shown_milestones:
                        shown_milestones.add(milestone)
                        if percent >= 100:
                            shown_milestones.add(100)  # Prevent final duplicate
                        if verbose:
                            _log_progress(
                                completed,
                                num_chunks,
                                start_time,
                                "batches",
                                verbose=verbose,
                            )
                        break

            except Exception as e:
                completed += 1
                _whisp_logger.error(f"Exception in chunk: {str(e)}")

    # Final progress only if 100% wasn't already shown
    if 100 not in shown_milestones and verbose:
        _log_progress(num_chunks, num_chunks, start_time, "batches", verbose=verbose)

    gdf = None
    combined_df = pd.DataFrame()
    if all_results:
        combined_df = pd.concat(all_results, ignore_index=True)
        all_results = []
        gc.collect()

        # Rename columns to use band names if available
        if band_names:
            if verbose:
                _whisp_logger.debug(
                    f"Renaming columns using {len(band_names)} band names..."
                )
            combined_df = rename_exactextract_columns(combined_df, band_names, ops)
        else:
            if verbose:
                _whisp_logger.debug("No band names available for column renaming")

    processed_count = len(combined_df) if not combined_df.empty else 0
    if verbose:
        _whisp_logger.debug(
            f"Extracted statistics for {processed_count}/{total_features} features"
        )
    return combined_df


# ============================================================================
# Main Workflow Functions
# ============================================================================


def whisp_stats_local(
    input_geojson_filepath,
    output_dir,
    image=None,
    extension_range=(0.002, 0.005),
    shift_geometries=True,
    shift_proportion=0.5,
    pixel_length=0.0002,
    add_random_features=False,
    max_distance=0.07,
    random_proportion=0.25,
    scale=10,
    max_download_workers=10,
    max_extract_workers=None,
    chunk_size=25,
    ops=None,
    cleanup_files=True,
    id_column="internal_id",
    include_context_bands=True,
    national_codes=None,
    unit_type="ha",
    decimal_places=3,
    external_id_column=None,
    custom_bands=None,
    # Format parameters (matching concurrent mode)
    remove_median_columns=True,
    convert_water_flag=True,
    water_flag_threshold=0.5,
    sort_column="plotId",
    geometry_audit_trail=False,
    verbose=True,
):
    """
    Privacy-preserving local statistics processing for Whisp.

    This function provides a complete workflow for processing GeoJSON features
    locally while obscuring exact plot locations. It chains together:
    1. Creating obscured bounding boxes with optional decoys
    2. Parallel downloading of GeoTIFF data from Earth Engine
    3. Creating a VRT mosaic of downloaded files
    4. Parallel local zonal statistics extraction
    5. Post-processing to match concurrent mode output format
    6. Optional cleanup of temporary files

    Args:
        input_geojson_filepath: Path to input GeoJSON file with features to process
        output_dir: Directory for downloaded GeoTIFFs and intermediate files
        image: Earth Engine image to use (default: combine_datasets())
        extension_range: Tuple of (min, max) degrees to extend bounding boxes
        shift_geometries: Whether to randomly shift bounding boxes
        shift_proportion: Proportion of features to shift (0-1)
        pixel_length: Grid alignment in degrees (~10m at equator = 0.0001)
        add_random_features: Whether to add decoy features
        max_distance: Maximum distance for random decoy features (degrees)
        random_proportion: Proportion of decoys relative to original features
        scale: Download resolution in meters (default 10m)
        max_download_workers: Number of parallel download threads (default: 10, higher
            values may trigger EE rate limits)
        max_extract_workers: Number of parallel extract processes (default: CPU count - 1)
        chunk_size: Features per chunk for parallel extraction
        ops: Statistics operations for exactextract (default: ['sum', 'median'] to match
            concurrent mode's combined reducer). The sum is used for area statistics,
            while median is used for admin_code lookup.
        cleanup_files: Whether to delete GeoTIFFs/VRT after processing
        id_column: Column name for feature IDs in output
        include_context_bands: Whether to include context bands (admin_code, water_flag)
            in the image (default: True)
        national_codes: List of ISO2 country codes to include national datasets
        unit_type: Output unit type - 'ha' (hectares) or 'percent' (default: 'ha')
        decimal_places: Decimal places for rounding (default: 3)
        external_id_column: Column name in input GeoJSON to preserve as external_id
        custom_bands: Custom band handling for validation (None=strict, list=preserve specified)
        remove_median_columns: Whether to remove '_median' columns after processing (default: True)
        convert_water_flag: Whether to convert water flag to boolean (default: True)
        water_flag_threshold: Threshold for water flag ratio (default: 0.5)
        sort_column: Column to sort output by (default: 'plotId', None to skip)
        geometry_audit_trail: If True, includes geo_original column with input geometry (default: False)
        verbose: If True, print progress messages (default: True). Set to False for quiet mode.

    Returns:
        pandas.DataFrame: Formatted zonal statistics matching concurrent mode output
    """
    from openforis_whisp.datasets import combine_datasets

    # Set up logger based on verbose flag
    logger = _whisp_logger
    if not verbose:
        # Suppress output by setting level to WARNING (only errors/warnings shown)
        _whisp_logger.setLevel(logging.WARNING)
    else:
        _whisp_logger.setLevel(logging.INFO)

    # Suppress GDAL/rasterio warnings (TIFF metadata issues are common but harmless)
    _suppress_gdal_warnings()

    # Default to both sum and median operations to match concurrent mode
    # sum: used for area statistics (converted to ha/percent)
    # median: used for admin_code lookup (categorical band needs mode/median, not sum)
    if ops is None:
        ops = ["sum", "median"]

    if max_extract_workers is None:
        max_extract_workers = max(1, os.cpu_count() - 1)

    # Ensure output directory exists
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    _whisp_logger.info("Mode: local")
    start_time = time.time()

    # Load GeoJSON to get feature count for progress reporting
    gdf = gpd.read_file(input_geojson_filepath)
    num_features = len(gdf)
    _whisp_logger.debug(f"Loaded {num_features} features")

    # Step 1: Create obscured bounding boxes
    _whisp_logger.debug("Creating obscured bounding boxes...")
    obscured_collection = convert_geojson_to_ee_bbox_obscured(
        input_geojson_filepath,
        extension_range=extension_range,
        shift_geometries=shift_geometries,
        shift_proportion=shift_proportion,
        pixel_length=pixel_length,
        add_random_features=add_random_features,
        max_distance=max_distance,
        random_proportion=random_proportion,
        verbose=verbose,
    )

    # Step 2: Download GeoTIFFs in parallel
    _whisp_logger.debug("Downloading GeoTIFF data from Earth Engine...")
    if image is None:
        image = combine_datasets(
            national_codes=national_codes,
            include_context_bands=include_context_bands,
        )

    # Get band names from the EE image (single getInfo call for both naming and tile size calculation)
    band_names = image.bandNames().getInfo()
    num_bands = len(band_names)
    _whisp_logger.debug(f"Image has {num_bands} bands")

    geotiff_paths = download_geotiffs_for_feature_collection(
        feature_collection=obscured_collection,
        output_dir=output_dir,
        image=image,
        scale=scale,
        max_workers=max_download_workers,
        num_bands=num_bands,  # Pass for accurate tile size calculation
    )

    if not geotiff_paths:
        raise RuntimeError("No GeoTIFF files were downloaded successfully")

    _whisp_logger.debug(f"Downloaded {len(geotiff_paths)} GeoTIFF files")

    # Step 3: Create VRT mosaic
    _whisp_logger.debug("Creating VRT mosaic...")
    vrt_path = create_vrt_from_folder(output_dir, verbose=verbose)
    _whisp_logger.debug(f"VRT created: {vrt_path}")

    # Step 4: Run parallel local zonal statistics
    _whisp_logger.debug("Running local zonal statistics...")
    stats_df = exact_extract_in_chunks_parallel(
        rasters=vrt_path,
        vector_file=input_geojson_filepath,
        chunk_size=chunk_size,
        ops=ops,
        max_workers=max_extract_workers,
        band_names=band_names,  # Pass band names for column renaming
        verbose=verbose,
    )

    # Cleanup temporary files if requested
    if cleanup_files:
        _whisp_logger.debug("Cleaning up temporary files...")
        delete_all_files_in_folder(output_dir, "*.tif", verbose=verbose)
        delete_all_files_in_folder(output_dir, "*.vrt", verbose=verbose)

    # ========================================================================
    # Post-processing to match concurrent mode output format
    # ========================================================================
    _whisp_logger.debug("Formatting output...")

    # Step 5a: Add plotId (1-indexed, same as concurrent mode)
    stats_df[plot_id_column] = [str(i) for i in range(1, len(stats_df) + 1)]

    # Step 5b: Extract centroid and geometry type from original GeoJSON (client-side)
    # Reload gdf (fresh, without modifications from earlier in the function)
    gdf = gpd.read_file(input_geojson_filepath)

    # Rename external_id column early (matching concurrent mode's _load_and_prepare_geojson)
    if external_id_column and external_id_column in gdf.columns:
        if external_id_column != "external_id":
            gdf = gdf.rename(columns={external_id_column: "external_id"})
        gdf["external_id"] = gdf["external_id"].astype(str)

    df_metadata = extract_centroid_and_geomtype_client(
        gdf, external_id_column=external_id_column, return_attributes_only=True
    )
    # Add plotId to metadata for merging
    df_metadata[plot_id_column] = [str(i) for i in range(1, len(df_metadata) + 1)]

    # Merge centroid/geometry metadata on plotId (includes external_id if present)
    stats_df = stats_df.merge(df_metadata, on=plot_id_column, how="left")

    # Ensure external_id column exists even if not provided
    if "external_id" not in stats_df.columns:
        stats_df["external_id"] = None

    # Step 5d: Add geometry column (GeoJSON string)
    stats_df[geometry_column] = gdf.geometry.apply(
        lambda g: json.dumps(mapping(g)) if g else None
    ).values

    # Step 5e: Join admin codes using the lookup dictionary
    # With ops=['sum', 'median'], admin_code_median exists (median is appropriate for categorical codes)
    # Fall back to admin_code_sum if median wasn't computed
    admin_col = (
        "admin_code_median"
        if "admin_code_median" in stats_df.columns
        else "admin_code_sum"
    )
    if admin_col in stats_df.columns:
        if admin_col != "admin_code_median":
            # Rename to match expected column name for join
            stats_df["admin_code_median"] = stats_df[admin_col]
        stats_df = join_admin_codes(
            stats_df, gaul_lookup_dict, id_col="admin_code_median"
        )

    # Step 5f: Format stats (unit conversion, strip _sum suffix, convert water flag)
    area_col = f"{geometry_area_column}_sum"  # "Area_sum"
    stats_df = format_stats_dataframe(
        df=stats_df,
        area_col=area_col,
        decimal_places=decimal_places,
        unit_type=unit_type,
        remove_columns=remove_median_columns,
        convert_water_flag=convert_water_flag,
        water_flag_threshold=water_flag_threshold,
        sort_column=sort_column,
    )

    # Step 5f-2: Reformat geometry type (MultiPolygon -> Polygon) and handle point areas
    try:
        stats_df = reformat_geometry_type(stats_df)
    except Exception as e:
        _whisp_logger.warning(f"Error reformatting geometry type: {e}")

    try:
        stats_df = set_point_geometry_area_to_zero(stats_df)
    except Exception as e:
        _whisp_logger.warning(f"Error setting point geometry area to zero: {e}")

    # Step 5g: Schema validation (reorder columns, type coercion)
    stats_df = validate_dataframe_using_lookups_flexible(
        df_stats=stats_df,
        national_codes=national_codes,
        custom_bands=custom_bands,
    )

    # Step 5g-2: Add geometry audit trail if requested (AFTER validation to preserve columns)
    if geometry_audit_trail:
        # Add original input geometry column for audit/compliance
        # Use pd.concat to avoid PerformanceWarning from DataFrame fragmentation
        geo_original_series = pd.Series(
            gdf.geometry.apply(lambda g: json.dumps(mapping(g)) if g else None).values,
            name="geo_original",
        )
        stats_df = pd.concat([stats_df, geo_original_series], axis=1)
        logger.info("Audit trail added: geo_original column")

    # Step 5h: Add processing metadata column (same format as concurrent mode)
    metadata_dict = {
        "whisp_version": get_version("openforis-whisp"),
        "processing_timestamp_utc": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S%z"
        ),
    }
    metadata_series = pd.Series(
        [metadata_dict] * len(stats_df), name="whisp_processing_metadata"
    )
    stats_df = pd.concat([stats_df, metadata_series], axis=1)

    total_time = time.time() - start_time
    _whisp_logger.info(
        f"Processing complete: {len(stats_df):,} features in {_format_time(total_time)}"
    )

    return stats_df


# ============================================================================
# Test Data Generation Functions
# ============================================================================


def generate_random_polygon(
    min_lon, min_lat, max_lon, max_lat, min_area_ha=1, max_area_ha=10, vertex_count=20
):
    """
    Generate a random polygon within bounds with approximate area in the specified range.
    Uses a robust approach that works well with high vertex counts.

    Args:
        min_lon, min_lat, max_lon, max_lat: Boundary coordinates
        min_area_ha: Minimum area in hectares
        max_area_ha: Maximum area in hectares
        vertex_count: Number of vertices for the polygon

    Returns:
        Tuple of (polygon, actual_area_ha)
    """
    poly = None
    actual_area_ha = 0

    def approximate_area_ha(polygon, center_lat):
        """Approximate area in hectares (much faster than geodesic)."""
        area_sq_degrees = polygon.area
        lat_factor = 111320  # meters per degree latitude
        lon_factor = 111320 * math.cos(math.radians(center_lat))
        return area_sq_degrees * lat_factor * lon_factor / 10000

    target_area_ha = random.uniform(min_area_ha, max_area_ha)
    center_lon = random.uniform(min_lon, max_lon)
    center_lat = random.uniform(min_lat, max_lat)

    # Initial size estimate (in degrees)
    initial_radius = math.sqrt(target_area_ha / (math.pi * 100)) * 0.01

    # Cap vertex count for stability
    effective_vertex_count = min(vertex_count, 100)

    # Primary approach: Create polygon using convex hull approach
    for attempt in range(5):
        try:
            thetas = np.linspace(0, 2 * math.pi, effective_vertex_count, endpoint=False)
            angle_randomness = min(0.2, 2.0 / effective_vertex_count)
            thetas += np.random.uniform(
                -angle_randomness, angle_randomness, size=effective_vertex_count
            )

            distance_factor = min(0.3, 3.0 / effective_vertex_count) + 0.7
            distances = initial_radius * np.random.uniform(
                1.0 - distance_factor / 2,
                1.0 + distance_factor / 2,
                size=effective_vertex_count,
            )

            xs = center_lon + distances * np.cos(thetas)
            ys = center_lat + distances * np.sin(thetas)

            xs = np.clip(xs, min_lon, max_lon)
            ys = np.clip(ys, min_lat, max_lat)

            vertices = list(zip(xs, ys))
            if vertices[0] != vertices[-1]:
                vertices.append(vertices[0])

            poly = Polygon(vertices)

            if not poly.is_valid:
                poly = make_valid(poly)
                if poly.geom_type != "Polygon":
                    continue

            actual_area_ha = approximate_area_ha(poly, center_lat)

            if min_area_ha * 0.8 <= actual_area_ha <= max_area_ha * 1.2:
                return poly, actual_area_ha

            if actual_area_ha > 0:
                scale_factor = math.sqrt(target_area_ha / actual_area_ha)
                initial_radius *= scale_factor

        except Exception as e:
            print(f"Error in convex hull method (attempt {attempt+1}): {e}")

    # Fallback: Star-like pattern
    for attempt in range(5):
        try:
            star_vertex_count = min(15, vertex_count)
            vertices = []

            for i in range(star_vertex_count):
                angle = 2 * math.pi * i / star_vertex_count
                if i % 2 == 0:
                    distance = initial_radius * random.uniform(0.7, 0.9)
                else:
                    distance = initial_radius * random.uniform(0.5, 0.6)
                angle += random.uniform(-0.1, 0.1)

                lon = center_lon + distance * math.cos(angle)
                lat = center_lat + distance * math.sin(angle)
                lon = min(max(lon, min_lon), max_lon)
                lat = min(max(lat, min_lat), max_lat)
                vertices.append((lon, lat))

            vertices.append(vertices[0])
            poly = Polygon(vertices)
            if not poly.is_valid:
                poly = make_valid(poly)
                if poly.geom_type != "Polygon":
                    continue

            actual_area_ha = approximate_area_ha(poly, center_lat)
            if actual_area_ha > 0:
                return poly, actual_area_ha

        except Exception as e:
            print(f"Error in star pattern method (attempt {attempt+1}): {e}")

    # Last resort - perturbed circle
    try:
        final_vertices = []
        for i in range(8):
            angle = 2 * math.pi * i / 8
            distance = initial_radius * random.uniform(0.95, 1.05)
            lon = center_lon + distance * math.cos(angle)
            lat = center_lat + distance * math.sin(angle)
            lon = min(max(lon, min_lon), max_lon)
            lat = min(max(lat, min_lat), max_lat)
            final_vertices.append((lon, lat))

        final_vertices.append(final_vertices[0])
        poly = Polygon(final_vertices)
        if not poly.is_valid:
            poly = make_valid(poly)
        actual_area_ha = approximate_area_ha(poly, center_lat)

    except Exception as e:
        print(f"Error in final fallback method: {e}")
        offset = initial_radius / 2
        poly = Polygon(
            [
                (center_lon, center_lat + offset),
                (center_lon + offset, center_lat - offset),
                (center_lon - offset, center_lat - offset),
                (center_lon, center_lat + offset),
            ]
        )
        actual_area_ha = approximate_area_ha(poly, center_lat)

    return poly, actual_area_ha


def create_geojson(
    bounds,
    num_polygons=25,
    min_area_ha=1,
    max_area_ha=10,
    min_number_vert=10,
    max_number_vert=20,
):
    """
    Create a GeoJSON FeatureCollection with random polygons within specified bounds.

    Args:
        bounds: List of [min_lon, min_lat, max_lon, max_lat]
        num_polygons: Number of polygons to generate
        min_area_ha: Minimum area in hectares
        max_area_ha: Maximum area in hectares
        min_number_vert: Minimum number of vertices per polygon
        max_number_vert: Maximum number of vertices per polygon

    Returns:
        dict: GeoJSON FeatureCollection
    """
    min_lon, min_lat, max_lon, max_lat = bounds

    features = []
    for i in range(num_polygons):
        vertices = random.randint(min_number_vert, max_number_vert)

        polygon, actual_area = generate_random_polygon(
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            min_area_ha=min_area_ha,
            max_area_ha=max_area_ha,
            vertex_count=vertices,
        )

        properties = {"internal_id": i + 1}
        feature = {
            "type": "Feature",
            "properties": properties,
            "geometry": mapping(polygon),
        }
        features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}
    return geojson


def reformat_geojson_properties(
    geojson_path,
    output_path=None,
    id_field="internal_id",
    start_index=1,
    remove_properties=False,
    add_uuid=False,
):
    """
    Add numeric IDs to features in an existing GeoJSON file and optionally remove properties.

    Args:
        geojson_path: Path to input GeoJSON file
        output_path: Path to save the output GeoJSON (if None, overwrites input)
        id_field: Name of the ID field to add
        start_index: Starting index for sequential IDs
        remove_properties: Whether to remove all existing properties (default: False)
        add_uuid: Whether to also add UUID field

    Returns:
        None
    """
    gdf = gpd.read_file(geojson_path)

    if remove_properties:
        gdf = gdf[["geometry"]].copy()

    gdf[id_field] = [i + start_index for i in range(len(gdf))]

    if add_uuid:
        gdf["uuid"] = [str(uuid.uuid4()) for _ in range(len(gdf))]

    output_path = output_path or geojson_path
    gdf.to_file(output_path, driver="GeoJSON")
    print(f"Added {id_field} to GeoJSON and saved to {output_path}")


def convert_geojson_to_ee_bbox(geojson_filepath) -> ee.FeatureCollection:
    """
    Reads a GeoJSON file, creates bounding boxes for each feature,
    and converts to Earth Engine FeatureCollection.

    This is a simpler version of convert_geojson_to_ee_bbox_obscured without
    the privacy-preserving features.

    Args:
        geojson_filepath: The filepath to the GeoJSON file.

    Returns:
        ee.FeatureCollection: Earth Engine FeatureCollection of bounding boxes.
    """
    if isinstance(geojson_filepath, (str, Path)):
        file_path = os.path.abspath(geojson_filepath)
        print(f"Reading GeoJSON file from: {file_path}")

        try:
            gdf = gpd.read_file(file_path)
        except Exception as e:
            raise ValueError(f"Error reading GeoJSON file: {str(e)}")
    else:
        raise ValueError("Input must be a file path (str or Path)")

    if len(gdf) == 0:
        raise ValueError("GeoJSON contains no features")

    if "internal_id" not in gdf.columns:
        gdf["internal_id"] = range(1, len(gdf) + 1)

    bbox_features = []
    for idx, row in gdf.iterrows():
        try:
            minx, miny, maxx, maxy = row.geometry.bounds
            ee_geometry = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

            properties = {col: row[col] for col in gdf.columns if col != "geometry"}
            for key, value in properties.items():
                if hasattr(value, "item"):
                    properties[key] = value.item()
                elif pd.isna(value):
                    properties[key] = None

            ee_feature = ee.Feature(ee_geometry, properties)
            bbox_features.append(ee_feature)

        except Exception as e:
            print(f"Error processing feature {idx}: {str(e)}")

    if not bbox_features:
        raise ValueError("No valid features found in GeoJSON")

    feature_collection = ee.FeatureCollection(bbox_features)
    print(
        f"Created Earth Engine FeatureCollection with {len(bbox_features)} bounding box features"
    )

    return feature_collection


# ============================================================================
# File Cleanup Functions
# ============================================================================


def delete_all_files_in_folder(
    folder_path, pattern=None, max_retries=5, retry_delay=0.5, verbose=True
):
    """
    Delete all files in a folder matching a pattern, with retry logic for locked files.

    Args:
        folder_path: Path to the folder
        pattern: Glob pattern to match files (e.g., "*.tif", "*vrt*")
                 If None, deletes all files
        max_retries: Maximum retry attempts for locked files (default: 5)
        retry_delay: Delay in seconds between retries (default: 0.5)
        verbose: If True, print progress messages (default: True)

    Returns:
        int: Number of files deleted
    """
    import fnmatch

    logger = _whisp_logger

    folder = Path(folder_path)
    if not folder.exists():
        if verbose:
            _whisp_logger.debug(f"Folder does not exist: {folder_path}")
        return 0

    deleted_count = 0
    failed_files = []

    for file_path in folder.iterdir():
        if file_path.is_file():
            if pattern is None or fnmatch.fnmatch(file_path.name, pattern):
                # Retry loop for locked files (common with ProcessPoolExecutor)
                for attempt in range(max_retries):
                    try:
                        file_path.unlink()
                        deleted_count += 1
                        break
                    except PermissionError:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay)
                        else:
                            failed_files.append(file_path.name)
                            _whisp_logger.warning(
                                f"Failed to delete {file_path.name} after {max_retries} attempts (file locked)"
                            )
                    except Exception as e:
                        failed_files.append(file_path.name)
                        _whisp_logger.error(f"Error deleting {file_path}: {e}")
                        break

    if verbose:
        _whisp_logger.debug(f"Deleted {deleted_count} files from {folder_path}")
    if failed_files:
        _whisp_logger.warning(
            f"Failed to delete {len(failed_files)} files: {failed_files[:5]}{'...' if len(failed_files) > 5 else ''}"
        )
    return deleted_count


def delete_folder(folder_path, max_retries=5, retry_delay=0.5):
    """
    Delete the entire folder and all its contents, with retry logic for locked files.

    Args:
        folder_path: Path to the folder to delete
        max_retries: Maximum retry attempts for locked files (default: 5)
        retry_delay: Delay in seconds between retries (default: 0.5)
    """
    import shutil

    folder = Path(folder_path)
    if not folder.exists():
        print(f"Folder does not exist: {folder_path}")
        return

    for attempt in range(max_retries):
        try:
            shutil.rmtree(folder)
            print(f"Deleted folder: {folder_path}")
            return
        except PermissionError:
            if attempt < max_retries - 1:
                print(
                    f"Folder locked, retrying in {retry_delay}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(retry_delay)
                gc.collect()  # Try to release any Python-held references
            else:
                print(
                    f"Failed to delete folder after {max_retries} attempts (files locked)"
                )
        except Exception as e:
            print(f"Error deleting folder {folder_path}: {e}")
            return

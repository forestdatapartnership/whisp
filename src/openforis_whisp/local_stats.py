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
from datetime import datetime

from shapely.geometry import Polygon, mapping
from shapely.validation import make_valid
from rio_vrt import build_vrt


# Set up module logger
logger = logging.getLogger("whisp_local_stats")


# ============================================================================
# GeoTIFF Download Functions
# ============================================================================


def download_geotiff_for_feature(
    ee_feature, image, output_dir, scale=10, max_retries=3, retry_delay=5
):
    """
    Download a GeoTIFF for a specific Earth Engine feature by clipping the image.

    Args:
        ee_feature: Earth Engine feature to clip the image to
        image: Earth Engine image to download (e.g., whisp.combine_datasets())
        output_dir: Directory to save the GeoTIFF
        scale: Resolution in meters (default 10m)
        max_retries: Maximum number of retry attempts for download
        retry_delay: Seconds to wait between retries

    Returns:
        output_path: Path to the downloaded GeoTIFF file
    """
    # Get the feature ID
    try:
        internal_id = ee_feature.get("internal_id").getInfo()
        logger.info(f"Downloading GeoTIFF for feature {internal_id}")
    except Exception as e:
        logger.error(f"Error getting internal_id from feature: {str(e)}")
        internal_id = f"unknown_{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Ensure output directory exists
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    # Create a unique filename
    filename = f"feature_{internal_id}.tif"
    output_path = output_dir / filename

    # If file already exists, don't re-download
    if output_path.exists():
        logger.info(f"File {filename} already exists, skipping download")
        return output_path

    # Track retries
    retries = 0

    while retries < max_retries:
        try:
            # Clip the image to the feature
            clipped_image = image.clip(ee_feature.geometry())

            # Generate the download URL with timeout handling
            logger.debug(f"Generating download URL for feature {internal_id}")
            start_time = time.time()
            download_url = clipped_image.getDownloadURL(
                {
                    "format": "GeoTIFF",
                    "region": ee_feature.geometry(),
                    "scale": scale,
                    "crs": "EPSG:4326",
                }
            )
            url_time = time.time() - start_time
            logger.debug(f"URL generated in {url_time:.2f}s: {download_url[:80]}...")

            # Download the image with timeout
            logger.info(f"Downloading to {output_path}")
            response = requests.get(download_url, timeout=300)  # 5-minute timeout

            if response.status_code == 200:
                # Check if the response is actually a GeoTIFF
                content_type = response.headers.get("Content-Type", "")
                if "tiff" in content_type.lower() or "zip" in content_type.lower():
                    with open(output_path, "wb") as f:
                        f.write(response.content)
                    logger.info(f"Successfully downloaded {filename}")
                    return output_path
                else:
                    # Log error if the response isn't a GeoTIFF
                    logger.error(f"Download returned non-TIFF content: {content_type}")
                    # Save the response for debugging
                    error_file = output_dir / f"error_{internal_id}.txt"
                    with open(error_file, "wb") as f:
                        f.write(response.content[:2000])
                    logger.error(f"Saved error content to {error_file}")
                    retries += 1
            else:
                logger.error(
                    f"Failed to download (status {response.status_code}): {response.text[:200]}"
                )
                retries += 1

            # Wait before retrying
            if retries < max_retries:
                sleep_time = retry_delay * (2**retries)  # Exponential backoff
                logger.info(
                    f"Retrying in {sleep_time} seconds (attempt {retries+1}/{max_retries})"
                )
                time.sleep(sleep_time)

        except Exception as e:
            logger.error(
                f"Error downloading feature {internal_id}: {str(e)}", exc_info=True
            )
            retries += 1
            if retries < max_retries:
                logger.info(
                    f"Retrying in {retry_delay} seconds (attempt {retries+1}/{max_retries})"
                )
                time.sleep(retry_delay)

    logger.error(f"Maximum retries reached for feature {internal_id}")
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
):
    """
    Download GeoTIFFs for an entire Earth Engine FeatureCollection, with parallel processing option.

    Args:
        feature_collection: Earth Engine FeatureCollection to process
        image: Earth Engine image to clip and download
        output_dir: Directory to save the GeoTIFFs (default: ~/Downloads/whisp_features)
        scale: Resolution in meters (default 10m)
        max_features: Maximum number of features to process (default: all)
        max_workers: Maximum number of parallel workers (default: None, sequential processing)
        max_retries: Maximum number of retry attempts for each download
        retry_delay: Base delay in seconds between retries (uses exponential backoff)

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
    logger.info(
        f"Processing Earth Engine FeatureCollection with {collection_size} features"
    )

    if max_features and max_features < collection_size:
        feature_collection = feature_collection.limit(max_features)
        collection_size = max_features
        logger.info(f"Limited to processing first {max_features} features")

    # Get features as a list
    features = feature_collection.toList(collection_size)

    # Create a function to download a single feature given its index
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
            )
        except Exception as e:
            logger.error(
                f"Error processing feature at index {index}: {str(e)}", exc_info=True
            )
            return None

    results = []

    # Parallel processing if max_workers is specified and > 1
    if max_workers and max_workers > 1:
        logger.info(f"Using parallel processing with {max_workers} workers")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_index = {
                executor.submit(download_feature, i): i for i in range(collection_size)
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    path = future.result()
                    if path:
                        results.append(path)
                        logger.info(f"Completed feature {index+1}/{collection_size}")
                    else:
                        logger.warning(
                            f"Failed to download feature {index+1}/{collection_size}"
                        )
                except Exception as e:
                    logger.error(
                        f"Exception occurred while processing feature {index+1}: {str(e)}"
                    )
    else:
        # Sequential processing
        logger.info("Processing features sequentially")
        for i in range(collection_size):
            logger.info(f"Processing feature {i+1}/{collection_size}")
            path = download_feature(i)
            if path:
                results.append(path)

    logger.info(
        f"Completed downloading {len(results)}/{collection_size} features successfully"
    )
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

    Returns:
        ee.FeatureCollection: Earth Engine FeatureCollection of bounding boxes
    """
    # Read the GeoJSON file using geopandas
    if isinstance(geojson_filepath, (str, Path)):
        file_path = os.path.abspath(geojson_filepath)
        print(f"Reading GeoJSON file from: {file_path}")

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
                    print(
                        f"Warning: No shifting applied to feature {idx} due to missing extension parameters"
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
            print(f"Error processing feature {idx}: {str(e)}")

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
            print(
                f"Added {len(random_features)} random decoy features to obscure real locations"
            )

    # Create the Earth Engine FeatureCollection
    feature_collection = ee.FeatureCollection(bbox_features)
    print(
        f"Created Earth Engine FeatureCollection with {len(bbox_features)} bounding box features"
    )

    return feature_collection


# ============================================================================
# VRT and Local Processing Functions
# ============================================================================


def create_vrt_from_folder(folder_path, exclude_pattern="random"):
    """
    Create a virtual raster (VRT) file from all TIF files in a folder,
    excluding any files with the specified pattern in the filename.

    Args:
        folder_path (str or Path): Path to the folder containing TIF files
        exclude_pattern (str): String pattern to exclude from filenames (default: "random")

    Returns:
        str: Path to the created VRT file
    """
    folder = Path(folder_path)

    # Get list of TIFF files, excluding those with the exclude_pattern in the filename
    tif_files = []
    for ext in ["*.tif", "*.tiff"]:
        for file_path in folder.glob(ext):
            if exclude_pattern.lower() not in file_path.name.lower():
                tif_files.append(str(file_path))

    # Check if any files were found
    if not tif_files:
        print(f"No suitable TIF files found in {folder_path}")
        return None

    print(f"Found {len(tif_files)} TIF files to include in the VRT")

    # Output VRT path
    output_vrt = str(folder / "combined_rasters.vrt")

    # Create the VRT file
    build_vrt(output_vrt, tif_files)

    print(f"VRT file created at: {output_vrt}")
    return output_vrt


# ============================================================================
# Parallel Zonal Statistics Functions
# ============================================================================


def _process_chunk_df(chunk_gdf, rasters, ops, chunk_idx, num_chunks):
    """
    Process a single chunk of features for exact_extract.
    This is a helper function for parallel processing.
    """
    from exactextract import exact_extract

    print(f"Starting chunk {chunk_idx+1}/{num_chunks} (features {len(chunk_gdf)})")
    chunk_start_time = time.time()
    try:
        chunk_results = exact_extract(
            progress=False, rast=rasters, vec=chunk_gdf, ops=ops, output="pandas"
        )
        gc.collect()
        chunk_time = time.time() - chunk_start_time
        print(f"Completed chunk {chunk_idx+1}/{num_chunks} in {chunk_time:.2f}s")
        return chunk_results
    except Exception as e:
        print(f"Error processing chunk {chunk_idx+1}/{num_chunks}: {str(e)}")
        return None
    finally:
        gc.collect()


def exact_extract_in_chunks_parallel(
    rasters, vector_file, chunk_size=25, ops=None, max_workers=4
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

    Returns:
        pd.DataFrame: Combined results from all chunks
    """
    if ops is None:
        ops = ["sum"]

    # Configure GDAL for multithreaded environment
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "EMPTY_DIR"
    os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif,.vrt"
    os.environ["VSI_CACHE"] = "TRUE"
    os.environ["VSI_CACHE_SIZE"] = "50000000"  # 50MB cache

    start_time = time.time()
    print(f"Reading vector file: {vector_file}")
    gdf = gpd.read_file(vector_file)
    total_features = len(gdf)
    num_chunks = (total_features + chunk_size - 1) // chunk_size
    print(f"Processing in {num_chunks} chunks of up to {chunk_size} features each")
    print(f"Using {max_workers} parallel workers")

    # Split the GeoDataFrame into chunks to avoid pickling the whole gdf
    chunks = [
        gdf.iloc[i * chunk_size : min((i + 1) * chunk_size, total_features)].copy()
        for i in range(num_chunks)
    ]

    all_results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_process_chunk_df, chunk, rasters, ops, i, num_chunks)
            for i, chunk in enumerate(chunks)
        ]
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            try:
                result = future.result()
                if result is not None and not result.empty:
                    all_results.append(result)
                    print(f"Results from chunk {i+1} stored")
            except Exception as e:
                print(f"Exception in chunk {i+1}: {str(e)}")

    gdf = None
    combined_df = pd.DataFrame()
    if all_results:
        print(f"Combining results from {len(all_results)} chunks...")
        combined_df = pd.concat(all_results, ignore_index=True)
        all_results = []
        gc.collect()

    total_time = time.time() - start_time
    processed_count = len(combined_df) if not combined_df.empty else 0
    print(
        f"Processing complete. Processed {processed_count}/{total_features} features in {total_time:.2f}s"
    )
    return combined_df


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


def delete_all_files_in_folder(folder_path, pattern=None):
    """
    Delete all files in a folder matching a pattern.

    Args:
        folder_path: Path to the folder
        pattern: Glob pattern to match files (e.g., "*.tif", "*vrt*")
                 If None, deletes all files

    Returns:
        int: Number of files deleted
    """
    import fnmatch

    folder = Path(folder_path)
    if not folder.exists():
        print(f"Folder does not exist: {folder_path}")
        return 0

    deleted_count = 0
    for file_path in folder.iterdir():
        if file_path.is_file():
            if pattern is None or fnmatch.fnmatch(file_path.name, pattern):
                try:
                    file_path.unlink()
                    deleted_count += 1
                except Exception as e:
                    print(f"Error deleting {file_path}: {e}")

    print(f"Deleted {deleted_count} files from {folder_path}")
    return deleted_count


def delete_folder(folder_path):
    """
    Delete the entire folder and all its contents.

    Args:
        folder_path: Path to the folder to delete
    """
    import shutil

    folder = Path(folder_path)
    if not folder.exists():
        print(f"Folder does not exist: {folder_path}")
        return

    try:
        shutil.rmtree(folder)
        print(f"Deleted folder: {folder_path}")
    except Exception as e:
        print(f"Error deleting folder {folder_path}: {e}")

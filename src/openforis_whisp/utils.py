import base64
import ee
import math
import os
import pandas as pd
import random
import numpy as np
import logging
import sys

import urllib.request
import os

import importlib.resources as pkg_resources

from dotenv import load_dotenv
from pathlib import Path

from shapely.geometry import Polygon, Point, mapping
from shapely.validation import make_valid

from .logger import StdoutLogger

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

logger = StdoutLogger(__name__)


def get_example_data_path(filename):
    """
    Get the path to an example data file included in the package.

    Parameters:
    -----------
    filename : str
        The name of the example data file.

    Returns:
    --------
    str
        The path to the example data file.
    """
    return os.path.join("..", "tests", "fixtures", filename)


def load_env_vars() -> None:
    """Loads the environment variables required for testing the codebase.

    Returns
    -------
    out : None
    """

    all_dotenv_paths = [Path(__file__).parents[2] / ".env", Path.cwd() / ".env"]
    dotenv_loaded = False

    for dotenv_path in all_dotenv_paths:
        logger.logger.debug(f"dotenv_path: {dotenv_path}")
        if dotenv_path.exists():
            dotenv_loaded = load_dotenv(dotenv_path)
            break

    if not dotenv_loaded:
        raise DotEnvNotFoundError
    logger.logger.info(f"Loaded evironment variables from '{dotenv_path}'")


def init_ee() -> None:
    """Initialize earth engine according to the environment"""

    # only do the initialization if the credential are missing
    if not ee.data._credentials:

        # if in test env use the private key
        if "EE_PRIVATE_KEY" in os.environ:

            # key need to be decoded in a file
            content = base64.b64decode(os.environ["EE_PRIVATE_KEY"]).decode()
            with open("ee_private_key.json", "w") as f:
                f.write(content)

            # connection to the service account
            service_account = "test-sepal-ui@sepal-ui.iam.gserviceaccount.com"
            credentials = ee.ServiceAccountCredentials(
                service_account, "ee_private_key.json"
            )
            ee.Initialize(credentials)
            logger.logger.info(f"Used env var")

        # if in local env use the local user credential
        else:
            try:
                load_env_vars()
                logger.logger.info("Called 'ee.Initialize()'.")
                ee.Initialize(project=os.environ["PROJECT"])
            except ee.ee_exception.EEException:
                logger.logger.info("Called 'ee.Authenticate()'.")
                ee.Authenticate()
                ee.Initialize(project=os.environ["PROJECT"])


def clear_ee_credentials():

    path_to_creds = Path().home() / ".config" / "earthengine" / "credentials"
    if not path_to_creds.exists():
        logger.logger.error(
            f"GEE credentials file '{path_to_creds}' not found, could not de-authenticate."
        )
    else:
        path_to_creds.unlink()
        logger.logger.warning(f"GEE credentials file deleted.")


def remove_geometry_from_feature_collection(feature_collection):
    """Define the function to remove geometry from features in a feature collection"""
    # Function to remove geometry from features
    def remove_geometry(feature):
        # Remove the geometry property
        feature = feature.setGeometry(None)
        return feature

    # Apply the function to remove geometry to the feature collection
    feature_collection_no_geometry = feature_collection.map(remove_geometry)
    return feature_collection_no_geometry


# Compute centroids of each polygon including the external_id_column
def get_centroid(feature, external_id_column="external_id"):
    keepProperties = [external_id_column]
    # Get the centroid of the feature's geometry.
    centroid = feature.geometry().centroid(1)
    # Return a new Feature, copying properties from the old Feature.
    return ee.Feature(centroid).copyProperties(feature, keepProperties)


def buffer_point_to_required_area(feature, area, area_unit):
    """buffers feature to get a given area (needs math library); area unit in 'ha' or 'km2' (the default)"""
    area = feature.get("REP_AREA")

    # buffer_size = get_radius_m_to_buffer_for_given_area(area,"km2")# should work but untested in this function

    buffer_size = (
        (ee.Number(feature.get("REP_AREA")).divide(math.pi)).sqrt().multiply(1000)
    )  # calculating radius in metres from REP_AREA in km2

    return ee.Feature(feature).buffer(buffer_size, 1)
    ### buffering (incl., max error parameter should be 0m. But put as 1m anyhow - doesn't seem to make too much of a difference for speed)


def get_radius_m_to_buffer_to_required_area(area, area_unit="km2"):
    """gets radius in metres to buffer to get an area (needs math library); area unit ha or km2 (the default)"""
    if area_unit == "km2":
        unit_fix_factor = 1000
    elif area_unit == "ha":
        unit_fix_factor = 100
    radius = ee.Number(area).divide(math.pi).sqrt().multiply(unit_fix_factor)
    return radius


class DotEnvNotFoundError(FileNotFoundError):
    def __init__(self) -> None:
        super().__init__(
            "Running tests requires setting an appropriate '.env' in the root directory or in your current working "
            "directory. You may copy and edit the '.env.template' file from the root directory or from the README.",
        )


def get_example_geojson(filename="geojson_example.geojson", cache=True):
    """
    Download example geojson file for testing whisp functionality.

    Parameters:
    -----------
    filename : str
        Local filename to save the geojson
    cache : bool
        If True, cache file in user directory to avoid re-downloading

    Returns:
    --------
    str
        Path to the downloaded geojson file
    """
    url = "https://raw.githubusercontent.com/forestdatapartnership/whisp/main/tests/fixtures/geojson_example.geojson"

    if cache:
        cache_dir = os.path.join(os.path.expanduser("~"), ".whisp_cache")
        os.makedirs(cache_dir, exist_ok=True)
        filepath = os.path.join(cache_dir, filename)

        if os.path.exists(filepath):
            return filepath
    else:
        filepath = filename

    try:
        urllib.request.urlretrieve(url, filepath)
        return filepath
    except Exception as e:
        raise RuntimeError(f"Failed to download example geojson: {e}")


def generate_random_polygon(
    min_lon, min_lat, max_lon, max_lat, min_area_ha=1, max_area_ha=10, vertex_count=20
):
    """
    Generate a random polygon with exact vertex count control.

    Parameters
    ----------
    min_lon : float
        Minimum longitude
    min_lat : float
        Minimum latitude
    max_lon : float
        Maximum longitude
    max_lat : float
        Maximum latitude
    min_area_ha : float
        Minimum area in hectares
    max_area_ha : float
        Maximum area in hectares
    vertex_count : int
        Exact number of vertices for the polygon

    Returns
    -------
    tuple
        (Polygon, actual_area_ha)
    """
    target_area_ha = random.uniform(min_area_ha, max_area_ha)
    center_lon = random.uniform(min_lon, max_lon)
    center_lat = random.uniform(min_lat, max_lat)

    # Estimate radius for target area
    target_area_m2 = target_area_ha * 10000  # hectares to square meters
    radius_meters = math.sqrt(target_area_m2 / math.pi)
    radius_degrees = radius_meters / (111320 * math.cos(math.radians(center_lat)))

    # Create center point
    center_point = Point(center_lon, center_lat)

    # Use buffer with resolution to control vertices for smaller vertex counts
    if vertex_count <= 20:
        poly = center_point.buffer(radius_degrees, resolution=vertex_count // 4)

    # Manual vertex creation for higher vertex counts (sine wave distortions for realistic shapes)
    if vertex_count > 20:
        angles = np.linspace(0, 2 * math.pi, vertex_count, endpoint=False)

        base_radius = radius_degrees

        # Smooth sine wave variations for natural look
        freq1 = random.uniform(2, 5)
        amp1 = random.uniform(0.08, 0.15)
        freq2 = random.uniform(8, 15)
        amp2 = random.uniform(0.03, 0.08)

        radius_variation = amp1 * np.sin(
            freq1 * angles + random.uniform(0, 2 * math.pi)
        ) + amp2 * np.sin(freq2 * angles + random.uniform(0, 2 * math.pi))

        radii = base_radius * (1.0 + radius_variation)
        radii = np.maximum(radii, base_radius * 0.6)

        xs = center_lon + radii * np.cos(angles)
        ys = center_lat + radii * np.sin(angles)

        xs = np.clip(xs, min_lon, max_lon)
        ys = np.clip(ys, min_lat, max_lat)

        vertices = list(zip(xs, ys))
        vertices.append(vertices[0])

        poly = Polygon(vertices)

        if not poly.is_valid:
            poly = make_valid(poly)
            if hasattr(poly, "geoms"):
                poly = max(poly.geoms, key=lambda p: p.area)

    else:
        # Resample to get exact vertex count for buffered circles
        coords = list(poly.exterior.coords)

        if len(coords) - 1 != vertex_count:
            angles = np.linspace(0, 2 * math.pi, vertex_count, endpoint=False)

            new_coords = []
            for angle in angles:
                x = center_lon + radius_degrees * math.cos(angle)
                y = center_lat + radius_degrees * math.sin(angle)

                dx = random.uniform(-radius_degrees * 0.08, radius_degrees * 0.08)
                dy = random.uniform(-radius_degrees * 0.08, radius_degrees * 0.08)

                new_x = np.clip(x + dx, min_lon, max_lon)
                new_y = np.clip(y + dy, min_lat, max_lat)
                new_coords.append((new_x, new_y))

            new_coords.append(new_coords[0])
            poly = Polygon(new_coords)

    # Calculate actual area
    area_sq_degrees = poly.area
    area_sq_meters = (
        area_sq_degrees * (111320 * math.cos(math.radians(center_lat))) ** 2
    )
    actual_area_ha = area_sq_meters / 10000

    return poly, actual_area_ha


def generate_test_polygons(
    bounds,
    num_polygons=25,
    min_area_ha=1,
    max_area_ha=10,
    min_number_vert=10,
    max_number_vert=20,
):
    """
    Generate synthetic test polygons with exact vertex count control.

    **Deprecated**: This is a legacy alias for generate_random_polygons().
    Use generate_random_polygons() for new code, which provides additional
    features like save_path and seed parameters.

    This utility is useful for testing WHISP processing with controlled test data,
    especially when you need polygons with specific characteristics (area, complexity).

    Parameters
    ----------
    bounds : list or ee.Geometry
        Either a list of [min_lon, min_lat, max_lon, max_lat] or an Earth Engine Geometry.
    num_polygons : int, optional
        Number of polygons to generate (default: 25)
    min_area_ha : float, optional
        Minimum area in hectares (default: 1)
    max_area_ha : float, optional
        Maximum area in hectares (default: 10)
    min_number_vert : int, optional
        Minimum number of vertices per polygon (default: 10)
    max_number_vert : int, optional
        Maximum number of vertices per polygon (default: 20)

    Returns
    -------
    dict
        GeoJSON FeatureCollection with generated polygons.

    See Also
    --------
    generate_random_polygons : Recommended replacement with additional options
    """
    return generate_random_polygons(
        bounds=bounds,
        num_polygons=num_polygons,
        min_area_ha=min_area_ha,
        max_area_ha=max_area_ha,
        min_number_vert=min_number_vert,
        max_number_vert=max_number_vert,
    )


def generate_random_features(
    bounds,
    feature_type="point",
    num_features=10,
    seed=None,
    min_area_ha=1,
    max_area_ha=10,
    min_number_vert=10,
    max_number_vert=20,
    multipolygon_pct=20,
    min_parts=2,
    max_parts=4,
    save_path=None,
    return_path=False,
):
    """
    Generate random test features (points, polygons, or mixed) with exact geographic bounds control.

    This utility is useful for testing WHISP processing with random feature data,
    especially when you need features with specific geographic characteristics.
    For polygon features, reuses the production polygon generation logic from
    generate_random_polygon() to ensure consistency.

    Parameters
    ----------
    bounds : list or ee.Geometry
        Either a list of [min_lon, min_lat, max_lon, max_lat] or an Earth Engine Geometry.
        Examples:
            - Simple bounds: [-81.0, -19.3, -31.5, 9.6]
            - EE Geometry: ee.FeatureCollection('USDOS/LSIB_SIMPLE/2017').filter(
                              ee.Filter.eq('country_na', 'Brazil')).first().geometry()
    feature_type : str, optional
        Type of features to generate:
        - 'point': Random points
        - 'polygon': Single-part polygons
        - 'mixed': Blend of single polygons and multipolygons (default: 'point')
    num_features : int, optional
        Number of features to generate (default: 10)
    seed : int or None, optional
        Random seed for reproducibility. None = different each run to avoid GEE caching (default: None)
    min_area_ha : float, optional
        Minimum area in hectares for polygons (default: 1)
    max_area_ha : float, optional
        Maximum area in hectares for polygons (default: 10)
    min_number_vert : int, optional
        Minimum vertices per polygon (default: 10)
    max_number_vert : int, optional
        Maximum vertices per polygon (default: 20)
    multipolygon_pct : float, optional
        Percentage of features that are multipolygons for 'mixed' type (0-100, default: 20)
    min_parts : int, optional
        Minimum polygon parts per multipolygon (default: 2)
    max_parts : int, optional
        Maximum polygon parts per multipolygon (default: 4)
    save_path : str or Path, optional
        Directory path where to save the GeoJSON file. If None, file is not saved (default: None)
    return_path : bool, optional
        If True, return the file path instead of the GeoJSON dict. Only used if save_path is provided (default: False)

    Returns
    -------
    dict or Path
        If save_path is None: GeoJSON FeatureCollection dict
        If save_path is provided and return_path=False: GeoJSON FeatureCollection dict
        If save_path is provided and return_path=True: Path to saved GeoJSON file

        All features include:
        - internal_id: Sequential feature ID
        - geometry_type: 'Point', 'Polygon', or 'MultiPolygon' (for distinguishing feature types)

        For polygons, features include:
        - requested_vertices: Requested vertex count
        - actual_vertices: Actual vertices created
        - requested_area_ha: Target area in hectares
        - actual_area_ha: Actual area in hectares

    Examples
    --------
    >>> import openforis_whisp as whisp
    >>>
    >>> # Generate random points (in-memory)
    >>> bounds = [-81.0, -19.3, -31.5, 9.6]
    >>> geojson = whisp.generate_random_features(bounds, feature_type='point', num_features=50)
    >>>
    >>> # Generate and save to file
    >>> from pathlib import Path
    >>> save_dir = Path.home() / 'downloads'
    >>> geojson = whisp.generate_random_features(
    ...     bounds,
    ...     feature_type='polygon',
    ...     num_features=100,
    ...     save_path=save_dir
    ... )
    >>>
    >>> # Generate mixed geometries and return file path
    >>> file_path = whisp.generate_random_features(
    ...     bounds,
    ...     feature_type='mixed',
    ...     num_features=100,
    ...     multipolygon_pct=30,
    ...     min_parts=2,
    ...     max_parts=5,
    ...     save_path=save_dir,
    ...     return_path=True
    ... )
    """
    # Validate feature_type
    if feature_type not in ("point", "polygon", "mixed"):
        raise ValueError(
            f"feature_type must be 'point', 'polygon', or 'mixed' (got {feature_type!r})"
        )

    # Validate num_features
    if num_features < 1:
        raise ValueError(f"num_features must be at least 1 (got {num_features})")

    # Validate multipolygon_pct
    if not (0 <= multipolygon_pct <= 100):
        raise ValueError(
            f"multipolygon_pct must be between 0-100 (got {multipolygon_pct})"
        )

    # Validate min/max parts
    if min_parts < 2:
        raise ValueError(f"min_parts must be at least 2 (got {min_parts})")
    if min_parts > max_parts:
        raise ValueError(
            f"min_parts ({min_parts}) cannot be greater than max_parts ({max_parts})"
        )

    # Extract and validate bounds
    if hasattr(bounds, "bounds"):  # It's an ee.Geometry
        logger.logger.info("Extracting bounds from Earth Engine Geometry...")
        try:
            bounds_geom = (
                bounds.bounds()
                if not hasattr(bounds, "coordinates")
                or bounds.type().getInfo() != "Rectangle"
                else bounds
            )
            bounds_coords = bounds_geom.coordinates().getInfo()[0]
            min_lon = min(coord[0] for coord in bounds_coords)
            max_lon = max(coord[0] for coord in bounds_coords)
            min_lat = min(coord[1] for coord in bounds_coords)
            max_lat = max(coord[1] for coord in bounds_coords)
            logger.logger.info(
                f"Bounds: [{min_lon:.2f}, {min_lat:.2f}, {max_lon:.2f}, {max_lat:.2f}]"
            )
        except Exception as e:
            raise ValueError(
                f"Failed to extract bounds from Earth Engine Geometry: {e}"
            )
    elif isinstance(bounds, (list, tuple)) and len(bounds) == 4:
        min_lon, min_lat, max_lon, max_lat = bounds
    else:
        raise ValueError(
            "bounds must be either:\n"
            "  - A list of [min_lon, min_lat, max_lon, max_lat]\n"
            "  - An Earth Engine Geometry (ee.Geometry, ee.Feature.geometry(), etc.)"
        )

    # Validate bounds
    if min_lon >= max_lon:
        raise ValueError(f"min_lon ({min_lon}) must be less than max_lon ({max_lon})")
    if min_lat >= max_lat:
        raise ValueError(f"min_lat ({min_lat}) must be less than max_lat ({max_lat})")

    # Validate vertex and area parameters
    if min_number_vert > max_number_vert:
        raise ValueError(
            f"min_number_vert ({min_number_vert}) cannot be greater than max_number_vert ({max_number_vert})"
        )
    if min_area_ha > max_area_ha:
        raise ValueError(
            f"min_area_ha ({min_area_ha}) cannot be greater than max_area_ha ({max_area_ha})"
        )

    # Set random seed if provided
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)

    logger.logger.info(f"Generating {num_features} test {feature_type} features...")

    features = []

    if feature_type == "point":
        # Generate point features
        for i in range(num_features):
            lon = random.uniform(min_lon, max_lon)
            lat = random.uniform(min_lat, max_lat)
            feature = {
                "type": "Feature",
                "properties": {
                    "internal_id": i + 1,
                    "name": f"Point_{i+1}",
                    "geometry_type": "Point",
                },
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
            }
            features.append(feature)

    elif feature_type == "polygon":
        # Generate single polygon features - reuse production polygon generation
        vertex_counts = np.random.randint(
            min_number_vert, max_number_vert + 1, num_features
        )

        for i in range(num_features):
            if i > 0 and i % 250 == 0:
                logger.logger.info(
                    f"Generated {i}/{num_features} polygons ({i/num_features*100:.0f}%)..."
                )

            polygon, actual_area = generate_random_polygon(
                min_lon,
                min_lat,
                max_lon,
                max_lat,
                min_area_ha=min_area_ha,
                max_area_ha=max_area_ha,
                vertex_count=vertex_counts[i],
            )

            actual_vertex_count = len(list(polygon.exterior.coords)) - 1

            properties = {
                "internal_id": i + 1,
                "geometry_type": "Polygon",
                "requested_vertices": int(vertex_counts[i]),
                "actual_vertices": int(actual_vertex_count),
                "requested_area_ha": round(random.uniform(min_area_ha, max_area_ha), 2),
                "actual_area_ha": round(actual_area, 2),
            }

            feature = {
                "type": "Feature",
                "properties": properties,
                "geometry": mapping(polygon),
            }

            features.append(feature)

    else:  # mixed
        # Generate blend of single polygons and multipolygons
        num_multipolygons = int(num_features * multipolygon_pct / 100)
        num_polygons = num_features - num_multipolygons

        # Generate single polygons
        if num_polygons > 0:
            vertex_counts = np.random.randint(
                min_number_vert, max_number_vert + 1, num_polygons
            )

            for i in range(num_polygons):
                if i > 0 and i % 250 == 0:
                    logger.logger.info(
                        f"Generated {i}/{num_features} features ({i/num_features*100:.0f}%)..."
                    )

                polygon, actual_area = generate_random_polygon(
                    min_lon,
                    min_lat,
                    max_lon,
                    max_lat,
                    min_area_ha=min_area_ha,
                    max_area_ha=max_area_ha,
                    vertex_count=vertex_counts[i],
                )

                actual_vertex_count = len(list(polygon.exterior.coords)) - 1

                properties = {
                    "internal_id": i + 1,
                    "geometry_type": "Polygon",
                    "requested_vertices": int(vertex_counts[i]),
                    "actual_vertices": int(actual_vertex_count),
                    "requested_area_ha": round(
                        random.uniform(min_area_ha, max_area_ha), 2
                    ),
                    "actual_area_ha": round(actual_area, 2),
                }

                feature = {
                    "type": "Feature",
                    "properties": properties,
                    "geometry": mapping(polygon),
                }

                features.append(feature)

        # Generate multipolygons
        if num_multipolygons > 0:
            for i in range(num_multipolygons):
                if i > 0 and i % 50 == 0:
                    logger.logger.info(
                        f"Generated {num_polygons + i}/{num_features} features ({(num_polygons + i)/num_features*100:.0f}%)..."
                    )

                num_parts = random.randint(min_parts, max_parts)
                polygon_parts = []
                total_area_ha = 0

                # Generate a target total area for this multipolygon
                target_total_area = random.uniform(min_area_ha, max_area_ha)
                # Divide target area among parts (with some randomness)
                part_areas = []
                remaining_area = target_total_area
                for part_idx in range(num_parts):
                    if part_idx == num_parts - 1:
                        # Last part gets all remaining area
                        part_area = remaining_area
                    else:
                        # Distribute remaining area with randomness
                        max_for_part = remaining_area * 0.8
                        part_area = random.uniform(remaining_area * 0.3, max_for_part)
                    part_areas.append(part_area)
                    remaining_area -= part_area

                for part, area_budget in enumerate(part_areas):
                    # Generate each part in the full bounds (randomization spreads them naturally)
                    polygon, actual_area = generate_random_polygon(
                        min_lon,
                        min_lat,
                        max_lon,
                        max_lat,
                        min_area_ha=area_budget * 0.8,
                        max_area_ha=area_budget * 1.2,
                        vertex_count=random.randint(min_number_vert, max_number_vert),
                    )

                    polygon_parts.append([list(polygon.exterior.coords)])
                    total_area_ha += actual_area

                properties = {
                    "internal_id": num_polygons + i + 1,
                    "geometry_type": "MultiPolygon",
                    "num_parts": num_parts,
                    "total_area_ha": round(total_area_ha, 2),
                }

                feature = {
                    "type": "Feature",
                    "properties": properties,
                    "geometry": {
                        "type": "MultiPolygon",
                        "coordinates": polygon_parts,
                    },
                }

                features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}

    logger.logger.info(f"Generated {num_features} test {feature_type} features!")

    # Save to file if save_path is provided
    if save_path is not None:
        import json

        save_path = Path(save_path)
        save_path.mkdir(parents=True, exist_ok=True)

        output_file = save_path / f"{feature_type}s_{num_features}_features.geojson"

        with open(output_file, "w") as f:
            json.dump(geojson, f, indent=2)

        logger.logger.info(f"GeoJSON saved to: {output_file}")

        if return_path:
            return output_file

    return geojson


def generate_random_points(bounds, num_features=10, seed=None, save_path=None):
    """
    Generate random test points with optional save.

    Simplified wrapper around generate_random_features for point-only generation.

    Parameters
    ----------
    bounds : list or ee.Geometry
        Either a list of [min_lon, min_lat, max_lon, max_lat] or an Earth Engine Geometry.
    num_features : int, optional
        Number of points to generate (default: 10)
    seed : int or None, optional
        Random seed for reproducibility (default: None)
    save_path : str or Path, optional
        Directory path where to save the GeoJSON file. If None, file is not saved (default: None)

    Returns
    -------
    dict or Path
        If save_path is None: GeoJSON FeatureCollection dict
        If save_path is provided: Path to saved GeoJSON file

    Examples
    --------
    >>> import openforis_whisp as whisp
    >>>
    >>> bounds = [-81.0, -19.3, -31.5, 9.6]
    >>> geojson = whisp.generate_random_points(bounds, num_features=100)
    >>>
    >>> # Generate and save
    >>> from pathlib import Path
    >>> file_path = whisp.generate_random_points(
    ...     bounds,
    ...     num_features=500,
    ...     save_path=Path.home() / 'downloads'
    ... )
    """
    return generate_random_features(
        bounds=bounds,
        feature_type="point",
        num_features=num_features,
        seed=seed,
        save_path=save_path,
        return_path=True if save_path else False,
    )


def generate_random_polygons(
    bounds,
    num_polygons=25,
    min_area_ha=1,
    max_area_ha=10,
    min_number_vert=10,
    max_number_vert=20,
    seed=None,
    save_path=None,
):
    """
    Generate random test polygons with optional save.

    Wrapper around generate_random_features for polygon-only generation.
    Uses the same production-quality polygon generation as generate_test_polygons.

    Parameters
    ----------
    bounds : list or ee.Geometry
        Either a list of [min_lon, min_lat, max_lon, max_lon] or an Earth Engine Geometry.
    num_polygons : int, optional
        Number of polygons to generate (default: 25)
    min_area_ha : float, optional
        Minimum area in hectares (default: 1)
    max_area_ha : float, optional
        Maximum area in hectares (default: 10)
    min_number_vert : int, optional
        Minimum number of vertices per polygon (default: 10)
    max_number_vert : int, optional
        Maximum number of vertices per polygon (default: 20)
    seed : int or None, optional
        Random seed for reproducibility (default: None)
    save_path : str or Path, optional
        Directory path where to save the GeoJSON file. If None, file is not saved (default: None)

    Returns
    -------
    dict or Path
        If save_path is None: GeoJSON FeatureCollection dict
        If save_path is provided: Path to saved GeoJSON file

    Examples
    --------
    >>> import openforis_whisp as whisp
    >>>
    >>> bounds = [-81.0, -19.3, -31.5, 9.6]
    >>> geojson = whisp.generate_random_polygons(bounds, num_polygons=50)
    >>>
    >>> # Generate and save
    >>> from pathlib import Path
    >>> file_path = whisp.generate_random_polygons(
    ...     bounds,
    ...     num_polygons=100,
    ...     min_area_ha=5,
    ...     max_area_ha=50,
    ...     save_path=Path.home() / 'downloads'
    ... )
    """
    return generate_random_features(
        bounds=bounds,
        feature_type="polygon",
        num_features=num_polygons,
        seed=seed,
        min_area_ha=min_area_ha,
        max_area_ha=max_area_ha,
        min_number_vert=min_number_vert,
        max_number_vert=max_number_vert,
        save_path=save_path,
        return_path=True if save_path else False,
    )

import pandas as pd
import geojson
from shapely.geometry import shape
from pathlib import Path

# Existing imports
from typing import List, Any, Union
from geojson import Feature, FeatureCollection, Polygon, Point
import json
import os
import geopandas as gpd
import ee


# ============================================================================
# HELPER FUNCTIONS FOR UNIFIED PROCESSING PATHWAY
# ============================================================================


def _sanitize_geodataframe(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Sanitize GeoDataFrame data types for JSON serialization.

    Converts problematic data types that cannot be directly serialized:
    - DateTime/Timestamp columns → ISO format strings
    - Object columns → strings
    - Skips geometry column

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with sanitized data types
    """
    gdf = gdf.copy()
    for col in gdf.columns:
        if col != gdf.geometry.name:  # Skip geometry column
            # Handle datetime/timestamp columns
            if pd.api.types.is_datetime64_any_dtype(gdf[col]):
                gdf[col] = gdf[col].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
            # Handle other problematic types
            elif gdf[col].dtype == "object":
                # Convert any remaining non-serializable objects to strings
                gdf[col] = gdf[col].astype(str)
    return gdf


def _ensure_wgs84_crs(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Ensure GeoDataFrame uses WGS 84 (EPSG:4326) coordinate reference system.

    - If CRS is None, assumes WGS 84
    - If CRS is not WGS 84, converts to WGS 84
    - If already WGS 84, returns unchanged

    Args:
        gdf (gpd.GeoDataFrame): Input GeoDataFrame

    Returns:
        gpd.GeoDataFrame: GeoDataFrame in WGS 84
    """
    if gdf.crs is None:
        # Assuming WGS 84 if no CRS defined
        return gdf
    elif gdf.crs != "EPSG:4326":
        return gdf.to_crs("EPSG:4326")
    return gdf


def _create_ee_feature_collection(
    geojson_data: dict, strip_z_coords: bool = True, input_source: str = "input"
) -> ee.FeatureCollection:
    """
    Create Earth Engine FeatureCollection from GeoJSON dict with error recovery.

    Attempts to create EE FeatureCollection. If it fails due to 3D coordinates
    and strip_z_coords is True, automatically strips Z values and retries.

    Args:
        geojson_data (dict): GeoJSON data dictionary
        strip_z_coords (bool): Whether to retry with 2D geometries on failure
        input_source (str): Description of input source for logging

    Returns:
        ee.FeatureCollection: Earth Engine FeatureCollection

    Raises:
        ee.EEException: If conversion fails even after retries
    """
    try:
        feature_collection = ee.FeatureCollection(
            create_feature_collection(geojson_data)
        )
        return feature_collection
    except ee.EEException as e:
        if "Invalid GeoJSON geometry" in str(e) and strip_z_coords:
            # Apply print_once deduplication for Z-coordinate stripping messages
            if not hasattr(_create_ee_feature_collection, "_printed_z_messages"):
                _create_ee_feature_collection._printed_z_messages = set()

            z_message_key = f"z_coords_{input_source}"
            if z_message_key not in _create_ee_feature_collection._printed_z_messages:
                print(
                    "Warning: Invalid GeoJSON geometry detected, likely due to 3D coordinates."
                )
                print("Attempting to fix by stripping Z coordinates...")
                _create_ee_feature_collection._printed_z_messages.add(z_message_key)

            # Apply Z-coordinate stripping
            geojson_data_fixed = _strip_z_coordinates_from_geojson(geojson_data)

            # Try again with the fixed data
            try:
                feature_collection = ee.FeatureCollection(
                    create_feature_collection(geojson_data_fixed)
                )

                success_message_key = f"z_coords_success_{input_source}"
                if (
                    success_message_key
                    not in _create_ee_feature_collection._printed_z_messages
                ):
                    print("Successfully converted after stripping Z coordinates")
                    _create_ee_feature_collection._printed_z_messages.add(
                        success_message_key
                    )

                return feature_collection
            except Exception as retry_error:
                raise ee.EEException(
                    f"Failed to convert GeoJSON even after stripping Z coordinates: {retry_error}"
                )
        else:
            raise e


def convert_geojson_to_ee(
    geojson_input: Union[str, Path, dict, gpd.GeoDataFrame],
    enforce_wgs84: bool = True,
    strip_z_coords: bool = True,
) -> ee.FeatureCollection:
    """
    Converts GeoJSON data to an Earth Engine FeatureCollection.

    Accepts flexible input types with a unified processing pathway:
    - File path (str or Path) → loads with GeoPandas
    - GeoJSON dict → uses directly
    - GeoDataFrame → uses directly

    Automatically handles:
    - CRS conversion to WGS 84 (EPSG:4326) if needed
    - DateTime/Timestamp columns → converts to ISO strings before JSON serialization
    - Non-serializable objects → converts to strings
    - 3D coordinates → strips Z values when necessary
    - Z-coordinate errors → retries with 2D geometries if enabled

    Args:
        geojson_input (Union[str, Path, dict, gpd.GeoDataFrame]):
            - File path (str or Path) to GeoJSON file
            - GeoJSON dictionary object
            - GeoPandas GeoDataFrame
        enforce_wgs84 (bool): Whether to enforce WGS 84 projection (EPSG:4326).
            Defaults to True. Only applies to file path and GeoDataFrame inputs.
        strip_z_coords (bool): Whether to automatically strip Z coordinates from 3D geometries.
            Defaults to True.

    Returns:
        ee.FeatureCollection: Earth Engine FeatureCollection created from the GeoJSON.

    Raises:
        ValueError: If input type is unsupported or GeoJSON validation fails.
        ee.EEException: If GeoJSON cannot be converted even after retries.
    """
    # UNIFIED INPUT NORMALIZATION: Convert all inputs to GeoDataFrame first
    if isinstance(geojson_input, gpd.GeoDataFrame):
        gdf = geojson_input.copy()
        input_source = "GeoDataFrame"
    elif isinstance(geojson_input, dict):
        # Convert dict to GeoDataFrame for unified processing
        gdf = gpd.GeoDataFrame.from_features(geojson_input.get("features", []))
        input_source = "dict"
    elif isinstance(geojson_input, (str, Path)):
        # Load file and convert to GeoDataFrame
        file_path = os.path.abspath(geojson_input)
        gdf = gpd.read_file(file_path)
        input_source = f"file ({file_path})"
    else:
        raise ValueError(
            f"Input must be a file path (str or Path), GeoJSON dict, or GeoDataFrame. "
            f"Got {type(geojson_input).__name__}"
        )

    # UNIFIED DATA SANITIZATION PATHWAY
    # Handle problematic data types before JSON conversion
    gdf = _sanitize_geodataframe(gdf)

    # UNIFIED CRS HANDLING
    if enforce_wgs84:
        gdf = _ensure_wgs84_crs(gdf)

    # UNIFIED GEOJSON CONVERSION
    geojson_data = json.loads(gdf.to_json())

    # UNIFIED VALIDATION
    validation_errors = validate_geojson(geojson_data)
    if validation_errors:
        raise ValueError(f"GeoJSON validation errors: {validation_errors}")

    # UNIFIED EE CONVERSION with error recovery
    return _create_ee_feature_collection(geojson_data, strip_z_coords, input_source)


def _strip_z_coordinates_from_geojson(geojson_data: dict) -> dict:
    """
    Helper function to strip Z coordinates from GeoJSON data.
    Converts 3D coordinates to 2D by removing Z values.

    Args:
        geojson_data (dict): GeoJSON data dictionary

    Returns:
        dict: GeoJSON data with Z coordinates stripped
    """

    def strip_z(geometry):
        """Remove Z coordinates from geometry to make it 2D"""
        if geometry["type"] == "MultiPolygon":
            geometry["coordinates"] = [
                [[[lon, lat] for lon, lat, *_ in ring] for ring in polygon]
                for polygon in geometry["coordinates"]
            ]
        elif geometry["type"] == "Polygon":
            geometry["coordinates"] = [
                [[lon, lat] for lon, lat, *_ in ring]
                for ring in geometry["coordinates"]
            ]
        elif geometry["type"] == "Point":
            if len(geometry["coordinates"]) > 2:
                geometry["coordinates"] = geometry["coordinates"][:2]
        elif geometry["type"] == "MultiPoint":
            geometry["coordinates"] = [coord[:2] for coord in geometry["coordinates"]]
        elif geometry["type"] == "LineString":
            geometry["coordinates"] = [
                [lon, lat] for lon, lat, *_ in geometry["coordinates"]
            ]
        elif geometry["type"] == "MultiLineString":
            geometry["coordinates"] = [
                [[lon, lat] for lon, lat, *_ in line]
                for line in geometry["coordinates"]
            ]
        return geometry

    # Create a deep copy to avoid modifying the original
    import copy

    geojson_copy = copy.deepcopy(geojson_data)

    # Process all features
    if "features" in geojson_copy:
        for feature in geojson_copy["features"]:
            if "geometry" in feature and feature["geometry"]:
                feature["geometry"] = strip_z(feature["geometry"])

    return geojson_copy


def convert_ee_to_geojson(ee_object, filename=None, indent=2, **kwargs):
    """Converts Earth Engine object to geojson.

    Args:
        ee_object (object): An Earth Engine object.
        filename (str, optional): The file path to save the geojson. Defaults to None.

    Returns:
        object: GeoJSON object.
    """

    try:
        if (
            isinstance(ee_object, ee.Geometry)
            or isinstance(ee_object, ee.Feature)
            or isinstance(ee_object, ee.FeatureCollection)
        ):
            json_object = ee_object.getInfo()
            if filename is not None:
                filename = os.path.abspath(filename)
                if not os.path.exists(os.path.dirname(filename)):
                    os.makedirs(os.path.dirname(filename))
                with open(filename, "w") as f:
                    f.write(json.dumps(json_object, indent=indent, **kwargs) + "\n")
            else:
                return json_object
        else:
            print("Could not convert the Earth Engine object to geojson")
    except Exception as e:
        raise Exception(e)


def convert_geojson_to_shapefile(geojson_path, shapefile_output_path):
    """
    Convert a GeoJSON file to a Shapefile and save it to a file.

    Parameters:
    - geojson_path: Path to the GeoJSON file.
    - shapefile_output_path: Path where the Shapefile will be saved.
    """
    with open(geojson_path, "r") as geojson_file:
        geojson = json.load(geojson_file)

    gdf = gpd.GeoDataFrame.from_features(geojson["features"])

    gdf.to_file(shapefile_output_path, driver="ESRI Shapefile")


def convert_shapefile_to_geojson(shapefile_path, geojson_output_path):
    """
    Convert a Shapefile to GeoJSON and save it to a file.

    Parameters:
    - shapefile_path: Path to the Shapefile.
    - geojson_output_path: Path where the GeoJSON file will be saved.
    """
    gdf = gpd.read_file(shapefile_path)

    geojson_str = gdf.to_json()

    with open(geojson_output_path, "w") as geojson_file:
        geojson_file.write(geojson_str)


def convert_shapefile_to_ee(shapefile_path):
    """
    Convert a zipped shapefile to an Earth Engine FeatureCollection.
    NB Making this as existing Geemap function shp_to_ee wouldnt work.
    Args:
    - shapefile_path (str): Path to the shapefile (.zip) to be converted.

    Returns:
    - ee.FeatureCollection: Earth Engine FeatureCollection created from the shapefile.
    """
    gdf = gpd.read_file(shapefile_path)

    geo_json = gdf.to_json()

    roi = ee.FeatureCollection(json.loads(geo_json))

    return roi


# def convert_ee_to_df(
#     ee_object,
#     columns=None,
#     remove_geom=False,
#     **kwargs,
# ):
#     """Converts an ee.FeatureCollection to pandas dataframe.

#     Args:
#         ee_object (ee.FeatureCollection): ee.FeatureCollection.
#         columns (list): List of column names. Defaults to None.
#         remove_geom (bool): Whether to remove the geometry column. Defaults to True.
#         kwargs: Additional arguments passed to ee.data.computeFeature.

#     Raises:
#         TypeError: ee_object must be an ee.FeatureCollection

#     Returns:
#         pd.DataFrame: pandas DataFrame
#     """
#     if isinstance(ee_object, ee.Feature):
#         ee_object = ee.FeatureCollection([ee_object])

#     if not isinstance(ee_object, ee.FeatureCollection):
#         raise TypeError("ee_object must be an ee.FeatureCollection")

#     try:
#         if remove_geom:
#             data = ee_object.map(
#                 lambda f: ee.Feature(None, f.toDictionary(f.propertyNames().sort()))
#             )
#         else:
#             data = ee_object

#         kwargs["expression"] = data
#         kwargs["fileFormat"] = "PANDAS_DATAFRAME"

#         df = ee.data.computeFeatures(kwargs)

#         if isinstance(columns, list):
#             df = df[columns]

#         if remove_geom and ("geometry" in df.columns):
#             df = df.drop(columns=["geometry"], axis=1)

#         # Sorting columns is not supported server-side and is removed from this function.

#         return df
#     except Exception as e:
#         raise Exception(e)


def convert_ee_to_df(
    ee_object,
    columns=None,
    remove_geom=False,
    sort_columns=False,
    **kwargs,
):
    """
    Converts an ee.FeatureCollection to pandas DataFrame, maximizing server-side operations.

    Args:
        ee_object (ee.FeatureCollection): ee.FeatureCollection.
        columns (list): List of column names to select (server-side if possible).
        remove_geom (bool): Remove geometry column server-side.
        kwargs: Additional arguments for ee.data.computeFeatures.

    Returns:
        pd.DataFrame: pandas DataFrame
    """
    import ee

    if isinstance(ee_object, ee.Feature):
        ee_object = ee.FeatureCollection([ee_object])

    if not isinstance(ee_object, ee.FeatureCollection):
        raise TypeError("ee_object must be an ee.FeatureCollection")

    # Server-side: select columns and remove geometry
    if columns is not None:
        ee_object = ee_object.select(columns)
    if remove_geom:
        ee_object = ee_object.map(lambda f: ee.Feature(None, f.toDictionary()))

    try:
        kwargs["expression"] = ee_object
        kwargs["fileFormat"] = "PANDAS_DATAFRAME"
        df = ee.data.computeFeatures(kwargs)

        return df
    except Exception as e:
        raise Exception(e)


def convert_ee_to_shapefile(feature_collection, shapefile_path):
    """
    Export an Earth Engine FeatureCollection to a shapefile.

    Parameters:
    - feature_collection: Earth Engine FeatureCollection to be exported.
    - shapefile_path: Path to save the shapefile.

    Returns:
    - Path to the saved shapefile.
    """

    geojson = convert_ee_to_geojson(feature_collection)

    features = geojson["features"]
    geoms = [shape(feature["geometry"]) for feature in features]
    properties = [feature["properties"] for feature in features]
    gdf = gpd.GeoDataFrame(properties, geometry=geoms)

    gdf.to_file(shapefile_path, driver="ESRI Shapefile")

    print(f"Shapefile saved to {shapefile_path}")

    return shapefile_path


def validate_geojson(input_data: Any) -> List[str]:
    """
    Validates GeoJSON data and filters out certain non-critical errors.

    :param input_data: GeoJSON data as a string, dict, or a file path
    :return: List of validation errors
    """
    errors = []

    if isinstance(input_data, (str, Path)):
        try:
            with open(input_data, "r") as f:
                geojson_data = f.read()
                geojson_obj = json.loads(geojson_data)
        except Exception as e:
            errors.append(f"Error reading file: {e}")
            return errors
    elif isinstance(input_data, dict):
        geojson_obj = input_data
    else:
        geojson_data = input_data
        try:
            geojson_obj = json.loads(geojson_data)
        except ValueError as e:
            errors.append(f"Invalid GeoJSON: {e}")
            return errors

    if "type" not in geojson_obj:
        errors.append("Missing 'type' field in GeoJSON.")

    return errors


def extract_features(geojson_obj: Any, features: List[Feature]) -> None:
    """
    Recursively extracts features from a GeoJSON object and adds them to the feature list.

    :param geojson_obj: GeoJSON object (could be geometry, feature, or feature collection)
    :param features: List of extracted features
    """
    if isinstance(geojson_obj, dict):
        obj_type = geojson_obj.get("type")

        if obj_type == "Feature":
            # Extract the actual Feature with properties
            geometry = geojson_obj.get("geometry", {})
            properties = geojson_obj.get("properties", {})

            if geometry and geometry.get("type"):
                features.append(Feature(geometry=geometry, properties=properties))

        elif obj_type == "FeatureCollection":
            # Process each feature in the collection
            for feature in geojson_obj.get("features", []):
                extract_features(feature, features)

        elif obj_type in [
            "Polygon",
            "Point",
            "MultiPolygon",
            "LineString",
            "MultiPoint",
            "MultiLineString",
        ]:
            # This is a raw geometry - create feature with empty properties
            features.append(Feature(geometry=geojson_obj, properties={}))

        elif obj_type == "GeometryCollection":
            # Handle geometry collections
            for geom in geojson_obj.get("geometries", []):
                extract_features(geom, features)

    elif isinstance(geojson_obj, list):
        # Handle lists of features/geometries
        for item in geojson_obj:
            extract_features(item, features)


def create_feature_collection(geojson_obj: Any) -> FeatureCollection:
    """
    Creates a FeatureCollection from a GeoJSON object.

    :param geojson_obj: GeoJSON object
    :return: GeoJSON FeatureCollection
    """
    features = []
    extract_features(geojson_obj, features)
    return FeatureCollection(features)


def convert_csv_to_geojson(
    csv_filepath: str, geojson_filepath: str, geo_column: str = "geo"
):
    """
    Convert a CSV file with a geo column into a GeoJSON file.

    Parameters
    ----------
    csv_filepath : str
        The filepath to the input CSV file.
    geojson_filepath : str
        The filepath to save the output GeoJSON file.
    geo_column : str, optional
        The name of the column containing GeoJSON geometries, by default "geo".

    Returns
    -------
    None
    """
    try:
        df = pd.read_csv(csv_filepath)

        convert_df_to_geojson(df, geojson_filepath, geo_column)

    except Exception as e:
        print(f"An error occurred while converting CSV to GeoJSON: {e}")


def convert_df_to_geojson(
    df: pd.DataFrame, geojson_filepath: str, geo_column: str = "geo"
):
    """
    Convert a DataFrame with a geo column into a GeoJSON file.

    Parameters
    ----------
    df : pd.DataFrame
        The DataFrame containing the data.
    geojson_filepath : str
        The filepath to save the output GeoJSON file.
    geo_column : str, optional
        The name of the column containing GeoJSON geometries, by default "geo".

    Returns
    -------
    None
    """
    try:
        if geo_column not in df.columns:
            raise ValueError(f"Geo column '{geo_column}' not found in the DataFrame.")

        features = []

        for index, row in df.iterrows():
            try:
                geojson_str = row[geo_column]

                geojson_str = geojson_str.replace("'", '"')

                geometry = geojson.loads(geojson_str)

                properties = row.drop(geo_column).to_dict()
                properties = {
                    k: (v if pd.notna(v) else None) for k, v in properties.items()
                }

                feature = geojson.Feature(geometry=geometry, properties=properties)

                features.append(feature)
            except Exception as e:
                continue

        feature_collection = geojson.FeatureCollection(features)

        with open(geojson_filepath, "w") as f:
            geojson.dump(feature_collection, f, indent=2)

        print(f"GeoJSON saved to {geojson_filepath}")

    except Exception as e:
        print(f"An error occurred while converting DataFrame to GeoJSON: {e}")

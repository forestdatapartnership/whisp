from typing import List, Any
from geojson import Feature, FeatureCollection, Polygon, Point

# import requests
import json

import geojson

import os

import geopandas as gpd
import ee
from shapely.geometry import shape
from pathlib import Path


def ee_to_geojson(ee_object, filename=None, indent=2, **kwargs):
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


def geojson_path_to_ee(geojson_filepath: Any) -> ee.FeatureCollection:
    """
    Reads a GeoJSON file from the given path and converts it to an Earth Engine FeatureCollection.

    Args:
        geojson_filepath (Any): The filepath to the GeoJSON file.

    Returns:
        ee.FeatureCollection: Earth Engine FeatureCollection created from the GeoJSON.
    """
    print(f"Received geojson_filepath: {geojson_filepath}")
    if isinstance(geojson_filepath, (str, Path)):
        # Read the GeoJSON file
        file_path = os.path.abspath(geojson_filepath)
        print(f"Reading GeoJSON file from: {file_path}")

        # Read the GeoJSON file
        with open(file_path, "r") as f:
            geojson_data = json.load(f)
    else:
        raise ValueError("Input must be a file path (str or Path)")

    # Validate the GeoJSON data
    validation_errors = validate_geojson(geojson_filepath)
    if validation_errors:
        raise ValueError(f"GeoJSON validation errors: {validation_errors}")

    # Create a FeatureCollection from the GeoJSON data
    # using the multipolygon splitting function extract_features
    feature_collection = ee.FeatureCollection(create_feature_collection(geojson_data))

    return feature_collection


def geojson_to_shapefile(geojson_path, shapefile_output_path):
    """
    Convert a GeoJSON file to a Shapefile and save it to a file.

    Parameters:
    - geojson_path: Path to the GeoJSON file.
    - shapefile_output_path: Path where the Shapefile will be saved.
    """
    # Read the GeoJSON file
    with open(geojson_path, "r") as geojson_file:
        geojson = json.load(geojson_file)

    # Convert to GeoDataFrame
    gdf = gpd.GeoDataFrame.from_features(geojson["features"])

    # Save to Shapefile
    gdf.to_file(shapefile_output_path, driver="ESRI Shapefile")


def shapefile_to_geojson(shapefile_path, geojson_output_path):
    """
    Convert a Shapefile to GeoJSON and save it to a file.

    Parameters:
    - shapefile_path: Path to the Shapefile.
    - geojson_output_path: Path where the GeoJSON file will be saved.
    """
    # Read the Shapefile
    gdf = gpd.read_file(shapefile_path)

    # Convert to GeoJSON
    geojson_str = gdf.to_json()

    # Write the GeoJSON string to a file
    with open(geojson_output_path, "w") as geojson_file:
        geojson_file.write(geojson_str)


def shapefile_to_ee(shapefile_path):
    """
    Convert a zipped shapefile to an Earth Engine FeatureCollection.
    NB Making this as existing Geemap function shp_to_ee wouldnt work.
    Args:
    - shapefile_path (str): Path to the shapefile (.zip) to be converted.

    Returns:
    - ee.FeatureCollection: Earth Engine FeatureCollection created from the shapefile.
    """
    # Unzip the shapefile
    # with zipfile.ZipFile(shapefile_path, "r") as zip_ref:
    #     zip_ref.extractall("shapefile")

    # Load the shapefile into a GeoDataFrame
    gdf = gpd.read_file(shapefile_path)  # "shapefile/test_ceo_all.shp")

    # Convert GeoDataFrame to GeoJSON
    geo_json = gdf.to_json()

    # Create a FeatureCollection from GeoJSON
    roi = ee.FeatureCollection(json.loads(geo_json))

    return roi


def ee_to_df(
    ee_object,
    columns=None,
    remove_geom=True,
    sort_columns=False,
    **kwargs,
):
    """Converts an ee.FeatureCollection to pandas dataframe.

    Args:
        ee_object (ee.FeatureCollection): ee.FeatureCollection.
        columns (list): List of column names. Defaults to None.
        remove_geom (bool): Whether to remove the geometry column. Defaults to True.
        sort_columns (bool): Whether to sort the column names. Defaults to False.
        kwargs: Additional arguments passed to ee.data.computeFeature.

    Raises:
        TypeError: ee_object must be an ee.FeatureCollection

    Returns:
        pd.DataFrame: pandas DataFrame
    """
    if isinstance(ee_object, ee.Feature):
        ee_object = ee.FeatureCollection([ee_object])

    if not isinstance(ee_object, ee.FeatureCollection):
        raise TypeError("ee_object must be an ee.FeatureCollection")

    try:
        if remove_geom:
            data = ee_object.map(
                lambda f: ee.Feature(None, f.toDictionary(f.propertyNames().sort()))
            )
        else:
            data = ee_object

        kwargs["expression"] = data
        kwargs["fileFormat"] = "PANDAS_DATAFRAME"

        df = ee.data.computeFeatures(kwargs)

        if isinstance(columns, list):
            df = df[columns]

        if remove_geom and ("geo" in df.columns):
            df = df.drop(columns=["geo"], axis=1)

        if sort_columns:
            df = df.reindex(sorted(df.columns), axis=1)

        return df
    except Exception as e:
        raise Exception(e)


def ee_to_shapefile(feature_collection, shapefile_path):
    """
    Export an Earth Engine FeatureCollection to a shapefile.

    Parameters:
    - feature_collection: Earth Engine FeatureCollection to be exported.
    - shapefile_path: Path to save the shapefile.

    Returns:
    - Path to the saved shapefile.
    """

    # Initialize Earth Engine
    # ee.Initialize()

    # Convert FeatureCollection to GeoJSON object using the function
    geojson = ee_to_geojson(feature_collection)

    # Convert GeoJSON features to GeoPandas GeoDataFrame
    features = geojson["features"]
    geoms = [shape(feature["geometry"]) for feature in features]
    properties = [feature["properties"] for feature in features]
    gdf = gpd.GeoDataFrame(properties, geometry=geoms)

    # Save GeoDataFrame as shapefile
    gdf.to_file(shapefile_path, driver="ESRI Shapefile")

    print(f"Shapefile saved to {shapefile_path}")

    return shapefile_path


###########from Whisp-app code here to align approaches (05/11/2024):
# https://github.com/forestdatapartnership/whisp-app/blob/main/src/utils/geojsonUtils.ts
# #####converted to python


def validate_geojson(input_data: Any) -> List[str]:
    """
    Validates GeoJSON data and filters out certain non-critical errors.

    :param input_data: GeoJSON data as a string or a file path
    :return: List of validation errors
    """
    errors = []

    # Check if input_data is a file path
    if isinstance(input_data, (str, Path)):
        try:
            # Read the GeoJSON file
            with open(input_data, "r") as f:
                geojson_data = f.read()
        except Exception as e:
            errors.append(f"Error reading file: {e}")
            return errors
    else:
        geojson_data = input_data

    try:
        geojson_obj = json.loads(geojson_data)
        if "type" not in geojson_obj:
            errors.append("Missing 'type' field in GeoJSON.")
    except ValueError as e:
        errors.append(f"Invalid GeoJSON: {e}")

    return errors


def extract_features(geometry: Any, features: List[Feature]) -> None:
    """
    Recursively extracts features from a geometry and adds them to the feature list.

    :param geometry: GeoJSON geometry
    :param features: List of extracted features
    """
    if geometry["type"] == "Polygon":
        features.append(Feature(geometry=Polygon(geometry["coordinates"])))
    elif geometry["type"] == "Point":
        features.append(Feature(geometry=Point(geometry["coordinates"])))
    elif geometry["type"] == "MultiPolygon":
        for polygon in geometry["coordinates"]:
            features.append(Feature(geometry=Polygon(polygon)))
    elif geometry["type"] == "GeometryCollection":
        for geom in geometry["geometries"]:
            extract_features(geom, features)
    elif geometry["type"] == "Feature":
        extract_features(geometry["geometry"], features)
    elif geometry["type"] == "FeatureCollection":
        for feature in geometry["features"]:
            extract_features(feature, features)


def create_feature_collection(geojson_obj: Any) -> FeatureCollection:
    """
    Creates a FeatureCollection from a GeoJSON object.

    :param geojson_obj: GeoJSON object
    :return: GeoJSON FeatureCollection
    """
    features = []
    extract_features(geojson_obj, features)
    return FeatureCollection(features)

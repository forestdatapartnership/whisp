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
    ee.Initialize()

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


## not working
# # flexible input (geojson file path or geojson object)
# def geojson_to_ee(input_data: Union[str, Path, dict], encoding='utf-8') -> ee.FeatureCollection:
#     """
#     Converts a GeoJSON object or a GeoJSON file path to an Earth Engine FeatureCollection.

#     Args:
#         input_data (str, Path, or dict): The GeoJSON file path or GeoJSON object.
#         encoding (str): The encoding of the GeoJSON file. Default is 'utf-8'.

#     Returns:
#         ee.FeatureCollection: Earth Engine FeatureCollection created from the GeoJSON.
#     """
#     print(f"Received input_data: {input_data}")
#     if isinstance(input_data, (str, Path)):
#         # Read the GeoJSON file
#         file_path = os.path.abspath(input_data)
#         print(f"Reading GeoJSON file from: {file_path}")
#         with open(file_path, 'r', encoding=encoding) as f:
#             geojson_data = json.load(f)
#     elif isinstance(input_data, dict):
#         geojson_data = input_data
#     else:
#         raise ValueError("Input must be a file path (str or Path) or a GeoJSON object (dict)")

#     # Create a FeatureCollection from the GeoJSON data
#     feature_collection = create_feature_collection(geojson_data)

#     return ee.FeatureCollection(feature_collection)


# def add_property_to_features(
#     feature_collection: FeatureCollection, property_name: str, property_value: Any
# ) -> FeatureCollection:
#     """
#     Adds a property to each feature in a FeatureCollection.

#     :param feature_collection: GeoJSON FeatureCollection
#     :param property_name: Name of the property to add
#     :param property_value: Value of the property
#     :return: Modified FeatureCollection with added property
#     """
#     modified_features = []
#     for feature in feature_collection["features"]:
#         feature["properties"] = feature.get("properties", {})
#         feature["properties"][property_name] = property_value
#         modified_features.append(feature)

#     return FeatureCollection(modified_features)


# def feature_to_wkt(feature):
#     """
#     Convert an ee.Feature with a geometry to a WKT representation.

#     Parameters:
#     - feature: An ee.Feature with a geometry.

#     Returns:
#     - wkt: The WKT representation of the geometry.
#     """

#     # Extract the geometry of the feature (polygon)
#     geometry = feature.geometry()

#     wkt = geometry_to_wkt(geometry)

#     return wkt


# def geometry_to_wkt(geometry):
#     """
#     Convert an ee.Feature with a polygon geometry to a WKT representation.

#     Parameters:
#     - feature: An ee.Feature with a polygon geometry.

#     Returns:
#     - wkt: The WKT representation of the polygon geometry.
#     """

#     # Get the coordinates of the polygon as a nested list
#     coordinates = geometry.coordinates().getInfo()[0]

#     # Construct the WKT string
#     wkt = coordinates_to_wkt(coordinates)
#     #'POLYGON((' + ', '.join([f'{lon} {lat}' for lon, lat in coordinates]) + '))'

#     return wkt


# def coordinates_to_wkt(coordinates):
#     """client side coordinates list"""
#     wkt = "POLYGON((" + ", ".join([f"{lon} {lat}" for lon, lat in coordinates]) + "))"
#     return wkt


# def collection_properties_to_df(collection, property_selection=None):
#     """creates a pandas dataframe from feature collection properties. NB SLOW but functions >5000 rows (unlike geemap_to_df)"""
#     nested_list = []

#     if property_selection is None:
#         collection_properties_list = collection.first().propertyNames().getInfo()
#     else:
#         collection_properties_list = property_selection

#     for property in collection_properties_list:
#         nested_list.append(collection.aggregate_array(property).getInfo())

#     nested_list_transposed = list(map(list, zip(*nested_list)))

#     return pd.DataFrame(data=nested_list_transposed, columns=collection_properties_list)


################not using as aligning with the Whisp-app/Whisp-api implementation (as splits multipolygons up)
# # adapted from geemap version https://geemap.org/common/#geemap.common.geojson_to_ee
# ## but to work on geojson path and remove altitiude
# def geojson_path_to_ee(file_path_geojson, geodesic=False, encoding="utf-8"):
#     """
#     Converts a geojson file to ee.FeatureCollection or ee.Geometry, stripping altitude information.
#     Code adapted from geemap version https://geemap.org/common/#geemap.common.geojson_to_ee
#     but to work on geojson path and remove altitiude.

#     Args:
#         file_path_geojson (str): The file path to a geojson file.
#         geodesic (bool, optional): Whether line segments should be interpreted as spherical geodesics. Defaults to False.
#         encoding (str, optional): The encoding of the geojson file. Defaults to "utf-8".

#     Returns:
#         ee_object: An ee.FeatureCollection or ee.Geometry object.
#     """

#     try:
#         # Read the geojson file
#         with open(os.path.abspath(file_path_geojson), encoding=encoding) as f:
#             geo_json = json.load(f)

#         # Helper function to remove altitude from coordinates
#         def remove_altitude(coords):
#             if isinstance(coords[0], list):  # Multi-coordinates
#                 return [remove_altitude(coord) for coord in coords]
#             else:
#                 return coords[
#                     :2
#                 ]  # Keep only the first two elements (longitude and latitude)

#         # Handle the geojson
#         if geo_json["type"] == "FeatureCollection":
#             for feature in geo_json["features"]:
#                 feature["geometry"]["coordinates"] = remove_altitude(
#                     feature["geometry"]["coordinates"]
#                 )
#                 if feature["geometry"]["type"] != "Point":
#                     feature["geometry"]["geodesic"] = geodesic
#             features = ee.FeatureCollection(geo_json)
#             return features

#         elif geo_json["type"] == "Feature":
#             geom = None
#             geometry = geo_json["geometry"]
#             geometry["coordinates"] = remove_altitude(geometry["coordinates"])
#             geom_type = geometry["type"]
#             if geom_type == "Point":
#                 geom = ee.Geometry.Point(geometry["coordinates"])
#             else:
#                 geom = ee.Geometry(geometry, "", geodesic)
#             return geom

#         elif geo_json["type"] == "GeometryCollection":
#             geometries = geo_json["geometries"]
#             for geometry in geometries:
#                 geometry["coordinates"] = remove_altitude(geometry["coordinates"])
#             ee_geometries = [ee.Geometry(geometry) for geometry in geometries]
#             return ee.FeatureCollection(ee_geometries)

#         else:
#             raise Exception(
#                 "Could not convert the geojson to ee.Geometry() or ee.FeatureCollection()"
#             )

#     except Exception as e:
#         print(
#             "Could not convert the geojson to ee.Geometry() or ee.FeatureCollection()"
#         )
#         raise Exception(e)

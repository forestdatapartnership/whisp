# import requests
import json

# import geojson
# import ee
# import geemap
import os

import geopandas as gpd
import ee
from shapely.geometry import shape

# import shutil
# import sys
# import pandas as pd
# from typing import Union, List


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


# adapted from geemap version https://geemap.org/common/#geemap.common.geojson_to_ee
## but to work on geojson path and remove altitiude
def geojson_to_ee(file_path_geojson, geodesic=False, encoding="utf-8"):
    """
    Converts a geojson file to ee.FeatureCollection or ee.Geometry, stripping altitude information.
    Code adapted from geemap version https://geemap.org/common/#geemap.common.geojson_to_ee
    but to work on geojson path and remove altitiude.

    Args:
        file_path_geojson (str): The file path to a geojson file.
        geodesic (bool, optional): Whether line segments should be interpreted as spherical geodesics. Defaults to False.
        encoding (str, optional): The encoding of the geojson file. Defaults to "utf-8".

    Returns:
        ee_object: An ee.FeatureCollection or ee.Geometry object.
    """

    try:
        # Read the geojson file
        with open(os.path.abspath(file_path_geojson), encoding=encoding) as f:
            geo_json = json.load(f)

        # Helper function to remove altitude from coordinates
        def remove_altitude(coords):
            if isinstance(coords[0], list):  # Multi-coordinates
                return [remove_altitude(coord) for coord in coords]
            else:
                return coords[
                    :2
                ]  # Keep only the first two elements (longitude and latitude)

        # Handle the geojson
        if geo_json["type"] == "FeatureCollection":
            for feature in geo_json["features"]:
                feature["geometry"]["coordinates"] = remove_altitude(
                    feature["geometry"]["coordinates"]
                )
                if feature["geometry"]["type"] != "Point":
                    feature["geometry"]["geodesic"] = geodesic
            features = ee.FeatureCollection(geo_json)
            return features

        elif geo_json["type"] == "Feature":
            geom = None
            geometry = geo_json["geometry"]
            geometry["coordinates"] = remove_altitude(geometry["coordinates"])
            geom_type = geometry["type"]
            if geom_type == "Point":
                geom = ee.Geometry.Point(geometry["coordinates"])
            else:
                geom = ee.Geometry(geometry, "", geodesic)
            return geom

        elif geo_json["type"] == "GeometryCollection":
            geometries = geo_json["geometries"]
            for geometry in geometries:
                geometry["coordinates"] = remove_altitude(geometry["coordinates"])
            ee_geometries = [ee.Geometry(geometry) for geometry in geometries]
            return ee.FeatureCollection(ee_geometries)

        else:
            raise Exception(
                "Could not convert the geojson to ee.Geometry() or ee.FeatureCollection()"
            )

    except Exception as e:
        print(
            "Could not convert the geojson to ee.Geometry() or ee.FeatureCollection()"
        )
        raise Exception(e)


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

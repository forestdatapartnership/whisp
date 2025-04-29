import os
import ee
import pandas as pd
from pathlib import Path
from .datasets import combine_datasets
import json
import country_converter as coco

import logging
import time
import requests
import geopandas as gpd

# import glob
import pandas as pd
from pathlib import Path
from datetime import datetime
import random
import math
import numpy as np
from shapely.geometry import Polygon
from shapely.validation import make_valid
from shapely.geometry import mapping
from exactextract import exact_extract
import concurrent.futures
from rio_vrt import build_vrt
from exactextract import exact_extract
import concurrent.futures
import uuid

from openforis_whisp.parameters.config_runtime import (
    percent_or_ha,
    plot_id_column,
    geo_id_column,
    geometry_type_column,
    geometry_area_column,
    geometry_area_column_formatting,
    centroid_x_coord_column,
    centroid_y_coord_column,
    iso3_country_column,
    iso2_country_column,
    admin_1_column,
    stats_unit_type_column,
    stats_area_columns_formatting,
    stats_percent_columns_formatting,
    water_flag,
)

from .data_conversion import (
    convert_ee_to_df,
    convert_geojson_to_ee,
    convert_ee_to_geojson,
    # convert_csv_to_geojson,
    convert_df_to_geojson,
)  # copied functions from whisp-api and geemap (accessed 2024) to avoid dependency
from .reformat import validate_dataframe_using_lookups

# NB functions that included "formatted" in the name apply a schema for validation and reformatting of the output dataframe. The schema is created from lookup tables.


# Configure logging
def setup_logging(log_level=logging.INFO):
    """Set up logging with consistent formatting."""
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    return logging.getLogger("whisp_processor")


logger = setup_logging()

###############################################
# functions for GEE stats processing (server side)
###############################################


def whisp_formatted_stats_geojson_to_df(
    input_geojson_filepath: Path | str,
    external_id_column=None,  # This variable is expected to be a string or None
    remove_geom=False,
) -> pd.DataFrame:
    """
    Main function for most users.
    Converts a GeoJSON file to a pandas DataFrame containing Whisp stats for the input ROI.
    Output df is validated against a panderas schema (created on the fly from the two lookup CSVs).

    This function first converts the provided GeoJSON file into an Earth Engine FeatureCollection.
    It then processes the FeatureCollection to extract relevant Whisp statistics,
    returning a structured DataFrame that aligns with the expected schema.

    If `external_id_column` is provided, it will be used to link external identifiers
    from the input GeoJSON to the output DataFrame.

    Parameters
    ----------
    input_geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.
    external_id_column : str, optional
        The column in the GeoJSON containing external IDs to be preserved in the output DataFrame in the external_id column.
    remove_geom : bool, default=True
        If True, the geometry of the GeoJSON is removed from the output DataFrame.

    Returns
    -------
    df_stats : pd.DataFrame
        The DataFrame containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_formatted_stats_ee_to_df(
        feature_collection, external_id_column, remove_geom
    )


def whisp_formatted_stats_geojson_to_geojson(
    input_geojson_filepath,
    output_geojson_filepath,
    external_id_column=None,
    geo_column: str = "geo",
):
    """
    Convert a formatted GeoJSON file with a geo column into a GeoJSON file containing Whisp stats.

    Parameters
    ----------
    input_geojson_filepath : str
        The filepath to the input GeoJSON file.
    output_geojson_filepath : str
        The filepath to save the output GeoJSON file.
    external_id_column : str, optional
        The name of the column containing external IDs, by default None.
    geo_column : str, optional
        The name of the column containing GeoJSON geometries, by default "geo".

    Returns
    -------
    None
    """
    df = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
    )
    # Convert the df to GeoJSON
    convert_df_to_geojson(df, output_geojson_filepath, geo_column)

    print(f"GeoJSON with Whisp stats saved to {output_geojson_filepath}")


def whisp_formatted_stats_ee_to_geojson(
    feature_collection: ee.FeatureCollection,
    output_geojson_filepath: str,
    external_id_column=None,  # This variable is expected to be a string or None
    geo_column: str = "geo",
):
    """
    Convert an Earth Engine FeatureCollection to a GeoJSON file containing Whisp stats.

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The feature collection of the ROI to analyze.
    output_geojson_filepath : str
        The filepath to save the output GeoJSON file.
    external_id_column : str, optional
        The name of the column containing external IDs, by default None.
    remove_geom : bool, optional
        Whether to remove the geometry column, by default False.
    geo_column : str, optional
        The name of the column containing GeoJSON geometries, by default "geo".

    Returns
    -------
    None
    """
    # Convert ee feature collection to a pandas dataframe
    df_stats = whisp_formatted_stats_ee_to_df(feature_collection, external_id_column)

    # Convert the df to GeoJSON
    convert_df_to_geojson(df_stats, output_geojson_filepath, geo_column)

    print(f"GeoJSON with Whisp stats saved to {output_geojson_filepath}")


def whisp_formatted_stats_ee_to_df(
    feature_collection: ee.FeatureCollection,
    external_id_column=None,  # This variable is expected to be a string or None
    remove_geom=False,
) -> pd.DataFrame:
    """
    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The feature collection of the ROI to analyze.

    Returns
    -------
    validated_df : pd.DataFrame
        The validated dataframe containing the Whisp stats for the input ROI.
    """
    # Convert ee feature collection to a pandas dataframe
    df_stats = whisp_stats_ee_to_df(feature_collection, external_id_column, remove_geom)

    validated_df = validate_dataframe_using_lookups(df_stats)
    return validated_df


### functions without additional formatting below (i.e., raw output from GEE processing without schema validation step)


def whisp_stats_geojson_to_df(
    input_geojson_filepath: Path | str,
    external_id_column=None,  # This variable is expected to be a string or None
    remove_geom=False,
) -> pd.DataFrame:
    """

    Parameters
    ----------
    input_geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_stats_ee_to_df(feature_collection, external_id_column, remove_geom)


def whisp_stats_geojson_to_ee(
    input_geojson_filepath: Path | str,
    external_id_column=None,  # This variable is expected to be a string or None
) -> ee.FeatureCollection:
    """

    Parameters
    ----------
    input_geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_stats_ee_to_ee(feature_collection, external_id_column)


def whisp_stats_geojson_to_geojson(
    input_geojson_filepath, output_geojson_filepath, external_id_column=None
):
    """
    Convert a GeoJSON file to a GeoJSON object containing Whisp stats for the input ROI.

    Parameters
    ----------
    input_geojson_filepath : str
        The filepath to the input GeoJSON file.
    output_geojson_filepath : str
        The filepath to save the output GeoJSON file.
    external_id_column : str, optional
        The name of the external ID column, by default None.

    Returns
    -------
    None
    """
    # Convert GeoJSON to Earth Engine FeatureCollection
    feature_collection = convert_geojson_to_ee(input_geojson_filepath)

    # Get stats as a FeatureCollection
    stats_feature_collection = whisp_stats_ee_to_ee(
        feature_collection, external_id_column
    )

    # Convert the stats FeatureCollection to GeoJSON
    stats_geojson = convert_ee_to_geojson(stats_feature_collection)

    # Save the GeoJSON to a file
    with open(output_geojson_filepath, "w") as f:
        json.dump(stats_geojson, f, indent=2)


def whisp_stats_geojson_to_drive(
    input_geojson_filepath: Path | str,
    external_id_column=None,  # This variable is expected to be a string or None
):
    """
    Parameters
    ----------
    geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    Message showing location of file in Google Drive
    """

    try:
        input_geojson_filepath = Path(input_geojson_filepath)
        if not input_geojson_filepath.exists():
            raise FileNotFoundError(f"File {input_geojson_filepath} does not exist.")

        # Assuming geojson_to_ee is properly imported from data_conversion.py
        feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

        return whisp_stats_ee_to_drive(feature_collection, external_id_column)

    except Exception as e:
        print(f"An error occurred: {e}")


def whisp_stats_ee_to_ee(feature_collection, external_id_column):
    """
    Process a feature collection to get statistics for each feature.

    Parameters:
        feature_collection (ee.FeatureCollection): The input feature collection.
        external_id_column (str): The name of the external ID column to check.

    Returns:
        ee.FeatureCollection: The output feature collection with statistics.
    """

    if external_id_column is not None:
        try:
            # Check if external_id_column is a property in feature_collection (server-side)
            def check_column_exists(feature):
                return ee.Algorithms.If(
                    feature.propertyNames().contains(external_id_column),
                    feature,
                    ee.Feature(
                        None
                    ),  # Return an empty feature if the column does not exist
                )

            feature_collection_with_check = feature_collection.map(check_column_exists)
            size_fc = feature_collection.size()
            valid_feature_count = feature_collection_with_check.filter(
                ee.Filter.notNull([external_id_column])
            ).size()

            # Raise an error if the column does not exist in any feature
            if valid_feature_count.neq(size_fc).getInfo():
                raise ValueError(
                    f"The column '{external_id_column}' is not a property throughout the feature collection."
                )

            # Set the geo_id_column
            feature_collection = feature_collection.map(
                lambda feature: feature.set(
                    geo_id_column, ee.String(feature.get(external_id_column))
                )
            )

        except Exception as e:
            # Handle the exception and provide a helpful error message
            print(
                f"An error occurred when trying to set the external_id_column: {external_id_column}. Error: {e}"
            )

    fc = get_stats(feature_collection)

    return add_id_to_feature_collection(dataset=fc, id_name=plot_id_column)


def whisp_stats_ee_to_df(
    feature_collection: ee.FeatureCollection,
    external_id_column=None,  # This variable is expected to be a string or None
    remove_geom=False,
) -> pd.DataFrame:
    """
    Convert a Google Earth Engine FeatureCollection to a pandas DataFrame and convert ISO3 to ISO2 country codes.

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The input FeatureCollection to analyze.
    external_id_column : str, optional
        The name of the external ID column, by default None.
    remove_geom : bool, optional
        Whether to remove the geometry column, by default True.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    try:
        df_stats = convert_ee_to_df(
            ee_object=whisp_stats_ee_to_ee(feature_collection, external_id_column),
            remove_geom=remove_geom,
        )
    except Exception as e:
        print(f"An error occurred during the conversion from EE to DataFrame: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    try:
        df_stats = convert_iso3_to_iso2(
            df=df_stats,
            iso3_column=iso3_country_column,
            iso2_column=iso2_country_column,
        )
    except Exception as e:
        print(f"An error occurred during the ISO3 to ISO2 conversion: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    return df_stats


def whisp_stats_ee_to_drive(
    feature_collection: ee.FeatureCollection, external_id_column=None
):

    try:
        task = ee.batch.Export.table.toDrive(
            collection=whisp_stats_ee_to_ee(feature_collection, external_id_column),
            description="whisp_output_table",
            # folder="whisp_results",
            fileFormat="CSV",
        )
        task.start()
        print(
            "Exporting to Google Drive: 'whisp_results/whisp_output_table.csv'. To track progress: https://code.earthengine.google.com/tasks"
        )
    except Exception as e:
        print(f"An error occurred during the export: {e}")


#### main stats functions

# Get stats for a feature or feature collection
def get_stats(feature_or_feature_col):
    # Check if the input is a Feature or a FeatureCollection
    if isinstance(feature_or_feature_col, ee.Feature):
        # If the input is a Feature, call the server-side function for processing
        print("feature")
        output = ee.FeatureCollection([get_stats_feature(feature_or_feature_col)])
    elif isinstance(feature_or_feature_col, ee.FeatureCollection):
        # If the input is a FeatureCollection, call the server-side function for processing
        output = get_stats_fc(feature_or_feature_col)
    else:
        output = "Check inputs: not an ee.Feature or ee.FeatureCollection"
    return output


# Get statistics for a feature collection
def get_stats_fc(feature_col):

    img_combined = combine_datasets()  # imported function

    out_feature_col = ee.FeatureCollection(
        feature_col.map(lambda feature: get_stats_feature(feature, img_combined))
    )
    # print(out_feature_col.first().getInfo()) # for testing

    return out_feature_col


# Get statistics for a single feature
def get_stats_feature(feature, img_combined):

    # img_combined = combine_datasets()

    reduce = img_combined.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=feature.geometry(),
        scale=10,
        maxPixels=1e10,
        tileScale=8,
    )

    # Get basic feature information
    feature_info = get_type_and_location(feature)

    # add statistics unit type (e.g., percentage or hectares) to dictionary
    stats_unit_type = ee.Dictionary({stats_unit_type_column: percent_or_ha})

    # Now, modified_dict contains all keys with the prefix added
    reduce_ha = reduce.map(
        lambda key, val: divide_and_format(ee.Number(val), ee.Number(10000))
    )

    # Get value for hectares
    area_ha = ee.Number(ee.Dictionary(reduce_ha).get(geometry_area_column))

    # Apply the function to each value in the dictionary using map()
    reduce_percent = reduce_ha.map(
        lambda key, val: percent_and_format(ee.Number(val), area_ha)
    )

    # Reformat the hectare statistics
    reducer_stats_ha = reduce_ha.set(
        geometry_area_column, area_ha.format(geometry_area_column_formatting)
    )  # area ha (formatted)

    # Reformat the percentage statistics
    reducer_stats_percent = reduce_percent.set(
        geometry_area_column, area_ha.format(geometry_area_column_formatting)
    )  # area ha (formatted)

    # Add country info onto hectare analysis results
    properties_ha = feature_info.combine(ee.Dictionary(reducer_stats_ha)).combine(
        stats_unit_type
    )

    # Add country info onto percentage analysis results
    properties_percent = feature_info.combine(
        ee.Dictionary(reducer_stats_percent)
    ).combine(stats_unit_type)

    # Choose whether to use hectares or percentage based on the `percent_or_ha` variable
    out_feature = ee.Algorithms.If(
        percent_or_ha == "ha",
        feature.set(properties_ha),  # .setGeometry(None),
        feature.set(properties_percent),  # .setGeometry(None),
    )

    return out_feature


# Get basic feature information - uses admin and water datasets in gee.
def get_type_and_location(feature):
    """Extracts basic feature information including country, admin area, geometry type, coordinates, and water flags."""

    # Get centroid of the feature's geometry
    centroid = feature.geometry().centroid(1)

    # Fetch location info from geoboundaries (country, admin)
    location = ee.Dictionary(get_geoboundaries_info(centroid))
    country = ee.Dictionary({iso3_country_column: location.get("shapeGroup")})

    admin_1 = ee.Dictionary(
        {admin_1_column: location.get("shapeName")}
    )  # Administrative level 1 (if available)

    # Prepare the water flag information
    water_all = water_flag_all_prep()
    water_flag_dict = value_at_point_flag(
        point=centroid, image=water_all, band_name=water_flag, output_name=water_flag
    )

    # Get the geometry type of the feature
    geom_type = ee.Dictionary({geometry_type_column: feature.geometry().type()})

    # Get the coordinates (latitude, longitude) of the centroid
    coords_list = centroid.coordinates()
    coords_dict = ee.Dictionary(
        {
            centroid_x_coord_column: coords_list.get(0),  # Longitude
            centroid_y_coord_column: coords_list.get(1),  # Latitude
        }
    )

    # Combine all the extracted info into a single dictionary
    feature_info = (
        country.combine(admin_1)
        .combine(geom_type)
        .combine(coords_dict)
        .combine(water_flag_dict)
    )

    return feature_info


# Define a function to divide each value by 10,000 and format it with one decimal place
def divide_and_format(val, unit):
    # Convert the image to an ee.Number, divide by 10,000, and format with one decimal place
    formatted_value = ee.Number.parse(
        ee.Number(ee.Number(val).divide(ee.Number(unit))).format(
            stats_area_columns_formatting
        )
    )
    # Return the formatted value
    return ee.Number(formatted_value)


# Define a function to divide by total area of geometry and multiply by 100
def percent_and_format(val, area_ha):
    formatted_value = ee.Number.parse(
        ee.Number(ee.Number(val).divide(area_ha).multiply(ee.Number(100))).format(
            stats_percent_columns_formatting
        )
    )
    # Return the formatted value
    return ee.Number(formatted_value)


# geoboundaries - admin units from a freqently updated database, allows commercial use (CC BY 4.0 DEED) (disputed territories may need checking)
def get_geoboundaries_info(geometry):
    gbounds_ADM0 = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM1")
    polygonsIntersectPoint = gbounds_ADM0.filterBounds(geometry)
    backup_dict = ee.Dictionary({"shapeGroup": "Unknown", "shapeName": "Unknown"})
    return ee.Algorithms.If(
        polygonsIntersectPoint.size().gt(0),
        polygonsIntersectPoint.first()
        .toDictionary()
        .select(["shapeGroup", "shapeName"]),
        backup_dict,
    )


#####
# water flag - to flag plots that may be erroneous (i.e., where errors may have occured in their creation / translation  and so fall in either the ocean or inland water -
def usgs_gsv_ocean_prep():  # TO DO: for speed export image as an asset at samne res as JRC
    # Initialize the Earth Engine API
    # ee.Initialize()

    # Load the datasets
    mainlands = ee.FeatureCollection(
        "projects/sat-io/open-datasets/shoreline/mainlands"
    )
    big_islands = ee.FeatureCollection(
        "projects/sat-io/open-datasets/shoreline/big_islands"
    )
    small_islands = ee.FeatureCollection(
        "projects/sat-io/open-datasets/shoreline/small_islands"
    )

    # Combine the datasets into one FeatureCollection
    gsv = ee.FeatureCollection([mainlands, big_islands, small_islands]).flatten()

    # Rasterize the combined FeatureCollection and make areas outside coast (i.e. ocean) as value 1
    # and then rename the band
    return ee.Image(1).paint(gsv).selfMask().rename("ocean")


def jrc_water_surface_prep():
    jrc_surface_water = ee.Image("JRC/GSW1_4/GlobalSurfaceWater")

    # use transition band
    jrc_transition = jrc_surface_water.select("transition")

    # select permanent water bodies:
    # remap the following classes to have a value of 1:
    # "Permanent", "New Permanent", and "Seasonal to Permanent" (i.e., classes 1,2 and 7).
    # All other classes as value 0.
    permanent_inland_water = jrc_transition.remap([1, 2, 7], [1, 1, 1], 0).unmask()

    # optional - clip to within coast line (not needed currently and extra processing)
    # permanent_inland_water = permanent_inland_water.where(usgs_gsv_ocean_prep(),0)

    return permanent_inland_water.rename("water_inland")


def water_flag_all_prep():
    # combine both where water surface is 1, then 1, else use non_land_gsv
    return (
        usgs_gsv_ocean_prep()
        .unmask()
        .where(jrc_water_surface_prep(), 1)
        .rename(water_flag)
    )


def value_at_point_flag(point, image, band_name, output_name):
    """Sample an image at the given point and make a dictionary output where the name is defined by output_name parameter"""
    sample = image.sample(region=point, scale=30, numPixels=1).first()

    # Get the value from the sampled point
    value = sample.get(band_name)  # assuming the band name is 'b1', change if necessary

    # Use a conditional statement to check if the value is 1
    result = value  # ee.Algorithms.If(ee.Number(value).eq(1), "True", "False")

    # Return the output dictionary
    return ee.Dictionary({output_name: result})  # .getInfo()


def add_id_to_feature_collection(dataset, id_name):
    """
    Adds an incremental (1,2,3 etc) 'id' property to each feature in the given FeatureCollection.

    Args:
    - dataset: ee.FeatureCollection, the FeatureCollection to operate on.

    Returns:
    - dataset_with_id: ee.FeatureCollection, the FeatureCollection with 'id' property added to each feature.
    """
    # Get the list of system:index values
    indexes = dataset.aggregate_array("system:index")

    # Create a sequence of numbers starting from 1 to the size of indexes
    ids = ee.List.sequence(1, indexes.size())

    # Create a dictionary mapping system:index to id
    id_by_index = ee.Dictionary.fromLists(indexes, ids)

    # Function to add 'id' property to each feature
    def add_id(feature):
        # Get the system:index of the feature
        system_index = feature.get("system:index")

        # Get the id corresponding to the system:index
        feature_id = id_by_index.get(system_index)

        # Set the 'id' property of the feature
        return feature.set(id_name, feature_id)

    # Map the add_id function over the dataset
    dataset_with_id = dataset.map(add_id)

    return dataset_with_id


# Function to add ID to features
def add_id_to_feature(feature, id_name):
    index = feature.get("system:index")
    return feature.set(id_name, index)


# Function to flag positive values
def flag_positive_values(feature, flag_positive):
    for prop_name in flag_positive:
        flag_value = ee.Algorithms.If(
            ee.Number(feature.get(prop_name)).gt(0), "True", "-"
        )
        feature = feature.set(prop_name, flag_value)
    return feature


# Function to exclude properties
def copy_properties_and_exclude(feature, exclude_properties_from_output):
    return ee.Feature(feature.geometry()).copyProperties(
        source=feature, exclude=exclude_properties_from_output
    )


def ee_image_checker(image):
    """
    Tests if the input is a valid ee.Image.

    Args:
        image: An ee.Image object.

    Returns:
        bool: True if the input is a valid ee.Image, False otherwise.
    """
    try:
        if ee.Algorithms.ObjectType(image).getInfo() == "Image":
            # Trigger some action on the image to ensure it's a valid image
            image.getInfo()  # This will raise an exception if the image is invalid
            return True
    except ee.EEException as e:
        print(f"Image validation failed with EEException: {e}")
    except Exception as e:
        print(f"Image validation failed with exception: {e}")
    return False


def keep_valid_images(image_list):
    """
    Filters a list to return only valid ee.Images.

    Args:
        image_list: List of ee.Image objects.

    Returns:
        list: List of valid ee.Image objects.
    """
    valid_imgs = []
    for image in image_list:
        if ee_image_checker(image):
            valid_imgs.append(image)
    return valid_imgs


def convert_iso3_to_iso2(df, iso3_column, iso2_column):
    """
    Converts ISO3 country codes to ISO2 codes and adds a new column to the DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame containing ISO3 country codes.
        iso3_column (str): The column name in the DataFrame with ISO3 country codes.
        iso2_column (str): The new column name to store ISO2 country codes.

    Returns:
        pd.DataFrame: Updated DataFrame with the new ISO2 column.
    """
    import country_converter as coco

    # Apply conversion from ISO3 to ISO2
    df[iso2_column] = df[iso3_column].apply(
        lambda x: coco.convert(names=x, to="ISO2")
        if x
        else "not found (disputed territory)"
    )

    return df


########################################
# Client side downloading and processing
##########################################


def generate_random_polygon(
    min_lon, min_lat, max_lon, max_lat, min_area_ha=1, max_area_ha=10, vertex_count=20
):
    """
    Generate a random polygon within bounds with approximate area in the specified range.
    Uses a robust approach that works well with high vertex counts and never falls back to squares.

    Args:
        min_lon, min_lat, max_lon, max_lat: Boundary coordinates
        min_area_ha: Minimum area in hectares
        max_area_ha: Maximum area in hectares
        vertex_count: Number of vertices for the polygon
    """
    # Initialize variables to ensure they're always defined
    poly = None
    actual_area_ha = 0

    # Simple function to approximate area in hectares (much faster)
    def approximate_area_ha(polygon, center_lat):
        # Get area in square degrees
        area_sq_degrees = polygon.area

        # Approximate conversion factor from square degrees to hectares
        # This varies with latitude due to the Earth's curvature
        lat_factor = 111320  # meters per degree latitude (approximately)
        lon_factor = 111320 * math.cos(
            math.radians(center_lat)
        )  # meters per degree longitude

        # Convert to square meters, then to hectares (1 ha = 10,000 sq m)
        return area_sq_degrees * lat_factor * lon_factor / 10000

    # Target area in hectares
    target_area_ha = random.uniform(min_area_ha, max_area_ha)

    # Select a center point within the bounds
    center_lon = random.uniform(min_lon, max_lon)
    center_lat = random.uniform(min_lat, max_lat)

    # Initial size estimate (in degrees)
    # Rough approximation: 0.01 degrees ~ 1km at equator
    initial_radius = math.sqrt(target_area_ha / (math.pi * 100)) * 0.01

    # Avoid generating too many points initially - cap vertex count for stability
    effective_vertex_count = min(
        vertex_count, 100
    )  # Cap at 100 to avoid performance issues

    # Primary approach: Create polygon using convex hull approach
    for attempt in range(5):  # First method gets 5 attempts
        try:
            # Generate random points in a circle around center with varying distance
            thetas = np.linspace(0, 2 * math.pi, effective_vertex_count, endpoint=False)

            # Add randomness to angles - smaller randomness for higher vertex counts
            angle_randomness = min(0.2, 2.0 / effective_vertex_count)
            thetas += np.random.uniform(
                -angle_randomness, angle_randomness, size=effective_vertex_count
            )

            # Randomize distances from center - less extreme for high vertex counts
            distance_factor = min(0.3, 3.0 / effective_vertex_count) + 0.7
            distances = initial_radius * np.random.uniform(
                1.0 - distance_factor / 2,
                1.0 + distance_factor / 2,
                size=effective_vertex_count,
            )

            # Convert to cartesian coordinates
            xs = center_lon + distances * np.cos(thetas)
            ys = center_lat + distances * np.sin(thetas)

            # Ensure points are within bounds
            xs = np.clip(xs, min_lon, max_lon)
            ys = np.clip(ys, min_lat, max_lat)

            # Create vertices list
            vertices = list(zip(xs, ys))

            # Close the polygon
            if vertices[0] != vertices[-1]:
                vertices.append(vertices[0])

            # Create polygon
            poly = Polygon(vertices)

            # Ensure it's valid
            if not poly.is_valid:
                poly = make_valid(poly)
                if poly.geom_type != "Polygon":
                    # If not a valid polygon, we'll try again
                    continue

            # Calculate approximate area
            actual_area_ha = approximate_area_ha(poly, center_lat)

            # Check if within target range
            if min_area_ha * 0.8 <= actual_area_ha <= max_area_ha * 1.2:
                return poly, actual_area_ha

            # Adjust size for next attempt based on ratio
            if actual_area_ha > 0:  # Avoid division by zero
                scale_factor = math.sqrt(target_area_ha / actual_area_ha)
                initial_radius *= scale_factor

        except Exception as e:
            print(f"Error in convex hull method (attempt {attempt+1}): {e}")

    # Second approach: Star-like pattern with controlled randomness
    # This is a fallback that will still create an irregular polygon, not a square
    for attempt in range(5):  # Second method gets 5 attempts
        try:
            # Use fewer vertices for stability in the fallback
            star_vertex_count = min(15, vertex_count)
            vertices = []

            # Create a star-like pattern with two radiuses
            for i in range(star_vertex_count):
                angle = 2 * math.pi * i / star_vertex_count

                # Alternate between two distances to create star-like shape
                if i % 2 == 0:
                    distance = initial_radius * random.uniform(0.7, 0.9)
                else:
                    distance = initial_radius * random.uniform(0.5, 0.6)

                # Add some irregularity to angles
                angle += random.uniform(-0.1, 0.1)

                # Calculate vertex position
                lon = center_lon + distance * math.cos(angle)
                lat = center_lat + distance * math.sin(angle)

                # Ensure within bounds
                lon = min(max(lon, min_lon), max_lon)
                lat = min(max(lat, min_lat), max_lat)

                vertices.append((lon, lat))

            # Close the polygon
            vertices.append(vertices[0])

            # Create polygon
            poly = Polygon(vertices)
            if not poly.is_valid:
                poly = make_valid(poly)
                if poly.geom_type != "Polygon":
                    continue

            actual_area_ha = approximate_area_ha(poly, center_lat)

            # We're less picky about size at this point, just return it
            if actual_area_ha > 0:
                return poly, actual_area_ha

            # Still try to adjust if we get another attempt
            if actual_area_ha > 0:
                scale_factor = math.sqrt(target_area_ha / actual_area_ha)
                initial_radius *= scale_factor

        except Exception as e:
            print(f"Error in star pattern method (attempt {attempt+1}): {e}")

    # Last resort - create a perturbed circle (never a square)
    try:
        # Create a circle-like shape with small perturbations
        final_vertices = []
        perturbed_vertex_count = 8  # Use a modest number for stability

        for i in range(perturbed_vertex_count):
            angle = 2 * math.pi * i / perturbed_vertex_count
            # Small perturbation
            distance = initial_radius * random.uniform(0.95, 1.05)

            # Calculate vertex position
            lon = center_lon + distance * math.cos(angle)
            lat = center_lat + distance * math.sin(angle)

            # Ensure within bounds
            lon = min(max(lon, min_lon), max_lon)
            lat = min(max(lat, min_lat), max_lat)

            final_vertices.append((lon, lat))

        # Close the polygon
        final_vertices.append(final_vertices[0])

        # Create polygon
        poly = Polygon(final_vertices)
        if not poly.is_valid:
            poly = make_valid(poly)

        actual_area_ha = approximate_area_ha(poly, center_lat)

    except Exception as e:
        print(f"Error in final fallback method: {e}")
        # If absolutely everything fails, create the simplest valid polygon (triangle)
        # This is different from a square and should be more compatible with your code
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

    # Return whatever we've created - never a simple square
    return poly, actual_area_ha


def generate_properties(area_ha, index):
    """
    Generate properties for features with sequential internal_id

    Args:
        area_ha: Area in hectares of the polygon
        index: Index of the feature to use for sequential ID
    """
    return {
        "internal_id": index + 1,  # Create sequential IDs starting from 1
    }


def create_geojson(
    bounds,
    num_polygons=25,
    min_area_ha=1,
    max_area_ha=10,
    min_number_vert=10,
    max_number_vert=20,
):
    """Create a GeoJSON file with random polygons within area range"""
    min_lon, min_lat, max_lon, max_lat = bounds
    # min_number_vert = 15
    # max_number_vert = 20

    features = []
    for i in range(num_polygons):
        # Random vertex count between 4 and 8
        # vertices = random.randint(4, 8)
        vertices = random.randint(min_number_vert, max_number_vert)

        # Generate polygon with area control
        polygon, actual_area = generate_random_polygon(
            min_lon,
            min_lat,
            max_lon,
            max_lat,
            min_area_ha=min_area_ha,
            max_area_ha=max_area_ha,
            vertex_count=vertices,
        )

        # Create GeoJSON feature with actual area
        properties = generate_properties(actual_area, index=i)
        feature = {
            "type": "Feature",
            "properties": properties,
            "geometry": mapping(polygon),
        }

        features.append(feature)

    # Create the GeoJSON feature collection
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
        GeoDataFrame with updated features
    """

    # Read the GeoJSON
    # print(f"Reading GeoJSON file: {geojson_path}")
    gdf = gpd.read_file(geojson_path)

    # Remove existing properties if requested
    if remove_properties:
        # Keep only the geometry column and drop all other columns
        gdf = gdf[["geometry"]].copy()
        # print(f"Removed all existing properties from features")

    # Add sequential numeric IDs
    gdf[id_field] = [i + start_index for i in range(len(gdf))]

    # Optionally add UUIDs
    if add_uuid:
        gdf["uuid"] = [str(uuid.uuid4()) for _ in range(len(gdf))]

    # Write the GeoJSON with added IDs
    output_path = output_path or geojson_path
    gdf.to_file(output_path, driver="GeoJSON")
    print(f"Added {id_field} to GeoJSON and saved to {output_path}")

    return None


# //Load GeoJSON features and add unique IDs
def convert_geojson_to_ee_bbox(geojson_filepath) -> ee.FeatureCollection:
    """
    Reads a GeoJSON file, creates bounding boxes for each feature,
    and converts to Earth Engine FeatureCollection.

    Args:
        geojson_filepath (Any): The filepath to the GeoJSON file.

    Returns:
        ee.FeatureCollection: Earth Engine FeatureCollection of bounding boxes.
    """
    # import os
    # from pathlib import Path
    # import geopandas as gpd
    # import ee

    # Read the GeoJSON file using geopandas
    if isinstance(geojson_filepath, (str, Path)):
        file_path = os.path.abspath(geojson_filepath)
        print(f"Reading GeoJSON file from: {file_path}")

        try:
            # Load GeoJSON directly with geopandas
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

    # Create a new GeoDataFrame with bounding boxes
    bbox_features = []
    for idx, row in gdf.iterrows():
        try:
            # Get the bounds of the geometry (minx, miny, maxx, maxy)
            minx, miny, maxx, maxy = row.geometry.bounds

            # Create an Earth Engine Rectangle geometry
            ee_geometry = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

            # Copy properties from the original feature
            properties = {col: row[col] for col in gdf.columns if col != "geometry"}

            # Convert numpy types to native Python types for proper serialization
            for key, value in properties.items():
                if hasattr(value, "item"):  # Check if it's a numpy type
                    properties[key] = value.item()  # Convert to Python native type
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

    # Create the Earth Engine FeatureCollection
    feature_collection = ee.FeatureCollection(bbox_features)
    print(
        f"Created Earth Engine FeatureCollection with {len(bbox_features)} bounding box features"
    )

    return feature_collection


# Download GeoTIFF for feature using Earth Engine
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
                    "format": "GeoTIFF",  # Note: Earth Engine accepts 'GeoTIFF'
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
                        f.write(
                            response.content[:2000]
                        )  # Save first part for debugging
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
    import logging
    import concurrent.futures
    from pathlib import Path
    import ee

    logger = logging.getLogger("whisp_processor")

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
    import random

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
    import random
    import math

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


def generate_random_geometries(gdf, max_distance, proportion=0.5):
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
        width_variation = random.uniform(0.8, 1.2)
        height_variation = random.uniform(0.8, 1.2)
        width *= width_variation
        height *= height_variation

        # Generate a random position within max_distance of the selected feature
        # First, select a random angle
        angle = random.uniform(0, 2 * math.pi)

        # Then, select a random distance (within max_distance)
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
            "internal_id": f"random_{i + 1000}",  # Use high numbers to avoid conflicts
            "obscured": True,
            "near_feature_id": random_feature_idx + 1,  # Store which feature it's near
        }

        # Create an Earth Engine feature
        ee_feature = ee.Feature(ee_geometry, properties)
        random_features.append(ee_feature)

    return random_features


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
    # Convert to Path object if it's a string
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

    # Print the files that will be included
    print(f"Found {len(tif_files)} TIF files to include in the VRT")

    # Output VRT path
    output_vrt = str(folder / "combined_rasters.vrt")

    # Create the VRT file
    vrt_file = build_vrt(output_vrt, tif_files)

    print(f"VRT file created at: {output_vrt}")
    return output_vrt


def convert_geojson_to_ee_bbox_obscured(
    geojson_filepath,
    extension_distance=None,
    extension_range=None,
    shift_geometries=False,
    shift_proportion=0.5,  # NEW: Control how much of extension is used for shifting
    pixel_length=0.0001,
    add_random_features=False,
    max_distance=0.1,
    random_proportion=0.5,
) -> ee.FeatureCollection:
    """
    Reads a GeoJSON file, creates bounding boxes for each feature,
    and converts to Earth Engine FeatureCollection with options to obscure locations.

    Args:
        geojson_filepath (str or Path): The filepath to the GeoJSON file
        extension_distance (float): Fixed distance to extend bounding boxes
        extension_range (list): [min_dist, max_dist] for random extension
        shift_geometries (bool): Whether to shift bounding boxes randomly
        shift_proportion (float): How much of extension can be used for shifting (0-1, default 0.5)
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
            # Load GeoJSON directly with geopandas
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
                # Determine max shift distance - limit by shift_proportion
                max_shift = 0
                if extension_distance is not None:
                    max_shift = extension_distance * shift_proportion
                elif extension_range is not None:
                    max_shift = (
                        extension_range[1] * shift_proportion
                    )  # Use max of range

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
                if hasattr(value, "item"):  # Check if it's a numpy type
                    properties[key] = value.item()  # Convert to Python native type
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
        random_features = generate_random_geometries(
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


def exact_extract_in_chunks_parallel(
    rasters, vector_file, chunk_size=25, ops=["sum"], max_workers=4
):
    """
    Process exactextract in parallel chunks of features

    Args:
        rasters: List of raster files or single raster path
        vector_file: Path to vector file (GeoJSON, shapefile, etc.)
        chunk_size: Number of features to process in each chunk
        ops: List of operations to perform
        max_workers: Maximum number of parallel processes/threads to use

    Returns:
        pd.DataFrame: Combined results
    """
    start_time = time.time()

    # Read the vector file
    print(f"Reading vector file: {vector_file}")
    gdf = gpd.read_file(vector_file)
    total_features = len(gdf)
    print(f"Total features to process: {total_features}")

    # Calculate number of chunks
    num_chunks = (total_features + chunk_size - 1) // chunk_size  # Ceiling division
    print(f"Processing in {num_chunks} chunks of up to {chunk_size} features each")
    print(f"Using {max_workers} parallel workers")

    # Function to process a single chunk
    def process_chunk(chunk_idx):
        start_idx = chunk_idx * chunk_size
        end_idx = min(start_idx + chunk_size, total_features)

        print(
            f"Starting chunk {chunk_idx+1}/{num_chunks} (features {start_idx+1}-{end_idx})"
        )
        chunk_start_time = time.time()

        # Extract the chunk
        chunk_gdf = gdf.iloc[start_idx:end_idx].copy()

        try:
            # Process this chunk
            chunk_results = exact_extract(
                progress=False,  # Disable progress bar for parallel processing to avoid mixed output
                rast=rasters,
                vec=chunk_gdf,
                # strategy="feature-sequential",
                ops=ops,
                output="pandas",
            )

            chunk_time = time.time() - chunk_start_time
            print(f"Completed chunk {chunk_idx+1}/{num_chunks} in {chunk_time:.2f}s")
            return chunk_results

        except Exception as e:
            print(f"Error processing chunk {chunk_idx+1}/{num_chunks}: {str(e)}")
            return None

    # Initialize empty DataFrame for results
    all_results = pd.DataFrame()

    # Process chunks in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all chunks for processing
        future_to_chunk = {
            executor.submit(process_chunk, chunk_idx): chunk_idx
            for chunk_idx in range(num_chunks)
        }

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_chunk):
            chunk_idx = future_to_chunk[future]
            try:
                result = future.result()
                if result is not None:
                    if all_results.empty:
                        all_results = result
                    else:
                        # Append to existing results
                        all_results = pd.concat(
                            [all_results, result], ignore_index=True
                        )

                    print(f"Chunk {chunk_idx+1} integrated into results")
            except Exception as e:
                print(f"Exception in chunk {chunk_idx+1}: {str(e)}")

    total_time = time.time() - start_time
    processed_count = len(all_results) if not all_results.empty else 0

    print(
        f"Processing complete. Processed {processed_count}/{total_features} features in {total_time:.2f}s"
    )

    return all_results

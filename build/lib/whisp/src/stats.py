import ee

# import functools
import pandas as pd

from pathlib import Path

from .datasets import combine_datasets

from ..parameters.config_runtime import (
    percent_or_ha,
    geometry_type_column,
    geometry_area_column,
    geometry_area_column_formatting,
    centroid_x_coord_column,
    centroid_y_coord_column,
    country_column,
    admin_1_column,
    stats_unit_type_column,
    stats_area_columns_formatting,
    stats_percent_columns_formatting,
    water_flag,
)

from .data_conversion import (
    ee_to_df,
    geojson_path_to_ee,
)  # copied functions from whisp-api and geemap (accessed 2024) to avoid dependency

from .reformat import (
    validate_dataframe_using_lookups,
)  # copied functions from whisp-api and geemap (accessed 2024) to avoid dependency


def whisp_formatted_stats_geojson_to_df(geojson_filepath: Path | str) -> pd.DataFrame:
    """

    Parameters
    ----------
    geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    feature_collection = geojson_path_to_ee(str(geojson_filepath))

    return whisp_formatted_stats_ee_to_df(feature_collection)


def whisp_stats_geojson_to_df(geojson_filepath: Path | str) -> pd.DataFrame:
    """

    Parameters
    ----------
    geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    feature_collection = geojson_path_to_ee(str(geojson_filepath))

    return whisp_stats_ee_to_df(feature_collection)


def whisp_stats_geojson_to_ee(geojson_filepath: Path | str) -> pd.DataFrame:
    """

    Parameters
    ----------
    geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    feature_collection = geojson_path_to_ee(str(geojson_filepath))

    return whisp_stats_ee_to_ee(feature_collection)


def whisp_stats_geojson_to_drive(geojson_filepath: Path | str):
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
        geojson_filepath = Path(geojson_filepath)
        if not geojson_filepath.exists():
            raise FileNotFoundError(f"File {geojson_filepath} does not exist.")

        # Assuming geojson_to_ee is properly imported from data_conversion.py
        feature_collection = geojson_path_to_ee(str(geojson_filepath))

        return whisp_stats_ee_to_drive(feature_collection)

    except Exception as e:
        print(f"An error occurred: {e}")


def whisp_stats_ee_to_ee(
    feature_collection: ee.FeatureCollection,
) -> ee.FeatureCollection:
    """

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The feature collection of the ROI to analyze.

    Returns
    -------
    feature_collection : ee.FeatureCollection
        The dataframe containing the Whisp stats for the input ROI.
    """
    return get_stats(feature_collection)


def whisp_stats_ee_to_df(feature_collection: ee.FeatureCollection) -> pd.DataFrame:
    """

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """

    return ee_to_df(whisp_stats_ee_to_ee(feature_collection))


def whisp_formatted_stats_ee_to_df(
    feature_collection: ee.FeatureCollection,
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
    df_stats = ee_to_df(whisp_stats_ee_to_ee(feature_collection))
    validated_df = validate_dataframe_using_lookups(df_stats)
    return validated_df


def whisp_stats_ee_to_drive(feature_collection: ee.FeatureCollection):

    try:
        task = ee.batch.Export.table.toDrive(
            collection=whisp_stats_ee_to_ee(feature_collection),
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
    print(out_feature_col.first().getInfo())

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
        feature.set(properties_ha).setGeometry(None),
        feature.set(properties_percent).setGeometry(None),
    )

    return out_feature


# Get basic feature information - uses admin and water datasets in gee.
def get_type_and_location(feature):
    """Extracts basic feature information including country, admin area, geometry type, coordinates, and water flags."""

    # Get centroid of the feature's geometry
    centroid = feature.geometry().centroid(1)

    # Fetch location info from geoboundaries (country, admin)
    location = ee.Dictionary(get_geoboundaries_info(centroid))
    country = ee.Dictionary({country_column: location.get("shapeGroup")})

    admin_1 = ee.Dictionary(
        {admin_1_column: location.get("shapeName")}
    )  # Administrative level 1 (if available)

    # Prepare the water flag information
    water_all = water_flag_all_prep()
    water_flag_dict = value_at_point_flag(centroid, water_all, "water_flag", water_flag)

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
    ee.Initialize()

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
        .rename("water_flag")
    )


def value_at_point_flag(point, image, band_name, output_name):
    """Sample an image at the given point and make a dictionary output where the name is defined by output_name parameter"""
    sample = image.sample(region=point, scale=30, numPixels=1).first()

    # Get the value from the sampled point
    value = sample.get(band_name)  # assuming the band name is 'b1', change if necessary

    # Use a conditional statement to check if the value is 1
    result = ee.Algorithms.If(ee.Number(value).eq(1), "True", "-")

    # Return the output dictionary
    return ee.Dictionary({output_name: result})  # .getInfo()


def reformat_whisp_fc(
    feature_collection,
    id_name=None,
    flag_positive=None,
    exclude_properties_from_output=None,
):
    """
    Process a FeatureCollection with various reformatting operations.

    Args:
    - feature_collection: ee.FeatureCollection, the FeatureCollection to operate on.
    - id_name: str, optional. Name of the ID property.
    - flag_positive: list, optional. List of property names to flag positive values.
    - exclude_properties_from_output: list, optional. List of property names to exclude_from_output.

    Returns:
    - processed_features: ee.FeatureCollection, FeatureCollection after processing.
    """

    if id_name:
        feature_collection = add_id_to_feature_collection(feature_collection, id_name)

    # Flag positive values if specified
    if flag_positive:
        feature_collection = feature_collection.map(
            lambda feature: flag_positive_values(feature, flag_positive)
        )

    # Exclude properties if specified
    if exclude_properties_from_output:
        feature_collection = feature_collection.map(
            lambda feature: copy_properties_and_exclude(
                feature, exclude_properties_from_output
            )
        )


# def get_stats_formatted(feature_or_feature_col, **kwargs) -> ee.FeatureCollection:
#     # Call to the original function to get stats
#     fc = get_stats(feature_or_feature_col)

#     # Directly apply the formatting logic, without a decorator
#     fc_formatted = reformat_whisp_fc(
#         fc,
#         id_name=kwargs.get("id_name"),
#         flag_positive=kwargs.get("flag_positive"),
#         exclude_properties_from_output=kwargs.get("exclude_properties_from_output"),
#     )

#     return fc_formatted


def add_id_to_feature_collection(dataset, id_name="PlotID"):
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

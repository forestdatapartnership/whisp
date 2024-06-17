import ee

from src.datasets import combine_datasets

from parameters.config_runtime import (
    percent_or_ha,
    plot_id_column,
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
)

import functools


########################################
### geoboundaries - freqently updated database, allows commercial use (CC BY 4.0 DEED) (disputed territories may need checking)
# def get_geoboundaries_info(geometry):
#     gbounds_ADM0 = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM0");
#     polygonsIntersectPoint = gbounds_ADM0.filterBounds(geometry)
#     return ee.Algorithms.If( polygonsIntersectPoint.size().gt(0), polygonsIntersectPoint.first().toDictionary().select(["shapeGroup"]), None );


def get_geoboundaries_info(geometry):
    gbounds_ADM0 = ee.FeatureCollection("WM/geoLab/geoBoundaries/600/ADM1")
    polygonsIntersectPoint = gbounds_ADM0.filterBounds(geometry)
    return ee.Algorithms.If(
        polygonsIntersectPoint.size().gt(0),
        polygonsIntersectPoint.first()
        .toDictionary()
        .select(["shapeGroup", "shapeName"]),
        None,
    )


### gaul boundaries - dated and need a lookup to get iso3 codes, but moving towards open licence
def get_gaul_info(geometry):
    gaul2 = ee.FeatureCollection("FAO/GAUL/2015/level2")
    polygonsIntersectPoint = gaul2.filterBounds(geometry)
    return ee.Algorithms.If(
        polygonsIntersectPoint.size().gt(0),
        polygonsIntersectPoint.first()
        .toDictionary()
        .select(["ADM0_NAME", "ADM1_NAME", "ADM2_NAME"]),
        None,
    )


### gadm - non-commercial use only
def get_gadm_info(geometry):
    gadm = ee.FeatureCollection(
        "projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/gadm_41_level_1"
    )
    polygonsIntersectPoint = gadm.filterBounds(geometry)
    return ee.Algorithms.If(
        polygonsIntersectPoint.size().gt(0),
        polygonsIntersectPoint.first().toDictionary().select(["GID_0", "COUNTRY"]),
        None,
    )


################### main stats functions


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


def get_stats_fc(feature_col):
    return ee.FeatureCollection(feature_col.map(get_stats_feature))


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


def get_stats_feature(feature):

    img_combined = combine_datasets()  # imported function

    reduce = img_combined.reduceRegion(
        reducer=ee.Reducer.sum(),
        geometry=feature.geometry(),
        scale=10,
        maxPixels=1e10,
        tileScale=8,
    )
    ######
    ## roi attributes
    ######
    ## get location

    ### gaul boundaries - dated, moving towards open licence
    # location = ee.Dictionary(get_gaul_info(feature.geometry()))

    # country = ee.Dictionary({country_column: location.get('ADM0_NAME')})

    ### gadm - non-commercial use only

    # location = ee.Dictionary(get_gadm_info(feature.geometry().centroid(1)))

    # country = ee.Dictionary({country_column: location.get('GID_0')})

    ### geoboundaries - freqently updated database; allows commercial use (CC BY 4.0 DEED)
    centroid = feature.geometry().centroid(1)

    location = ee.Dictionary(get_geoboundaries_info(centroid))

    country = ee.Dictionary({country_column: location.get("shapeGroup")})

    admin_1 = ee.Dictionary(
        {admin_1_column: location.get("shapeName")}
    )  # if running adm1

    geom_type = ee.Dictionary({geometry_type_column: feature.geometry().type()})

    coords_list = centroid.coordinates()  # list of lat lon coords for centroid

    # Create a dictionary with latitude and longitude keys  #coords.get(1).format('%.2f')
    coords_dict = ee.Dictionary(
        {
            centroid_x_coord_column: coords_list.get(0),
            centroid_y_coord_column: coords_list.get(1),
        }
    )

    stats_unit_type = ee.Dictionary({stats_unit_type_column: percent_or_ha})

    # combine info on country, geometry type and coordinates into a single dictionary
    feature_info = (
        country.combine(admin_1)
        .combine(geom_type)
        .combine(coords_dict)
        .combine(stats_unit_type)
    )

    ####

    # Now, modified_dict contains all keys with the prefix added
    reduce_ha = reduce.map(
        lambda key, val: divide_and_format(ee.Number(val), ee.Number(10000))
    )

    # Get val for hectares
    area_ha = ee.Number(ee.Dictionary(reduce_ha).get(geometry_area_column))

    ####

    # Apply the function (defined above) to each value in the dictionary using map()
    reduce_percent = reduce_ha.map(
        lambda key, val: percent_and_format(ee.Number(val), area_ha)
    )

    # Reformat
    reducer_stats_ha = reduce_ha.set(
        geometry_area_column, area_ha.format(geometry_area_column_formatting)
    )  # area ha (to a set number of decimal places)

    reducer_stats_percent = reduce_percent.set(
        geometry_area_column, area_ha.format(geometry_area_column_formatting)
    )  # area ha (to a set number of decimal places)

    # add country info on to ha analysis results
    properties_ha = feature_info.combine(ee.Dictionary(reducer_stats_ha))

    # add country info on to percent analysis results
    properties_percent = feature_info.combine(ee.Dictionary(reducer_stats_percent))

    # choose whether ha or percent based on the percent_or_ha variable (definined in paramters.config_runtime)
    out_feature = ee.Algorithms.If(
        percent_or_ha == "ha",
        feature.set(properties_ha).setGeometry(None),
        feature.set(properties_percent).setGeometry(None),
    )

    return out_feature


############3


def reformat_whisp_fc(
    feature_collection,
    order=None,
    id_name=None,
    flag_positive=None,
    round_properties=None,
    exclude_properties=None,
    select_and_rename=None,
):
    """
    Process a FeatureCollection with various reformatting operations.

    Args:
    - feature_collection: ee.FeatureCollection, the FeatureCollection to operate on.
    - order: list, optional. Desired order of properties.
    - id_name: str, optional. Name of the ID property.
    - flag_positive: list, optional. List of property names to flag positive values.
    - round_properties: list, optional. List of property names to round to whole numbers.
    - exclude_properties: list, optional. List of property names to exclude.
    - select_and_rename: bool, optional. If True, select and rename properties.

    Returns:
    - processed_features: ee.FeatureCollection, FeatureCollection after processing.
    """

    # Reorder properties if specified
    if order:
        feature_collection = feature_collection.map(
            lambda feature: reorder_properties(feature, order)
        )

    if id_name:
        feature_collection = add_id_to_feature_collection(feature_collection, id_name)

    # Flag positive values if specified
    if flag_positive:
        feature_collection = feature_collection.map(
            lambda feature: flag_positive_values(feature, flag_positive)
        )

    # Round properties to whole numbers if specified
    if round_properties:
        feature_collection = feature_collection.map(
            lambda feature: round_properties_to_whole_numbers(feature, round_properties)
        )

    # Exclude properties if specified
    if exclude_properties:
        feature_collection = feature_collection.map(
            lambda feature: copy_properties_and_exclude(feature, exclude_properties)
        )

    # Function to select and rename properties
    def select_and_rename_properties(feature):
        first_feature = ee.Feature(feature_collection.first())
        property_names = first_feature.propertyNames().getInfo()
        new_property_names = [prop.replace("_", " ") for prop in property_names]
        return feature.select(property_names, new_property_names)

    # Select and rename properties if specified
    if select_and_rename:

        feature_collection = feature_collection.map(select_and_rename_properties)

    return feature_collection


def stats_formatter_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> ee.FeatureCollection:
        feature_or_feature_col = args[0]
        fc = func(*args, **kwargs)
        fc_formatted = reformat_whisp_fc(
            fc,
            order=kwargs.get("order"),
            id_name=kwargs.get("id_name"),
            flag_positive=kwargs.get("flag_positive"),
            round_properties=kwargs.get("round_properties"),
            exclude_properties=kwargs.get("exclude_properties"),
            select_and_rename=kwargs.get("select_and_rename"),
        )
        return fc_formatted

    return wrapper


@stats_formatter_decorator
def get_stats_formatted(feature_or_feature_col, **kwargs) -> ee.FeatureCollection:
    fc = get_stats(feature_or_feature_col)
    return fc


##tidying functions


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


# Function to reorder properties
def reorder_properties(feature, order):
    properties = {key: feature.get(key) for key in order}
    return ee.Feature(feature.geometry(), properties)


# Function to add ID to features
def add_id_to_feature(feature, join):
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


# Function to round properties to whole numbers
def round_properties_to_whole_numbers(feature, round_properties):
    for prop_name in round_properties:
        prop_value = feature.get(prop_name)
        prop_value_rounded = ee.Number(prop_value).round()
        feature = feature.set(prop_name, prop_value_rounded)
    return feature


# Function to exclude properties
def copy_properties_and_exclude(feature, exclude_properties):
    return ee.Feature(feature.geometry()).copyProperties(
        source=feature, exclude=exclude_properties
    )


# # Function to select and rename properties
# def select_and_rename_properties(feature):
#     first_feature = ee.Feature(feature_collection.first())
#     property_names = first_feature.propertyNames().getInfo()
#     new_property_names = [prop.replace('_', ' ') for prop in property_names]
#     return feature.select(property_names, new_property_names)

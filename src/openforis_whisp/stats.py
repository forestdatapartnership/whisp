import ee
import pandas as pd
from pathlib import Path
from .datasets import combine_datasets
import json
import country_converter as coco
from openforis_whisp.parameters.config_runtime import (
    plot_id_column,
    external_id_column,
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


def whisp_formatted_stats_geojson_to_df(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    remove_geom=False,
    national_codes=None,
    unit_type="ha",
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
            The column in the GeoJSON containing external IDs to be preserved in the output DataFrame.
            This column must exist as a property in ALL features of the GeoJSON file.
            Use debug_feature_collection_properties() to inspect available properties if you encounter errors.
        remove_geom : bool, default=False
            If True, the geometry of the GeoJSON is removed from the output DataFrame.
        national_codes : list, optional
            List of ISO2 country codes to include national datasets.
        unit_type: str, optional
            Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
        -------
        df_stats : pd.DataFrame
            The DataFrame containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_formatted_stats_ee_to_df(
        feature_collection,
        external_id_column,
        remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,  # Fixed: now it's a keyword argument
    )


def whisp_formatted_stats_geojson_to_geojson(
    input_geojson_filepath,
    output_geojson_filepath,
    external_id_column=None,
    geo_column: str = "geo",
    national_codes=None,
    unit_type="ha",
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
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
    -------
    None
    """
    df = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
        national_codes=national_codes,
        unit_type=unit_type,
    )
    # Convert the df to GeoJSON
    convert_df_to_geojson(df, output_geojson_filepath, geo_column)

    print(f"GeoJSON with Whisp stats saved to {output_geojson_filepath}")


def whisp_formatted_stats_ee_to_geojson(
    feature_collection: ee.FeatureCollection,
    output_geojson_filepath: str,
    external_id_column=None,
    geo_column: str = "geo",
    national_codes=None,
    unit_type="ha",
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
    geo_column : str, optional
        The name of the column containing GeoJSON geometries, by default "geo".
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
    Returns
    -------
    None
    """
    # Convert ee feature collection to a pandas dataframe
    df_stats = whisp_formatted_stats_ee_to_df(
        feature_collection,
        external_id_column,
        national_codes=national_codes,
        unit_type=unit_type,
    )

    # Convert the df to GeoJSON
    convert_df_to_geojson(df_stats, output_geojson_filepath, geo_column)

    print(f"GeoJSON with Whisp stats saved to {output_geojson_filepath}")


def whisp_formatted_stats_ee_to_df(
    feature_collection: ee.FeatureCollection,
    external_id_column=None,
    remove_geom=False,
    national_codes=None,
    unit_type="ha",
) -> pd.DataFrame:
    """
    Convert a feature collection to a validated DataFrame with Whisp statistics.

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The feature collection of the ROI to analyze.
    external_id_column : str, optional
        The name of the external ID column, by default None.
    remove_geom : bool, optional
        Whether to remove the geometry column, by default False.
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
    -------
    validated_df : pd.DataFrame
        The validated dataframe containing the Whisp stats for the input ROI.
    """
    # Convert ee feature collection to a pandas dataframe
    df_stats = whisp_stats_ee_to_df(
        feature_collection,
        external_id_column,
        remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,
    )

    # Pass national_codes to validation function to filter schema
    validated_df = validate_dataframe_using_lookups(
        df_stats, national_codes=national_codes
    )
    return validated_df


### functions without additional formatting below (i.e., raw output from GEE processing without schema validation step)


def whisp_stats_geojson_to_df(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    remove_geom=False,
    national_codes=None,
    unit_type="ha",
) -> pd.DataFrame:
    """
    Convert a GeoJSON file to a pandas DataFrame with Whisp statistics.

    Parameters
    ----------
    input_geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.
    external_id_column : str, optional
        The name of the external ID column, by default None.
    remove_geom : bool, optional
        Whether to remove the geometry column, by default False.
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_stats_ee_to_df(
        feature_collection,
        external_id_column,
        remove_geom,
        national_codes=national_codes,
        unit_type=unit_type,
    )


def whisp_stats_geojson_to_ee(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    national_codes=None,
) -> ee.FeatureCollection:
    """
    Convert a GeoJSON file to an Earth Engine FeatureCollection with Whisp statistics.

    Parameters
    ----------
    input_geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.
    external_id_column : str, optional
        The name of the external ID column, by default None.
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.

    Returns
    -------
    ee.FeatureCollection
        The feature collection containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_stats_ee_to_ee(
        feature_collection, external_id_column, national_codes=national_codes
    )


def whisp_stats_geojson_to_geojson(
    input_geojson_filepath,
    output_geojson_filepath,
    external_id_column=None,
    national_codes=None,
    unit_type="ha",
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
        The name of the column containing external IDs, by default None.
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
    -------
    None
    """
    # Convert GeoJSON to Earth Engine FeatureCollection
    feature_collection = convert_geojson_to_ee(input_geojson_filepath)

    # Get stats as a FeatureCollection
    stats_feature_collection = whisp_stats_ee_to_ee(
        feature_collection,
        external_id_column,
        national_codes=national_codes,
        unit_type=unit_type,
    )

    # Convert the stats FeatureCollection to GeoJSON
    stats_geojson = convert_ee_to_geojson(stats_feature_collection)

    # Save the GeoJSON to a file
    with open(output_geojson_filepath, "w") as f:
        json.dump(stats_geojson, f, indent=2)


def whisp_stats_geojson_to_drive(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    national_codes=None,
    unit_type="ha",
):
    """
    Export Whisp statistics for a GeoJSON file to Google Drive.

    Parameters
    ----------
    input_geojson_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.
    external_id_column : str, optional
        The name of the external ID column, by default None.
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
    -------
    Message showing location of file in Google Drive
    """
    try:
        input_geojson_filepath = Path(input_geojson_filepath)
        if not input_geojson_filepath.exists():
            raise FileNotFoundError(f"File {input_geojson_filepath} does not exist.")

        feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

        return whisp_stats_ee_to_drive(
            feature_collection,
            external_id_column,
            national_codes=national_codes,
            unit_type=unit_type,
        )

    except Exception as e:
        print(f"An error occurred: {e}")


def whisp_stats_ee_to_ee(
    feature_collection,
    external_id_column,
    national_codes=None,
    unit_type="ha",
    keep_properties=None,
):
    """
    Process a feature collection to get statistics for each feature.

    Parameters:
        feature_collection (ee.FeatureCollection): The input feature collection.
        external_id_column (str): The name of the external ID column to check.
        national_codes (list, optional): List of ISO2 country codes to include national datasets.
        unit_type (str): Whether to use hectares ("ha") or percentage ("percent"), default "ha".
        keep_properties (None, bool, or list, optional): Properties to keep from the input features.
            - None: Remove all properties (default behavior)
            - True: Keep all properties
            - list: Keep only the specified properties

    Returns:
        ee.FeatureCollection: The output feature collection with statistics.
    """
    if external_id_column is not None:
        try:
            # Validate that the external_id_column exists in all features
            validation_result = validate_external_id_column(
                feature_collection, external_id_column
            )

            if not validation_result["is_valid"]:
                raise ValueError(validation_result["error_message"])

            # First handle property selection, but preserve the external_id_column
            if keep_properties is not None:
                if keep_properties == True:
                    # Keep all properties including external_id_column
                    pass  # No need to modify feature_collection
                elif isinstance(keep_properties, list):
                    # Ensure external_id_column is included in the list
                    if external_id_column not in keep_properties:
                        keep_properties = keep_properties + [external_id_column]
                    feature_collection = feature_collection.select(keep_properties)
                else:
                    raise ValueError(
                        "keep_properties must be None, True, or a list of property names."
                    )

            # Set the external_id with robust null handling
            def set_external_id_safely_and_clean(feature):
                external_id_value = feature.get(external_id_column)
                # Use server-side null checking and string conversion
                external_id_value = ee.Algorithms.If(
                    ee.Algorithms.IsEqual(external_id_value, None),
                    "unknown",
                    ee.String(external_id_value),
                )
                # Create a new feature with the standardized external_id column
                # Note: we use "external_id" as the standardized column name, not the original external_id_column name
                return ee.Feature(feature.set("external_id", external_id_value))

            feature_collection = feature_collection.map(
                set_external_id_safely_and_clean
            )

            # Finally, clean up to keep only geometry and external_id if keep_properties is None
            if keep_properties is None:
                feature_collection = feature_collection.select(["external_id"])

        except Exception as e:
            # Handle the exception and provide a helpful error message
            print(
                f"An error occurred when trying to set the external_id_column: {external_id_column}. Error: {e}"
            )
            raise e  # Re-raise the exception to stop execution
    else:
        feature_collection = _keep_fc_properties(feature_collection, keep_properties)

    fc = get_stats(
        feature_collection, national_codes=national_codes, unit_type=unit_type
    )

    return add_id_to_feature_collection(dataset=fc, id_name=plot_id_column)


def _keep_fc_properties(feature_collection, keep_properties):
    # If keep_properties is specified, select only those properties
    if keep_properties is None:
        feature_collection = feature_collection.select([])
    elif keep_properties == True:
        # If keep_properties is true, select all properties
        first_feature_props = feature_collection.first().propertyNames().getInfo()
        feature_collection = feature_collection.select(first_feature_props)
    elif isinstance(keep_properties, list):
        feature_collection = feature_collection.select(keep_properties)
    else:
        raise ValueError(
            "keep_properties must be None, True, or a list of property names."
        )
    return feature_collection


def whisp_stats_ee_to_df(
    feature_collection: ee.FeatureCollection,
    external_id_column=None,
    remove_geom=False,
    national_codes=None,
    unit_type="ha",
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
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    try:
        df_stats = convert_ee_to_df(
            ee_object=whisp_stats_ee_to_ee(
                feature_collection,
                external_id_column,
                national_codes=national_codes,
                unit_type=unit_type,
            ),
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
    feature_collection: ee.FeatureCollection,
    external_id_column=None,
    national_codes=None,
    unit_type="ha",
):
    """
     Export Whisp statistics for a feature collection to Google Drive.

     Parameters
     ----------
     feature_collection : ee.FeatureCollection
         The feature collection to analyze.
     external_id_column : str, optional
         The name of the external ID column, by default None.
     national_codes : list, optional
         List of ISO2 country codes to include national datasets.
    unit_type : str, optional
         Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
     Returns
     -------
     None
    """
    try:
        task = ee.batch.Export.table.toDrive(
            collection=whisp_stats_ee_to_ee(
                feature_collection,
                external_id_column,
                national_codes=national_codes,
                unit_type=unit_type,
            ),
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
def get_stats(feature_or_feature_col, national_codes=None, unit_type="ha"):
    """
     Get stats for a feature or feature collection with optional filtering by national codes.

     Parameters
     ----------
     feature_or_feature_col : ee.Feature or ee.FeatureCollection
         The input feature or feature collection to analyze
     national_codes : list, optional
         List of ISO2 country codes to include national datasets
    unit_type : str, optional
         Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
     Returns
     -------
     ee.FeatureCollection
         Feature collection with calculated statistics
    """
    # Check if the input is a Feature or a FeatureCollection
    if isinstance(feature_or_feature_col, ee.Feature):
        # If the input is a Feature, call the server-side function for processing
        print("feature")
        # For a single feature, we need to combine datasets with the national_codes filter
        img_combined = combine_datasets(national_codes=national_codes)
        output = ee.FeatureCollection(
            [
                get_stats_feature(
                    feature_or_feature_col, img_combined, unit_type=unit_type
                )
            ]
        )
    elif isinstance(feature_or_feature_col, ee.FeatureCollection):
        # If the input is a FeatureCollection, call the server-side function for processing
        output = get_stats_fc(
            feature_or_feature_col, national_codes=national_codes, unit_type=unit_type
        )
    else:
        output = "Check inputs: not an ee.Feature or ee.FeatureCollection"
    return output


# Get statistics for a feature collection
def get_stats_fc(feature_col, national_codes=None, unit_type="ha"):
    """
     Calculate statistics for a feature collection using Whisp datasets.

     Parameters
     ----------
     feature_col : ee.FeatureCollection
         The input feature collection to analyze
     national_codes : list, optional
         List of ISO2 country codes (e.g., ["BR", "US"]) to include national datasets.
         If provided, only national datasets for these countries and global datasets will be used.
         If None (default), only global datasets will be used.
    unit_type : str, optional
         Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
     Returns
     -------
     ee.FeatureCollection
         Feature collection with calculated statistics
    """
    img_combined = combine_datasets(
        national_codes=national_codes
    )  # Pass national_codes to combine_datasets

    out_feature_col = ee.FeatureCollection(
        feature_col.map(
            lambda feature: get_stats_feature(
                feature, img_combined, unit_type=unit_type
            )
        )
    )
    # print(out_feature_col.first().getInfo()) # for testing

    return out_feature_col


# Get statistics for a single feature


def get_stats_feature(feature, img_combined, unit_type="ha"):
    """
    Get statistics for a single feature using a pre-combined image.

    Parameters
    ----------
    feature : ee.Feature
        The feature to analyze
    img_combined : ee.Image
        Pre-combined image with all the datasets
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".

    Returns
    -------
    ee.Feature
        Feature with calculated statistics
    """
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
    stats_unit_type = ee.Dictionary({stats_unit_type_column: unit_type})

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

    # Choose whether to use hectares or percentage based on the parameter instead of global variable
    out_feature = ee.Algorithms.If(
        unit_type == "ha",
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
        lambda x: (
            coco.convert(names=x, to="ISO2") if x else "not found (disputed territory)"
        )
    )

    return df


def validate_external_id_column(feature_collection, external_id_column):
    """
    Validates that the external_id_column exists in all features of the collection.

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The feature collection to validate
    external_id_column : str
        The name of the external ID column to check

    Returns
    -------
    dict
        Dictionary with validation results including:
        - 'is_valid': bool indicating if column exists in all features
        - 'total_features': int total number of features
        - 'features_with_column': int number of features that have the column
        - 'available_properties': list of properties available in first feature
        - 'error_message': str error message if validation fails
    """
    try:
        # Get total number of features
        total_features = feature_collection.size().getInfo()

        if total_features == 0:
            return {
                "is_valid": False,
                "total_features": 0,
                "features_with_column": 0,
                "available_properties": [],
                "error_message": "Feature collection is empty",
            }

        # Get available properties from first feature
        first_feature_props = feature_collection.first().propertyNames().getInfo()

        # Check if external_id_column exists in all features
        def check_column_exists(feature):
            has_column = feature.propertyNames().contains(external_id_column)
            return feature.set("_has_external_id", has_column)

        features_with_check = feature_collection.map(check_column_exists)
        features_with_column = (
            features_with_check.filter(ee.Filter.eq("_has_external_id", True))
            .size()
            .getInfo()
        )

        is_valid = features_with_column == total_features

        error_message = None
        if not is_valid:
            missing_count = total_features - features_with_column
            error_message = (
                f"The column '{external_id_column}' is missing from {missing_count} "
                f"out of {total_features} features in the collection. "
                f"Available properties in first feature: {first_feature_props}"
            )

        return {
            "is_valid": is_valid,
            "total_features": total_features,
            "features_with_column": features_with_column,
            "available_properties": first_feature_props,
            "error_message": error_message,
        }

    except Exception as e:
        return {
            "is_valid": False,
            "total_features": 0,
            "features_with_column": 0,
            "available_properties": [],
            "error_message": f"Error during validation: {str(e)}",
        }


def debug_feature_collection_properties(feature_collection, max_features=5):
    """
    Debug helper function to inspect the properties of features in a collection.

    Parameters
    ----------
    feature_collection : ee.FeatureCollection
        The feature collection to inspect
    max_features : int, optional
        Maximum number of features to inspect, by default 5

    Returns
    -------
    dict
        Dictionary with debugging information about the feature collection
    """
    try:
        total_features = feature_collection.size().getInfo()

        if total_features == 0:
            return {"total_features": 0, "error": "Feature collection is empty"}

        # Limit the number of features to inspect
        features_to_check = min(max_features, total_features)
        limited_fc = feature_collection.limit(features_to_check)

        # Get properties for each feature
        def get_feature_properties(feature):
            return ee.Dictionary(
                {
                    "properties": feature.propertyNames(),
                    "geometry_type": feature.geometry().type(),
                }
            )

        feature_info = limited_fc.map(get_feature_properties).getInfo()

        return {
            "total_features": total_features,
            "inspected_features": features_to_check,
            "feature_details": [
                {
                    "feature_index": i,
                    "properties": feature_info["features"][i]["properties"][
                        "properties"
                    ],
                    "geometry_type": feature_info["features"][i]["properties"][
                        "geometry_type"
                    ],
                }
                for i in range(len(feature_info["features"]))
            ],
        }

    except Exception as e:
        return {"error": f"Error during debugging: {str(e)}"}

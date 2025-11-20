import ee
import pandas as pd
from pathlib import Path
from .datasets import combine_datasets
import json
import logging
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
from .reformat import (
    validate_dataframe_using_lookups,
    validate_dataframe_using_lookups_flexible,
)

# NB functions that included "formatted" in the name apply a schema for validation and reformatting of the output dataframe. The schema is created from lookup tables.

# ============================================================================
# PERFORMANCE OPTIMIZATION: Cache expensive Earth Engine datasets
# ============================================================================
# These images/collections are loaded once and reused across all features
# to avoid repeated expensive operations. This saves 7-15 seconds per analysis.

_WATER_FLAG_IMAGE = None
_admin_boundaries_FC = None


def get_water_flag_image():
    """
    Get cached water flag image.

    OPTIMIZATION: Water flag image is created once and reused for all features.
    This avoids recreating ocean/water datasets for every feature (previously
    called in get_type_and_location for each feature).

    Returns
    -------
    ee.Image
        Cached water flag image
    """
    global _WATER_FLAG_IMAGE
    if _WATER_FLAG_IMAGE is None:
        _WATER_FLAG_IMAGE = water_flag_all_prep()
    return _WATER_FLAG_IMAGE


def get_admin_boundaries_fc():
    """
    Get cached GAUL 2024 L1 administrative boundary feature collection.

    OPTIMIZATION: GAUL 2024 L1 collection is loaded once and reused for all features.
    This avoids loading the large FeatureCollection for every feature (previously
    called in get_admin_boundaries_info for each feature).

    Returns
    -------
    ee.FeatureCollection
        Cached GAUL 2024 L1 administrative boundary feature collection
    """
    global _admin_boundaries_FC
    if _admin_boundaries_FC is None:
        _admin_boundaries_FC = ee.FeatureCollection(
            "projects/sat-io/open-datasets/FAO/GAUL/GAUL_2024_L1"
        )
    return _admin_boundaries_FC


def whisp_formatted_stats_geojson_to_df_legacy(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    national_codes=None,
    unit_type="ha",
    whisp_image=None,
    custom_bands=None,  # New parameter
) -> pd.DataFrame:
    """
        Legacy function for basic Whisp stats extraction.

        DEPRECATED: This is the original implementation maintained for backward compatibility.
        Use whisp_formatted_stats_geojson_to_df() for new code, which provides automatic
        optimization, formatting, and schema validation.

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
        whisp_image : ee.Image, optional
            Pre-combined multiband Earth Engine Image containing all Whisp datasets.
            If provided, this image will be used instead of combining datasets based on national_codes.
            If None, datasets will be combined automatically using national_codes parameter.
        custom_bands : list or dict, optional
            Custom band information for extra columns. Can be:
            - List of band names: ['Aa_test', 'elevation']
            - Dict with types: {'Aa_test': 'float64', 'elevation': 'float32'}
            - None: preserves all extra columns automatically

    Returns
        -------
        df_stats : pd.DataFrame
            The DataFrame containing the Whisp stats for the input ROI.
    """
    # Convert GeoJSON to Earth Engine FeatureCollection
    # Note: Geometry validation/cleaning should be done before calling this function
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_formatted_stats_ee_to_df(
        feature_collection,
        external_id_column,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,
        custom_bands=custom_bands,  # Pass through
    )


def whisp_formatted_stats_geojson_to_df(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    remove_geom=False,
    national_codes=None,
    unit_type="ha",
    whisp_image=None,
    custom_bands=None,
    mode: str = "sequential",
    batch_size: int = 10,
    max_concurrent: int = 20,
    geometry_audit_trail: bool = False,
    status_file: str = None,
) -> pd.DataFrame:
    """
    Main entry point for converting GeoJSON to Whisp statistics.

    Routes to the appropriate processing mode with automatic formatting and validation.

    Converts a GeoJSON file to a pandas DataFrame containing Whisp stats for the input ROI.
    Output DataFrame is validated against a Panderas schema (created from lookup CSVs).
    Results are automatically formatted and unit-converted (ha or percent).

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
    national_codes : list, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.
        If provided, this image will be used instead of combining datasets based on national_codes.
        If None, datasets will be combined automatically using national_codes parameter.
    custom_bands : list or dict, optional
        Custom band information for extra columns. Can be:
        - List of band names: ['Aa_test', 'elevation']
        - Dict with types: {'Aa_test': 'float64', 'elevation': 'float32'}
        - None: preserves all extra columns automatically
    mode : str, optional
        Processing mode, by default "concurrent":
        - "concurrent": Uses high-volume endpoint with concurrent batching (recommended for large files)
        - "sequential": Uses standard endpoint for sequential processing (more stable)
        - "legacy": Uses original implementation (basic stats extraction only, no formatting)
    batch_size : int, optional
        Features per batch for concurrent/sequential modes, by default 10.
        Only applicable for "concurrent" and "sequential" modes.
    max_concurrent : int, optional
        Maximum concurrent EE calls for concurrent mode, by default 20.
        Only applicable for "concurrent" mode.
    geometry_audit_trail : bool, default True
        If True (default), includes audit trail columns:
        - geo_original: Original input geometry
        - geometry_type_original: Original geometry type
        - geometry_type: Processed geometry type (from EE)
        - geometry_type_changed: Boolean flag if geometry changed
        - geometry_degradation_type: Description of how it changed

        Processing metadata stored in df.attrs['processing_metadata'].
        These columns enable full transparency for geometry modifications during processing.
    status_file : str, optional
        Path to JSON status file or directory for real-time progress tracking.
        If a directory is provided, creates 'whisp_processing_status.json' in that directory.
        Updates every 3 minutes and at progress milestones (5%, 10%, etc.).
        Format: {"status": "processing", "progress": "450/1000", "percent": 45.0,
                 "elapsed_sec": 120, "eta_sec": 145, "updated_at": "2025-11-13T14:23:45"}
        Most useful for large concurrent jobs. Works in both concurrent and sequential modes.

    Returns
    -------
    df_stats : pd.DataFrame
        The DataFrame containing the Whisp stats for the input ROI,
        automatically formatted and validated.

    Examples
    --------
    >>> # Use concurrent processing (default, recommended for large datasets)
    >>> df = whisp_formatted_stats_geojson_to_df("data.geojson")

    >>> # Use sequential processing for more stable/predictable results
    >>> df = whisp_formatted_stats_geojson_to_df(
    ...     "data.geojson",
    ...     mode="sequential"
    ... )

    >>> # Adjust concurrency parameters
    >>> df = whisp_formatted_stats_geojson_to_df(
    ...     "large_data.geojson",
    ...     mode="concurrent",
    ...     max_concurrent=30,
    ...     batch_size=15
    ... )

    >>> # Use legacy mode for backward compatibility (basic extraction only)
    >>> df = whisp_formatted_stats_geojson_to_df(
    ...     "data.geojson",
    ...     mode="legacy"
    ... )
    """
    # Import here to avoid circular imports
    try:
        from openforis_whisp.advanced_stats import (
            whisp_formatted_stats_geojson_to_df_fast,
        )
    except ImportError:
        # Fallback to legacy if advanced_stats not available
        mode = "legacy"

    logger = logging.getLogger("whisp")

    if mode == "legacy":
        # Log info if batch_size or max_concurrent were passed but won't be used
        if batch_size != 10 or max_concurrent != 20:
            unused = []
            if batch_size != 10:
                unused.append(f"batch_size={batch_size}")
            if max_concurrent != 20:
                unused.append(f"max_concurrent={max_concurrent}")
            logger.info(
                f"Mode is 'legacy': {', '.join(unused)}\n"
                "parameter(s) are not used in legacy mode."
            )
        # Use original implementation (basic stats extraction only)
        return whisp_formatted_stats_geojson_to_df_legacy(
            input_geojson_filepath=input_geojson_filepath,
            external_id_column=external_id_column,
            national_codes=national_codes,
            unit_type=unit_type,
            whisp_image=whisp_image,
            custom_bands=custom_bands,
        )
    elif mode in ("concurrent", "sequential"):
        # Log info if batch_size or max_concurrent are not used in sequential mode
        if mode == "sequential":
            unused = []
            if batch_size != 10:
                unused.append(f"batch_size={batch_size}")
            if max_concurrent != 20:
                unused.append(f"max_concurrent={max_concurrent}")
            if unused:
                logger.info(
                    f"Mode is 'sequential': {', '.join(unused)}\n"
                    "parameter(s) are not used in sequential (single-threaded) mode."
                )
        # Route to fast function with explicit mode (skip auto-detection)
        return whisp_formatted_stats_geojson_to_df_fast(
            input_geojson_filepath=input_geojson_filepath,
            external_id_column=external_id_column,
            national_codes=national_codes,
            unit_type=unit_type,
            whisp_image=whisp_image,
            custom_bands=custom_bands,
            mode=mode,  # Pass mode directly (concurrent or sequential)
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            geometry_audit_trail=geometry_audit_trail,
            status_file=status_file,
        )
    else:
        raise ValueError(
            f"Invalid mode '{mode}'. Must be 'concurrent', 'sequential', or 'legacy'."
        )


def whisp_formatted_stats_geojson_to_geojson(
    input_geojson_filepath,
    output_geojson_filepath,
    external_id_column=None,
    geo_column: str = "geo",
    national_codes=None,
    unit_type="ha",
    whisp_image=None,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.

    Returns
    -------
    None
    """
    df = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=input_geojson_filepath,
        external_id_column=external_id_column,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,  # Pass through
    )
    # Convert the df to GeoJSON
    convert_df_to_geojson(df, output_geojson_filepath, geo_column)

    # Suppress verbose output
    # print(f"GeoJSON with Whisp stats saved to {output_geojson_filepath}")


def whisp_formatted_stats_ee_to_geojson(
    feature_collection: ee.FeatureCollection,
    output_geojson_filepath: str,
    external_id_column=None,
    geo_column: str = "geo",
    national_codes=None,
    unit_type="ha",
    whisp_image=None,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.
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
        whisp_image=whisp_image,  # Pass through
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
    whisp_image=None,
    custom_bands=None,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.
    custom_bands : list or dict, optional
        Custom band information for extra columns.

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
        whisp_image=whisp_image,
    )

    # Use flexible validation that handles custom bands
    validated_df = validate_dataframe_using_lookups_flexible(
        df_stats, national_codes=national_codes, custom_bands=custom_bands
    )
    return validated_df


### functions without additional formatting below (i.e., raw output from GEE processing without schema validation step)


def whisp_stats_geojson_to_df(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    national_codes=None,
    unit_type="ha",
    whisp_image=None,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_stats_ee_to_df(
        feature_collection,
        external_id_column,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,  # Pass through
    )


def whisp_stats_geojson_to_ee(
    input_geojson_filepath: Path | str,
    external_id_column=None,
    national_codes=None,
    whisp_image=None,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.

    Returns
    -------
    ee.FeatureCollection
        The feature collection containing the Whisp stats for the input ROI.
    """
    feature_collection = convert_geojson_to_ee(str(input_geojson_filepath))

    return whisp_stats_ee_to_ee(
        feature_collection,
        external_id_column,
        national_codes=national_codes,
        whisp_image=whisp_image,  # Pass through
    )


def whisp_stats_geojson_to_geojson(
    input_geojson_filepath,
    output_geojson_filepath,
    external_id_column=None,
    national_codes=None,
    unit_type="ha",
    whisp_image=None,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.

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
        whisp_image=whisp_image,  # Pass through
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
    whisp_image=None,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.

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
            whisp_image=whisp_image,  # Pass through
        )

    except Exception as e:
        print(f"An error occurred: {e}")


def whisp_stats_ee_to_ee(
    feature_collection,
    external_id_column,
    national_codes=None,
    unit_type="ha",
    keep_properties=None,
    whisp_image=None,
    validate_external_id=True,
    validate_bands=False,  # New parameter
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
        whisp_image (ee.Image, optional): Pre-combined multiband Earth Engine Image containing
            all Whisp datasets. If provided, this image will be used instead of combining
            datasets based on national_codes.
        validate_external_id (bool, optional): If True, validates that external_id_column exists
            in all features (default: True). Set to False to skip validation and save 2-4 seconds.
            Only disable if you're confident the column exists in all features.

    Returns:
        ee.FeatureCollection: The output feature collection with statistics.
    """
    if external_id_column is not None:
        try:
            # OPTIMIZATION: Make validation optional to save 2-4 seconds
            # Validation includes multiple .getInfo() calls which are slow
            if validate_external_id:
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
        feature_collection,
        national_codes=national_codes,
        unit_type=unit_type,
        whisp_image=whisp_image,  # Pass through
        validate_bands=validate_bands,
    )

    return add_id_to_feature_collection(dataset=fc, id_name=plot_id_column)


def _keep_fc_properties(feature_collection, keep_properties):
    """
    Filter feature collection properties based on keep_properties parameter.

    OPTIMIZATION: When keep_properties is True, we no longer call .getInfo()
    to get property names. Instead, we simply return the collection as-is,
    since True means "keep all properties". This saves 1-2 seconds.
    """
    # If keep_properties is specified, select only those properties
    if keep_properties is None:
        feature_collection = feature_collection.select([])
    elif keep_properties == True:
        # If keep_properties is true, keep all properties
        # No need to call .select() or .getInfo() - just return as-is
        pass
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
    whisp_image=None,
    validate_bands=False,  # New parameter
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
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """
    # First, do the whisp processing to get the EE feature collection with stats
    try:
        try:
            stats_feature_collection = whisp_stats_ee_to_ee(
                feature_collection,
                external_id_column,
                national_codes=national_codes,
                unit_type=unit_type,
                whisp_image=whisp_image,  # Pass through
                validate_bands=False,  # try withoutb validation first
            )
        except Exception as e:
            print(f"An error occurred during Whisp stats processing: {e}")
            raise e

        # Then, convert the EE feature collection to DataFrame
        try:
            df_stats = convert_ee_to_df(
                ee_object=stats_feature_collection,
                remove_geom=remove_geom,
            )
        except Exception as e:
            print(f"An error occurred during the conversion from EE to DataFrame: {e}")
            raise e

    except:  # retry with validation of whisp input datasets
        try:
            stats_feature_collection = whisp_stats_ee_to_ee(
                feature_collection,
                external_id_column,
                national_codes=national_codes,
                unit_type=unit_type,
                whisp_image=whisp_image,
                validate_bands=True,  # If error, try with validation
            )
        except Exception as e:
            print(f"An error occurred during Whisp stats processing: {e}")
            raise e

        # Then, convert the EE feature collection to DataFrame
        try:
            df_stats = convert_ee_to_df(
                ee_object=stats_feature_collection,
                remove_geom=remove_geom,
            )
        except Exception as e:
            print(f"An error occurred during the conversion from EE to DataFrame: {e}")
            raise e
    try:
        df_stats = convert_iso3_to_iso2(
            df=df_stats,
            iso3_column=iso3_country_column,
            iso2_column=iso2_country_column,
        )
    except Exception as e:
        print(f"An error occurred during the ISO3 to ISO2 conversion: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    # NEW: Set area to 0 for point geometries
    try:
        df_stats = set_point_geometry_area_to_zero(df_stats)
    except Exception as e:
        print(f"An error occurred during point geometry area adjustment: {e}")
        # Continue without the adjustment rather than failing completely

    # Reformat geometry types (MultiPolygon -> Polygon)
    try:
        df_stats = reformat_geometry_type(df_stats)
    except Exception as e:
        print(f"An error occurred during geometry type reformatting: {e}")
        # Continue without the adjustment rather than failing completely

    return df_stats


def set_point_geometry_area_to_zero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the geometry area column to 0 for features with Point geometry type.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing geometry type and area columns

    Returns
    -------
    pd.DataFrame
        DataFrame with area set to 0 for Point geometries
    """
    # Check if required columns exist
    if geometry_type_column not in df.columns:
        print(
            f"Warning: {geometry_type_column} column not found. Skipping area adjustment for points."
        )
        return df

    # Create a copy to avoid modifying the original
    df_modified = df.copy()

    # Set area to 0 where geometry type is Point
    point_mask = df_modified[geometry_type_column] == "Point"
    df_modified.loc[point_mask, geometry_area_column] = 0.0

    # Log the changes
    num_points = point_mask.sum()
    if num_points > 0:
        print(f"Set area to 0 for {num_points} Point geometries")

    return df_modified


def reformat_geometry_type(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reformat geometry type classification in the DataFrame output.
    Standardizes MultiPolygon geometry type to Polygon for consistent output.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing geometry type column

    Returns
    -------
    pd.DataFrame
        DataFrame with standardized geometry types
    """
    # Check if required columns exist
    if geometry_type_column not in df.columns:
        print(
            f"Warning: {geometry_type_column} column not found. Skipping geometry type reformatting."
        )
        return df

    # Create a copy to avoid modifying the original
    df_modified = df.copy()

    # Reformat MultiPolygon to Polygon
    multipolygon_mask = df_modified[geometry_type_column] == "MultiPolygon"
    df_modified.loc[multipolygon_mask, geometry_type_column] = "Polygon"

    # Log the changes
    num_reformatted = multipolygon_mask.sum()
    # if num_reformatted > 0:
    #     print(f"Reformatted {num_reformatted} MultiPolygon geometries to Polygon")

    return df_modified


def whisp_stats_ee_to_drive(
    feature_collection: ee.FeatureCollection,
    external_id_column=None,
    national_codes=None,
    unit_type="ha",
    whisp_image=None,  # New parameter
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
    whisp_image : ee.Image, optional
         Pre-combined multiband Earth Engine Image containing all Whisp datasets.
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
                whisp_image=whisp_image,  # Pass through
            ),
            description="whisp_output_table",
            # folder="whisp_results",
            fileFormat="CSV",
        )
        task.start()
        print(
            "Exporting to Google Drive: 'whisp_output_table.csv'. To track progress: https://code.earthengine.google.com/tasks"
        )
    except Exception as e:
        print(f"An error occurred during the export: {e}")


#### main stats functions


# Get stats for a feature or feature collection
def get_stats(
    feature_or_feature_col,
    national_codes=None,
    unit_type="ha",
    whisp_image=None,
    validate_bands=False,
):
    """
    Get stats for a feature or feature collection with optional pre-combined image.

    Parameters
    ----------
    feature_or_feature_col : ee.Feature or ee.FeatureCollection
        The input feature or feature collection to analyze
    national_codes : list, optional
        List of ISO2 country codes to include national datasets.
        Only used if whisp_image is None.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
    whisp_image : ee.Image, optional
        Pre-combined multiband Earth Engine Image containing all Whisp datasets.
        If provided, this will be used instead of combining datasets based on national_codes.
        If None, datasets will be combined automatically using national_codes parameter.
    Returns
    -------
    ee.FeatureCollection
        Feature collection with calculated statistics
    """

    # Use provided image or combine datasets
    if whisp_image is not None:
        img_combined = whisp_image
        print("Using provided whisp_image")
    else:
        img_combined = combine_datasets(
            national_codes=national_codes,
            validate_bands=validate_bands,
            include_context_bands=False,
        )
        print(f"Combining datasets with national_codes: {national_codes}")

    # Check if the input is a Feature or a FeatureCollection
    if isinstance(feature_or_feature_col, ee.Feature):
        print("Processing single feature")
        # OPTIMIZATION: Create cached images for single feature processing
        water_all = get_water_flag_image()
        bounds_ADM1 = get_admin_boundaries_fc()
        output = ee.FeatureCollection(
            [
                get_stats_feature(
                    feature_or_feature_col,
                    img_combined,
                    unit_type=unit_type,
                    water_all=water_all,
                    bounds_ADM1=bounds_ADM1,
                )
            ]
        )
    elif isinstance(feature_or_feature_col, ee.FeatureCollection):
        print("Processing feature collection")
        output = get_stats_fc(
            feature_or_feature_col,
            national_codes=national_codes,
            unit_type=unit_type,
            img_combined=img_combined,  # Pass the image directly
        )
    else:
        output = "Check inputs: not an ee.Feature or ee.FeatureCollection"
    return output


# Get statistics for a feature collection
def get_stats_fc(feature_col, national_codes=None, unit_type="ha", img_combined=None):
    """
    Calculate statistics for a feature collection using Whisp datasets.

    OPTIMIZATION: Creates water flag and admin_boundaries images once and reuses
    them for all features instead of recreating them for each feature.
    This saves 7-15 seconds per analysis.

    Parameters
    ----------
    feature_col : ee.FeatureCollection
        The input feature collection to analyze
    national_codes : list, optional
        List of ISO2 country codes (e.g., ["BR", "US"]) to include national datasets.
        If provided, only national datasets for these countries and global datasets will be used.
        If None (default), only global datasets will be used.
        Only used if img_combined is None.
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
    img_combined : ee.Image, optional
        Pre-combined multiband image containing all Whisp datasets.
        If provided, this will be used instead of combining datasets based on national_codes.
    Returns
    -------
    ee.FeatureCollection
        Feature collection with calculated statistics
    """
    # OPTIMIZATION: Create cached images once before processing features
    # These will be reused for all features instead of being recreated each time
    water_all = get_water_flag_image()
    bounds_ADM1 = get_admin_boundaries_fc()

    out_feature_col = ee.FeatureCollection(
        feature_col.map(
            lambda feature: get_stats_feature(
                feature,
                img_combined,
                unit_type=unit_type,
                water_all=water_all,
                bounds_ADM1=bounds_ADM1,
            )
        )
    )
    # print(out_feature_col.first().getInfo()) # for testing

    return out_feature_col


# Get statistics for a single feature
# Note: This function doesn't need whisp_image parameter since it already accepts img_combined directly


def get_stats_feature(
    feature, img_combined, unit_type="ha", water_all=None, bounds_ADM1=None
):
    """
    Get statistics for a single feature using a pre-combined image.

    OPTIMIZATION: Accepts cached water/admin_boundaries images to avoid recreating
    them for every feature.

    Parameters
    ----------
    feature : ee.Feature
        The feature to analyze
    img_combined : ee.Image
        Pre-combined image with all the datasets
    unit_type : str, optional
        Whether to use hectares ("ha") or percentage ("percent"), by default "ha".
    water_all : ee.Image, optional
        Cached water flag image
    bounds_ADM1 : ee.FeatureCollection, optional
        Cached admin_boundaries feature collection

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

    # Get basic feature information with cached images
    feature_info = get_type_and_location(feature, water_all, bounds_ADM1)

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
def get_type_and_location(feature, water_all=None, bounds_ADM1=None):
    """
    Extracts basic feature information including country, admin area, geometry type, coordinates, and water flags.

    OPTIMIZATION: Accepts cached water flag image and admin_boundaries collection
    to avoid recreating them for every feature (saves 7-15 seconds per analysis).

    Parameters
    ----------
    feature : ee.Feature
        The feature to extract information from
    water_all : ee.Image, optional
        Cached water flag image. If None, creates it.
    bounds_ADM1 : ee.FeatureCollection, optional
        Cached admin_boundaries feature collection. If None, loads it.

    Returns
    -------
    ee.Dictionary
        Dictionary with feature information
    """
    # Get centroid of the feature's geometry
    centroid = feature.geometry().centroid(0.1)

    # OPTIMIZATION: Use cached admin_boundaries
    if bounds_ADM1 is None:
        bounds_ADM1 = get_admin_boundaries_fc()

    # Fetch location info from GAUL 2024 L1 (country, admin)
    location = ee.Dictionary(get_admin_boundaries_info(centroid, bounds_ADM1))
    country = ee.Dictionary({iso3_country_column: location.get("iso3_code")})

    admin_1 = ee.Dictionary(
        {admin_1_column: location.get("gaul1_name")}
    )  # Administrative level 1 (from GAUL 2024 L1)

    # OPTIMIZATION: Use cached water flag image
    if water_all is None:
        water_all = get_water_flag_image()

    # OPTIMIZATION: Use cached water flag image
    if water_all is None:
        water_all = get_water_flag_image()

    # Prepare the water flag information
    water_flag_dict = value_at_point_flag(
        point=centroid, image=water_all, band_name=water_flag, output_name=water_flag
    )

    # Get the geometry type of the feature
    geom_type = ee.Dictionary({geometry_type_column: feature.geometry().type()})

    # Get the coordinates (latitude, longitude) of the centroid
    coords_list = centroid.coordinates()
    coords_dict = ee.Dictionary(
        {
            centroid_x_coord_column: ee.Number(coords_list.get(0)).format(
                "%.6f"
            ),  # Longitude (6 dp)
            centroid_y_coord_column: ee.Number(coords_list.get(1)).format(
                "%.6f"
            ),  # Latitude (6 dp)
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


# GAUL 2024 L1 - admin units from FAO, allows commercial use
def get_admin_boundaries_info(geometry, bounds_ADM1=None):
    """
    Get GAUL 2024 L1 info for a geometry (country ISO3 code and admin boundary name).

    OPTIMIZATION: Accepts cached GAUL 2024 L1 FeatureCollection to avoid
    reloading it for every feature (saves 2-5 seconds per analysis).

    Parameters
    ----------
    geometry : ee.Geometry
        The geometry to query
    bounds_ADM1 : ee.FeatureCollection, optional
        Cached GAUL 2024 L1 feature collection. If None, loads it.

    Returns
    -------
    ee.Dictionary
        Dictionary with iso3_code (country) and gaul1_name (admin boundary name)
    """
    if bounds_ADM1 is None:
        bounds_ADM1 = get_admin_boundaries_fc()

    polygonsIntersectPoint = bounds_ADM1.filterBounds(geometry)
    backup_dict = ee.Dictionary({"iso3_code": "Unknown", "gaul1_name": "Unknown"})
    return ee.Algorithms.If(
        polygonsIntersectPoint.size().gt(0),
        polygonsIntersectPoint.first()
        .toDictionary()
        .select(["iso3_code", "gaul1_name"]),
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


# helper function to set area to 0 for point geometries
def set_point_geometry_area_to_zero(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set the geometry area column to 0 for features with Point geometry type.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing geometry type and area columns

    Returns
    -------
    pd.DataFrame
        DataFrame with area set to 0 for Point geometries
    """
    # Check if required columns exist
    if geometry_type_column not in df.columns:
        print(
            f"Warning: {geometry_type_column} column not found. Skipping area adjustment for points."
        )
        return df

    if geometry_area_column not in df.columns:
        print(
            f"Warning: {geometry_area_column} column not found. Skipping area adjustment for points."
        )
        return df

    # Create a copy to avoid modifying the original
    df_modified = df.copy()

    # Set area to 0 where geometry type is Point
    point_mask = df_modified[geometry_type_column] == "Point"
    df_modified.loc[point_mask, geometry_area_column] = 0.0

    # Log the changes
    num_points = point_mask.sum()
    # if num_points > 0:
    #     print(f"Set area to 0 for {num_points} Point geometries")

    return df_modified

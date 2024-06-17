import ee
import geemap
import pandas as pd
from pathlib import Path

from parameters.config_runtime import (
    plot_id_column,
    keep_system_index,
    keep_original_properties,
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
    threshold_to_drive,
)
from src.datasets import combine_datasets
from src.tidy_tables import order_list_from_lookup
from src.pd_schemas import DfGEEDatasetsType
from src.logger import StdoutLogger
from src.stats import get_stats_formatted


logger = StdoutLogger()


def whisp_stats_from_geojson_roi(roi_filepath: Path | str) -> pd.DataFrame:
    """

    Parameters
    ----------
    roi_filepath : Path | str
        The filepath to the GeoJSON of the ROI to analyze.

    Returns
    -------
    df_stats : pd.DataFrame
        The dataframe containing the Whisp stats for the input ROI.
    """

    roi = geemap.geojson_to_ee(str(roi_filepath), "r")

    batch_size = ee.FeatureCollection(roi).size().getInfo()
    if batch_size > threshold_to_drive:
        raise MaxBatchSizeError(batch_size)

    processing_lists = make_processing_lists_from_gee_datasets_lookup()
    (
        exclude_list,
        all_datasets_list,
        presence_only_flag_list,
        decimal_place_column_list,
        column_order_list,
    ) = processing_lists
    assert_all_datasets_found_in_lookup(all_datasets_list)

    stats_fc_formatted = get_stats_formatted(
        roi,
        id_name=plot_id_column,
        flag_positive=presence_only_flag_list,
        exclude_properties=exclude_list,
    )

    if keep_system_index:
        stats_fc_formatted = add_system_index_as_property_to_fc(stats_fc_formatted)
        column_order_list.extend(
            [
                element
                for element in ["system:index"]
                if element not in column_order_list
            ]
        )

    if keep_original_properties:
        original_columns = roi.first().propertyNames().getInfo()
        column_order_list.extend(
            [
                element
                for element in original_columns
                if element not in column_order_list
            ]
        )

    df_stats = geemap.ee_to_df(stats_fc_formatted).rename(
        columns={"system_index": "system:index"}
    )
    df_stats = df_stats.reindex(columns=column_order_list)
    return df_stats


def make_processing_lists_from_gee_datasets_lookup(
    lookup_gee_datasets_csv: Path | str = DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
) -> (list[str], list[str], list[str], list[str], list[str]):
    """Generates processing lists for the datasets found in the lookup table.

    Parameters
    ----------
    lookup_gee_datasets_csv: Path | str, default: DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH
        The path to the GEE datasets lookup table.

    Returns
    -------
    exclude_list : list[str]
        The datasets to exclude from the analysis.
    all_datasets_list : list[str]
    The datasets found.
    presence_only_flag_list : list[str]
        The `prensence_only_flag` binary value for the datasets.
    decimal_place_column_list : list[str]
        The decimal place value for the datasets.
    order_list : list[str]
        The order value for the datasets.
    """

    lookup_gee_datasets_df: DfGEEDatasetsType = pd.read_csv(lookup_gee_datasets_csv)
    exclude_list = list(
        lookup_gee_datasets_df["dataset_name"][(lookup_gee_datasets_df["exclude"] == 1)]
    )

    # use the exclude list to filter lookup so all subsequent lists don't contain them
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        (lookup_gee_datasets_df["exclude"] != 1)
    ]
    all_datasets_list = list(lookup_gee_datasets_df["dataset_name"])
    presence_only_flag_list = list(
        lookup_gee_datasets_df["dataset_name"][
            (lookup_gee_datasets_df["presence_only_flag"] == 1)
        ]
    )
    decimal_place_column_list = [
        i for i in all_datasets_list if i not in presence_only_flag_list
    ]
    order_list = order_list_from_lookup(lookup_gee_datasets_df)

    return (
        exclude_list,
        all_datasets_list,
        presence_only_flag_list,
        decimal_place_column_list,
        order_list,
    )


def assert_all_datasets_found_in_lookup(all_datasets_list: list[str]) -> None:
    """Issues a warning of not all listed datasets were found in GEE.

    Parameters
    ----------
    all_datasets_list : list[str]
        The list of datasets found in the lookup table.

    Returns
    -------
    out : None
    """

    multiband_image_list = combine_datasets().bandNames()

    in_both_lists = multiband_image_list.filter(
        ee.Filter.inList("item", all_datasets_list)
    )
    not_in_multiband = multiband_image_list.filter(
        ee.Filter.inList("item", all_datasets_list).Not()
    )
    not_in_lookup = ee.List(all_datasets_list).filter(
        ee.Filter.inList("item", multiband_image_list).Not()
    )

    logger.logger.info(
        f"number_in_multiband_datasets_list: {multiband_image_list.length().getInfo()}"
    )
    logger.logger.info(f"number_in_both_lists: {in_both_lists.length().getInfo()}")
    logger.logger.info(f"not_in_multiband: {not_in_multiband.getInfo()}")

    in_lookup = multiband_image_list.containsAll(ee.List(all_datasets_list)).getInfo()
    logger.logger.info(f"Datasets present in lookup: {in_lookup}")
    if not in_lookup:
        logger.logger.warning(f"Missing from lookup: {not_in_lookup.getInfo()}")


def add_system_index_as_property_to_feature(feature: ee.Feature) -> ee.Feature:
    """Adds the system index to a feature.

    Parameters
    ----------
    feature : ee.Feature
        The input feature.

    Returns
    -------
    enriched_feature : ee.Feature
        The enriched feature.
    """

    # Get the system:index of the feature
    system_index = feature.get("system:index")
    # Set the 'id' property of the feature
    enriched_feature = feature.set("system_index", system_index)
    return enriched_feature


def add_system_index_as_property_to_fc(
    feature_col: ee.FeatureCollection,
) -> ee.FeatureCollection:
    """Adds the system index to a feature collection as a property.

    Parameters
    ----------
    feature_col : ee.FeatureCollection
        The input collection.

    Returns
    -------
    enriched_feature : ee.FeatureCollection
        The enriched feature collection.
    """

    enriched_fc = feature_col.map(add_system_index_as_property_to_feature)
    return enriched_fc


class MaxBatchSizeError(Exception):
    """Returns an error if the batch size to process is larger than the config threshold value.

    Parameters
    ----------
    batch_size : int
        The size of the batch to process.
    """

    def __init__(self, batch_size: int):
        super().__init__(
            f"batch_size > threshold_to_drive ({batch_size} > {threshold_to_drive}). Drive-based batch "
            f"processing not implemented yet."
        )

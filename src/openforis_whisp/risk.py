import pandas as pd

from .pd_schemas import data_lookup_type
from .logger import StdoutLogger, get_whisp_logger


from openforis_whisp.parameters.config_runtime import (
    geometry_area_column,
    read_lookup_table,
    stats_unit_type_column,
)

from openforis_whisp.reformat import filter_lookup_by_country_codes

# could embed this in each function below that uses lookup_gee_datasets_df.
lookup_gee_datasets_df: data_lookup_type = read_lookup_table()

logger = StdoutLogger(__name__)

_METADATA_COLUMN = "whisp_processing_metadata"
_RISK_FLAGS = ("use_for_risk_pcrop", "use_for_risk_acrop", "use_for_risk_timber")


def risk_inputs_left_out(unavailable, lookup=None):
    """
    Of the datasets a stats run left out (the short names in unavailable_datasets), the ones that
    feed a risk tree, each with the risk outputs it feeds, e.g. {"RADD_after_2020": ["pcrop", ...]}.
    `lookup` defaults to the full lookup table; whisp_risk passes the one filtered by country.
    """
    from openforis_whisp.datasets import unavailable_dataset_names

    lookup = lookup_gee_datasets_df if lookup is None else lookup
    preps = lookup["corresponding_variable"].dropna().unique().tolist()
    short_names = dict(zip(preps, unavailable_dataset_names(preps)))
    affected = {}
    for name in unavailable:
        preps_for_name = [p for p, short in short_names.items() if short == name]
        rows = lookup[lookup["corresponding_variable"].isin(preps_for_name)]
        feeds = [
            f.replace("use_for_risk_", "") for f in _RISK_FLAGS if (rows[f] == 1).any()
        ]
        if feeds:
            affected[name] = feeds
    return affected


def _as_metadata(value):
    """A whisp_processing_metadata value as a dict (it is a string if read back from a CSV)."""
    if isinstance(value, str):
        import ast

        try:
            value = ast.literal_eval(value)
        except (ValueError, SyntaxError):
            # not a Python literal: try JSON (true/false/null)
            import json

            try:
                value = json.loads(value)
            except ValueError:
                return {}
    return value if isinstance(value, dict) else {}


def _note_risk_inputs_left_out(df, lookup=None):
    """
    If whisp_processing_metadata lists unavailable_datasets that feed a risk tree, warn through
    the whisp logger (which reaches API job messages) and add risk_inputs_unavailable to each row's
    metadata, since risk is then worked out without them. Rows from different runs (e.g. results
    joined together) are each checked.
    """
    if _METADATA_COLUMN not in df.columns or df.empty:
        return df
    metas = [_as_metadata(m) for m in df[_METADATA_COLUMN]]
    # Always start from scratch: a rerun of whisp_risk (say with different national_codes) must
    # replace the previous run's note, not keep it
    had_note = any("risk_inputs_unavailable" in m for m in metas)
    metas = [
        {k: v for k, v in m.items() if k != "risk_inputs_unavailable"} for m in metas
    ]
    unavailable = sorted({n for m in metas for n in m.get("unavailable_datasets", [])})
    affected = risk_inputs_left_out(unavailable, lookup) if unavailable else {}
    if not affected and not had_note:
        return df
    if affected:
        detail = "; ".join(f"{name} ({', '.join(f)})" for name, f in affected.items())
        get_whisp_logger().warning(
            f"Risk worked out without unavailable dataset(s) that feed it: {detail}"
        )
    df = df.copy()
    # Only rewrite rows whose note changes; the others keep their original value
    new_values = []
    for original, m in zip(df[_METADATA_COLUMN], metas):
        left_out = [n for n in m.get("unavailable_datasets", []) if n in affected]
        if left_out:
            new_values.append({**m, "risk_inputs_unavailable": left_out})
        elif "risk_inputs_unavailable" in _as_metadata(original):
            new_values.append(m)  # previous run's note removed
        else:
            new_values.append(original)
    df[_METADATA_COLUMN] = new_values
    return df


# requires lookup_gee_datasets_df


# Add function to detect unit type from dataframe
def detect_unit_type(df, explicit_unit_type=None):
    """
    Determine the unit type from the dataframe or use the override value.

    Args:
        df (DataFrame): Input DataFrame.
        explicit_unit_type (str, optional): Override unit type ('ha' or 'percent').

    Returns:
        str: The unit type to use for calculations.

    Raises:
        ValueError: If the unit type can't be determined and no override is provided,
                   or if there are mixed unit types in the dataframe.
    """
    # If override is provided, use it
    if explicit_unit_type is not None:
        if explicit_unit_type not in ["ha", "percent"]:
            raise ValueError(
                f"Invalid unit type: {explicit_unit_type}. Must be 'ha' or 'percent'."
            )
        return explicit_unit_type

    # Check if unit type column exists in the dataframe
    if stats_unit_type_column not in df.columns:
        raise ValueError(
            f"Column '{stats_unit_type_column}' not found in dataframe. "
            "Please provide 'explicit_unit_type' parameter to specify the unit type."
        )

    # Get unique values from the column
    unit_types = df[stats_unit_type_column].unique()

    # No rows: the unit makes no difference, and whisp_risk still adds its output columns
    if len(unit_types) == 0:
        return "ha"

    # Check for mixed unit types
    if len(unit_types) > 1:
        raise ValueError(
            f"Mixed unit types in dataframe: {unit_types}. All rows must use the same unit type."
        )

    # Get the single unit type
    unit_type = unit_types[0]

    # Validate that the unit type is recognized
    if unit_type not in ["ha", "percent"]:
        raise ValueError(
            f"Unrecognized unit type: {unit_type}. Must be 'ha' or 'percent'."
        )

    return unit_type


def _dedupe_keep_order(items: list) -> list:
    """Return items with duplicates removed, preserving first-seen order."""
    seen: set = set()
    return [x for x in items if not (x in seen or seen.add(x))]


# Update whisp_risk to accept and pass the unit_type parameter
def whisp_risk(
    df: pd.DataFrame,
    ind_1_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_2_pcent_threshold: float = 50,  # higher than the others: commodity presence in 2020 leads to a low risk outcome, so it must cover most of the plot
    ind_3_pcent_threshold: float = 50,  # higher than the others: pre-2020 disturbance leads to a low risk outcome, so it must cover most of the plot
    ind_4_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_5_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_6_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_7_pcent_threshold: float = 10,  # Ind_07, kept in the output for existing users; no decision tree reads it
    ind_8_pcent_threshold: float = 10,  # Ind_08, kept in the output for existing users; no decision tree reads it
    ind_9_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_10_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_11_pcent_threshold: float = 10,  # Ind_11, kept in the output for existing users; no decision tree reads it
    ind_12_pcent_threshold: float = 10,
    ind_13_pcent_threshold: float = 10,
    ind_15_pcent_threshold: float = 50,  # like Ind_02: agriculture in 2020 leads to a low risk outcome on the timber decision tree, so it must cover most of the plot
    ind_16_pcent_threshold: float = 10,
    ind_18_pcent_threshold: float = 10,  # observed regenerating / planted forest in 2025 (Ind_18)
    ind_19_pcent_threshold: float = 10,  # ForTy V2 primary 2020 no longer primary in 2025 (Ind_19)
    ind_20_pcent_threshold: float = 10,
    ind_21_pcent_threshold: float = 10,
    ind_22_pcent_threshold: float = 10,
    ind_1_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_2_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_3_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_4_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_5_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_6_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_7_input_columns: pd.Series = None,
    ind_8_input_columns: pd.Series = None,
    ind_9_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_10_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_11_input_columns: pd.Series = None,
    ind_12_input_columns: pd.Series = None,
    ind_13_input_columns: pd.Series = None,
    ind_15_input_columns: pd.Series = None,
    ind_16_input_columns: pd.Series = None,
    ind_18_input_columns: pd.Series = None,
    ind_19_input_columns: pd.Series = None,
    ind_20_input_columns: pd.Series = None,
    ind_21_input_columns: pd.Series = None,
    ind_22_input_columns: pd.Series = None,
    ind_1_name: str = "Ind_01_treecover",
    ind_2_name: str = "Ind_02_commodities",
    ind_3_name: str = "Ind_03_disturbance_before_2020",
    ind_4_name: str = "Ind_04_disturbance_after_2020",
    ind_5_name: str = "Ind_05_primary_2020",
    ind_6_name: str = "Ind_06_nat_reg_forest_2020",
    ind_7_name: str = "Ind_07_planted_plantations_2020",
    ind_8_name: str = "Ind_08_planted_plantations_after_2020",
    ind_9_name: str = "Ind_09_treecover_after_2020",
    ind_10_name: str = "Ind_10_agri_after_2020",
    ind_11_name: str = "Ind_11_logging_concession_before_2020",
    ind_12_name: str = "Ind_12_other_land_2020",
    ind_13_name: str = "Ind_13_other_land_after_2020",
    ind_15_name: str = "Ind_15_cropland_2020",
    ind_16_name: str = "Ind_16_plantation_presence_2025",
    ind_18_name: str = "Ind_18_regen_planted_2025",
    ind_19_name: str = "Ind_19_primary_change_after_2020",
    ind_20_name: str = "Ind_20_planted_forest_2020",
    ind_21_name: str = "Ind_21_plantation_forest_2020",
    ind_22_name: str = "Ind_22_plantation_forest_gain_after_2020",
    low_name: str = "no",
    high_name: str = "yes",
    explicit_unit_type: str = None,
    national_codes: list[str] = None,  # List of ISO2 country codes to filter by
    custom_bands_info: dict = None,  # New parameter for custom band risk info
    drop_unused_columns: bool = False,  # Remove columns not used in risk calculations
    include_working_columns: bool = False,  # also return the timber decision tree's working columns
) -> pd.DataFrame:
    """
    Adds the risk column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_pcent_threshold (float, optional): Percentage threshold for indicator 1 (treecover). Defaults to 10.
        ind_2_pcent_threshold (float, optional): Percentage threshold for indicator 2 (commodities). Defaults to 50, so commodity presence in 2020 only leads to a low risk outcome when it covers most of the plot.
        ind_3_pcent_threshold (float, optional): Percentage threshold for indicator 3 (disturbance before 2020). Defaults to 50, so pre-2020 disturbance only leads to a low risk outcome when it covers most of the plot.
        ind_4_pcent_threshold (float, optional): Percentage threshold for indicator 4 (disturbance after 2020). Defaults to 10.
        ind_5_pcent_threshold (float, optional): Percentage threshold for indicator 5 (primary forest 2020). Defaults to 10.
        ind_6_pcent_threshold (float, optional): Percentage threshold for indicator 6 (naturally regenerating forest 2020). Defaults to 10.
        ind_20_pcent_threshold (float, optional): Percentage threshold for indicator 20 (planted forest 2020). Defaults to 10.
        ind_21_pcent_threshold (float, optional): Percentage threshold for indicator 21 (plantation forest 2020). Defaults to 10.
        ind_22_pcent_threshold (float, optional): Percentage threshold for indicator 22 (plantation forest gain after 2020, from primary or naturally regenerating forest in 2020). Defaults to 10.
        ind_9_pcent_threshold (float, optional): Percentage threshold for indicator 9 (treecover after 2020). Defaults to 10.
        ind_10_pcent_threshold (float, optional): Percentage threshold for indicator 10 (agriculture after 2020). Defaults to 10.
        ind_1_input_columns (pd.Series, optional): Input columns for indicator 1. Defaults to columns for the treecover theme.
        ind_2_input_columns (pd.Series, optional): Input columns for indicator 2. Defaults to columns for the commodities theme.
        ind_3_input_columns (pd.Series, optional): Input columns for indicator 3. Defaults to columns for disturbance before 2020.
        ind_4_input_columns (pd.Series, optional): Input columns for indicator 4. Defaults to columns for disturbance after 2020.
        ind_5_input_columns (pd.Series, optional): Input columns for indicator 5. Defaults to columns for primary forest 2020.
        ind_6_input_columns (pd.Series, optional): Input columns for indicator 6. Defaults to columns for naturally regenerating forest 2020.
        ind_20_input_columns (pd.Series, optional): Input columns for indicator 20. Defaults to columns for planted forest 2020.
        ind_21_input_columns (pd.Series, optional): Input columns for indicator 21. Defaults to columns for plantation forest 2020.
        ind_22_input_columns (pd.Series, optional): Input columns for indicator 22. Defaults to the plantation forest gain columns (theme_timber=plantation_forest_gain_after_2020).
        ind_9_input_columns (pd.Series, optional): Input columns for indicator 9. Defaults to columns for treecover after 2020.
        ind_10_input_columns (pd.Series, optional): Input columns for indicator 10. Defaults to columns for agriculture after 2020.
        ind_1_name (str, optional): Name of indicator 1 column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of indicator 2 column. Defaults to "Ind_02_commodities".
        ind_3_name (str, optional): Name of indicator 3 column. Defaults to "Ind_03_disturbance_before_2020".
        ind_4_name (str, optional): Name of indicator 4 column. Defaults to "Ind_04_disturbance_after_2020".
        ind_5_name (str, optional): Name of indicator 5 column. Defaults to "Ind_05_primary_2020".
        ind_6_name (str, optional): Name of indicator 6 column. Defaults to "Ind_06_nat_reg_forest_2020".
        ind_20_name (str, optional): Name of indicator 20 column. Defaults to "Ind_20_planted_forest_2020".
        ind_21_name (str, optional): Name of indicator 21 column. Defaults to "Ind_21_plantation_forest_2020".
        ind_22_name (str, optional): Name of indicator 22 column. Defaults to "Ind_22_plantation_forest_gain_after_2020".
        ind_12_name (str, optional): Name of indicator 12 column. Defaults to "Ind_12_other_land_2020".
        ind_13_name (str, optional): Name of indicator 13 column. Defaults to "Ind_13_other_land_after_2020".
        ind_9_name (str, optional): Name of indicator 9 column. Defaults to "Ind_09_treecover_after_2020".
        ind_10_name (str, optional): Name of indicator 10 column. Defaults to "Ind_10_agri_after_2020".
        ind_15_pcent_threshold (float, optional): Percentage threshold for indicator 15 (timber agriculture 2020). Defaults to 50, matching Ind_02: a yes leads to a low outcome, so it must cover most of the plot.
        ind_15_input_columns (pd.Series, optional): Input columns for indicator 15. Defaults to pre-existing 2020 agriculture/commodity columns flagged for timber.
        ind_15_name (str, optional): Name of indicator 15 column. Defaults to "Ind_15_cropland_2020".
        ind_16_pcent_threshold (float, optional): Percentage threshold for indicator 16 (multi-year plantation presence 2021-2024). Defaults to 10.
        ind_16_input_columns (pd.Series, optional): Input columns for indicator 16. Defaults to the multi-year silviculture-presence columns flagged for timber (theme_timber=plantation_presence_2025).
        ind_16_name (str, optional): Name of indicator 16 column. Defaults to "Ind_16_plantation_presence_2025".
        ind_18_pcent_threshold (float, optional): Percentage threshold for indicator 18 (observed regenerating / planted forest in 2025). Defaults to 10.
        ind_18_input_columns (pd.Series, optional): Input columns for indicator 18. Defaults to the 2025 regenerating / planted forest columns flagged for timber (theme_timber=regen_planted_2025), i.e. ForTy_v2_regen_planted_latest; with none flagged, the regenerating leg falls back to no disturbance after 2020.
        ind_18_name (str, optional): Name of indicator 18 column. Defaults to "Ind_18_regen_planted_2025".
        ind_19_pcent_threshold (float, optional): Percentage threshold for indicator 19 (primary forest in 2020 that ForTy V2 itself no longer calls primary in 2025; on the timber primary leg a change with no disturbance behind it gives more-info instead of LOW). Defaults to 10.
        ind_19_input_columns (pd.Series, optional): Input columns for indicator 19. Defaults to the primary-change columns flagged for timber (theme_timber=primary_change_after_2020), i.e. ForTy_v2_primary_change_after_2020.
        ind_19_name (str, optional): Name of indicator 19 column. Defaults to "Ind_19_primary_change_after_2020".
        low_name (str, optional): Value shown in table if less than or equal to the threshold. Defaults to "no".
        high_name (str, optional): Value shown in table if more than the threshold. Defaults to "yes".
        explicit_unit_type (str, optional): Override the autodetected unit type ('ha' or 'percent').
                                      If not provided, will detect from dataframe 'unit' column.
        national_codes (list[str], optional): List of ISO2 country codes to filter national datasets by. Defaults to None.
        custom_bands_info (dict, optional): Custom band risk information. Dict format:
            {
                'band_name': {
                    'theme': 'treecover',  # or 'commodities', 'disturbance_before', 'disturbance_after'
                    'theme_timber': 'primary',  # or 'naturally_reg_2020', 'planted_2020', 'plantation_2020', etc.
                    'use_for_risk_pcrop': 1,  # 0 or 1 - include in perennial crop risk
                    'use_for_risk_acrop': 1,  # 0 or 1 - include in annual crop risk
                    'use_for_risk_timber': 1,  # 0 or 1
                }
            }
            If None, custom bands won't be included in risk calculations.
        drop_unused_columns (bool, optional): If True, removes dataset columns not used in risk calculations,
            keeping only context/metadata columns, datasets used in indicators, indicator columns,
            and final risk columns. Defaults to False (backward compatible).
        include_working_columns (bool, optional): If True, also returns the columns the timber decision tree
            uses along the way and that are not part of the output yet (Ind_13, Ind_16, Ind_18, primary_2025,
            regen_planted_still_2025, plantation_forest_2020), e.g. for the pathway viewer. Defaults to False.

        Ind_07_planted_plantations_2020, Ind_08_planted_plantations_after_2020 and
        Ind_11_logging_concession_before_2020 are kept in the output for existing users; no decision tree
        reads them (their thresholds, input columns and names can still be set as before).

    Returns:
        pd.DataFrame: DataFrame with added risk columns.
    """
    # Determine the unit type
    unit_type = detect_unit_type(df, explicit_unit_type)
    print(f"Using unit type: {unit_type}")

    lookup_df_copy = lookup_gee_datasets_df.copy()

    # Add custom bands to lookup if provided
    if custom_bands_info:
        lookup_df_copy = add_custom_bands_info_to_lookup(
            lookup_df_copy, custom_bands_info, df.columns
        )
        print(f"Including custom bands: {list(custom_bands_info.keys())}")
    if national_codes:
        print(f"Including additional national data for: {national_codes}")
    # Filter by national codes
    filtered_lookup_gee_datasets_df = filter_lookup_by_country_codes(
        lookup_df=lookup_df_copy,
        filter_col="ISO2_code",
        national_codes=national_codes,
    )

    # Say so if the stats run left out a dataset that feeds a risk tree (national datasets only
    # count when their country is included)
    df = _note_risk_inputs_left_out(df, filtered_lookup_gee_datasets_df)

    # Get indicator columns (now includes custom bands)
    if ind_1_input_columns is None:
        # also the tree cover part of the timber forest gate, so its rows carry use_for_risk_timber=1 too
        ind_1_input_columns = get_cols_ind_01_treecover(filtered_lookup_gee_datasets_df)
    if ind_2_input_columns is None:
        # Union of pcrop + acrop commodity datasets. The two LUT columns are currently held
        # identical (each equal to this union, i.e. the combined commodity signal that was the
        # single use_for_risk column up to v3.0.0a14), so this union is decision-neutral and
        # either column faithfully describes what its tree consumes. The perennial/annual split
        # is NOT yet wired: both crop decision trees consume this same Ind_02. To implement it, repopulate
        # use_for_risk_pcrop/acrop with distinct values and build a per-crop Ind_02 here; the
        # getter already accepts risk_col for that.
        _pcrop_cols = get_cols_ind_02_commodities(
            filtered_lookup_gee_datasets_df, risk_col="use_for_risk_pcrop"
        )
        _acrop_cols = get_cols_ind_02_commodities(
            filtered_lookup_gee_datasets_df, risk_col="use_for_risk_acrop"
        )
        ind_2_input_columns = _dedupe_keep_order(_pcrop_cols + _acrop_cols)
    if ind_3_input_columns is None:
        ind_3_input_columns = get_cols_ind_03_dist_before_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_4_input_columns is None:
        ind_4_input_columns = get_cols_ind_04_dist_after_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_5_input_columns is None:
        ind_5_input_columns = get_cols_ind_05_primary_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_6_input_columns is None:
        ind_6_input_columns = get_cols_ind_06_nat_reg_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_7_input_columns is None:
        ind_7_input_columns = get_cols_ind_07_planted_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_8_input_columns is None:
        ind_8_input_columns = get_cols_ind_08_planted_after_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_9_input_columns is None:
        ind_9_input_columns = get_cols_ind_09_treecover_after_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_10_input_columns is None:
        ind_10_input_columns = get_cols_ind_10_agri_after_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_11_input_columns is None:
        ind_11_input_columns = get_cols_ind_11_logging_before_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_12_input_columns is None:
        ind_12_input_columns = get_cols_ind_12_other_land_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_13_input_columns is None:
        ind_13_input_columns = get_cols_ind_13_other_land_after_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_15_input_columns is None:
        ind_15_input_columns = get_cols_ind_15_cropland_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_16_input_columns is None:
        ind_16_input_columns = get_cols_ind_16_plantation_presence_2025(
            filtered_lookup_gee_datasets_df
        )
    if ind_18_input_columns is None:
        ind_18_input_columns = get_cols_ind_18_regen_planted_2025(
            filtered_lookup_gee_datasets_df
        )
    if ind_19_input_columns is None:
        ind_19_input_columns = get_cols_ind_19_primary_change_after_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_20_input_columns is None:
        ind_20_input_columns = get_cols_ind_20_planted_forest_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_21_input_columns is None:
        ind_21_input_columns = get_cols_ind_21_plantation_forest_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_22_input_columns is None:
        ind_22_input_columns = get_cols_ind_22_plantation_forest_gain_after_2020(
            filtered_lookup_gee_datasets_df
        )

    # Check range of values
    check_range(ind_1_pcent_threshold)
    check_range(ind_2_pcent_threshold)
    check_range(ind_3_pcent_threshold)
    check_range(ind_4_pcent_threshold)
    check_range(ind_5_pcent_threshold)
    check_range(ind_6_pcent_threshold)
    check_range(ind_7_pcent_threshold)
    check_range(ind_8_pcent_threshold)
    check_range(ind_9_pcent_threshold)
    check_range(ind_10_pcent_threshold)
    check_range(ind_11_pcent_threshold)
    check_range(ind_12_pcent_threshold)
    check_range(ind_13_pcent_threshold)
    check_range(ind_15_pcent_threshold)
    check_range(ind_16_pcent_threshold)
    check_range(ind_18_pcent_threshold)
    check_range(ind_19_pcent_threshold)
    check_range(ind_20_pcent_threshold)
    check_range(ind_21_pcent_threshold)
    check_range(ind_22_pcent_threshold)

    input_cols = [
        ind_1_input_columns,
        ind_2_input_columns,
        ind_3_input_columns,
        ind_4_input_columns,
        ind_5_input_columns,
        ind_6_input_columns,
        ind_7_input_columns,
        ind_8_input_columns,
        ind_9_input_columns,
        ind_10_input_columns,
        ind_11_input_columns,
        ind_12_input_columns,
        ind_13_input_columns,
        ind_15_input_columns,
        ind_16_input_columns,
        ind_18_input_columns,
        ind_19_input_columns,
        ind_20_input_columns,
        ind_21_input_columns,
        ind_22_input_columns,
    ]
    thresholds = [
        ind_1_pcent_threshold,
        ind_2_pcent_threshold,
        ind_3_pcent_threshold,
        ind_4_pcent_threshold,
        ind_5_pcent_threshold,
        ind_6_pcent_threshold,
        ind_7_pcent_threshold,
        ind_8_pcent_threshold,
        ind_9_pcent_threshold,
        ind_10_pcent_threshold,
        ind_11_pcent_threshold,
        ind_12_pcent_threshold,
        ind_13_pcent_threshold,
        ind_15_pcent_threshold,
        ind_16_pcent_threshold,
        ind_18_pcent_threshold,
        ind_19_pcent_threshold,
        ind_20_pcent_threshold,
        ind_21_pcent_threshold,
        ind_22_pcent_threshold,
    ]
    names = [
        ind_1_name,
        ind_2_name,
        ind_3_name,
        ind_4_name,
        ind_5_name,
        ind_6_name,
        ind_7_name,
        ind_8_name,
        ind_9_name,
        ind_10_name,
        ind_11_name,
        ind_12_name,
        ind_13_name,
        ind_15_name,
        ind_16_name,
        ind_18_name,
        ind_19_name,
        ind_20_name,
        ind_21_name,
        ind_22_name,
    ]

    # Work on a copy, without the indicator and risk columns of an earlier whisp_risk run
    working_columns = [
        ind_13_name,
        ind_16_name,
        ind_18_name,
        "primary_2025",
        "regen_planted_still_2025",
        "plantation_forest_2020",
    ]
    previous_outputs = (
        names
        + working_columns
        + ["risk_pcrop", "risk_acrop", "risk_timber", "risk_timber_pathway"]
    )
    df = df.drop(columns=[c for c in previous_outputs if c in df.columns])

    missing = [c for cols in input_cols for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Columns needed for the risk indicators are missing: {_dedupe_keep_order(missing)}"
        )

    df_w_indicators = add_indicators(
        df,
        input_cols,
        thresholds,
        names,
        low_name,
        high_name,
        unit_type,  # Pass the unit type
    )

    # The timber decision tree reads the same disturbance-after-2020 evidence as the crop decision trees (Ind_04: TMF
    # deforestation / degradation, Hansen loss, the RADD / GLAD / DIST alerts and, for Brazil, PRODES /
    # DETER). A June 2026 pool that also held the per-year loss bands flagged nearly every primary pixel;
    # today's Ind_04 adds little over Hansen + TMF at plot level, and on the primary leg it gives more-info,
    # not HIGH, so an extra alert only asks for a closer look.

    # "Still primary in 2025" for the timber decision tree: primary in 2020 (Ind_05), no disturbance after 2020
    # (Ind_04) and no change in ForTy V2's own 2020 -> 2025 primary class (Ind_19). Either one sends the
    # plot to more-info on the tree, not LOW. Ind_19 is judged against ForTy's own 2020 class (primary only another
    # layer calls primary is not a change), and a change share rather than a 2025 presence share, which at
    # 10 % of a plot would call a mostly cleared plot still primary. With no Ind_19 row flagged this is the
    # interim 2020-minus-disturbance rule.
    # collected in a list and assigned once, so rows sharing an index label each keep their own value
    primary_2025 = []
    for _, row in df_w_indicators.iterrows():
        primary_2025.append(
            high_name
            if (
                row[ind_5_name] == high_name
                and row[ind_4_name] == low_name
                and row[ind_19_name] == low_name
            )
            else low_name
        )
    df_w_indicators["primary_2025"] = primary_2025

    # "Still regenerating / planted forest in 2025" for the regen leg: regenerating or planted in 2020
    # (Ind_06 or Ind_20) AND, where the lookup flags an observed 2025 source (Ind_18, e.g.
    # ForTy_v2_regen_planted_latest), regenerating or planted forest in 2025 (Ind_18). Unlike primary, the
    # disturbance stack is NOT subtracted once an observed state exists: regenerating forest tolerates
    # disturbance (fire, windthrow, selective logging) and stays forest, and the two changes that count as
    # degradation for it (-> plantation, -> other land) are separate questions in the tree. Positive 2025
    # evidence replaces the interim absence-of-disturbance rule (caveat 27); with no Ind_18 source flagged
    # the interim rule (NOT Ind_04) applies. Ind_09 canopy stays out of the tree.
    observed_regen_planted_2025 = len(ind_18_input_columns) > 0
    regen_planted_still_2025 = []
    for _, row in df_w_indicators.iterrows():
        regen_planted_2020 = (
            row[ind_6_name] == high_name or row[ind_20_name] == high_name
        )
        if observed_regen_planted_2025:
            still_2025 = row[ind_18_name] == high_name
        else:
            still_2025 = row[ind_4_name] == low_name
        regen_planted_still_2025.append(
            high_name if (regen_planted_2020 and still_2025) else low_name
        )
    df_w_indicators["regen_planted_still_2025"] = regen_planted_still_2025

    # Plantation forest in 2020 for the forest gate: plantation (Ind_21) with no agriculture in 2020
    # (Ind_02 or Ind_15). The plantation forest maps (IIASA, ForTy V2) also fire on rubber and oil palm,
    # which would then read as deforestation on replanting; with agriculture mapped in 2020 the plot stays in
    # the non-forest half and reads "agriculture 2020".
    plantation_forest_2020 = []
    for _, row in df_w_indicators.iterrows():
        plantation_forest_2020.append(
            high_name
            if (
                row[ind_21_name] == high_name
                and row[ind_2_name] == low_name
                and row[ind_15_name] == low_name
            )
            else low_name
        )
    df_w_indicators["plantation_forest_2020"] = plantation_forest_2020

    # these "add_" functions modify the 'df_w_indicators' dataframe in place
    add_risk_pcrop_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_3_name=ind_3_name,
        ind_4_name=ind_4_name,
        low_name=low_name,
        high_name=high_name,
    )

    add_risk_acrop_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_4_name=ind_4_name,
        low_name=low_name,
        high_name=high_name,
    )

    add_risk_timber_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_5_name=ind_5_name,
        ind_6_name=ind_6_name,
        ind_20_name=ind_20_name,
        ind_21_name=ind_21_name,
        ind_22_name=ind_22_name,
        ind_9_name=ind_9_name,
        ind_10_name=ind_10_name,
        ind_12_name=ind_12_name,
        ind_13_name=ind_13_name,
        ind_15_name=ind_15_name,
        ind_16_name=ind_16_name,
        ind_4_name=ind_4_name,
        ind_18_name=ind_18_name,
        primary_2025_name="primary_2025",
        regen_planted_still_2025_name="regen_planted_still_2025",
        plantation_forest_2020_name="plantation_forest_2020",
        low_name=low_name,
        high_name=high_name,
    )

    # Filter to risk-relevant columns if requested (after all columns added)
    if drop_unused_columns:
        df_w_indicators = filter_to_risk_columns(df_w_indicators, input_cols, names)

    # The timber decision tree's working columns stay out of the output for now: Ind_13, Ind_16 and Ind_18
    # are states after 2020, to be replaced by changes measured within one dataset, and the three derived
    # columns can be rebuilt from emitted indicators. risk_timber_pathway says which way each plot went.
    if not include_working_columns:
        df_w_indicators = df_w_indicators.drop(
            columns=[c for c in working_columns if c in df_w_indicators.columns]
        )

    return df_w_indicators


def add_risk_pcrop_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_3_name: str,
    ind_4_name: str,
    low_name: str = "no",
    high_name: str = "yes",
) -> data_lookup_type:
    """
    Adds the risk column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Ind_02_commodities".
        ind_3_name (str, optional): Name of third indicator column. Defaults to "Ind_03_disturbance_before_2020".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Ind_04_disturbance_after_2020".
        low_name (str, optional): Indicator value for less than or equal to the threshold. Defaults to "no".
        high_name (str, optional): Indicator value for more than the threshold. Defaults to "yes".

    Returns:
        DataFrame: DataFrame with added 'risk' column.
    """

    # collected in a list and assigned once, so rows sharing an index label each keep their own risk
    risks = []
    for _, row in df.iterrows():
        # If any of the first three indicators suggest low risk, set risk to "low"
        if (
            row[ind_1_name] == low_name
            or row[ind_2_name] == high_name
            or row[ind_3_name] == high_name
        ):
            risks.append("low")
        # If none of the first three indicators suggest low risk and Indicator 4 suggests no risk, set risk to "more_info_needed"
        elif row[ind_4_name] == low_name:
            risks.append("more_info_needed")
        # If none of the above conditions are met, set risk to "high"
        else:
            risks.append("high")
    df["risk_pcrop"] = risks

    return df


def add_risk_acrop_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_4_name: str,
    low_name: str = "no",
    high_name: str = "yes",
) -> data_lookup_type:
    """
    Adds the risk column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Ind_02_commodities".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Ind_04_disturbance_after_2020".
        low_name (str, optional): Indicator value for less than or equal to the threshold. Defaults to "no".
        high_name (str, optional): Indicator value for more than the threshold. Defaults to "yes".

    Returns:
        DataFrame: DataFrame with added 'risk' column.
    """

    # soy risk
    risks = []
    for _, row in df.iterrows():
        # If there is no tree cover in 2020, set risk_soy to "low"
        if row[ind_1_name] == low_name or row[ind_2_name] == high_name:
            risks.append("low")
        # If there is tree cover in 2020 and distrubances post 2020, set risk_soy to "high"
        elif row[ind_1_name] == high_name and row[ind_4_name] == high_name:
            risks.append("high")
        # If tree cover and no disturbances post 2020, set risk to "more_info_needed"
        else:
            risks.append("more_info_needed")
    df["risk_acrop"] = risks

    return df


# ---- ROOT timber decision tree, declarative spec (single source of truth) --------------------------
# Each node is a LEAF {"risk", "pathway"} or a DECISION {"q": <derived-boolean name>, "yes": node,
# "no": node}. _eval_timber_tree walks it against the per-plot booleans built in add_risk_timber_col.
# The tree is DATA (no lambdas), so a later generator can walk the SAME structure to emit the Mermaid
# diagram, the viewer JS walk and the map pathway(), keeping the display copies from drifting.
def _timber_leaf(risk, pathway):
    return {"risk": risk, "pathway": pathway}


TIMBER_ROOT_TREE = {
    "q": "forest_2020",
    "no": {  # non-forest half: no forest in 2020, so nothing to deforest/degrade
        "q": "other_land_2020",
        "yes": {
            "q": "other_land_2025",
            "yes": _timber_leaf("low", "low: other land 2020 (stable)"),
            "no": _timber_leaf(
                "more_info_needed", "more-info: other land 2020 changed"
            ),
        },
        "no": {
            "q": "agriculture_2020",
            "yes": _timber_leaf("low", "low: agriculture 2020"),
            "no": _timber_leaf(
                "more_info_needed", "more-info: non-forest 2020, unclassified"
            ),
        },
    },
    "yes": {  # forest half: was forest in 2020
        "q": "agriculture_2025",
        "yes": _timber_leaf("high", "high: deforestation"),
        "no": {
            "q": "primary_2020",  # PRIMARY checked FIRST among the forest classes (before regen AND
            # plantation): a genuine primary plot that a broad plantation_2020 layer ALSO flags is no longer
            # stolen into the stable-plantation branch and masked, so its degradation HIGH can still fire
            # (caveat 28 fix; plantation_2020 is now checked LAST, below).
            "yes": {
                "q": "plantation_2025",  # Ind_22 gain = degradation, checked FIRST
                "yes": _timber_leaf(
                    "high", "high: primary -> plantation forest (degradation)"
                ),
                # Then DISTURBANCE after 2020 (Ind_04) -> MORE INFO. Disturbance alone is not a non-compliant
                # end state (in the EUDR transition table, as one example, primary forest that is logged or
                # burnt and regrows naturally is deforestation- and degradation-free); HIGH needs evidence of
                # the end state (agriculture above, plantation gain just before). But ForTy V2 still calls a
                # third or more of cleared primary "primary" in some regions, so a disturbed plot must never
                # read still primary -> LOW: it goes to more-info for a closer look. A driver layer could
                # later separate natural disturbance (#241).
                "no": {
                    "q": "disturbed_after_2020",
                    "yes": _timber_leaf(
                        "more_info_needed",
                        "more-info: primary 2020, disturbed after 2020",
                    ),
                    # Undisturbed: still primary (no change in ForTy V2's own 2020 -> 2025 primary class) -> LOW;
                    # a ForTy change with no disturbance behind it -> more-info (a modelled change alone neither
                    # clears nor condemns the plot).
                    "no": {
                        "q": "primary_2025",
                        "yes": _timber_leaf("low", "low: still primary"),
                        "no": _timber_leaf(
                            "more_info_needed",
                            "more-info: primary 2020, changed without disturbance",
                        ),
                    },
                },
            },
            "no": {
                "q": "regen_planted_2020",  # REGEN checked before plantation, same caveat-28 reason
                "yes": {
                    "q": "plantation_2025",  # Ind_22 gain = degradation, checked FIRST
                    "yes": _timber_leaf(
                        "high", "high: regenerating -> plantation forest (degradation)"
                    ),
                    # Per the 2026-07-29 planned diagram the 2025 state is asked FIRST, as a positive
                    # question: still regenerating / planted forest in 2025? Derived upstream as
                    # regen_planted_2020 AND the OBSERVED Ind_18 (ForTy V2 2025) where a source is flagged,
                    # else AND NOT Ind_04_disturbance_after_2020 (the interim rule). Broad canopy
                    # (Ind_09) stays out. Only a plot that is no longer regen / planted forest goes on to the
                    # other-land question, then more-info.
                    "no": {
                        "q": "regen_planted_still_2025",
                        "yes": _timber_leaf("low", "low: regen stayed forest"),
                        "no": {
                            "q": "other_land_2025",
                            "yes": _timber_leaf("low", "low: regen 2020 -> other land"),
                            "no": _timber_leaf(
                                "more_info_needed",
                                "more-info: regen 2020, no later state",
                            ),
                        },
                    },
                },
                # PLANTATION checked LAST: only a plot that is NOT primary and NOT regen reaches here, so a
                # plantation_2020 commission over genuine primary/regen forest can no longer pre-empt and mask
                # the degradation HIGH (caveat 28). A true established plantation still lands here as before.
                "no": {
                    "q": "plantation_2020",
                    "yes": {  # existing plantation: no degradation HIGH here
                        "q": "plantation_presence_2025",
                        "yes": _timber_leaf("low", "low: still plantation forest"),
                        "no": {
                            "q": "other_land_2025",
                            "yes": _timber_leaf(
                                "low", "low: plantation forest 2020 -> other land"
                            ),
                            "no": _timber_leaf(
                                "more_info_needed",
                                "more-info: plantation forest 2020, no later state",
                            ),
                        },
                    },
                    # forest by treecover but no primary/regen/plantation class: give it the other-land
                    # chance before defaulting to more-info (revives code 14, the generic forest -> other land).
                    "no": {
                        "q": "other_land_2025",
                        "yes": _timber_leaf("low", "low: forest 2020 -> other land"),
                        "no": _timber_leaf(
                            "more_info_needed", "more-info: forest 2020, unclassified"
                        ),
                    },
                },
            },
        },
    },
}


def _eval_timber_tree(yes):
    """Walk TIMBER_ROOT_TREE against a dict of per-plot booleans; return (risk_timber, risk_timber_pathway)."""
    node = TIMBER_ROOT_TREE
    while "q" in node:
        node = node["yes"] if yes[node["q"]] else node["no"]
    return node["risk"], node["pathway"]


def add_risk_timber_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_12_name: str,
    ind_5_name: str,
    ind_6_name: str,
    ind_20_name: str,
    ind_21_name: str,
    ind_22_name: str,
    ind_9_name: str,
    ind_10_name: str,
    ind_13_name: str,
    primary_2025_name: str,
    ind_15_name: str = "Ind_15_cropland_2020",
    ind_16_name: str = "Ind_16_plantation_presence_2025",
    ind_4_name: str = "Ind_04_disturbance_after_2020",
    ind_18_name: str = "Ind_18_regen_planted_2025",
    regen_planted_still_2025_name: str = "regen_planted_still_2025",
    plantation_forest_2020_name: str = "plantation_forest_2020",
    low_name: str = "no",
    high_name: str = "yes",
) -> data_lookup_type:
    """
    Adds the risk_timber column from the timber decision tree (TIMBER_ROOT_TREE above), which checks the
    2020 state and the 2025 state of each plot, following the land-use transitions of the EUDR table (as
    one example of a framework).

    Planted (Ind_20_planted_forest_2020) is grouped with naturally regenerating (Ind_06_nat_reg_forest_2020)
    into one "regenerating or planted" class, because the two are not reliably separable in the data;
    plantation (Ind_21_plantation_forest_2020, Ind_22_plantation_forest_gain_after_2020) is kept separate as the class
    that marks degradation.

    The questions, in order:
    - Forest in 2020? Ind_01_treecover, Ind_05_primary_2020, Ind_06_nat_reg_forest_2020,
      Ind_20_planted_forest_2020, or Ind_21_plantation_forest_2020 with no agriculture in 2020 (the derived
      plantation_forest_2020).
    - Not forest in 2020: other land 2020 (Ind_12_other_land_2020) gives low if still other land in 2025
      (Ind_13_other_land_after_2020), else more info; otherwise agriculture 2020 (Ind_02_commodities or
      Ind_15_cropland_2020) gives low, else more info.
    - Forest in 2020: agriculture after 2020 (Ind_10_agri_after_2020) gives high (deforestation). Then by
      2020 class, primary first, then regenerating or planted, then plantation, so a plot mapped as two
      classes takes the first:
      - primary: plantation gain (Ind_22_plantation_forest_gain_after_2020) high; disturbed after 2020
        (Ind_04_disturbance_after_2020) more info; still primary (primary_2025) low, else more info;
      - regenerating or planted: plantation gain high; still regenerating or planted
        (regen_planted_still_2025) low; other land 2025 low; else more info;
      - plantation: still a plantation (Ind_16_plantation_presence_2025) low; other land 2025 low; else
        more info;
      - none of the three: other land 2025 low, else more info.

    The derived 2025 states come from whisp_risk: primary_2025 = Ind_05 AND NOT Ind_04 AND NOT
    Ind_19_primary_change_after_2020; regen_planted_still_2025 = (Ind_06 OR Ind_20) AND
    Ind_18_regen_planted_2025 where a source is flagged, else AND NOT Ind_04. Plantation gain is a gain
    (0 on a plantation already standing in 2020), so stability uses the presence layer, Ind_16.

    Also writes risk_timber_pathway: the label of the end point reached, aligned with the map's pathway
    codes.

    low_name / high_name: the indicator values for at or below / above the threshold (default "no" / "yes").

    Returns:
        DataFrame with risk_timber and risk_timber_pathway columns added.
    """

    # collected in lists and assigned once, so rows sharing an index label each keep their own outcome
    risks, pathways = [], []
    for _, row in df.iterrows():
        primary_2020 = row[ind_5_name] == high_name
        # ROOT tree checks primary BEFORE regen, so the regen branch and the forest gate use plain
        # (Ind_06 OR Ind_20) with NO "and not primary" carve-out: a primary+regen overlap resolves to
        # primary by ORDER, not exclusion.
        regen_planted_2020 = (
            row[ind_6_name] == high_name or row[ind_20_name] == high_name
        )
        plantation_2020 = row[ind_21_name] == high_name
        treecover_2020 = row[ind_1_name] == high_name
        # "Forest in 2020?" gate: tree cover (Ind_01, shared with the crop decision trees) OR a 2020 forest
        # type (primary, regenerating, planted, or plantation without agriculture in 2020), so any forest
        # evidence sends the plot to the forest half, where its deforestation and degradation are checked.
        forest_2020 = (
            treecover_2020
            or primary_2020
            or regen_planted_2020
            or row[plantation_forest_2020_name] == high_name
        )

        yes_locals = {
            "forest_2020": forest_2020,
            "other_land_2020": row[ind_12_name] == high_name,
            "other_land_2025": row[ind_13_name] == high_name,
            "agriculture_2020": (row[ind_2_name] == high_name)
            or (row[ind_15_name] == high_name),
            "agriculture_2025": row[ind_10_name] == high_name,
            "plantation_2020": plantation_2020,
            # Ind_16 PRESENCE (stability) -> stable-plantation LOW, plantation-2020 branch only.
            "plantation_presence_2025": row[ind_16_name] == high_name,
            "primary_2020": primary_2020,
            # Ind_04 asked directly on the primary leg: disturbed primary -> more-info.
            "disturbed_after_2020": row[ind_4_name] == high_name,
            # primary_2025 derived upstream = Ind_05 AND NOT Ind_04 AND NOT Ind_19 (ForTy change).
            "primary_2025": row[primary_2025_name] == high_name,
            # Ind_09_treecover_after_2020 stays DROPPED from the tree (too broad: generic ESRI canopy; and
            # regionally uneven: TMF regrowth is tropics-only). Still regenerating / planted forest in 2025 =
            # DERIVED upstream as regen_planted_2020 AND the OBSERVED Ind_18 where a source is flagged (else
            # AND NOT Ind_04, the interim rule). (ind_9_name and ind_18_name are retained for signature
            # stability; Ind_18 is consumed through the derived regen column, Ind_09 is still emitted.)
            "regen_planted_still_2025": row[regen_planted_still_2025_name] == high_name,
            "regen_planted_2020": regen_planted_2020,
            # Ind_22 GAIN (new plantation) = degradation -> HIGH, checked FIRST in the primary/regen branches.
            "plantation_2025": row[ind_22_name] == high_name,
        }
        risk_timber, pathway = _eval_timber_tree(yes_locals)
        risks.append(risk_timber)
        pathways.append(pathway)
    df["risk_timber"] = risks
    df["risk_timber_pathway"] = pathways

    return df


def add_indicators(
    df: data_lookup_type,
    input_cols: list[str],
    thresholds: list[float],
    names: list[str],
    low_name: str = "no",
    high_name: str = "yes",
    unit_type: str = None,
) -> data_lookup_type:
    for input_col, threshold, name in zip(input_cols, thresholds, names):
        df = add_indicator_column(
            df=df,
            input_columns=input_col,
            threshold=threshold,
            new_column_name=name,
            low_name=low_name,
            high_name=high_name,
            sum_comparison=False,
            unit_type=unit_type,  # Pass the unit type
        )
    return df


def add_indicator_column(
    df: data_lookup_type,
    input_columns: list[str],
    threshold: float,
    new_column_name: str,
    low_name: str = "no",
    high_name: str = "yes",
    sum_comparison: bool = False,
    unit_type: str = None,
) -> data_lookup_type:
    """Add a new column to the DataFrame based on the specified columns, threshold, and comparison sign."""

    # Create a new column and initialize with low_name
    new_column = pd.Series(low_name, index=df.index, name=new_column_name)

    if sum_comparison:
        # Sum all values in specified columns and compare to threshold
        sum_values = df[input_columns].sum(axis=1)
        new_column[sum_values > threshold] = high_name
    else:
        # Check if any values in specified columns are above the threshold
        for col in input_columns:
            if unit_type == "ha":
                df[geometry_area_column] = pd.to_numeric(
                    df[geometry_area_column], errors="coerce"
                )

                # Handle points (Area = 0) separately
                is_point = df[geometry_area_column] == 0

                # For points: any value > 0 exceeds threshold
                point_mask = is_point & (df[col] > 0)
                new_column[point_mask] = high_name

                # For polygons: convert to percentage and check threshold
                polygon_mask = ~is_point
                if polygon_mask.any():
                    val_to_check = clamp(
                        (
                            (
                                df.loc[polygon_mask, col]
                                / df.loc[polygon_mask, geometry_area_column]
                            )
                            * 100
                        ),
                        0,
                        100,
                    )
                    new_column[polygon_mask & (val_to_check > threshold)] = high_name
            else:
                # For percentage values, use direct comparison
                val_to_check = df[col]
                new_column[val_to_check > threshold] = high_name

    # Concatenate the new column to the DataFrame
    df = pd.concat([df, new_column], axis=1)
    return df


def get_cols_ind_01_treecover(lookup_gee_datasets_df, risk_col="use_for_risk_pcrop"):
    """
    Generate a list of dataset names for the treecover theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.
    risk_col (str): Column to filter on for risk inclusion. Use 'use_for_risk_pcrop' or 'use_for_risk_acrop'.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the treecover theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df[risk_col] == 1)
            & (lookup_gee_datasets_df["theme"] == "treecover")
        ]
    )


def get_cols_ind_02_commodities(lookup_gee_datasets_df, risk_col="use_for_risk_pcrop"):
    """
    Generate a list of dataset names for the commodities theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.
    risk_col (str): Column to filter on for risk inclusion. Use 'use_for_risk_pcrop' or 'use_for_risk_acrop'.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the commodities theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df[risk_col] == 1)
            & (lookup_gee_datasets_df["theme"] == "commodities")
        ]
    )


def get_cols_ind_15_cropland_2020(lookup_gee_datasets_df):
    # Generic cropland in 2020 (theme_timber=cropland_2020: GLAD cropland, ESRI crops), maps that name no
    # crop and so are not in Ind_02_commodities. The timber decision tree reads "agriculture in 2020" as
    # Ind_02 OR Ind_15, at the same 50% threshold.
    df = lookup_gee_datasets_df[lookup_gee_datasets_df["exclude_from_output"] != 1]
    return list(
        df["name"][
            (df["use_for_risk_timber"] == 1) & (df["theme_timber"] == "cropland_2020")
        ]
    )


def get_cols_ind_03_dist_before_2020(
    lookup_gee_datasets_df, risk_col="use_for_risk_pcrop"
):
    """
    Generate a list of dataset names for the disturbance before 2020 theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.
    risk_col (str): Column to filter on for risk inclusion. Use 'use_for_risk_pcrop' or 'use_for_risk_acrop'.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the disturbance before 2020 theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df[risk_col] == 1)
            & (lookup_gee_datasets_df["theme"] == "disturbance_before")
        ]
    )


def get_cols_ind_18_regen_planted_2025(lookup_gee_datasets_df):
    """
    Dataset names for an observed regenerating or planted forest state in 2025 (theme_timber=
    regen_planted_2025), e.g. ForTy V2's 2025 naturally regenerating and planted classes. Feeds the timber
    decision tree's "still regenerating or planted forest in 2025?" question on the regenerating leg,
    through the derived regen_planted_still_2025. With no such row flagged for timber, whisp_risk falls
    back to "no disturbance after 2020".
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "regen_planted_2025")
        ]
    )


def get_cols_ind_19_primary_change_after_2020(lookup_gee_datasets_df):
    """
    Dataset names for primary forest in 2020 that ForTy V2 itself no longer calls primary in 2025
    (theme_timber=primary_change_after_2020). On the timber primary leg a change with no post-2020 disturbance
    behind it goes to more-info instead of LOW. Returns [] while no row carries use_for_risk_timber=1, in
    which case Ind_19_primary_change_after_2020 is always "no".
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "primary_change_after_2020")
        ]
    )


def get_cols_ind_04_dist_after_2020(
    lookup_gee_datasets_df, risk_col="use_for_risk_pcrop"
):
    """
    Generate a list of dataset names for the disturbance after 2020 theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.
    risk_col (str): Column to filter on for risk inclusion. Use 'use_for_risk_pcrop' or 'use_for_risk_acrop'.

    Returns:
    list: List of dataset names set to be used in the risk calculations  for the disturbance after 2020 theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df[risk_col] == 1)
            & (lookup_gee_datasets_df["theme"] == "disturbance_after")
        ]
    )


def get_cols_ind_05_primary_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for primary forests in 2020

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the degradation - primary forest in 2020, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "primary")
        ]
    )


def get_cols_ind_06_nat_reg_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for naturally_reg_2020 forests in 2020

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the degradation - naturally_reg_2020 in 2020, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "naturally_reg_2020")
        ]
    )


def get_cols_ind_20_planted_forest_2020(lookup_gee_datasets_df):
    """
    Dataset names for planted forest (non-plantation) in 2020 (theme_timber=planted_2020).
    Split out from the old merged Ind_07; grouped with nat-regen (Ind_06) in the timber decision tree.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "planted_2020")
        ]
    )


def get_cols_ind_21_plantation_forest_2020(lookup_gee_datasets_df):
    """
    Dataset names for plantation forest in 2020 (theme_timber=plantation_2020).
    The operative plantation class in the timber decision tree (stable-plantation / degradation logic).
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "plantation_2020")
        ]
    )


def get_cols_ind_22_plantation_forest_gain_after_2020(lookup_gee_datasets_df):
    """
    Dataset names for plantation forest gain after 2020 (theme_timber=plantation_forest_gain_after_2020):
    ForTy_v2_plantation_forest_gain_after_2020 and, in Brazil, nBR_MapBiomas_col10_silviculture_gain_after_2020,
    each counting plantation forest only where its own map had primary or naturally regenerating forest in
    2020 (the EUDR degradation conversion). Asked on the primary and naturally regenerating branches of the
    timber decision tree, which say where the plot came from.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (
                lookup_gee_datasets_df["theme_timber"]
                == "plantation_forest_gain_after_2020"
            )
        ]
    )


def get_cols_ind_07_planted_2020(lookup_gee_datasets_df):
    """
    Dataset names for Ind_07_planted_plantations_2020, kept in the output for existing users: planted or
    plantation forest in 2020, i.e. the datasets of Ind_20_planted_forest_2020 and Ind_21_plantation_forest_2020
    together, plus any custom band with the earlier theme_timber value planted_plantation_2020. No
    decision tree reads it.
    """
    old_theme = lookup_gee_datasets_df[
        (lookup_gee_datasets_df["exclude_from_output"] != 1)
        & (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
        & (lookup_gee_datasets_df["theme_timber"] == "planted_plantation_2020")
    ]
    return _dedupe_keep_order(
        get_cols_ind_20_planted_forest_2020(lookup_gee_datasets_df)
        + get_cols_ind_21_plantation_forest_2020(lookup_gee_datasets_df)
        + list(old_theme["name"])
    )


def get_cols_ind_08_planted_after_2020(lookup_gee_datasets_df):
    """
    Dataset names for Ind_08_planted_plantations_after_2020, kept in the output for existing users. No
    dataset carries its theme_timber value (planted_plantation_after_2020), so it is "no" as before (see
    #195); plantation forest gain after 2020 is Ind_22_plantation_forest_gain_after_2020. No
    decision tree reads it.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (
                lookup_gee_datasets_df["theme_timber"]
                == "planted_plantation_after_2020"
            )
        ]
    )


def get_cols_ind_11_logging_before_2020(lookup_gee_datasets_df):
    """
    Dataset names for Ind_11_logging_concession_before_2020 (theme_timber=logging_concession), kept in the
    output for existing users. The timber decision tree no longer reads it, so its rows carry
    use_for_risk_timber=0 and are picked by theme_timber alone.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            lookup_gee_datasets_df["theme_timber"] == "logging_concession"
        ]
    )


def get_cols_ind_09_treecover_after_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for treecover post 2020

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the degradation - treecover post 2020, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "treecover_after_2020")
        ]
    )


def get_cols_ind_10_agri_after_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for croplands post 2020

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the degradation - croplands post 2020, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "agri_after_2020")
        ]
    )


def get_cols_ind_12_other_land_2020(lookup_gee_datasets_df):
    """
    Dataset names for other land in 2020 (theme_timber=other_land_2020: ESRI other land, global mining
    and, in Brazil, MapBiomas other land). The first question in the non-forest half of the timber
    decision tree.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "other_land_2020")
        ]
    )


def get_cols_ind_13_other_land_after_2020(lookup_gee_datasets_df):
    """
    Dataset names for other land in 2025 (theme_timber=other_land_after_2020: ESRI other land 2025,
    global mining and, in Brazil, MapBiomas other land 2024 and mining gain). A presence in 2025, not a
    change: the timber decision tree asks it after other land 2020 and on the forest legs.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "other_land_after_2020")
        ]
    )


def get_cols_ind_16_plantation_presence_2025(lookup_gee_datasets_df):
    """
    Dataset names for a multi-year plantation PRESENCE signal (theme_timber=plantation_presence_2025).
    Sources: ForTy V2 plantation 2025 and, in Brazil, MapBiomas Collection 10 silviculture in any year
    2021-2024. This is a presence / not-loss layer (NOT the gain), used ONLY by the plantation-2020
    stability check in the timber decision tree ("still a plantation in 2025?"). The GAIN layer (Ind_22) is 0 on any plantation
    already standing in 2020, so using it for stability wrongly fails stable plantations to MORE INFO;
    this presence layer fixes that while Ind_22 stays the degradation signal on the regen/primary nodes.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "plantation_presence_2025")
        ]
    )


def clamp(
    value: float | pd.Series, min_val: float, max_val: float
) -> float | pd.Series:
    """
    Clamp a value or a Pandas Series within a specified range.

    Args:
        value (float | pd.Series): The value or series to be clamped.
        min_val (float): The minimum value of the range.
        max_val (float): The maximum value of the range.

    Returns:
        float | pd.Series: The clamped value or series within the range.
    """
    if isinstance(value, pd.Series):
        return value.clip(lower=min_val, upper=max_val)
    else:
        return max(min_val, min(value, max_val))


def check_range(value: float) -> None:
    if not (0 <= value <= 100):
        raise ValueError("Value must be between 0 and 100.")


def get_context_metadata_columns() -> list[str]:
    """
    Get list of context/metadata column names from lookup table.

    Returns
    -------
    list[str]
        List of column names marked as context_and_metadata
    """
    return list(
        lookup_gee_datasets_df["name"][
            lookup_gee_datasets_df["theme"] == "context_and_metadata"
        ]
    )


def filter_to_risk_columns(
    df: pd.DataFrame, input_cols: list[list[str]], names: list[str]
) -> pd.DataFrame:
    """
    Filter DataFrame to only columns relevant for risk calculations.

    Keeps:
    - Context/metadata columns (plotId, Area, Country, etc.)
    - Dataset columns used in risk indicators
    - Indicator columns (Ind_01_treecover, etc.)
    - Risk columns (risk_pcrop, risk_acrop, risk_timber)

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with all columns
    input_cols : list[list[str]]
        List of lists containing dataset column names used in each indicator
    names : list[str]
        Names of indicator columns

    Returns
    -------
    pd.DataFrame
        Filtered DataFrame with only risk-relevant columns
    """
    # Get context/metadata columns
    context_cols = get_context_metadata_columns()

    # Flatten input_cols to get dataset columns used in risk
    dataset_cols = []
    for col_list in input_cols:
        dataset_cols.extend(col_list)

    # Risk output columns (present in df if function called at end)
    risk_cols = ["risk_pcrop", "risk_acrop", "risk_timber", "risk_timber_pathway"]

    # Derived columns computed in whisp_risk and kept in the output: primary_2025 (Ind_05 AND NOT Ind_04
    # AND NOT Ind_19) and regen_planted_still_2025 (Ind_06 or Ind_20, AND observed Ind_18 where flagged,
    # else AND NOT Ind_04).
    derived_cols = [
        "primary_2025",
        "regen_planted_still_2025",
        "plantation_forest_2020",
    ]

    # Post-processing metadata columns (added after validation, not in schema CSV)
    metadata_cols = ["whisp_processing_metadata", "geo_original"]

    # Build set of all columns to keep (for fast lookup)
    cols_to_keep_set = set(
        context_cols + dataset_cols + names + risk_cols + derived_cols + metadata_cols
    )

    # Preserve original DataFrame column order, filter to only columns we want to keep
    cols_to_keep = [col for col in df.columns if col in cols_to_keep_set]

    # Log dropped columns at debug level
    dropped_cols = [col for col in df.columns if col not in cols_to_keep_set]
    if dropped_cols:
        logger.debug(
            f"Dropped {len(dropped_cols)} columns: {', '.join(sorted(dropped_cols))}"
        )

    return df[cols_to_keep]


def add_custom_bands_info_to_lookup(
    lookup_df: pd.DataFrame, custom_bands_info: dict, df_columns: list
) -> pd.DataFrame:
    """
    Add custom bands to the lookup DataFrame for risk calculations.

    Parameters
    ----------
    lookup_df : pd.DataFrame
        Original lookup DataFrame
    custom_bands_info : dict
        Custom band definitions with risk info
    df_columns : list
        List of columns in the actual data DataFrame

    Returns
    -------
    pd.DataFrame
        Lookup DataFrame with custom bands added
    """
    custom_rows = []

    for band_name, band_info in custom_bands_info.items():
        # Only add bands that actually exist in the DataFrame
        if band_name in df_columns:
            custom_row = {
                "name": band_name,  # Use the band name as provided
                "theme": band_info.get(
                    "theme", pd.NA
                ),  # default to empty if not provided
                "theme_timber": band_info.get(
                    "theme_timber", pd.NA
                ),  # default to empty if not provided
                "use_for_risk_pcrop": band_info.get(
                    "use_for_risk_pcrop", 0
                ),  # default to 0 if not provided
                "use_for_risk_acrop": band_info.get(
                    "use_for_risk_acrop", 0
                ),  # default to 0 if not provided
                "use_for_risk_timber": band_info.get(
                    "use_for_risk_timber", 0
                ),  # default to 0 if not provided
                "exclude_from_output": 0,  # 0 here is so we don't exclude custom bands
                "ISO2_code": pd.NA,  # Global, i.e., empty string, by default
                # Add other required columns with defaults
                "col_type": "float64",  # default to float64 if not provided
                "is_nullable": 1,
                "is_required": 0,
                "order": 9999,  # Put at end unless specified otherwise
                "corresponding_variable": pd.NA,  # not necessary for custom bands
            }
            custom_rows.append(custom_row)

    if custom_rows:
        custom_df = pd.DataFrame(custom_rows)
        # Combine with original lookup
        lookup_df = pd.concat([lookup_df, custom_df], ignore_index=True)

    return lookup_df

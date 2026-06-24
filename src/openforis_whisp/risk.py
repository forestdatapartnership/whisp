import pandas as pd

from .pd_schemas import data_lookup_type
from .logger import StdoutLogger


from openforis_whisp.parameters.config_runtime import (
    geometry_area_column,
    DEFAULT_LOOKUP_TABLE_PATH,
    stats_unit_type_column,
)

from openforis_whisp.reformat import filter_lookup_by_country_codes

# could embed this in each function below that uses lookup_gee_datasets_df.
lookup_gee_datasets_df: data_lookup_type = pd.read_csv(DEFAULT_LOOKUP_TABLE_PATH)

logger = StdoutLogger(__name__)


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
    df: data_lookup_type,  # CHECK THIS
    ind_1_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_2_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_3_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_4_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_5_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_6_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_7a_pcent_threshold: float = 10,
    ind_7b_pcent_threshold: float = 10,
    ind_8a_pcent_threshold: float = 10,
    ind_8b_pcent_threshold: float = 10,
    ind_9_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_10_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_11_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_12_pcent_threshold: float = 10,
    ind_13_pcent_threshold: float = 10,
    ind_14_pcent_threshold: float = 10,
    ind_1_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_2_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_3_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_4_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_5_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_6_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_7a_input_columns: pd.Series = None,
    ind_7b_input_columns: pd.Series = None,
    ind_8a_input_columns: pd.Series = None,
    ind_8b_input_columns: pd.Series = None,
    ind_9_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_10_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_11_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_12_input_columns: pd.Series = None,
    ind_13_input_columns: pd.Series = None,
    ind_14_input_columns: pd.Series = None,
    ind_1_name: str = "Ind_01_treecover",
    ind_2_name: str = "Ind_02_commodities",
    ind_3_name: str = "Ind_03_disturbance_before_2020",
    ind_4_name: str = "Ind_04_disturbance_after_2020",
    ind_5_name: str = "Ind_05_primary_2020",
    ind_6_name: str = "Ind_06_nat_reg_forest_2020",
    ind_7a_name: str = "Ind_07a_planted_2020",
    ind_7b_name: str = "Ind_07b_plantation_2020",
    ind_8a_name: str = "Ind_08a_planted_after_2020",
    ind_8b_name: str = "Ind_08b_plantation_after_2020",
    ind_9_name: str = "Ind_09_treecover_after_2020",
    ind_10_name: str = "Ind_10_agri_after_2020",
    ind_11_name: str = "Ind_11_logging_concession_before_2020",
    ind_12_name: str = "Ind_12_other_land_2020",
    ind_13_name: str = "Ind_13_other_land_after_2020",
    ind_14_name: str = "Ind_14_primary_2025",
    low_name: str = "no",
    high_name: str = "yes",
    explicit_unit_type: str = None,
    national_codes: list[str] = None,  # List of ISO2 country codes to filter by
    custom_bands_info: dict = None,  # New parameter for custom band risk info
    drop_unused_columns: bool = False,  # Remove columns not used in risk calculations
) -> data_lookup_type:
    """
    Adds the risk column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_pcent_threshold (float, optional): Percentage threshold for indicator 1 (treecover). Defaults to 10.
        ind_2_pcent_threshold (float, optional): Percentage threshold for indicator 2 (commodities). Defaults to 10.
        ind_3_pcent_threshold (float, optional): Percentage threshold for indicator 3 (disturbance before 2020). Defaults to 10.
        ind_4_pcent_threshold (float, optional): Percentage threshold for indicator 4 (disturbance after 2020). Defaults to 10.
        ind_5_pcent_threshold (float, optional): Percentage threshold for indicator 5 (primary forest 2020). Defaults to 10.
        ind_6_pcent_threshold (float, optional): Percentage threshold for indicator 6 (naturally regenerating forest 2020). Defaults to 10.
        ind_7a_pcent_threshold (float, optional): Percentage threshold for indicator 7a (planted forest 2020). Defaults to 10.
        ind_7b_pcent_threshold (float, optional): Percentage threshold for indicator 7b (plantation forest 2020). Defaults to 10.
        ind_8a_pcent_threshold (float, optional): Percentage threshold for indicator 8a (planted forest after 2020). Defaults to 10.
        ind_8b_pcent_threshold (float, optional): Percentage threshold for indicator 8b (plantation forest after 2020). Defaults to 10.
        ind_9_pcent_threshold (float, optional): Percentage threshold for indicator 9 (treecover after 2020). Defaults to 10.
        ind_10_pcent_threshold (float, optional): Percentage threshold for indicator 10 (agriculture after 2020). Defaults to 10.
        ind_11_pcent_threshold (float, optional): Percentage threshold for indicator 11 (logging concession before 2020). Defaults to 10.
        ind_1_input_columns (pd.Series, optional): Input columns for indicator 1. Defaults to columns for the treecover theme.
        ind_2_input_columns (pd.Series, optional): Input columns for indicator 2. Defaults to columns for the commodities theme.
        ind_3_input_columns (pd.Series, optional): Input columns for indicator 3. Defaults to columns for disturbance before 2020.
        ind_4_input_columns (pd.Series, optional): Input columns for indicator 4. Defaults to columns for disturbance after 2020.
        ind_5_input_columns (pd.Series, optional): Input columns for indicator 5. Defaults to columns for primary forest 2020.
        ind_6_input_columns (pd.Series, optional): Input columns for indicator 6. Defaults to columns for naturally regenerating forest 2020.
        ind_7a_input_columns (pd.Series, optional): Input columns for indicator 7a. Defaults to columns for planted forest 2020.
        ind_7b_input_columns (pd.Series, optional): Input columns for indicator 7b. Defaults to columns for plantation forest 2020.
        ind_8a_input_columns (pd.Series, optional): Input columns for indicator 8a. Defaults to columns for planted forest after 2020.
        ind_8b_input_columns (pd.Series, optional): Input columns for indicator 8b. Defaults to columns for plantation forest after 2020.
        ind_9_input_columns (pd.Series, optional): Input columns for indicator 9. Defaults to columns for treecover after 2020.
        ind_10_input_columns (pd.Series, optional): Input columns for indicator 10. Defaults to columns for agriculture after 2020.
        ind_11_input_columns (pd.Series, optional): Input columns for indicator 11. Defaults to columns for logging concession before 2020.
        ind_1_name (str, optional): Name of indicator 1 column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of indicator 2 column. Defaults to "Ind_02_commodities".
        ind_3_name (str, optional): Name of indicator 3 column. Defaults to "Ind_03_disturbance_before_2020".
        ind_4_name (str, optional): Name of indicator 4 column. Defaults to "Ind_04_disturbance_after_2020".
        ind_5_name (str, optional): Name of indicator 5 column. Defaults to "Ind_05_primary_2020".
        ind_6_name (str, optional): Name of indicator 6 column. Defaults to "Ind_06_nat_reg_forest_2020".
        ind_7a_name (str, optional): Name of indicator 7a column. Defaults to "Ind_07a_planted_2020".
        ind_7b_name (str, optional): Name of indicator 7b column. Defaults to "Ind_07b_plantation_2020".
        ind_8a_name (str, optional): Name of indicator 8a column. Defaults to "Ind_08a_planted_after_2020".
        ind_8b_name (str, optional): Name of indicator 8b column. Defaults to "Ind_08b_plantation_after_2020".
        ind_12_name (str, optional): Name of indicator 12 column. Defaults to "Ind_12_other_land_2020".
        ind_13_name (str, optional): Name of indicator 13 column. Defaults to "Ind_13_other_land_after_2020".
        ind_9_name (str, optional): Name of indicator 9 column. Defaults to "Ind_09_treecover_after_2020".
        ind_10_name (str, optional): Name of indicator 10 column. Defaults to "Ind_10_agri_after_2020".
        ind_11_name (str, optional): Name of indicator 11 column. Defaults to "Ind_11_logging_concession_before_2020".
        low_name (str, optional): Value shown in table if less than or equal to the threshold. Defaults to "no".
        high_name (str, optional): Value shown in table if more than the threshold. Defaults to "yes".
        explicit_unit_type (str, optional): Override the autodetected unit type ('ha' or 'percent').
                                      If not provided, will detect from dataframe 'unit' column.
        national_codes (list[str], optional): List of ISO2 country codes to filter national datasets by. Defaults to None.
        custom_bands_info (dict, optional): Custom band risk information. Dict format:
            {
                'band_name': {
                    'theme': 'treecover',  # or 'commodities', 'disturbance_before', 'disturbance_after'
                    'theme_timber': 'primary',  # or 'naturally_reg_2020', 'planted_plantation_2020', etc.
                    'use_for_risk_pcrop': 1,  # 0 or 1 - include in perennial crop risk
                    'use_for_risk_acrop': 1,  # 0 or 1 - include in annual crop risk
                    'use_for_risk_timber': 1,  # 0 or 1
                }
            }
            If None, custom bands won't be included in risk calculations.
        drop_unused_columns (bool, optional): If True, removes dataset columns not used in risk calculations,
            keeping only context/metadata columns, datasets used in indicators, indicator columns,
            and final risk columns. Defaults to False (backward compatible).

    Returns:
        data_lookup_type: DataFrame with added risk columns.
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

    # Get indicator columns (now includes custom bands)
    if ind_1_input_columns is None:
        ind_1_input_columns = get_cols_ind_01_treecover(filtered_lookup_gee_datasets_df)
    if ind_2_input_columns is None:
        # Union of pcrop + acrop commodity datasets so Ind_02_commodities remains the
        # combined signal (same behaviour as the old use_for_risk column). The per-decision-tree
        # split is documented in the LUT and the getter accepts risk_col for future use.
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
    if ind_7a_input_columns is None:
        ind_7a_input_columns = get_cols_ind_07a_planted_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_7b_input_columns is None:
        ind_7b_input_columns = get_cols_ind_07b_plantation_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_8a_input_columns is None:
        ind_8a_input_columns = get_cols_ind_08a_planted_after_2020(
            filtered_lookup_gee_datasets_df
        )
    if ind_8b_input_columns is None:
        ind_8b_input_columns = get_cols_ind_08b_plantation_after_2020(
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
    if ind_14_input_columns is None:
        ind_14_input_columns = get_cols_ind_14_primary_2025(
            filtered_lookup_gee_datasets_df
        )

    # Check range of values
    check_range(ind_1_pcent_threshold)
    check_range(ind_2_pcent_threshold)
    check_range(ind_3_pcent_threshold)
    check_range(ind_4_pcent_threshold)
    check_range(ind_5_pcent_threshold)
    check_range(ind_6_pcent_threshold)
    check_range(ind_7a_pcent_threshold)
    check_range(ind_7b_pcent_threshold)
    check_range(ind_8a_pcent_threshold)
    check_range(ind_8b_pcent_threshold)
    check_range(ind_9_pcent_threshold)
    check_range(ind_10_pcent_threshold)
    check_range(ind_11_pcent_threshold)
    check_range(ind_12_pcent_threshold)
    check_range(ind_13_pcent_threshold)
    check_range(ind_14_pcent_threshold)

    input_cols = [
        ind_1_input_columns,
        ind_2_input_columns,
        ind_3_input_columns,
        ind_4_input_columns,
        ind_5_input_columns,
        ind_6_input_columns,
        ind_7a_input_columns,
        ind_7b_input_columns,
        ind_8a_input_columns,
        ind_8b_input_columns,
        ind_9_input_columns,
        ind_10_input_columns,
        ind_11_input_columns,
        ind_12_input_columns,
        ind_13_input_columns,
        ind_14_input_columns,
    ]
    thresholds = [
        ind_1_pcent_threshold,
        ind_2_pcent_threshold,
        ind_3_pcent_threshold,
        ind_4_pcent_threshold,
        ind_5_pcent_threshold,
        ind_6_pcent_threshold,
        ind_7a_pcent_threshold,
        ind_7b_pcent_threshold,
        ind_8a_pcent_threshold,
        ind_8b_pcent_threshold,
        ind_9_pcent_threshold,
        ind_10_pcent_threshold,
        ind_11_pcent_threshold,
        ind_12_pcent_threshold,
        ind_13_pcent_threshold,
        ind_14_pcent_threshold,
    ]
    names = [
        ind_1_name,
        ind_2_name,
        ind_3_name,
        ind_4_name,
        ind_5_name,
        ind_6_name,
        ind_7a_name,
        ind_7b_name,
        ind_8a_name,
        ind_8b_name,
        ind_9_name,
        ind_10_name,
        ind_11_name,
        ind_12_name,
        ind_13_name,
        ind_14_name,
    ]
    [check_range(threshold) for threshold in thresholds]

    df_w_indicators = add_indicators(
        df,
        input_cols,
        thresholds,
        names,
        low_name,
        high_name,
        unit_type,  # Pass the unit type
    )

    # Derived "still primary or observed primary in 2025" signal used by the timber tree. Two
    # contributions, OR-ed: (1) primary in 2020 with no post-2020 disturbance (Ind_05 and not Ind_04),
    # a "primary cannot expand" lower bound; (2) Ind_14, an observed primary-2025 layer (the MapBiomas
    # Collection-10 primary proxy for now, Brazil only). The second lets a regenerating-2020 plot that
    # reads as primary in 2025 take the matured-to-primary path. Ind_14 is interim and reversible: drop
    # use_for_risk_timber on its lookup row and it returns to empty, leaving the derived-only behaviour.
    for index, row in df_w_indicators.iterrows():
        df_w_indicators.at[index, "primary_2025"] = (
            high_name
            if (
                (row[ind_5_name] == high_name and row[ind_4_name] == low_name)
                or row[ind_14_name] == high_name
            )
            else low_name
        )

    # these "add_" functions modify the 'df_w_indicators' dataframe in place
    add_risk_pcrop_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_3_name=ind_3_name,
        ind_4_name=ind_4_name,
    )

    add_risk_acrop_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_4_name=ind_4_name,
    )

    add_risk_timber_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_5_name=ind_5_name,
        ind_6_name=ind_6_name,
        ind_7a_name=ind_7a_name,
        ind_7b_name=ind_7b_name,
        ind_8a_name=ind_8a_name,
        ind_8b_name=ind_8b_name,
        ind_9_name=ind_9_name,
        ind_10_name=ind_10_name,
        ind_12_name=ind_12_name,
        ind_13_name=ind_13_name,
        primary_2025_name="primary_2025",
    )

    # Filter to risk-relevant columns if requested (after all columns added)
    if drop_unused_columns:
        df_w_indicators = filter_to_risk_columns(df_w_indicators, input_cols, names)

    return df_w_indicators


def add_risk_pcrop_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_3_name: str,
    ind_4_name: str,
) -> data_lookup_type:
    """
    Adds the risk column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Ind_02_commodities".
        ind_3_name (str, optional): Name of third indicator column. Defaults to "Ind_03_disturbance_before_2020".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Ind_04_disturbance_after_2020".

    Returns:
        DataFrame: DataFrame with added 'risk' column.
    """

    for index, row in df.iterrows():
        # If any of the first three indicators suggest low risk, set risk to "low"
        if (
            row[ind_1_name] == "no"
            or row[ind_2_name] == "yes"
            or row[ind_3_name] == "yes"
        ):
            df.at[index, "risk_pcrop"] = "low"
        # If none of the first three indicators suggest low risk and Indicator 4 suggests no risk, set risk to "more_info_needed"
        elif row[ind_4_name] == "no":
            df.at[index, "risk_pcrop"] = "more_info_needed"
        # If none of the above conditions are met, set risk to "high"
        else:
            df.at[index, "risk_pcrop"] = "high"

    return df


def add_risk_acrop_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_4_name: str,
) -> data_lookup_type:
    """
    Adds the risk column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Ind_02_commodities".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Ind_04_disturbance_after_2020".

    Returns:
        DataFrame: DataFrame with added 'risk' column.
    """

    # soy risk
    for index, row in df.iterrows():
        # If there is no tree cover in 2020, set risk_soy to "low"
        if row[ind_1_name] == "no" or row[ind_2_name] == "yes":
            df.at[index, "risk_acrop"] = "low"
        # If there is tree cover in 2020 and distrubances post 2020, set risk_soy to "high"
        elif row[ind_1_name] == "yes" and row[ind_4_name] == "yes":
            df.at[index, "risk_acrop"] = "high"
        # If tree cover and no disturbances post 2020, set risk to "more_info_needed"
        else:
            df.at[index, "risk_acrop"] = "more_info_needed"

    return df


def add_risk_timber_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_12_name: str,
    ind_5_name: str,
    ind_6_name: str,
    ind_7a_name: str,
    ind_7b_name: str,
    ind_8a_name: str,
    ind_8b_name: str,
    ind_9_name: str,
    ind_10_name: str,
    ind_13_name: str,
    primary_2025_name: str,
) -> data_lookup_type:
    """
    Adds the risk_timber column based on the WHISP timber decision tree, the elaborate FAO
    "diagram A" land-use-transition logic that checks both the 2020 state and the 2025 state.

    Planted (Ind_07a) is grouped with naturally regenerating (Ind_06) into a "regenerating-planted"
    class, because the two are not reliably separable in the data; plantation (Ind_07b / Ind_08b)
    is kept separate as the operative degradation class.

    2020 states: primary (Ind_05), regenerating-planted (Ind_06 or Ind_07a), plantation (Ind_07b).
    2025 states: primary_2025 (derived: Ind_05 and not Ind_04), regenerating-planted-2025
    (treecover after 2020 Ind_09, or planted after 2020 Ind_08a), plantation_2025 (Ind_08b),
    other land 2025 (Ind_13), agriculture 2025 (Ind_10).

    Rules (priority order):
    0. Other land use 2020 (Ind_12) -> LOW (known non-forest in 2020, outside EUDR scope).
    1. Agriculture/commodity 2020 (Ind_02) -> LOW (pre-2020 land use, outside EUDR scope).
    2. Agriculture after 2020 (Ind_10) -> HIGH (deforestation). Negative gate: rules 0 and 1 already
       removed other-land-2020 and commodity-2020, so any new agriculture here is conversion from
       forest/unknown. Replaces the old positive any-forest-2020 guard, which missed deforestation where
       forest-2020 products are thin (Atlantic Forest, Borneo).
    3. Primary 2020 -> still primary (primary_2025) or other land 2025 (Ind_13) -> LOW; -> plantation 2025
       (Ind_08b) = degradation -> HIGH (per the EUDR transition matrix, Art 2(7)); otherwise MORE INFO.
    4. Regenerating-planted 2020 -> matured to primary (primary_2025) -> LOW; stayed regenerating-planted
       (Ind_09 / Ind_08a) AND treecover 2020 (Ind_01) -> LOW; -> plantation 2025 (Ind_08b) = degradation
       -> HIGH; -> other land 2025 (Ind_13) -> LOW; otherwise MORE INFO NEEDED. Degradation-to-plantation
       is flagged HIGH here, not on the primary branch. The stayed-forest LOW is gated on 2020 treecover
       so an ESRI-only 2025 signal cannot earn a LOW where the JRC baseline saw no forest (#229).
    5. Plantation 2020 -> plantation 2025 (Ind_08b) or other land 2025 (Ind_13) -> LOW; otherwise MORE
       INFO NEEDED (the diagram routes an unconfirmed 2020 plantation to more info, not high).
    6. Nothing recognised in 2020 -> MORE INFO NEEDED ("if we do not know the 2020 state, we do not
       know"; an unclassified plot is not assumed compliant).

    Choices that differ from a literal reading of diagram A, noted for the meeting:
    - Rule 2 is a negative gate (deforestation unless known non-forest in 2020 via rules 0/1), replacing
      the old positive any-forest-2020 guard. Rule 4's #229 2020-treecover gate remains the one code-only
      refinement not drawn as a diagram node (it stops ESRI-only commission earning an unearned LOW).
    - Rule 0 (other land use 2020 -> LOW) uses Ind_12 = ESRI built/water/snow + MapBiomas non-forest 2020
      (Brazil wall-to-wall, so all non-forest land use there -> LOW). Outside Brazil the non-forest set is
      still thin (ESRI built/water/snow only), so the negative gate (rule 2) can over-flag there until a
      global non-forest-2020 layer is wired.
    - The primary branch is evaluated before the regenerating-planted and plantation branches, so a plot
      overlapping several 2020 classes is assessed against the most-protected class. Diagram A lists the
      plantation branch first.
    - Plantation 2025 (Ind_08b) is LIVE for Brazil via MapBiomas silviculture 2024
      (nBR_MapBiomas_col10_silviculture_2024) but has no global after-2020 layer (only the dormant ForTy
      demo), so rule 4's regen -> plantation HIGH and rule 5's stable-plantation LOW fire in Brazil and
      fall to MORE INFO NEEDED elsewhere until a global layer is wired.

    Returns:
        DataFrame with risk_timber column added.
    """

    for index, row in df.iterrows():
        primary_2020 = row[ind_5_name] == "yes"
        regen_planted_2020 = row[ind_6_name] == "yes" or row[ind_7a_name] == "yes"
        plantation_2020 = row[ind_7b_name] == "yes"

        treecover_2020 = row[ind_1_name] == "yes"
        primary_2025 = row[primary_2025_name] == "yes"
        regen_planted_2025 = row[ind_9_name] == "yes" or row[ind_8a_name] == "yes"
        plantation_2025 = row[ind_8b_name] == "yes"
        other_land_2025 = row[ind_13_name] == "yes"
        other_land_2020 = row[ind_12_name] == "yes"

        # "Was there forest to lose in 2020?" = treecover (Ind_01: broad global canopy, EUFO/JRC GFC2020 +
        # TMF + MapBiomas forest) OR any forest CLASS (primary / regen-planted / plantation). Drives the
        # deforestation gate (Rule 2) so a NON-forest plot (grassland / bare / cropland) that turns into
        # agriculture is NOT mis-flagged as deforestation. This matters because risk_timber is emitted for
        # EVERY plot (pcrop / acrop / cattle plots too), so a no-forest plot must never show a HIGH timber
        # deforestation verdict in the output table. Treecover is broad/global so the gate stays robust
        # where the forest-CLASS layers are thin (Atlantic Forest, Borneo); the class terms catch forest the
        # canopy layer missed (e.g. a plantation the treecover product did not register).
        forest_2020 = (
            treecover_2020 or primary_2020 or regen_planted_2020 or plantation_2020
        )

        # Rule 0 (OL2020): other land use in 2020 -> LOW (known non-forest in 2020, outside EUDR scope).
        if other_land_2020:
            df.at[index, "risk_timber"] = "low"
        # Rule 1: agriculture / commodity in 2020 -> LOW (pre-2020 land use, outside EUDR scope).
        elif row[ind_2_name] == "yes":
            df.at[index, "risk_timber"] = "low"
        # Rule 2: deforestation = had forest in 2020 (forest_2020) AND agriculture after 2020 -> HIGH. The
        # forest_2020 gate (treecover OR a forest class) means a genuine NON-forest -> agriculture change
        # (grassland / savanna / bare / pre-existing cropland -> crop) is NOT mis-flagged as deforestation;
        # it falls through to the forest-class branches / MORE INFO. This keeps the risk_timber column
        # credible on NON-timber plots: a no-forest plot never shows a HIGH timber deforestation verdict.
        # Gating on treecover OR forest-class (not treecover alone) catches forest the canopy layer missed
        # (e.g. a plantation -> ag plot with no treecover_2020), and stays coverage-robust where the forest-
        # CLASS layers are thin (Atlantic Forest, Borneo) because treecover is broad and global.
        elif forest_2020 and row[ind_10_name] == "yes":
            df.at[index, "risk_timber"] = "high"
        # The 2020 forest-class checks run in the SAME ORDER as the diagram: PLANTATION first, then
        # regenerating-planted, then primary (primary LAST). Order only matters for a plot flagged as more
        # than one 2020 class (datasets disagree / overlap), and the diagram order is deliberate: a plot
        # known to be a PLANTATION in 2020 is handled as a plantation (managed, established before the
        # cutoff), not mis-read as a primary that later "degraded" to plantation. So plantation_2020 takes
        # precedence over primary_2020 here.
        #
        # Rule 3: plantation 2020. Plantation 2025 (Ind_08b) or other land 2025 -> LOW; otherwise MORE
        # INFO NEEDED (the diagram routes an unconfirmed 2020 plantation to more info, not high).
        elif plantation_2020:
            if plantation_2025 or other_land_2025:
                df.at[index, "risk_timber"] = "low"
            else:
                df.at[index, "risk_timber"] = "more_info_needed"
        # Rule 4: regenerating-planted 2020. Order follows the diagram: matured to primary -> stayed
        # regenerating-planted -> plantation (degradation) -> other land -> more info.
        elif regen_planted_2020:
            if primary_2025:
                # Regenerating forest matured to primary = compliant. Fires via Ind_14 (IFL 2025; the
                # MapBiomas proxy is available but disabled, see #233), so only as live as Ind_14 coverage.
                df.at[index, "risk_timber"] = "low"
            elif regen_planted_2025 and treecover_2020 and not plantation_2025:
                # Stayed regenerating/planted. Gated on 2020 treecover (Ind_01, the JRC/GLAD-family
                # baseline) so an ESRI-only 2025 treecover signal cannot earn a LOW where the strict 2020
                # baseline saw no forest (#229). AND NOT plantation_2025 so a NEW plantation (degradation)
                # is not masked by this LOW: a plot that is both treecover-after and new-plantation falls
                # through to the plantation-2025 HIGH below (H6 fix; live now that MapBiomas silviculture
                # GAIN feeds Ind_08b in Brazil). Code refinement, not drawn on the diagram.
                df.at[index, "risk_timber"] = "low"
            elif plantation_2025:
                # Regenerating-planted -> NEW plantation after 2020 = degradation -> HIGH (Ind_08b, fed by
                # MapBiomas silviculture GAIN in Brazil; globally dormant until a global plantation-after
                # layer is wired).
                df.at[index, "risk_timber"] = "high"
            elif other_land_2025:
                df.at[index, "risk_timber"] = "low"
            else:
                df.at[index, "risk_timber"] = "more_info_needed"
        # Rule 5: primary 2020 (checked LAST, matching the diagram). Still primary (primary_2025) or other
        # land 2025 -> LOW; -> plantation 2025 (Ind_08b) = degradation -> HIGH; otherwise MORE INFO NEEDED.
        # The plantation -> HIGH is per the EUDR transition matrix, which marks primary -> plantation (and
        # -> planted, -> OWL) as DEGRADATION (Art 2(7)). An earlier simplified diagram had dropped this
        # node; restored here. Fires via the MapBiomas silviculture GAIN (Ind_08b) in Brazil; primary ->
        # planted and primary -> OWL degradation stay blind for want of those after-2020 layers.
        elif primary_2020:
            if primary_2025 or other_land_2025:
                df.at[index, "risk_timber"] = "low"
            elif plantation_2025:
                df.at[index, "risk_timber"] = "high"
            else:
                df.at[index, "risk_timber"] = "more_info_needed"
        # Rule 6: nothing recognised in 2020 (not other-land, not commodity, not any forest class) ->
        # MORE INFO NEEDED. "If we do not know the 2020 state, we do not know": an unclassified plot is no
        # longer assumed compliant. (The other-land-2020 node IS wired now, at Rule 0 / Ind_12, so genuine
        # known-non-forest plots leave at Rule 0; only truly unclassified plots reach here.)
        else:
            df.at[index, "risk_timber"] = "more_info_needed"

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


def get_cols_ind_07a_planted_2020(lookup_gee_datasets_df):
    """
    Dataset names for planted forest (non-plantation) in 2020 (theme_timber=planted_2020).
    Split out from the old merged Ind_07; grouped with nat-regen (Ind_06) in the timber tree.
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


def get_cols_ind_07b_plantation_2020(lookup_gee_datasets_df):
    """
    Dataset names for plantation forest in 2020 (theme_timber=plantation_2020).
    The operative plantation class in the timber tree (stable-plantation / degradation logic).
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


def get_cols_ind_08a_planted_after_2020(lookup_gee_datasets_df):
    """
    Dataset names for planted forest after 2020 (theme_timber=planted_after_2020).
    Dormant (returns []) until a post-2020 split layer (e.g. ForTy post-2020) is added.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "planted_after_2020")
        ]
    )


def get_cols_ind_08b_plantation_after_2020(lookup_gee_datasets_df):
    """
    Dataset names for plantation forest after 2020 (theme_timber=plantation_after_2020).
    The degradation "to" class (natural/regen-planted -> plantation). Dormant (returns [])
    until a post-2020 split layer (ForTy post-2020 / MapBiomas C10) is added.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "plantation_after_2020")
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


def get_cols_ind_11_logging_before_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for logging concessions (2020 if available)

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the degradation - logging concessions, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "logging_concession")
        ]
    )


def get_cols_ind_12_other_land_2020(lookup_gee_datasets_df):
    """
    Dataset names for other land in 2020 (theme_timber=other_land_2020; ESRI built/bare/water/snow).
    Output indicator; the diagram-A other-land-2020 side short-circuit is dropped, so a non-forest
    plot reaches LOW by falling through the tree rather than via this indicator.
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
    Dataset names for other land after 2020 (theme_timber=other_land_after_2020; ESRI built/bare/
    water/snow for 2025). Feeds the diagram-A "primary -> other land 2025 -> LOW" node.
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


def get_cols_ind_14_primary_2025(lookup_gee_datasets_df):
    """
    Dataset names for an observed primary forest signal in 2025 (theme_timber=primary_2025). Interim
    source: the MapBiomas Collection-10 primary proxy (native forest minus secondary, Brazil). OR-ed
    into the derived primary_2025 so a regenerating-2020 plot read as primary in 2025 can take the
    matured-to-primary path. Reversible by dropping use_for_risk_timber on the lookup row.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "primary_2025")
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
    risk_cols = ["risk_pcrop", "risk_acrop", "risk_timber"]

    # Derived indicator columns (computed in whisp_risk, not from a dataset getter)
    derived_cols = ["primary_2025"]

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

import pandas as pd

from .pd_schemas import data_lookup_type


from openforis_whisp.parameters.config_runtime import (
    geometry_area_column,
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
    stats_unit_type_column,  # Add this import
)

from openforis_whisp.reformat import filter_lookup_by_country_codes

# could embed this in each function below that uses lookup_gee_datasets_df.
lookup_gee_datasets_df: data_lookup_type = pd.read_csv(
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH
)


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


# Update whisp_risk to accept and pass the unit_type parameter
def whisp_risk(
    df: data_lookup_type,  # CHECK THIS
    ind_1_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_2_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_3_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_4_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_5_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_6_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_7_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_8_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_9_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_10_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_11_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_1_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_2_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_3_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_4_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_5_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_6_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_7_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_8_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_9_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_10_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_11_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
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
    low_name: str = "no",
    high_name: str = "yes",
    explicit_unit_type: str = None,
    national_codes: list[str] = None,  # List of ISO2 country codes to filter by
) -> data_lookup_type:
    """
    Adds the EUDR (European Union Deforestation Risk) column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_pcent_threshold (int, optional): Percentage threshold for the first indicator. Defaults to 10.
        ind_2_pcent_threshold (int, optional): Percentage threshold for the second indicator. Defaults to 10.
        ind_3_pcent_threshold (int, optional): Percentage threshold for the third indicator. Defaults to 0.
        ind_4_pcent_threshold (int, optional): Percentage threshold for the fourth indicator. Defaults to 0.
        ind_1_input_columns (list, optional): List of input columns for the first indicator. Defaults to columns for the treecover theme.
        ind_2_input_columns (list, optional): List of input columns for the second indicator. Defaults to columns for the commodities theme.
        ind_3_input_columns (list, optional): List of input columns for the third indicator. Defaults to columns for disturbance before 2020.
        ind_4_input_columns (list, optional): List of input columns for the fourth indicator. Defaults to columns for disturbance after 2020.
        ind_1_name (str, optional): Name of the first indicator column. Defaults to "Indicator_1_treecover".
        ind_2_name (str, optional): Name of the second indicator column. Defaults to "Indicator_2_commodities".
        ind_3_name (str, optional): Name of the third indicator column. Defaults to "Indicator_3_disturbance_before_2020".
        ind_4_name (str, optional): Name of the fourth indicator column. Defaults to "Indicator_4_disturbance_after_2020".
        low_name (str, optional): Value shown in table if less than or equal to the threshold. Defaults to "no".
        high_name (str, optional): Value shown in table if more than the threshold. Defaults to "yes".
        explicit_unit_type (str, optional): Override the autodetected unit type ('ha' or 'percent').
                                      If not provided, will detect from dataframe 'unit' column.

    Returns:
        data_lookup_type: DataFrame with added 'EUDR_risk' column.
    """
    # Determine the unit type to use based on input data and overrid
    unit_type = detect_unit_type(df, explicit_unit_type)

    print(f"Using unit type: {unit_type}")

    lookup_df_copy = lookup_gee_datasets_df.copy()

    # filter by national codes (even if None - this removes all country columns unless specified)
    filtered_lookup_gee_datasets_df = filter_lookup_by_country_codes(
        lookup_df=lookup_df_copy,
        filter_col="ISO2_code",
        national_codes=national_codes,
    )

    # Rest of the function remains the same, but pass unit_type to add_indicators
    if ind_1_input_columns is None:
        ind_1_input_columns = get_cols_ind_01_treecover(filtered_lookup_gee_datasets_df)
    if ind_2_input_columns is None:
        ind_2_input_columns = get_cols_ind_02_commodities(
            filtered_lookup_gee_datasets_df
        )
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

    df_w_indicators_and_risk_pcrop = add_eudr_risk_pcrop_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_3_name=ind_3_name,
        ind_4_name=ind_4_name,
    )

    df_w_indicators_and_risk_acrop = add_eudr_risk_acrop_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_4_name=ind_4_name,
    )

    df_w_indicators_and_risk_timber = add_eudr_risk_timber_col(
        df=df_w_indicators,
        ind_2_name=ind_2_name,
        ind_5_name=ind_5_name,
        ind_6_name=ind_6_name,
        ind_7_name=ind_7_name,
        ind_8_name=ind_8_name,
        ind_9_name=ind_9_name,
        ind_10_name=ind_10_name,
        ind_11_name=ind_11_name,
    )

    return df_w_indicators_and_risk_timber


def add_eudr_risk_pcrop_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_3_name: str,
    ind_4_name: str,
) -> data_lookup_type:
    """
    Adds the EUDR (European Union Deforestation Risk) column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Ind_02_commodities".
        ind_3_name (str, optional): Name of third indicator column. Defaults to "Ind_03_disturbance_before_2020".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Ind_04_disturbance_after_2020".

    Returns:
        DataFrame: DataFrame with added 'EUDR_risk' column.
    """

    for index, row in df.iterrows():
        # If any of the first three indicators suggest low risk, set EUDR_risk to "low"
        if (
            row[ind_1_name] == "no"
            or row[ind_2_name] == "yes"
            or row[ind_3_name] == "yes"
        ):
            df.at[index, "risk_pcrop"] = "low"
        # If none of the first three indicators suggest low risk and Indicator 4 suggests no risk, set EUDR_risk to "more_info_needed"
        elif row[ind_4_name] == "no":
            df.at[index, "risk_pcrop"] = "more_info_needed"
        # If none of the above conditions are met, set EUDR_risk to "high"
        else:
            df.at[index, "risk_pcrop"] = "high"

    return df


def add_eudr_risk_acrop_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_4_name: str,
) -> data_lookup_type:
    """
    Adds the EUDR (European Union Deforestation Risk) column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Ind_01_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Ind_02_commodities".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Ind_04_disturbance_after_2020".

    Returns:
        DataFrame: DataFrame with added 'EUDR_risk' column.
    """

    # soy risk
    for index, row in df.iterrows():
        # If there is no tree cover in 2020, set EUDR_risk_soy to "low"
        if row[ind_1_name] == "no" or row[ind_2_name] == "yes":
            df.at[index, "risk_acrop"] = "low"
        # If there is tree cover in 2020 and distrubances post 2020, set EUDR_risk_soy to "high"
        elif row[ind_1_name] == "yes" and row[ind_4_name] == "yes":
            df.at[index, "risk_acrop"] = "high"
        # If tree cover and no disturbances post 2020, set EUDR_risk to "more_info_needed"
        else:
            df.at[index, "risk_acrop"] = "more_info_needed"

    return df


def add_eudr_risk_timber_col(
    df: data_lookup_type,
    ind_2_name: str,
    ind_5_name: str,
    ind_6_name: str,
    ind_7_name: str,
    ind_8_name: str,
    ind_9_name: str,
    ind_10_name: str,
    ind_11_name: str,
) -> data_lookup_type:
    """
    Adds the EUDR (European Union Deforestation Risk) column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Ind_02_commodities".
        ind_5_name (str, optional): Name of fifth indicator column. Defaults to "Ind_05_primary_2020".
        ind_6_name (str, optional): Name of sixth indicator column. Defaults to "Ind_06_nat_reg_forest_2020".
        ind_7_name (str, optional): Name of seventh indicator column. Defaults to "Ind_07_planted_plantations_2020".
        ind_8_name (str, optional): Name of eighth indicator column. Defaults to "Ind_08_planted_plantations_after_2020".
        ind_9_name (str, optional): Name of ninth indicator column. Defaults to "Ind_09_treecover_after_2020".
        ind_10_name (str, optional): Name of tenth indicator column. Defaults to "Ind_10_agri_after_2020".
        ind_11_name (str, optional): Name of eleventh indicator column. Defaults to "Ind_11_logging_concession_before_2020".

    Returns:
        DataFrame: DataFrame with added 'EUDR_risk' column.
    """

    for index, row in df.iterrows():
        # If there is a commodity in 2020 (ind_2_name) 
        # OR if there is planted-plantation in 2020 (ind_7_name) AND no agriculture in 2023 (ind_10_name), set EUDR_risk_timber to "low"
        if row[ind_2_name] == "yes" or (
            row[ind_7_name] == "yes" and row[ind_10_name] == "no"
        ):
            df.at[index, "risk_timber"] = "low"
        # If there is a natural forest primary (ind_5_name) or naturally regenerating (ind_6_name) or planted forest (ind_7_name) in 2020 AND agricultural after 2020 (ind_10_name), set EUDR_timber to high
        elif (
            row[ind_5_name] == "yes"
            or row[ind_6_name] == "yes"
            or row[ind_7_name] == "yes"
        ) and row[ind_10_name] == "yes":
            df.at[index, "risk_timber"] = "high"
        # If there is a natural forest primary (ind_5_name) or naturally regenerating (ind_6_name) AND planted after 2020 (ind_8_name), set EUDR_risk to "high"
        elif (row[ind_5_name] == "yes" or row[ind_6_name] == "yes") and row[
            ind_8_name
        ] == "yes":
            df.at[index, "risk_timber"] = "high"
        # No data yet on OWL conversion 
        # If primary or naturally regenerating or planted forest in 2020 and OWL in 2023, set EUDR_risk to high
        # elif (row[ind_5_name] == "yes" or row[ind_6_name] == "yes" or row[ind_7_name] == "yes") and row[ind_10_name] == "yes":
        #    df.at[index, 'EUDR_risk_timber'] = "high"

        # If there is a natural primary forest (ind_5_name) OR naturally regenerating in 2020 (ind_6_name) AND an information on management practice any time (ind_11_name) OR tree cover or regrowth post 2020 (ind_9_name), set EUDR_risk_timber to "low"
        elif (row[ind_5_name] == "yes" or row[ind_6_name] == "yes") and (
            row[ind_9_name] == "yes" or row[ind_11_name] == "yes"
        ):
            df.at[index, "risk_timber"] = "low"
        # If primary (ind_5_name) OR naturally regenerating in 2020 (ind_6_name) and no other info, set EUDR_risk to "more_info_needed"
        elif row[ind_5_name] == "yes" or row[ind_6_name] == "yes":
            df.at[index, "risk_timber"] = "more_info_needed"
        # If none of the above conditions are met, set EUDR_risk to "low"
        else:
            df.at[index, "risk_timber"] = "low"

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


# Update add_indicator_column to use the unit_type parameter
def add_indicator_column(
    df: data_lookup_type,
    input_columns: list[str],
    threshold: float,
    new_column_name: str,
    low_name: str = "no",
    high_name: str = "yes",
    sum_comparison: bool = False,
    unit_type: str = None,  # unit_type parameter
) -> data_lookup_type:
    """
    Add a new column to the DataFrame based on the specified columns, threshold, and comparison sign.

    Parameters:
        df (data_lookup_type): The pandas DataFrame to which the column will be added.
        input_columns (list): List of column names to check for threshold.
        threshold (float): The threshold value to compare against.
        new_column_name (str): The name of the new column to be added.
        The '>' sign is used for comparisons.
        When 'sum comparison' == True, then the threshold is compared to the sum of all those listed in 'input_columns', as opposed to when Flalse, when each column in the list is compared to the threshold individually
        low_name (str): The name for the value when below or equal to threshold (default is 'no').
        high_name (str): The name for the value when above threshold (default is 'yes').
        sum_comparison (bool): If True, sum all values in input_columns and compare to threshold (default is False).
        unit_type (str): Whether values are in "ha" or "percent".

    Returns:
        data_lookup_type: The DataFrame with the new column added.
    """
    # Create a new column and initialize with low_name
    new_column = pd.Series(low_name, index=df.index, name=new_column_name)

    # Default behavior: use '>' for single column comparison
    if sum_comparison:
        # Sum all values in specified columns and compare to threshold
        sum_values = df[input_columns].sum(axis=1)
        new_column[sum_values > threshold] = high_name
    else:
        # Check if any values in specified columns are above the threshold and update the new column accordingly
        for col in input_columns:
            # So that threshold is always in percent, if outputs are in ha, the code converts to percent (based on dividing by the geometry_area_column column.
            # Clamping is needed due to differences in decimal places (meaning input values may go just over 100)
            if unit_type == "ha":
                df[geometry_area_column] = pd.to_numeric(
                    df[geometry_area_column], errors="coerce"
                )
                val_to_check = clamp(
                    ((df[col] / df[geometry_area_column]) * 100), 0, 100
                )
            else:
                val_to_check = df[col]
            new_column[val_to_check > threshold] = high_name

    # Concatenate the new column to the DataFrame
    df = pd.concat([df, new_column], axis=1)
    return df


def get_cols_ind_01_treecover(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for the treecover theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the treecover theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk"] == 1)
            & (lookup_gee_datasets_df["theme"] == "treecover")
        ]
    )


def get_cols_ind_02_commodities(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for the commodities theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the commodities theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk"] == 1)
            & (lookup_gee_datasets_df["theme"] == "commodities")
        ]
    )


def get_cols_ind_03_dist_before_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for the disturbance before 2020 theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the disturbance before 2020 theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk"] == 1)
            & (lookup_gee_datasets_df["theme"] == "disturbance_before")
        ]
    )


def get_cols_ind_04_dist_after_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for the disturbance after 2020 theme, excluding those marked for exclusion.

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations  for the disturbance after 2020 theme, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk"] == 1)
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


def get_cols_ind_07_planted_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for planted and plantation forests in 2020

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the degradation - planted and plantation forests in 2020, excluding those marked for exclusion.
    """
    lookup_gee_datasets_df = lookup_gee_datasets_df[
        lookup_gee_datasets_df["exclude_from_output"] != 1
    ]
    return list(
        lookup_gee_datasets_df["name"][
            (lookup_gee_datasets_df["use_for_risk_timber"] == 1)
            & (lookup_gee_datasets_df["theme_timber"] == "planted_plantation_2020")
        ]
    )


def get_cols_ind_08_planted_after_2020(lookup_gee_datasets_df):
    """
    Generate a list of dataset names for planted and plantation forests post 2020

    Args:
    lookup_gee_datasets_df (pd.DataFrame): DataFrame containing dataset information.

    Returns:
    list: List of dataset names set to be used in the risk calculations for the degradation - planted and plantation forests post 2020, excluding those marked for exclusion.
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

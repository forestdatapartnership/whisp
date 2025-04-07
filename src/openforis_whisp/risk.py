import pandas as pd

from .pd_schemas import data_lookup_type

from openforis_whisp.parameters.config_runtime import (
    percent_or_ha,
    geometry_area_column,
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH,
)

# could embed this in each function below that uses lookup_gee_datasets_df.
lookup_gee_datasets_df: data_lookup_type = pd.read_csv(
    DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH
)


# requires lookup_gee_datasets_df


def whisp_risk(
    df: data_lookup_type,  # CHECK THIS
    ind_1_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_2_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_3_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_4_pcent_threshold: float = 10,  # default values (draft decision tree and parameters)
    ind_5_pcent_threshold: float = 10,   # default values (draft decision tree and parameters)
    ind_6_pcent_threshold: float = 10,   # default values (draft decision tree and parameters)
    ind_7_pcent_threshold: float = 10,   # default values (draft decision tree and parameters)
    ind_8_pcent_threshold: float = 10,   # default values (draft decision tree and parameters)
    ind_9_pcent_threshold: float = 10,   # default values (draft decision tree and parameters)
    ind_10_pcent_threshold: float = 10,   # default values (draft decision tree and parameters)
    ind_11_pcent_threshold: float = 10,   # default values (draft decision tree and parameters)    
    ind_1_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_2_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_3_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_4_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_5_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_6_input_columns: pd.Series = None, # see lookup_gee_datasets for details
    ind_7_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_8_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_9_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_10_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_11_input_columns: pd.Series = None,  # see lookup_gee_datasets for details
    ind_1_name: str = "Indicator_1_treecover",
    ind_2_name: str = "Indicator_2_commodities",
    ind_3_name: str = "Indicator_3_disturbance_before_2020",
    ind_4_name: str = "Indicator_4_disturbance_after_2020",
    ind_5_name: str = "Indicator_5_primary_2020",
    ind_6_name: str ="Indicator_6_nat_reg_forest_2020",
    ind_7_name: str ="Indicator_7_planted_plantations_2020",
    ind_8_name: str ="Indicator_8_planted_plantations_post_2020",
    ind_9_name: str ="Indicator_9_treecover_post_2020",
    ind_10_name: str ="Indicator_10_logging_concession",
    ind_11_name: str ="Indicator_11_agri_post_2020",    
    low_name: str = "no",
    high_name: str = "yes",
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

    Returns:
        data_lookup_type: DataFrame with added 'EUDR_risk' column.
    """

    if ind_1_input_columns is None:
        ind_1_input_columns = get_cols_ind_1_treecover(lookup_gee_datasets_df)
    if ind_2_input_columns is None:
        ind_2_input_columns = get_cols_ind_2_commodities(lookup_gee_datasets_df)
    if ind_3_input_columns is None:
        ind_3_input_columns = get_cols_ind_3_dist_before_2020(lookup_gee_datasets_df)
    if ind_4_input_columns is None:
        ind_4_input_columns = get_cols_ind_4_dist_after_2020(lookup_gee_datasets_df)
    if ind_5_input_columns is None:
        ind_5_input_columns = get_cols_ind_5_primary_2020(lookup_gee_datasets_df)
    if ind_6_input_columns is None:
        ind_6_input_columns = get_cols_ind_6_nat_reg_2020(lookup_gee_datasets_df)
    if ind_7_input_columns is None:
        ind_7_input_columns = get_cols_ind_7_planted_2020(lookup_gee_datasets_df)    
    if ind_8_input_columns is None:
        ind_8_input_columns = get_cols_ind_8_planted_post_2020(lookup_gee_datasets_df)
    if ind_9_input_columns is None:
        ind_9_input_columns = get_cols_ind_9_treecover_post_2020(lookup_gee_datasets_df) 
    if ind_10_input_columns is None:
        ind_10_input_columns = get_cols_ind_10_logging(lookup_gee_datasets_df) 
    if ind_11_input_columns is None:
        ind_11_input_columns = get_cols_ind_11_agri_post_2020(lookup_gee_datasets_df) 

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
    names = [ind_1_name, ind_2_name, ind_3_name, ind_4_name,ind_5_name,ind_6_name,ind_7_name,ind_8_name,ind_9_name,ind_10_name,ind_11_name]
    [check_range(threshold) for threshold in thresholds]

    df_w_indicators = add_indicators(
        df,
        input_cols,
        thresholds,
        names,
        low_name,
        high_name,
    )

    df_w_indicators_and_risk = add_eudr_risk_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_3_name=ind_3_name,
        ind_4_name=ind_4_name,
    )

    df_w_indicators_and_risk_soy = add_eudr_risk_soy_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name,
        ind_2_name=ind_2_name,
        ind_3_name=ind_3_name,
        ind_4_name=ind_4_name,
    )
    
    df_w_indicators_and_risk_timber = add_eudr_risk_timber_col(
        df=df_w_indicators,
        ind_1_name=ind_1_name, 
        ind_2_name=ind_2_name, 
        ind_3_name=ind_3_name, 
        ind_4_name=ind_4_name,
        ind_5_name=ind_5_name, 
        ind_6_name=ind_6_name, 
        ind_7_name=ind_7_name, 
        ind_8_name=ind_8_name,
        ind_9_name=ind_9_name,
        ind_10_name=ind_10_name,
        ind_11_name=ind_11_name
    )

    return df_w_indicators_and_risk_timber


def add_eudr_risk_col(
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
        ind_1_name (str): Name of first indicator column.
        ind_2_name (str): Name of second indicator column.
        ind_3_name (str): Name of third indicator column.
        ind_4_name (str): Name of fourth indicator column.

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
            df.at[index, "EUDR_risk"] = "low"
        # If none of the first three indicators suggest low risk and Indicator 4 suggests no risk, set EUDR_risk to "more_info_needed"
        elif row[ind_4_name] == "no":
            df.at[index, "EUDR_risk"] = "more_info_needed"
        # If none of the above conditions are met, set EUDR_risk to "high"
        else:
            df.at[index, "EUDR_risk"] = "high"

    return df


def add_eudr_risk_soy_col(
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
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Indicator_1_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Indicator_2_commodities".
        ind_3_name (str, optional): Name of third indicator column. Defaults to "Indicator_3_disturbance_before_2020".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Indicator_4_disturbance_after_2020".

    Returns:
        DataFrame: DataFrame with added 'EUDR_risk' column.
    """

    # soy risk
    for index, row in df.iterrows():
        # If there is no tree cover in 2020, set EUDR_risk_soy to "low"
        if row[ind_1_name] == "no" or row[ind_2_name] == "yes":
            df.at[index, 'EUDR_risk_soy'] = "low"
        # If there is tree cover in 2020 and distrubances post 2020, set EUDR_risk_soy to "high"
        elif row[ind_1_name] == "yes" and row[ind_4_name] == "yes":
            df.at[index, 'EUDR_risk_soy'] = "high"
        # If tree cover and no disturbances post 2020, set EUDR_risk to "more_info_needed"
        else :
            df.at[index, 'EUDR_risk_soy'] = "more_info_needed"

    return df
    
def add_eudr_risk_timber_col(
    df: data_lookup_type,
    ind_1_name: str,
    ind_2_name: str,
    ind_3_name: str,
    ind_4_name: str,
    ind_5_name: str,
    ind_6_name: str,
    ind_7_name: str,
    ind_8_name: str,
    ind_9_name: str,
    ind_10_name: str,
    ind_11_name: str,
)-> data_lookup_type:
    """
    Adds the EUDR (European Union Deforestation Risk) column to the DataFrame based on indicator values.

    Args:
        df (DataFrame): Input DataFrame.
        ind_1_name (str, optional): Name of first indicator column. Defaults to "Indicator_1_treecover".
        ind_2_name (str, optional): Name of second indicator column. Defaults to "Indicator_2_commodities".
        ind_3_name (str, optional): Name of third indicator column. Defaults to "Indicator_3_disturbance_before_2020".
        ind_4_name (str, optional): Name of fourth indicator column. Defaults to "Indicator_4_disturbance_after_2020".

    Returns:
        DataFrame: DataFrame with added 'EUDR_risk' column.
    """
          
    for index, row in df.iterrows():
        # If there is a commodity in 2020 OR if there is planted-plantation in 2020 AND no agriculture in 2023, set EUDR_risk_degrad to "low"
        if row[ind_2_name] == "yes" or (row[ind_7_name] == "yes" and row[ind_11_name] == "no"):
            df.at[index, 'EUDR_risk_timber'] = "low"
        # If there is no tree cover, set EUDR_risk_degrad to "low"? no because of unstocked forests
        # if row[ind_1_name] == "no" or row[ind_3_name] == "yes" or row[ind_7_name] == "yes":
        #   df.at[index, 'EUDR_risk_degrad'] = "low"
         # If primary or naturally regenerating or planted forest in 2020 AND agricultural use in 2023, set EUDR_risk to high
        elif (row[ind_5_name] == "yes" or row[ind_6_name] == "yes" or row[ind_7_name] == "yes") and row[ind_11_name] == "yes":
            df.at[index, 'EUDR_risk_timber'] = "high"    
        #If primary or naturally regenerating AND planted post 2020, set EUDR_risk to "high"
        elif (row[ind_5_name] == "yes" or row[ind_6_name] == "yes") and row[ind_8_name] == "yes":
            df.at[index, 'EUDR_risk_timber'] = "high"
        # If primary or naturally regenerating or planted forest in 2020 and OWL in 2023, set EUDR_risk to high
        #elif (row[ind_5_name] == "yes" or row[ind_6_name] == "yes" or row[ind_7_name] == "yes") and row[ind_11_name] == "yes":
        #    df.at[index, 'EUDR_risk_timber'] = "high" 
        
        # If primary forest OR naturally regenerating AND an information on management practice OR tree cover post 2020, set EUDR_risk_degrad to "low"
        elif (row[ind_5_name] == "yes" or row[ind_6_name] == "yes") and (row[ind_9_name] == "yes" or row[ind_10_name] == "yes"):
            df.at[index, 'EUDR_risk_timber'] = "low"           
        # If primary or naturally regenerating and no other info, set EUDR_risk to "more_info_needed"
        elif row[ind_5_name] == "yes" or row[ind_6_name] == "yes" :
            df.at[index, 'EUDR_risk_timber'] = "more_info_needed"
        # If none of the above conditions are met, set EUDR_risk to "high"
        else:
            df.at[index, 'EUDR_risk_timber'] = "high"
            
    return df
    
def add_indicators(
    df: data_lookup_type,
    input_cols: list[str],
    thresholds: list[float],
    names: list[str],
    low_name: str = "no",
    high_name: str = "yes",
) -> data_lookup_type:
    for input_col, threshold, name in zip(input_cols, thresholds, names):
        df = add_indicator_column(
            df=df,
            input_columns=input_col,
            threshold=threshold,
            new_column_name=name,
            low_name=low_name,
            high_name=high_name,
        )

    return df


def add_indicator_column(
    df: data_lookup_type,
    input_columns: list[str],
    threshold: float,
    new_column_name: str,
    low_name: str = "yes",
    high_name: str = "no",
    sum_comparison: bool = False,
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
            if percent_or_ha == "ha":
                val_to_check = clamp(
                    ((df[col] / df[geometry_area_column]) * 100), 0, 100
                )
            else:
                val_to_check = df[col]
            new_column[val_to_check > threshold] = high_name

    # Concatenate the new column to the DataFrame
    df = pd.concat([df, new_column], axis=1)
    return df


def get_cols_ind_1_treecover(lookup_gee_datasets_df):
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


def get_cols_ind_2_commodities(lookup_gee_datasets_df):
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


def get_cols_ind_3_dist_before_2020(lookup_gee_datasets_df):
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


def get_cols_ind_4_dist_after_2020(lookup_gee_datasets_df):
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

def get_cols_ind_5_primary_2020(lookup_gee_datasets_df):
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
    return list(lookup_gee_datasets_df["name"][
        (lookup_gee_datasets_df["use_for_risk_timber"] == 1) &
        (lookup_gee_datasets_df["theme_timber"] == "primary")
    ])

def get_cols_ind_6_nat_reg_2020(lookup_gee_datasets_df):
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
    return list(lookup_gee_datasets_df["name"][
        (lookup_gee_datasets_df["use_for_risk_timber"] == 1) &
        (lookup_gee_datasets_df["theme_timber"] == "naturally_reg_2020")
    ])

def get_cols_ind_7_planted_2020(lookup_gee_datasets_df):
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
    return list(lookup_gee_datasets_df["name"][
        (lookup_gee_datasets_df["use_for_risk_timber"] == 1) &
        (lookup_gee_datasets_df["theme_timber"] == "planted_plantation_2020")
    ])
def get_cols_ind_8_planted_post_2020(lookup_gee_datasets_df):
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
    return list(lookup_gee_datasets_df["name"][
        (lookup_gee_datasets_df["use_for_risk_timber"] == 1) &
        (lookup_gee_datasets_df["theme_timber"] == "planted_plantation_post_2020")
    ])
def get_cols_ind_9_treecover_post_2020(lookup_gee_datasets_df):
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
    return list(lookup_gee_datasets_df["name"][
        (lookup_gee_datasets_df["use_for_risk_timber"] == 1) &
        (lookup_gee_datasets_df["theme_timber"] == "treecover_post2020")
    ])
def get_cols_ind_10_logging(lookup_gee_datasets_df):
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
    return list(lookup_gee_datasets_df["name"][
        (lookup_gee_datasets_df["use_for_risk_timber"] == 1) &
        (lookup_gee_datasets_df["theme_timber"] == "logging_concession")
    ])  
def get_cols_ind_11_agri_post_2020(lookup_gee_datasets_df):
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
    return list(lookup_gee_datasets_df["name"][
        (lookup_gee_datasets_df["use_for_risk_timber"] == 1) &
        (lookup_gee_datasets_df["theme_timber"] == "agri_post_2020")
    ])          
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
import pandas as pd
import ee


def calculate_eudr_risk(df):
    for index, row in df.iterrows():
        if row['Treecover_indicator'] == "no":
            df.at[index, 'EUDR_risk'] = "low"
        elif row['Commodities_indicator'] == "yes":
            df.at[index, 'EUDR_risk'] = "low"
        elif row['Disturbance_pre_2020_indicator'] == "yes":
            df.at[index, 'EUDR_risk'] = "low"
        elif row['Disturbance_post_2020_indicator'] == "no":
            df.at[index, 'EUDR_risk'] = "more_info_needed"
        else:
            df.at[index, 'EUDR_risk'] = "high"
    return df


# If 'Treecover_indicator' is "no", or 'Commodities_indicator' is "yes", or 'Disturbance_pre_2020_indicator' is "yes", then set 'EUDR_risk' to "low".
# If 'Disturbance_post_2020_indicator' is "yes", (and previous condition is not true), then set 'EUDR_risk' to "high".
# If none of the above conditions are met, set 'EUDR_risk' to "more_info_needed".

# def calculate_eudr_risk(df):
#     for index, row in df.iterrows():
#         if (row['Treecover_indicator'] == "no" or
#             row['Commodities_indicator'] == "yes" or
#             row['Disturbance_pre_2020_indicator'] == "yes"):
#             df.at[index, 'EUDR_risk'] = "low"
#         elif row['Disturbance_post_2020_indicator'] == "yes":
#             df.at[index, 'EUDR_risk'] = "high"
#         else:
#             df.at[index, 'EUDR_risk'] = "more_info_needed"
#     return df



def add_indicator_column(df, columns_to_check, threshold, new_column_name, comparison_sign=None, low_name='low', high_name='high', sum_comparison=False):
    """
    Add a new column to the DataFrame based on the specified columns, threshold, and comparison sign.

    Parameters:
        df (DataFrame): The pandas DataFrame to which the column will be added.
        columns_to_check (list): List of column names to check for threshold.
        threshold (float): The threshold value to compare against.
        new_column_name (str): The name of the new column to be added.
        comparison_sign (str): The comparison sign to use ('>', '>=', '=', '<', '<=') (default is None).
                               If None or unrecognized, default behavior is to use '>' for single column comparison
                               and sum all values in columns_to_check for sum comparison.
        low_name (str): The name for the value when below the threshold (default is 'low').
        high_name (str): The name for the value when above the threshold (default is 'high').
        sum_comparison (bool): If True, sum all values in columns_to_check and compare to threshold (default is False).

    Returns:
        DataFrame: The DataFrame with the new column added.
    """
    # Create a new column and initialize with low_name
    df[new_column_name] = low_name
    
    if sum_comparison:
        # Sum all values in specified columns and compare to threshold
        sum_values = df[columns_to_check].sum(axis=1)
        if comparison_sign == '>':
            df.loc[sum_values > threshold, new_column_name] = high_name
        elif comparison_sign == '>=':
            df.loc[sum_values >= threshold, new_column_name] = high_name
        elif comparison_sign == '=':
            df.loc[sum_values == threshold, new_column_name] = high_name
        elif comparison_sign == '<':
            df.loc[sum_values < threshold, new_column_name] = high_name
        elif comparison_sign == '<=':
            df.loc[sum_values <= threshold, new_column_name] = high_name
        else:
            # Default behavior: use '>' for sum comparison
            df.loc[sum_values > threshold, new_column_name] = high_name
    else:
        # Check if any values in specified columns are above the threshold and update the new column accordingly
        for col in columns_to_check:
            if comparison_sign == '>':
                df.loc[df[col] > threshold, new_column_name] = high_name
            elif comparison_sign == '>=':
                df.loc[df[col] >= threshold, new_column_name] = high_name
            elif comparison_sign == '=':
                df.loc[df[col] == threshold, new_column_name] = high_name
            elif comparison_sign == '<':
                df.loc[df[col] < threshold, new_column_name] = high_name
            elif comparison_sign == '<=':
                df.loc[df[col] <= threshold, new_column_name] = high_name
            else:
                # Default behavior: use '>' for single column comparison
                df.loc[df[col] > threshold, new_column_name] = high_name
    return df


def add_indicator_column_from_csv(csv_file, columns_to_check, threshold, new_column_name, comparison_sign=None,low_name='low', high_name='high', sum_comparison=False, output_file=None):
    """
    Read a CSV file into a DataFrame, add a new column based on specified columns and threshold,
    and optionally export the DataFrame as a CSV file.

    Parameters:
        csv_file (str): The path to the CSV file.
        columns_to_check (list): List of column names to check for threshold.
        threshold (float): The threshold value above which the new column will be set.
        new_column_name (str): The name of the new column to be added.
        low_name (str): The name for the value when below the threshold (default is 'low').
        high_name (str): The name for the value when above the threshold (default is 'high').
        sum_comparison (bool): If True, sum all values in columns_to_check and compare to threshold (default is False).
        output_file (str, optional): The name of the CSV file to export the DataFrame (default is None).

    Returns:
        DataFrame or None: The DataFrame with the new column added if output_file is not provided, otherwise None.
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)
    
    # Call the add_risk_column function
    df = add_risk_column(df, columns_to_check, threshold, new_column_name, low_name, high_name, sum_comparison)
    
    # Export the DataFrame as a CSV file if output_file is provided
    if output_file:
        df.to_csv(output_file, index=False)
        print(f'exported to {output_file}')
    else:
        return df


def create_wildcard_column_list(df, wildcard_patterns):
    """
    Create a list of column names based on multiple wildcard patterns.

    Parameters:
        df (DataFrame): The pandas DataFrame to search for columns.
        wildcard_patterns (list): List of wildcard patterns to match column names.

    Returns:
        list: List of column names matching the wildcard patterns.
    """
    column_lists = [df.filter(like=pattern).columns.tolist() for pattern in wildcard_patterns]
    return [col for sublist in column_lists for col in sublist]



def select_years_in_range(string_list, min_year, max_year):
    """
    Select strings from the list where the last four characters, when turned into integers,
    are in the specified range of years.

    Parameters:
        string_list (list): List of strings.
        min_year (int): The minimum year of the range.
        max_year (int): The maximum year of the range.

    Returns:
        list: List of strings where the last four characters are in the specified range of years.
    """
    selected_strings = []
    for string in string_list:
        if len(string) >= 4:
            last_four_digits = string[-4:]
            try:
                year = int(last_four_digits)
                if year in range(min_year, max_year + 1):
                    selected_strings.append(string)
            except ValueError:
                pass
    return selected_strings



def order_list_from_lookup(lookup_gee_datasets_df):
    return lookup_gee_datasets_df.sort_values(by=['dataset_order'])["dataset_name"].tolist() # names sorted by list
    
def create_column_list_from_lookup(lookup_gee_datasets_df,prefix_columns_list):
    # ordered_dataset_df= lookup_gee_datasets_df.sort_values(by=['dataset_order'])
    
    # column_order_list = list(ordered_dataset_df["dataset_name"])
    column_order_list = lookup_gee_datasets_df.sort_values(by=['dataset_order'])["dataset_name"].tolist()

    # adds in a list of columns to the start of the order list (i.e. the geo_id, geometry area column and country columns), if left blank nothing added
    column_order_list =  prefix_columns_list + order_list_from_lookup(lookup_gee_datasets_df)

    return column_order_list
    
                                   
def reorder_columns_by_lookup(df,lookup_gee_datasets_df,dataset_order_column,dataset_name_column,prefix_columns_list=[]):    
    """ reorder columns by creating an ordered list from a lookup_gee_datasets_df containing column order and dataset names that match those in results dataframe"""
    column_order_list = create_column_list_from_lookup(lookup_gee_datasets_df,prefix_columns_list)
    # ordered_dataset_df= lookup_gee_datasets_df.sort_values(by=['dataset_order'])
    
    # column_order_list = list(ordered_dataset_df["dataset_name"])
    
    # # adds in a list of columns to the start of the order list (i.e. the geo_id, geometry area column and country columns), if left blanmk nothing added
    # column_order_list = prefix_columns_list + column_order_list

    df_reordered  = df.reindex(columns=column_order_list) # reorder by list

    return df_reordered


def make_lookup_from_feature_col(feature_col,join_column,lookup_column,join_column_new_name=False,lookup_column_new_name=False):
    """makes a lookup table (pandas dataframe) from two columns in a feature collection (duplicates removed)"""
    
    list_join_column = feature_col.aggregate_array(join_column).getInfo()
    
    list_lookup_column = feature_col.aggregate_array(lookup_column).getInfo()
    
    #make dataframe from columns
    lookup_table = pd.DataFrame({join_column:list_join_column,
                                      lookup_column:list_lookup_column}) 
    #removes duplicates
    lookup_table= lookup_table.drop_duplicates()

    # rename columns if specified
    if join_column_new_name!=False:
        lookup_table.rename(columns={join_column:join_column_new_name},inplace=True)
        
    if lookup_column_new_name!=False:
        lookup_table.rename(columns={lookup_column:lookup_column_new_name},inplace=True)
        
    return lookup_table

def truncate_strings_in_list(input_list, max_length):
    """as name suggests, useful for exporting to shapefiles fort instance where col name length is limited"""
    return [string[:max_length] for string in input_list]



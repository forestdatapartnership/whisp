import pandas as pd
import ee

def tidy_dataframe_after_pivot (df):
    """Tidying dataframe after long-to-wide reformatting, incl. removes unwanted levels, column names"""
    # df.columns = df.columns.droplevel(0) #remove sum
    df.columns = df.columns.get_level_values(1)
    df.columns.name = None               #remove "dataset_name" label
    df = df.reset_index()    #index to columns
    df.columns = df.columns.get_level_values(0)
    return df


def reorder_columns_by_lookup(df,lookup_df,dataset_order_column,dataset_name_column,prefix_columns_list=[]):    
    """ reorder columns by creating an ordered list from a lookup_df containing column order and dataset names that match those in results dataframe"""

    ordered_dataset_df= lookup_df.sort_values(by=['dataset_order'])
    
    column_order_list = list(ordered_dataset_df["dataset_name"])
    
    # adds in a list of columns to the start of the order list (i.e. the geo_id, geometry area column and country columns), if left blanmk nothing added
    column_order_list = prefix_columns_list + column_order_list

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
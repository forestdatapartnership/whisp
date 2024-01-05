import pandas as pd

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
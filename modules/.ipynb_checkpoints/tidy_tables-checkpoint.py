import pandas as pd

def tidy_dataframe_after_pivot (df):
    """Tidying dataframe after long-to-wide reformatting, incl. removes unwanted levels, column names"""
    # df.columns = df.columns.droplevel(0) #remove sum
    df.columns = df.columns.get_level_values(1)
    df.columns.name = None               #remove "dataset_name" label
    df = df.reset_index()    #index to columns
    df.columns = df.columns.get_level_values(0)
    return df

# def tidy_dataframe_after_pivot (df):
#     """Tidying dataframe after long-to-wide reformatting, incl. removes unwanted levels, column names"""
#     df.columns = df.columns.droplevel(0) #remove sum
#     df.columns.name = None               #remove "dataset_name" label
#     df = df.reset_index()    #index to columns
#     return df

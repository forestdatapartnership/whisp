import os
import ee

ee.Initialize()
import pandas as pd

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

lookup_table = make_lookup_from_feature_col(
    feature_col=ee.FeatureCollection("projects/ee-andyarnellgee/assets/gadm_41_level_1"),
    join_column="fid",lookup_column="GID_0",
    join_column_new_name="fid",
    lookup_column_new_name="Country")
    
lookup_output_csv_path="parameters/lookup_gadm_country_codes_to_iso3.csv"
lookup_table.to_csv(path_or_buf=lookup_output_csv_path,header=True,index=False) # save lookup table as CSV

lookup_table = make_lookup_from_feature_col(
    feature_col=ee.FeatureCollection("projects/ee-andyarnellgee/assets/gadm_41_level_1"),
    join_column="fid",lookup_column="COUNTRY",
    join_column_new_name="fid",
    lookup_column_new_name="Country")
    
lookup_output_csv_path="parameters/lookup_gadm_country_codes_to_names.csv"
lookup_table.to_csv(path_or_buf=lookup_output_csv_path,header=True,index=False) # save lookup table as CSV
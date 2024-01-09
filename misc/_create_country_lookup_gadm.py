import os
import ee

ee.Initialize()
import pandas as pd

lookup_table = tidy_tables.make_lookup_from_feature_col(
    feature_col=ee.FeatureCollection("projects/ee-andyarnellgee/assets/gadm_41_level_1"),
    join_column="fid",lookup_column="GID_0",
    join_column_new_name="fid",
    lookup_column_new_name="Country_ISO3")
    
lookup_output_csv_path="parameters/lookup_GADM_country_codes_to_ISO3.csv"
lookup_table.to_csv(path_or_buf=lookup_output_csv_path,header=True,index=False) # save lookup table as CSV

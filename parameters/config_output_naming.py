import os
import ee
ee.Initialize()

debug = True # True or False: get print messages or not (e.g. for debugging code) - could go in a generag config file

targetImageCollId = "users/andyarnell10/fdap_dpi/imageCol_trial"

out_path = os.path.join('/home/sepal-user/fdap/')

#long csv (temporary format)
out_long_csv_name = 'temp_stats_long_format.csv' 

dataset_column_name = "dataset_name"

#wide csv (main output format)
out_file_long=out_path+out_long_csv_name

out_wide_csv_name = 'output_stats_wide_format.csv' #set output name

out_file_wide = out_path+out_wide_csv_name #set full path for output csv


#output column names 
geometry_area_column = "Shape_area_hectares"

geo_id_column = "Geo_id"


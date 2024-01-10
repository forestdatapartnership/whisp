import os
import ee

ee.Initialize()


#if exporting to an image collection
target_image_col_id = "projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/work_in_progress/whisp_image_col_v0"

out_path = os.path.join('/home/sepal-user/whisp/')

#long csv (temporary format)
out_long_csv_name = 'temp_stats_long_format.csv' 

dataset_column_name = "dataset_name"

#wide csv (main output format)
out_file_long=out_path+out_long_csv_name

out_wide_csv_name = 'output_stats_wide_format.csv' #set output name

out_file_wide = out_path+out_wide_csv_name #set full path for output csv


#output column names 
geometry_area_column = "Area_ha"

geo_id_column = "Geo_id"

plot_id_column = "PLOTID"
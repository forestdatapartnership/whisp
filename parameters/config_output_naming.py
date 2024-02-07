import os
import ee

ee.Initialize()


#if exporting to an image collection
target_image_col_id = "projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/work_in_progress/whisp_image_col_v0"

out_path = os.path.join('/home/sepal-user/whisp/')

#wide csv (main output format)
out_wide_csv_name = 'whisp_output_table.csv' #set output name

out_file_wide = out_path+out_wide_csv_name #set full path for output csv

out_shapefile_name = "shapefile_for_ceo.shp.zip"

out_shapefile = out_path + out_shapefile_name


#output column names 
geometry_area_column = "Area_ha"

geo_id_column = "Geo_id"

plot_id_column = "PLOTID"
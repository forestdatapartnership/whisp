import os
import ee
import pandas as pd
ee.Initialize()

debug = True  # get print messages or not (e.g. for debugging code etc) (True or False)

percent_or_ha = "ha" #units

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

#whisp outputs formatting

#do you want to keep system:index from input feature collection? NB it's useful for making joins after processing
keep_system_index = True

# do you keep other properties from input feature collection?
keep_original_properties = True

#lookup path
path_lookup_gee_datasets_df = "parameters/lookup_gee_datasets.csv"

lookup_gee_datasets_df = pd.read_csv(path_lookup_gee_datasets_df)

#for stroing backup/temp csv files
temp_csvs_folder_name = "backup_csvs"

temp_csvs_folder_path = out_path + temp_csvs_folder_name

######################
# # if need to expand units in future
# UNIT_TYPE = {
# # type: [selector, name]
# "area": [0, "area"],
# "percent": [1, "percent"]
# "proportion": [2, "proportion"]
# }

# UNIT = {
# # acronym: [factor, name]
# "ha": [10000, "hectares"],
# "sqkm": [1000000, "square kilometers"]
# }

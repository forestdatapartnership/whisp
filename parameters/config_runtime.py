import os
import ee
import pandas as pd
ee.Initialize()

debug = True  # get print messages or not (e.g. for debugging code etc) (True or False)

from parameters.config_directory import RESULTS_DIR,BACKUP_CSVS_DIR 

#unit choice ("ha" or "percent")
percent_or_ha = "ha"  

out_directory = RESULTS_DIR

#wide csv (main output format)
out_csv_name = 'whisp_output_table.csv' #set output name

out_csv = out_directory / out_csv_name #set full path for output csv

out_shapefile_name = "shapefile_for_ceo.shp.zip"

out_shapefile = out_directory / out_shapefile_name


### whisp outputs formatting

# output column names 
geometry_area_column = "Plot_area_ha" 

stats_unit_type_column = "Unit"

country_column = "Country"

centroid_x_coord_column = "Centroid_lat" 

centroid_y_coord_column = "Centroid_lon"

geo_id_column = "Geo_id"

geometry_type_column = "Geometry_type" 

plot_id_column = "Plot_ID"


# reformatting numbers to decimal places (e.g. '%.3f' is 3 dp)
geometry_area_column_formatting = '%.3f' 

stats_area_columns_formatting = '%.3f' 

stats_percent_columns_formatting = '%.0f'   


# ordering prefix columns: inserted before stats columns (plot metadata and stats unit type). 
prefix_columns_list =[geo_id_column, geometry_area_column, geometry_type_column, country_column, centroid_x_coord_column, centroid_y_coord_column, stats_unit_type_column] 

#do you want to keep system:index from input feature collection? NB it's useful for making joins after processing
keep_system_index = True

# do you keep other properties from input feature collection?
keep_original_properties = False

#lookup path - for dataset info
path_lookup_gee_datasets_df = "parameters/lookup_gee_datasets.csv"

lookup_gee_datasets_df = pd.read_csv(path_lookup_gee_datasets_df)



### risk indicator parameters

# lists of columns to check (could shift to lookup_gee_datasets.csv in future)

# treecover
cols_ind_1_treecover = ['EUFO_2020','GLAD_Primary', 'TMF_undist', 'JAXA_FNF_2020', 'GFC_TC_2020', 'ESA_TC_2020'] 

# commodities
cols_ind_2_commodities = ['TMF_plant', 'Oil_palm_Descals', 'Oil_palm_FDaP', 'Cocoa_ETH', 'Cocoa_bnetd']

# disturbance before 2020
cols_ind_3_dist_before_2020= ["GFC_loss_before_2020", "MODIS_fire_before_2020", "TMF_deg_before_2020", "TMF_loss_before_2020"]

# disturbance after 2020
cols_ind_4_dist_after_2020 = ["RADD_after_2020", "GFC_loss_after_2020", "MODIS_fire_after_2020", "TMF_deg_after_2020", "TMF_loss_after_2020"]


### Temp output parameters

# #for storing backup/temp csv files
temp_csvs_folder_path = BACKUP_CSVS_DIR

###################### wishlist - more options for units
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

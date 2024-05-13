import os
import ee
import pandas as pd
ee.Initialize()

debug = True  # get print messages or not (e.g. for debugging code etc) (True or False)

percent_or_ha = "ha" #units 

out_path = os.path.join('/home/sepal-user/whisp/')


#wide csv (main output format)
out_csv_name = 'whisp_output_table.csv' #set output name

out_csv = out_path + out_csv_name #set full path for output csv

out_shapefile_name = "shapefile_for_ceo.shp.zip"

out_shapefile = out_path + out_shapefile_name


#output column names 
geometry_area_column = "Area_ha"

geo_id_column = "Geo_id"

geometry_type_column = "Geometry_type" 

plot_id_column = "PlotID"

prefix_columns_list =[geo_id_column, geometry_area_column,geometry_type_column, "Country", "lat", "lon"] 

#whisp outputs formatting

#do you want to keep system:index from input feature collection? NB it's useful for making joins after processing
keep_system_index = True

# do you keep other properties from input feature collection?
keep_original_properties = False

#lookup path
path_lookup_gee_datasets_df = "parameters/lookup_gee_datasets.csv"

lookup_gee_datasets_df = pd.read_csv(path_lookup_gee_datasets_df)

# for risk indicators 
# lists of columns to check (could shift to lookup_gee_datasets.csv in future)

# treecover
cols_ind_1_treecover = ['EUFO_2020','GLAD_Primary', 'TMF_undist', 'JAXA_FNF_2020', 'GFC_TC_2020', 'ESA_TC_2020'] 

# commodities
cols_ind_2_commodities = ['TMF_plant', 'Oil_palm_Descals', 'Oil_palm_FDaP', 'Cocoa_ETH', 'Cocoa_bnetd']

# disturbance before 2020
cols_ind_3_dist_before_2020= ["GFC_loss_before_2020", "MODIS_fire_before_2020", "TMF_deg_before_2020", "TMF_loss_before_2020"]

# disturbance after 2020
cols_ind_4_dist_after_2020 = ["RADD_after_2020", "GFC_loss_after_2020", "MODIS_fire_after_2020", "TMF_deg_after_2020", "TMF_loss_after_2020"]


#for storing backup/temp csv files
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

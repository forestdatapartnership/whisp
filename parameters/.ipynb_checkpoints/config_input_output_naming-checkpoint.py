import os
import ee
ee.Initialize()

debug = True # True or False: get print messages or not (e.g. for debugging code)

out_path = os.path.join('/home/sepal-user/fdap/')

#long csv (temporary format)
out_long_csv_name = 'temp_stats_long_format.csv' 

dataset_column_name = "dataset_name"

#wide csv (main output format)
out_file_long=out_path+out_long_csv_name

out_wide_csv_name = 'output_stats_wide_format.csv' #set output name

out_file_wide = out_path+out_wide_csv_name #set full path for output csv


# lookup table paths

#country codes
path_lookup_country_codes_to_names = "parameters/lookup_GAUL_country_codes_to_names.csv"

## recoding/reclassifying JRC tropical moist forest classes to EUDR classes 
## i.e. representing undisturbed, disturbed and planatations
path_lookup_recoding_JRC_TMF_product = "parameters/lookup_recoding_JRC_TMF_product.csv"

#from (original) class code
from_class_column_name_JRC_TMF ='JRC_TMF_original_class_value'

#to (reclassified) class code
to_class_column_name_JRC_TMF = 'Remap_EUDR_value'


#output column names 
geometry_area_column = "Shape_area_hectares"

geo_id_column = "Geo_id"


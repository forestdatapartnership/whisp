### general runtime parameters 
# (for naming of outputs see also parameters/config_output_naming.py)

import ee

use_existing_image_collection = True  # faster (if one exists), else creates on the fly in GEE. Set to True or False.

update_iCol_properties = False # adds time so only put as True if need to update them based on changes to the lookup_gee_datasets.csv

debug = True  # get print messages or not (e.g. for debugging code etc) (True or False)




# what datasets to exclude from results
exclusion_list_dataset_ids = []

# country dataset choice 
country_dataset_id = 18   ##### for referenece: 18 = GADM, 16 = GAUL

##### country_name_iso3_or_both = "iso3" # to add in at some point?




## export to image collection asset parameters

export_icol = False  # choose to export datasets to an image collection asset (makes faster data loading times). Set to True or False.

make_empty_image_coll = True # if true then code will add an empty image collection (see parmaters.output_naming), if one doesn't exist already. Set to True or False.

skip_export_if_asset_exists = True # if image with same dataset_id exists in image collection, avoid exporting. Default: True

# exportRegion = ee.Geometry.Rectangle([-180, -90, 180, 90], null, false) # if want global

# for extent of image collection
exportRegion = ee.FeatureCollection("FAO/GAUL_SIMPLIFIED_500m/2015/level0").filter(
        ee.Filter.inList("ADM0_NAME",["Côte d'Ivoire", "Indonesia","Malaysia","Ghana"])).geometry()

####################################################place elsewhere if time

if country_dataset_id == 16:
    country_dataset_name = "GAUL_adm0_code" 
    admin_code_col_name = "ADM0_CODE" 
    path_lookup_country_codes_to_iso3 = "parameters/lookup_gaul_country_codes_to_iso3.csv" 
    path_lookup_country_codes_to_names = "parameters/lookup_gaul_country_codes_to_names.csv" 
    country_dataset_to_exclude = 18 # could make more flexible if more country datasets included
    
if country_dataset_id == 18:
    country_dataset_name = "GADM_fid_code" 
    admin_code_col_name = "fid" 
    path_lookup_country_codes_to_iso3 = "parameters/lookup_gadm_country_codes_to_iso3.csv"
    path_lookup_country_codes_to_names = "parameters/lookup_gadm_country_codes_to_iso3.csv"
    country_dataset_to_exclude = 16 # could make more flexible if more country datasets included
    
exclusion_list_dataset_ids = exclusion_list_dataset_ids + [country_dataset_to_exclude] # could make it more flexible if more country datasets included


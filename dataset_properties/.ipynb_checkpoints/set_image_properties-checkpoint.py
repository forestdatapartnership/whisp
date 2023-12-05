# #### Create dictionary of images and image names
# - prep for reduceRegions statistics so name of datasets/image is added to area stats
# - sets "system:index" property of each image
# - result is an image collection with 

#temp code - ideal use a lookup table
import os
import ee
import pandas as pd 

from datasets import *

from parameters.config_output_naming import debug
from parameters.config_lookups import lookup_gee_datasets

ee.Initialize()

# dict_list = lookup_gee_datasets.to_dict('records')
# dict_list

image_names_dict0 = {
                "GFC_Tree_Cover_2020":gfc_treecover_2020_area_hectares,
                "ESRI_Trees_2020":esri_trees_2020_area_hectares,
                "JAXA_Forest_non_forest_2020":JAXA_forestNonForest_2020_area_hectares,
                "GLAD_LULC_Stable_Tree_2020":glad_stable_tree_2020_area_hectares,
                "TMF_undisturbed_forest_2020":JRC_TMF_undisturbed_2020_area_hectares,
                "Primary_Humid_Tropical_Forest_2020": primary_HT_forests_2020_area_hectares,
                "TMF_disturbed_forest_2020": JRC_TMF_disturbed_2020_area_hectares,
                "Local_RADD_alerts":latest_radd_alert_confirmed_recent_area_hectares,
                "TMF_plantation":JRC_TMF_plantation_area_hectares,
                "Oil_palm_Descals": oil_palm_descals_binary_area_hectares,
                "FDaP_palm_plantations": FDaP_palm_2020_model_area_hectares,
                "Cocoa_plantations_Kalischek": cocoa_map_kalischek_threshold_area_hectares,
                "Protected_area":protected_areas_WDPA_area_hectares,
                "Other_Effective_area_based_Conservation_Measure":OECM_2023_area_hectares,
                "Key_Biodiversity_Area": kba_2023_area_hectares,
                "GAUL_boundaries_adm0_code_reproj":GAUL_boundaries_adm0_code_reproj
              }

#create empty dictionary to be populated
image_names_dict={} 
    
#set image names ("system:index") from keys in dictionary, and store as new one
for i in range(len(image_names_dict0)):
    dataset_name = (list(image_names_dict0.keys())[i]) #get dataset name
    image = (list(image_names_dict0.values())[i]) #get image
    updated_image=image.set("system:index",dataset_name) #set dataset name as image name i.e., "system:index"
    instance={dataset_name:updated_image} #combine
    image_names_dict.update(instance) #update into new dictionary

del image_names_dict0 # remove old dictionary

#make into a new image collection
images_IC = ee.ImageCollection(list(image_names_dict.values()))
                      
##checks
if debug: print ("number of images: ",len(image_names_dict))

images_IC


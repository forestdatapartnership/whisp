import ee
import os

ee.Initialize()

from datasets import *

from parameters.config_runtime import debug

import modules.area_stats as area_stats

# from parameters import lookup_gee_datasets

from datasets.template_images import template_image_1

image_function_list = ee.List([
         birdlife_kbas_biodiversity_prep(dataset_id=15, 
                                         template_image=template_image_1),
         creaf_descals_palm_prep(dataset_id=10),
         esri_lulc_trees_prep(dataset_id=2),
         eth_kalischek_cocoa_prep(dataset_id=12),
         fao_gaul_countries_prep(dataset_id=16, 
                                 template_image=template_image_1),
         fdap_palm_prep(dataset_id=11),
         glad_gfc_10pc_prep(dataset_id=1),
         glad_lulc_stable_prep(dataset_id=4),
         glad_pht_prep(dataset_id=6),
         jaxa_forest_prep(dataset_id=3),
         jrc_gfc_2020_prep(dataset_id=17),
         jrc_tmf_disturbed_prep(dataset_id=7),
         jrc_tmf_plantations_prep(dataset_id=9),
         jrc_tmf_undisturbed_prep(dataset_id=5),
         wcmc_wdpa_protection_prep(dataset_id=13, 
                                   template_image=template_image_1),
         wcmc_oecms_protection_prep(dataset_id=14, 
                                    template_image=template_image_1),
         wur_radd_alerts_prep(dataset_id=8)
])

image_IC_binary = ee.ImageCollection(image_function_list)

if debug: print ("dataset_ids in image_IC_binary collection:", image_IC_binary.aggregate_array("dataset_id").getInfo())

def image_coll_binary_to_area_w_properties_w_exceptions(image_collection,exception_dataset_id,debug=False):

    images_to_convert = image_collection.filter(ee.Filter.neq("dataset_id",exception_dataset_id))

    image_staying_binary = image_collection.filter(ee.Filter.eq("dataset_id",exception_dataset_id))

    images_w_area = images_to_convert.map(area_stats.binary_to_area_w_properties)

    combined_image_collection = images_w_area.merge(image_staying_binary)

    if debug: print ("dataset_ids in final image_collection:", combined_image_collection.aggregate_array("dataset_id").getInfo())
    
    return combined_image_collection

images_IC = image_coll_binary_to_area_w_properties_w_exceptions(image_collection=image_IC_binary,
                                                                exception_dataset_id=16,
                                                                debug=debug)



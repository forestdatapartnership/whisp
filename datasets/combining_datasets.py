import ee
import os

from modules.gee_initialize import initialize_ee

initialize_ee()

import pandas as pd

from datasets import *

from parameters.config_runtime import debug

import modules.area_stats as area_stats

from parameters import lookup_gee_datasets

from datasets.template_images import template_image_1

country_allocation_dataset_id_list =  list(lookup_gee_datasets["dataset_id"]     
                                              [(lookup_gee_datasets["country_allocation_stats_only"]==1)])

# runs prep scripts and adds the
image_function_list = ee.List([
         birdlife_kbas_biodiversity_prep(dataset_id=15, 
                                         template_image=template_image_1),
         creaf_descals_palm_prep(dataset_id=10),
         esa_worldcover_trees_prep(dataset_id=19),
         esri_lulc_trees_prep(dataset_id=2),
         eth_kalischek_cocoa_prep(dataset_id=12),
         fao_gaul_countries_prep(dataset_id=16, 
                                 template_image=template_image_1),
         fdap_palm_prep(dataset_id=11),
         gadm_countries_prep(dataset_id=18,
                             template_image=template_image_1),      
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

images_iCol = area_stats.image_coll_binary_to_area_w_properties_w_exceptions(
    image_collection=image_IC_binary,
    exception_dataset_id_list=country_allocation_dataset_id_list,
    debug=debug)



import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

from datasets.glad_gfc_raw import gfc

ee.Initialize()
dataset_id = 6

primary_HT_forests_2001_raw = ee.ImageCollection('UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1')

#get band and mosaic
primary_HT_forests_2001 = primary_HT_forests_2001_raw.select("Primary_HT_forests").mosaic().selfMask();


gfc_loss_2001_2020 = gfc.select(['lossyear']).lte(20) # get loss pixels since 2000 and up to and including 2020\

#remove GFC loss pixels from 2001-2020 (as previous technique with GFC, above)
primary_HT_forests_2020 = primary_HT_forests_2001.where(gfc_loss_2001_2020.eq(1),0)#.selfMask()

primary_HT_forests_2020_area_hectares = area_stats.binary_to_area_hectares(primary_HT_forests_2020)

primary_HT_forests_2020_area_hectares = area_stats.set_scale_property_from_image(primary_HT_forests_2020_area_hectares,primary_HT_forests_2001_raw.first(),0,debug=True).set("dataset_id",dataset_id)


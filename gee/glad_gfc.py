import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()


gfc = ee.Image("UMD/hansen/global_forest_change_2022_v1_10")

gfc_treecover_2000 = gfc.select(['treecover2000']) #get tree cover in 2000

gfc_loss_2001_2020 = gfc.select(['lossyear']).lte(20) # get loss pixels since 2000 and up to and including 2020

gfc_treecover_2020 = gfc_treecover_2000.where(gfc_loss_2001_2020.eq(1),0) # remove loss from original tree cover

gfc_treecover_2020_binary= gfc_treecover_2020.gt(10) #FAO 10% definition...

gfc_treecover_2020_area_hectares = area_stats.binary_to_area_hectares(gfc_treecover_2020_binary)

gfc_treecover_2020_area_hectares = area_stats.set_scale_property_from_image(gfc_treecover_2020_area_hectares,gfc)

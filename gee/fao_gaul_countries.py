import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()

GAUL_boundaries_poly = ee.FeatureCollection("FAO/GAUL/2015/level0");

template = ee.Image("UMD/hansen/global_forest_change_2022_v1_10");

GAUL_boundaries_adm0_code = GAUL_boundaries_poly.reduceToImage(["ADM0_CODE"],ee.Reducer.mode())  #make into image with the admn0 country code as the value

crs_template = gfc.select(0).projection().crs().getInfo()

GAUL_boundaries_adm0_code_reproj = GAUL_boundaries_adm0_code.reproject(
  crs= crs_template,
  scale= area_stats.get_scale_from_image(gfc),
).int8()


GAUL_boundaries_adm0_code_reproj = area_stats.set_scale_property_from_image(GAUL_boundaries_adm0_code_reproj,gfc,0,verbose=True)
GAUL_boundaries_adm0_code_reproj

# GAUL_boundaries.aggregate_array("ADM0_CODE").distinct()
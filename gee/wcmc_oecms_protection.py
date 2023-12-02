import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()

OECM_poly_1 = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/WDPA_OECM_polygons1")
OECM_poly_2 = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/WDPA_OECM_polygons2")
OECM_poly_3 = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/WDPA_OECM_polygons3")

OECM_2023_poly = ee.FeatureCollection([OECM_poly_1,OECM_poly_2,OECM_poly_3]).flatten().filter(ee.Filter.eq("PA_DEF","0")) #collating uploaded shapefiles and filtering for only OECMs 

OECM_2023_poly.limit(10)

OECM_2023_binary = OECM_2023_poly.reduceToImage(["WDPAID"],ee.Reducer.count()).gt(0).selfMask() #convert to image

#reproject based on gfc data
crs_template = gfc.select(0).projection().crs().getInfo()

OECM_2023_binary_reproj = OECM_2023_binary.reproject(
  crs= crs_template,
  scale= area_stats.get_scale_from_image(gfc),
).int8()

OECM_2023_area_hectares = area_stats.binary_to_area_hectares(OECM_2023_binary_reproj)

OECM_2023_area_hectares = area_stats.set_scale_property_from_image(OECM_2023_area_hectares,gfc,0,verbose=True)


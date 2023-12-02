import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()

kbas_2023_poly = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/KBAsGlobal_2023_March_01_POL");##uploaded - may need rights

kba_2023_overlap = kbas_2023_poly.reduceToImage(['SitRecID'],'count').selfMask()  #make into raster - remove mask if want 0s

kba_2023_binary = kba_2023_overlap.gte(0)

#reproject based on gfc data
crs_template = gfc.select(0).projection().crs().getInfo()

kba_2023_binary_reproj = kba_2023_binary.reproject(
  crs= crs_template,
  scale= area_stats.get_scale_from_image(gfc),
).int8()

kba_2023_area_hectares = area_stats.binary_to_area_hectares(kba_2023_binary_reproj)

kba_2023_area_hectares = area_stats.set_scale_property_from_image(kba_2023_area_hectares,gfc,0,verbose=True)


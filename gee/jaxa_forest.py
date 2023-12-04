import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()

JAXA_forestNonForest_raw = ee.ImageCollection('JAXA/ALOS/PALSAR/YEARLY/FNF4');

JAXA_forestNonForest_2020 =  JAXA_forestNonForest_raw.filterDate('2020-01-01', '2020-12-31').select('fnf').mosaic();

#select all trees (i.e. both dense and non-dense forest classes)
JAXA_forestNonForest_2020_binary = JAXA_forestNonForest_2020.lte(2)

JAXA_forestNonForest_2020_area_hectares = area_stats.binary_to_area_hectares(JAXA_forestNonForest_2020_binary)

JAXA_forestNonForest_2020_area_hectares = area_stats.set_scale_property_from_image(JAXA_forestNonForest_2020_area_hectares,JAXA_forestNonForest_raw.first(),0,debug=True)
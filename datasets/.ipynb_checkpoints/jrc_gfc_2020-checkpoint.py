import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

dataset_id=17

JRC_GFC_2020_raw = ee.ImageCollection("JRC/GFC2020/V1");

JRC_GFC_2020 = JRC_GFC_2020_raw.mosaic();

JRC_GFC_2020_area_hectares  = area_stats.binary_to_area_hectares(JRC_GFC_2020)

JRC_GFC_2020_area_hectares = area_stats.set_scale_property_from_image(
    JRC_GFC_2020_area_hectares,JRC_GFC_2020_raw.first(),debug=True).set("dataset_id",dataset_id)
    
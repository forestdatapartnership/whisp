import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

dataset_id = 12

ee.Initialize()

# cocoa_map_kalischek = ee.ImageCollection('projects/ee-nk-cocoa/assets/cocoa_map');

cocoa_map_kalischek_threshold = ee.Image('projects/ee-nk-cocoa/assets/cocoa_map_threshold_065');

cocoa_map_kalischek_threshold_area_hectares = area_stats.binary_to_area_hectares(cocoa_map_kalischek_threshold)

cocoa_map_kalischek_threshold_area_hectares = area_stats.set_scale_property_from_image(cocoa_map_kalischek_threshold_area_hectares,cocoa_map_kalischek_threshold,0,debug=True).set("dataset_id",dataset_id)

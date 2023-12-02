import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()

FDaP_palm_2020_model_raw = ee.ImageCollection("projects/forestdatapartnership/assets/palm/palm_2020_model_20231026");
FDaP_palm_2020_model = FDaP_palm_2020_model_raw.mosaic().gt(0.9).selfMask()

FDaP_palm_2020_model_area_hectares = area_stats.binary_to_area_hectares(FDaP_palm_2020_model)
FDaP_palm_2020_model_area_hectares = area_stats.set_scale_property_from_image(FDaP_palm_2020_model_area_hectares,FDaP_palm_2020_model_raw.first(),0,verbose=True)


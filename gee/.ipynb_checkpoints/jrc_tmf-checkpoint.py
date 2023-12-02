import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()

JRC_TMF_transitions_raw = ee.ImageCollection(
    'projects/JRC/TMF/v1_2021/TransitionMap_Subtypes') # raw data

JRC_TMF_transitions = JRC_TMF_transitions_raw.mosaic() ### NB check why 2021?  

#remap to 3 classes relevant to EUDR 
JRC_TMF_transitions_remap = image_prep.remap_image_from_csv_cols(
    image=JRC_TMF_transitions,
    csv_path=path_lookup_recoding_JRC_TMF_product,
    from_col=from_class_column_name_JRC_TMF,
    to_col=to_class_column_name_JRC_TMF,
    default_value=9999);

## 1) undisturbed forest, 2) disturbed forest and 3) plantation
JRC_TMF_undisturbed_2020 = JRC_TMF_transitions_remap.eq(1)

JRC_TMF_disturbed_2020 = JRC_TMF_transitions_remap.eq(2)

JRC_TMF_plantation  = JRC_TMF_transitions_remap.eq(3)

JRC_TMF_undisturbed_2020_area_hectares =area_stats.binary_to_area_hectares(JRC_TMF_undisturbed_2020)
JRC_TMF_disturbed_2020_area_hectares  = area_stats.binary_to_area_hectares(JRC_TMF_disturbed_2020)
JRC_TMF_plantation_area_hectares  = area_stats.binary_to_area_hectares(JRC_TMF_plantation)

JRC_TMF_undisturbed_2020_area_hectares = area_stats.set_scale_property_from_image(JRC_TMF_undisturbed_2020_area_hectares,
                                                                  JRC_TMF_transitions_raw.first())
JRC_TMF_disturbed_2020_area_hectares = area_stats.set_scale_property_from_image(JRC_TMF_disturbed_2020_area_hectares,
                                                                  JRC_TMF_transitions_raw.first())
JRC_TMF_plantation_area_hectares = area_stats.set_scale_property_from_image(JRC_TMF_plantation_area_hectares,
                                                                  JRC_TMF_transitions_raw.first())
                                     
if debug: print (JRC_TMF_undisturbed_2020_area_hectares.get("scale").getInfo())
# JRC_TMF_transitions_main = JRC_TMF_transitions.where((JRC_TMF_transitions.gte(10)).And(JRC_TMF_transitions.lte(12)), 10) 
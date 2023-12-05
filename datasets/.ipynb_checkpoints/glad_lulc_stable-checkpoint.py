import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

ee.Initialize()

glad_landcover_2020 = ee.Image('projects/glad/GLCLU2020/v2/LCLUC_2020');
landmask = ee.Image("projects/glad/OceanMask").lte(1)
glad_landcover_2020 = glad_landcover_2020.updateMask(landmask);

#trees
glad_landcover_2020_main = glad_landcover_2020.where(
    (glad_landcover_2020.gte(25)).And(glad_landcover_2020.lte(48)), 25) #stable trees

glad_landcover_2020_main = glad_landcover_2020_main.where(
    (glad_landcover_2020.gte(125)).And(glad_landcover_2020.lte(148)), 25) #stable trees

# glad_landcover_2020_main = glad_landcover_2020_main.where(
    # (glad_landcover_2020.gte(49)).And(glad_landcover_2020.lte(72)), 49)

# glad_landcover_2020_main = glad_landcover_2020_main.where(
#     (glad_landcover_2020.gte(149)).And(glad_landcover_2020.lte(172)), 49)

# glad_landcover_2020_main = glad_landcover_2020_main.where(
#     (glad_landcover_2020.gte(73)).And(glad_landcover_2020.lte(96)), 73)

# glad_landcover_2020_main = glad_landcover_2020_main.where(
#     (glad_landcover_2020.gte(173)).And(glad_landcover_2020.lte(196)), 73)

glad_stable_tree_2020 = glad_landcover_2020_main.eq(25) #binary stable trees (TO CHECK - height definititions)

glad_stable_tree_2020 = area_stats.set_scale_property_from_image(
    glad_stable_tree_2020,glad_landcover_2020)

glad_stable_tree_2020_area_hectares = area_stats.binary_to_area_hectares(
    glad_stable_tree_2020)

glad_stable_tree_2020_area_hectares = area_stats.set_scale_property_from_image(
    glad_stable_tree_2020_area_hectares,glad_landcover_2020,debug=True)
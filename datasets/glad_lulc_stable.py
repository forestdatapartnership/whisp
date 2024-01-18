import os
import ee

# dataset_id = 4

def glad_lulc_stable_prep(dataset_id):
    import modules.area_stats as area_stats

    glad_landcover_2020 = ee.Image('projects/glad/GLCLU2020/v2/LCLUC_2020').updateMask(ee.Image("projects/glad/OceanMask").lte(1));

    #trees
    glad_landcover_2020_main = glad_landcover_2020.where(
        (glad_landcover_2020.gte(27)).And(glad_landcover_2020.lte(48)), 27) #stable trees over 5m

    glad_landcover_2020_main = glad_landcover_2020_main.where(
        (glad_landcover_2020.gte(127)).And(glad_landcover_2020.lte(148)), 27) #stable trees over 5m

    glad_stable_tree_2020 = glad_landcover_2020_main.eq(27) #binary map, for stable trees over 5m

    glad_stable_tree_2020 = area_stats.set_scale_property_from_image(
        glad_stable_tree_2020,glad_landcover_2020,debug=True)

    output_image = glad_stable_tree_2020
    
    return output_image.set("dataset_id",dataset_id)
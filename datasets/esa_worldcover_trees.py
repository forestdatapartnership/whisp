import os
import ee

# dataset_id = 19

def esa_worldcover_trees_prep(dataset_id):
    
    import modules.area_stats as area_stats
    
    esa_worldcover_2020_raw = ee.Image("ESA/WorldCover/v100/2020");
    
    esa_worldcover_trees_2020 = esa_worldcover_2020_raw.eq(95).Or(esa_worldcover_2020_raw.eq(10)) #get trees and mnangroves
    
    esa_worldcover_trees_2020 = area_stats.set_scale_property_from_image(
        esa_worldcover_trees_2020,esa_worldcover_2020_raw,0,debug=True)

    output_image = esa_worldcover_trees_2020
    
    return output_image.set("dataset_id",dataset_id)

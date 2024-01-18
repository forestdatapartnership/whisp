import os
import ee

# dataset_id = 12

def eth_kalischek_cocoa_prep(dataset_id):
    import modules.area_stats as area_stats
    #keep as function incase some thresholds are used/recommended instead of 0.65 found online
    # cocoa_map_kalischek = ee.ImageCollection('projects/ee-nk-cocoa/assets/cocoa_map');

    cocoa_map_kalischek_threshold = ee.Image('projects/ee-nk-cocoa/assets/cocoa_map_threshold_065');

    cocoa_map_kalischek_threshold = area_stats.set_scale_property_from_image(
        cocoa_map_kalischek_threshold,cocoa_map_kalischek_threshold,0,debug=True)

    output_image  = cocoa_map_kalischek_threshold
    
    return output_image.set("dataset_id",dataset_id)
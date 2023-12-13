import os
import ee

# import modules.image_prep as image_prep
# import modules.area_stats as area_stats

# dataset_id=17

ee.Initialize()

def jrc_gfc_2020_prep(dataset_id):
    
    import modules.area_stats as area_stats
    
    JRC_GFC_2020_raw = ee.ImageCollection("JRC/GFC2020/V1");

    JRC_GFC_2020 = JRC_GFC_2020_raw.mosaic();

    JRC_GFC_2020 = area_stats.set_scale_property_from_image(
        JRC_GFC_2020,JRC_GFC_2020_raw.first(),debug=True)

    output_image = JRC_GFC_2020
    
    return output_image.set("dataset_id",dataset_id)
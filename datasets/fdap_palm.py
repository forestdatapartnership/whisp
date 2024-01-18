import os
import ee

dataset_id= 11

def fdap_palm_prep(dataset_id):
    
    import modules.area_stats as area_stats
    
    FDaP_palm_2020_model_raw = ee.ImageCollection("projects/forestdatapartnership/assets/palm/palm_2020_model_20231026");
    
    FDaP_palm_2020_model = FDaP_palm_2020_model_raw.mosaic().gt(0.9).selfMask()

    FDaP_palm_2020_model = area_stats.set_scale_property_from_image(
        FDaP_palm_2020_model,FDaP_palm_2020_model_raw.first(),0,debug=True)
    
    output_image = FDaP_palm_2020_model
    
    return output_image.set("dataset_id",dataset_id)
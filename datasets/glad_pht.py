import os
import ee

# dataset_id = 6

def glad_pht_prep(dataset_id):
    
    import modules.area_stats as area_stats
    
    primary_HT_forests_2001_raw = ee.ImageCollection('UMD/GLAD/PRIMARY_HUMID_TROPICAL_FORESTS/v1')

    #get band and mosaic
    primary_HT_forests_2001 = primary_HT_forests_2001_raw.select("Primary_HT_forests").mosaic().selfMask();

    from datasets.glad_gfc_raw import gfc # so use a common asset defined once
    
    gfc_loss_2001_2020 = gfc.select(['lossyear']).lte(20) # get loss pixels since 2000 and up to and including 2020\

    #remove GFC loss pixels from 2001-2020 (as previous technique with GFC, above)
    primary_HT_forests_2020 = primary_HT_forests_2001.where(gfc_loss_2001_2020.eq(1),0)#.selfMask()

    primary_HT_forests_2020 = area_stats.set_scale_property_from_image(
        primary_HT_forests_2020,primary_HT_forests_2001_raw.first(),0,debug=True)

    output_image = primary_HT_forests_2020
    
    return output_image.set("dataset_id",dataset_id)


import os
import ee

dataset_id = 1

def glad_gfc_10pc_prep(dataset_id):
    
    import modules.area_stats as area_stats
    # gfc = ee.Image("UMD/hansen/global_forest_change_2022_v1_10")
    
    from datasets.glad_gfc_raw import gfc # so use a common asset
    
    gfc_treecover_2000 = gfc.select(['treecover2000']) #get tree cover in 2000

    gfc_loss_2001_2020 = gfc.select(['lossyear']).lte(20) # get loss pixels since 2000 and up to and including 2020

    gfc_treecover_2020 = gfc_treecover_2000.where(gfc_loss_2001_2020.eq(1),0) # remove loss from original tree cover

    gfc_treecover_2020_binary= gfc_treecover_2020.gt(10) #FAO 10% definition...
    
    gfc_treecover_2020_binary = area_stats.set_scale_property_from_image(gfc_treecover_2020_binary,gfc,debug=True)
    output_image = gfc_treecover_2020_binary
    
    return output_image.set("dataset_id",dataset_id)
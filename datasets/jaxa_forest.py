import os
import ee

# dataset_id= 3

def jaxa_forest_prep(dataset_id):
    
    import modules.area_stats as area_stats
    
    JAXA_forestNonForest_raw = ee.ImageCollection('JAXA/ALOS/PALSAR/YEARLY/FNF4');

    JAXA_forestNonForest_2020 =  JAXA_forestNonForest_raw.filterDate('2020-01-01', '2020-12-31').select('fnf').mosaic();

    #select all trees (i.e. both dense and non-dense forest classes)
    JAXA_forestNonForest_2020_binary = JAXA_forestNonForest_2020.lte(2)

    JAXA_forestNonForest_2020_binary = area_stats.set_scale_property_from_image(
        JAXA_forestNonForest_2020_binary,JAXA_forestNonForest_raw.first(),0,debug=True)
    
    output_image = JAXA_forestNonForest_2020_binary
    
    return output_image.set("dataset_id",dataset_id)
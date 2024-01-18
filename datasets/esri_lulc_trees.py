import os
import ee

# dataset_id = 2

def esri_lulc_trees_prep(dataset_id):
    import modules.area_stats as area_stats
    esri_lulc10 = ee.ImageCollection("projects/sat-io/open-datasets/landcover/ESRI_Global-LULC_10m_TS");

    esri_lulc10_2020 = esri_lulc10.filterDate('2020-01-01','2020-12-31').map(
        lambda image:
        image.remap([1,2,4,5,7,8,9,10,11],
                    [1,2,3,4,5,6,7,8,9])).mosaic()

    esri_trees_2020 = esri_lulc10_2020.eq(2) #get trees    NB check flooded veg class

    esri_trees_2020 = area_stats.set_scale_property_from_image(
        esri_trees_2020,esri_lulc10.first(),0,debug=True)

    output_image = esri_trees_2020
    
    return output_image.set("dataset_id",dataset_id)
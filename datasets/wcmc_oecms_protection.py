import os
import ee

# dataset_id = 14

def wcmc_oecms_protection_prep(dataset_id, template_image):
    
    import modules.area_stats as area_stats
    
    from datasets.reproject_to_template import reproject_to_template
    
    OECM_poly_1 = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/WDPA_OECM_polygons1")
    OECM_poly_2 = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/WDPA_OECM_polygons2")
    OECM_poly_3 = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/WDPA_OECM_polygons3")

    OECM_2023_poly_raw = ee.FeatureCollection([OECM_poly_1,OECM_poly_2,OECM_poly_3]).flatten()

    # OECM_2023_poly_raw = asset
    
    OECM_2023_poly = OECM_2023_poly_raw.filter(ee.Filter.eq("PA_DEF","0")) #collating uploaded shapefiles and filtering for only OECMs 

    OECM_2023_poly.limit(10)

    OECM_2023_binary = OECM_2023_poly.reduceToImage(["WDPAID"],ee.Reducer.count()).gt(0).selfMask() #convert to image

    #reproject based on template (tyically gfc data - approx 30m res)
    OECM_2023_binary_reproj = reproject_to_template(OECM_2023_binary,template_image)
    
    OECM_2023_binary_reproj = area_stats.set_scale_property_from_image(OECM_2023_binary_reproj,template_image,0,debug=True)

    output_image = OECM_2023_binary_reproj
    
    return output_image.set("dataset_id",dataset_id)
import os
import ee

# dataset_id = 16

def fao_gaul_countries_prep(dataset_id,template_image):
    
    import modules.area_stats as area_stats
    
    from datasets.reproject_to_template import reproject_to_template
    
    
    # path_lookup_country_codes_to_names = "parameters/lookup_gaul_country_codes_to_names.csv"

    gaul_boundaries_poly = ee.FeatureCollection("FAO/GAUL/2015/level0");

    gaul_code_column = "ADM0_CODE"

    template = ee.Image("UMD/hansen/global_forest_change_2022_v1_10");

    gaul_boundaries_adm0_code = gaul_boundaries_poly.reduceToImage([gaul_code_column],ee.Reducer.mode())  #make into image with the admn0 country code as the value

    #reproject based on template (tyically gfc data - approx 30m res)
    gaul_boundaries_adm0_code_reproj = reproject_to_template(gaul_boundaries_adm0_code,template_image)

    gaul_boundaries_adm0_code_reproj = area_stats.set_scale_property_from_image(
        gaul_boundaries_adm0_code_reproj,template_image,0,debug=True)

    output_image = gaul_boundaries_adm0_code_reproj
    
    return output_image.set("dataset_id",dataset_id)
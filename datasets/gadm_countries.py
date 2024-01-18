import os
import ee

# dataset_id = 18

def gadm_countries_prep(dataset_id,template_image):
    
    import modules.area_stats as area_stats
    
    from datasets.reproject_to_template import reproject_to_template
    
    # path_lookup_country_codes_to_names = "parameters/lookup_gadm_country_codes_to_names.csv"

    gadm_boundaries_poly = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/gadm_41_level_1");

    gadm_code_column = "fid"

    template = ee.Image("UMD/hansen/global_forest_change_2022_v1_10");

    gadm_boundaries_adm0_code = gadm_boundaries_poly.reduceToImage([gadm_code_column],ee.Reducer.mode())  #make into image with the admn0 country code as the value

    #reproject based on template (tyically gfc data - approx 30m res)
    gadm_boundaries_adm0_code_reproj = reproject_to_template(gadm_boundaries_adm0_code,template_image)

    gadm_boundaries_adm0_code_reproj = area_stats.set_scale_property_from_image(
        gadm_boundaries_adm0_code_reproj,template_image,0,debug=True)

    output_image = gadm_boundaries_adm0_code_reproj
    
    return output_image.set("dataset_id",dataset_id)
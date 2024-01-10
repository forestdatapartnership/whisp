import os
import ee

# import modules.image_prep as image_prep
# import modules.area_stats as area_stats

# from datasets.template_images import template_image_1

# dataset_id= 15 

def birdlife_kbas_biodiversity_prep(dataset_id,template_image):
    ##uploaded - may need rights
    import modules.area_stats as area_stats
    
    from datasets.reproject_to_template import reproject_to_template

    ##uploaded data - need rights. DO NOT SHARE (ion progress)
    
    kbas_2023_poly = ee.FeatureCollection("projects/ee-andyarnellgee/assets/p0004_commodity_mapper_support/raw/KBAsGlobal_2023_March_01_POL");

    kba_2023_overlap = kbas_2023_poly.reduceToImage(['SitRecID'],'count').selfMask()  #make into raster - remove mask if want 0s

    kba_2023_binary = kba_2023_overlap.gte(0)
    
    kba_2023_binary_reproj = reproject_to_template(kba_2023_binary,template_image)
    
    kba_2023_binary_reproj = area_stats.set_scale_property_from_image(
         kba_2023_binary_reproj,template_image,0,debug=True)
    
    output_image = kba_2023_binary_reproj
    
    return output_image.set("dataset_id",dataset_id)


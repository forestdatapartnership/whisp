import os
import ee

# import modules.image_prep as image_prep
# import modules.area_stats as area_stats

# from datasets.jrc_tmf_reclassify import JRC_TMF_transitions_remap,JRC_TMF_transitions_raw

ee.Initialize()

dataset_id = 9


def jrc_tmf_plantations_prep(dataset_id):
    
    import modules.area_stats as area_stats

#     from datasets.jrc_tmf_reclassifyimport jrc_tmf_prep
      
#     ##recoded values for tmf: 1) undisturbed forest, 2) disturbed forest and 3) plantation           
#     JRC_TMF_transitions_remap = jrc_tmf_prep(ee.ImageCollection('projects/JRC/TMF/v1_2021/TransitionMap_Subtypes'))

    from datasets.jrc_tmf_reclassify import JRC_TMF_transitions_remap,JRC_TMF_transitions_raw
    
    JRC_TMF_plantation  = JRC_TMF_transitions_remap.eq(3) #select plantation

    JRC_TMF_plantation = area_stats.set_scale_property_from_image(JRC_TMF_plantation,
                                                                      JRC_TMF_transitions_raw.first(),debug=True)
    output_image = JRC_TMF_plantation
    
    return output_image.set("dataset_id",dataset_id)
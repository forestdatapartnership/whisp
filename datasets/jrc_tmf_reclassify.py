import os
import ee

# import modules.image_prep as image_prep
# import modules.area_stats as area_stats

# from parameters.config_image_prep import 
#     path_lookup_recoding_JRC_TMF_product,
#     from_class_column_name_JRC_TMF,
#     to_class_column_name_JRC_TMF

ee.Initialize()

# def jrc_tmf_prep(asset):
    
import modules.image_prep as image_prep # get recode function - maybe a better way to do this

from parameters.config_lookups import path_lookup_recoding_JRC_TMF_product # maybe a better way to do this

## recoding/reclassifying JRC tropical moist forest classes to EUDR classes 
## i.e. representing undisturbed, disturbed and planatations
JRC_TMF_transitions_raw = ee.ImageCollection('projects/JRC/TMF/v1_2021/TransitionMap_Subtypes') # raw data
 
JRC_TMF_transitions = JRC_TMF_transitions_raw.mosaic() ### NB check why 2021?  
   
#from (original) class code
from_class_column_name_JRC_TMF ='JRC_TMF_original_class_value' # add into parameters?

#to (reclassified) class code
to_class_column_name_JRC_TMF = 'Remap_EUDR_value' # add into parameters?

    #remap to 3 classes relevant to EUDR 
JRC_TMF_transitions_remap = image_prep.remap_image_from_csv_cols(
    image=JRC_TMF_transitions,
    csv_path=path_lookup_recoding_JRC_TMF_product,
    from_col=from_class_column_name_JRC_TMF,
    to_col=to_class_column_name_JRC_TMF,
    default_value=9999);

#     output_image = JRC_TMF_transitions_remap
    
#     return output_image




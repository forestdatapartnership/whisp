#paths for lookup tables
import os
import pandas as pd
from parameters.config_runtime import exclusion_list_dataset_ids, path_lookup_country_codes_to_names, path_lookup_country_codes_to_iso3

### main lookup table
path_lookup_gee_datasets = "parameters/lookup_gee_datasets.csv"

lookup_gee_datasets = pd.read_csv(path_lookup_gee_datasets)

# filter out datasets using an exclusion list 
lookup_gee_datasets = lookup_gee_datasets[~lookup_gee_datasets['dataset_id'].isin(exclusion_list_dataset_ids)]

try: 
    lookup_country_codes_to_names = pd.read_csv(path_lookup_country_codes_to_names)
except:
    print(path_lookup_country_codes_to_names, "does not exist")


try: 
    lookup_country_codes_to_iso3 = pd.read_csv(path_lookup_country_codes_to_iso3)
except:
    print(path_lookup_country_codes_to_iso3, "does not exist")    


#dataset specific LUT (could be shifted)
path_lookup_recoding_jrc_tmf_product = "parameters/lookup_recoding_jrc_tmf_product.csv"

lookup_recoding_jrc_tmf_product = pd.read_csv(path_lookup_recoding_jrc_tmf_product)

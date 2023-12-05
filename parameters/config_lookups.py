#paths for lookup tables
import os
import pandas as pd

path_lookup_country_codes_to_names = "parameters/lookup_GAUL_country_codes_to_names.csv" 

lookup_country_codes_to_names = pd.read_csv(path_lookup_country_codes_to_names)


path_lookup_gee_datasets = "parameters/lookup_gee_datasets.csv"

lookup_gee_datasets = pd.read_csv(path_lookup_gee_datasets)


path_lookup_recoding_JRC_TMF_product = "parameters/lookup_recoding_JRC_TMF_product.csv"

lookup_recoding_JRC_TMF_product = pd.read_csv(path_lookup_recoding_JRC_TMF_product)



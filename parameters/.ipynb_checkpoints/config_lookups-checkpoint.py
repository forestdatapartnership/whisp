#paths for lookup tables
import os
import pandas as pd

path_lookup_country_codes_to_names = "parameters/lookup_GAUL_country_codes_to_names.csv" 

lookup_country_codes_to_names = pd.read_csv(path_lookup_country_codes_to_names)


path_lookup_country_codes_to_ISO3 = "parameters/lookup_GAUL_country_codes_to_ISO3.csv" 
lookup_country_codes_to_ISO3 = pd.read_csv(path_lookup_country_codes_to_ISO3) # NB TEMP fix onkly: some missing - small island states and source not ideal (had to add in GAUk code for South Sudan!)
# CUW		CuraÃ§ao
# BLM		Saint BarthÃ©lemy
# MAF		Saint Martin (French Part)
# 		Sark
# SXM		Sint Maarten (Dutch part)
# UMI		United States Minor Outlying Islands



path_lookup_gee_datasets = "parameters/lookup_gee_datasets.csv"

lookup_gee_datasets = pd.read_csv(path_lookup_gee_datasets)




path_lookup_recoding_JRC_TMF_product = "parameters/lookup_recoding_JRC_TMF_product.csv"

lookup_recoding_JRC_TMF_product = pd.read_csv(path_lookup_recoding_JRC_TMF_product)



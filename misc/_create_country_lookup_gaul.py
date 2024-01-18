import os
import ee

from modules.gee_initialize import initialize_ee

initialize_ee()

import pandas as pd
    
list_GAUL_boundaries_poly_admn0_code = GAUL_boundaries_poly.aggregate_array("ADM0_CODE").distinct().getInfo()

list_GAUL_boundaries_poly_country_name = GAUL_boundaries_poly.aggregate_array("ADM0_NAME").distinct().getInfo()

GAUL_lookup_table = pd.DataFrame({"ADM0_CODE":list_GAUL_boundaries_poly_admn0_code,"ADM0_NAME":list_GAUL_boundaries_poly_country_name}) #make dataframe from columns in GAUL

GAUL_lookup_table.rename(columns={"ADM0_NAME":"Country"},inplace=True) # rename column

GAUL_lookup_table.to_csv(path_or_buf="parameters/lookup_GAUL_country_codes_to_names.csv",header=True,index=False) # save lookup table as CSV

if debug: GAUL_lookup_table
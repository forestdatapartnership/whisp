#temp code - ideally use a lookup table from csv or similar

import os
import ee

from parameters.config_lookups import lookup_gee_datasets

from datasets import latest_radd_alert_confirmed_recent_area_hectares,protected_areas_WDPA_area_hectares,OECM_2023_area_hectares,kba_2023_area_hectares

ee.Initialize()

#deforestation alerts
# set property so run stats for a buffer around site; 
# and show presence only as output 
latest_radd_alert_confirmed_recent_area_hectares = latest_radd_alert_confirmed_recent_area_hectares.setMulti(
    {"alerts_buffer":1,"presence_only_flag":1})

#important sites: 1) protected areas and 2) KBAs (likely future protectred areas) 
# show presence only as output 
protected_areas_WDPA_area_hectares = protected_areas_WDPA_area_hectares.set("presence_only_flag",1)

OECM_2023_area_hectares = OECM_2023_area_hectares.set("presence_only_flag",1)

kba_2023_area_hectares = kba_2023_area_hectares.set("presence_only_flag",1)

print("Processed")
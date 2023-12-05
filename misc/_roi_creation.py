import os
import ee

from modules.json_to_ee import json_to_feature_with_id
from parameters.config_output_naming import geo_id_column

ee.Initialize()

roi = json_to_feature_with_id(poly_json,geo_id,geo_id_column) 
roi = ee.FeatureCollection(roi) # currently set up for a feature collection
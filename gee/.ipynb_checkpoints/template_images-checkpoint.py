import os
import ee

ee.Initialize()

gfc = ee.Image("UMD/hansen/global_forest_change_2022_v1_10")
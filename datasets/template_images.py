import os
import ee

ee.Initialize()

from datasets.glad_gfc_raw import gfc 

template_image_1 = gfc
import os
import ee

import modules.image_prep as image_prep
import modules.area_stats as area_stats

from datasets.jrc_tmf_prep import JRC_TMF_transitions_remap,JRC_TMF_transitions_raw

ee.Initialize()

dataset_id = 7

## 1) undisturbed forest, 2) disturbed forest and 3) plantation

JRC_TMF_disturbed_2020 = JRC_TMF_transitions_remap.eq(2)

JRC_TMF_disturbed_2020_area_hectares  = area_stats.binary_to_area_hectares(JRC_TMF_disturbed_2020)

JRC_TMF_disturbed_2020_area_hectares = area_stats.set_scale_property_from_image(JRC_TMF_disturbed_2020_area_hectares,
                                                        JRC_TMF_transitions_raw.first(),debug=True).set("dataset_id",dataset_id)
                                     
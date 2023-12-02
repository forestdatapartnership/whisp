# import os 
# import ee
# ee.Initialize()

# JAXA_forestNonForest_raw = ee.ImageCollection('JAXA/ALOS/PALSAR/YEARLY/FNF4');

# JAXA_forestNonForest_2020 =  JAXA_forestNonForest_raw.filterDate('2020-01-01', '2020-12-31').select('fnf').mosaic();

# #select all trees (i.e. both dense and non-dense forest classes)
# JAXA_forestNonForest_2020_binary = JAXA_forestNonForest_2020.lte(2)

#define buffer distance (m)
local_alerts_buffer_radius = 2000 

# Define how many days back for alerts
how_many_days_back = -(365*2)  # must be negative



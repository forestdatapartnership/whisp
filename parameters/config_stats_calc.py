import ee

## reducer choice for zonal statistics
reducer_choice = ee.Reducer.sum().combine(  #main stats based on area of pixel
  reducer2=ee.Reducer.count(),sharedInputs=True).combine(
    reducer2=ee.Reducer.mode(), sharedInputs=True) ## mode for country allocation (majority pixel count) 

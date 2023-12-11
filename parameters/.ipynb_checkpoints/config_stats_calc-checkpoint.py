
# stats by pixel_area
to_pixel_area = True

## reducer choice for zonal statistics
reducer_choice = ee.Reducer.sum().combine(  #main stats based on area of pixel
  reducer2=ee.Reducer.count(),sharedInputs=True).combine(
    reducer2=ee.Reducer.mode(), sharedInputs=True) ##used for country allocation (majority pixel count on country code raster)

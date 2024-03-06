feature_collection = fc

# Convert feature collection to a list
feature_list = feature_collection.toList(feature_collection.size())

# Map each feature to its geometry coordinates
coordinates_list = feature_list.map(lambda feature: ee.Feature(feature).geometry().coordinates())
coordinates_list

# first_poly_coords = coordinates_list.get(0)

wkt_list=[]

coordinates_list.get(1)
for i in range(coordinates_list.size().getInfo()):
    coords = (coordinates_list.get(i))
    coords_client = coords.getInfo()[0]
    wkt = coordinates_to_wkt(coords_client)
    wkt_list.append(wkt)

print (wkt_list)




geo_id_list =[]

for i in range(len(wkt_list)):

    geo_id = wkt_to_geo_id(wkt_list[i],
    token,
    session,
    asset_registry_base,
    debug=False)
    
    geo_id_list.append(geo_id)

print(geo_id_list)



## slows things down (unless use client side maybe)
# if ee.Algorithms.ObjectType(roi).getInfo() == "FeatureCollection": 
#     fc_stats_combined = reduceStatsIC(roi,images_IC)
# else:
#     fc_stats_combined = reduceStatsIC(ee.FeatureCollection([roi]),images_IC)    

#fc_stats_combined



# def add_area_km2_property_to_feature_collection(fc,geometry_area_column):
#     roi = roi.map(add_area_km2_property_to_feature)

# def add_area_km2_property_to_feature_collection(fc,geometry_area_column):
#     def add_area_km2_property_to_feature (feature):
#         feature = feature.set(geometry_area_column,feature.area().divide(1e6))#add area
#         return feature
#     outFC = fc.map(add_area_km2_property_to_feature)
#     return outFC


def reduceStatsIC (featureCollection,imageCollection,reducer_choice):
    "Calculating summary statistics for an image where it falls in a feature's bounds. This is then repoeated for others in an image collection""" 
    roi = featureCollection
    def reduceStats (image):
        scale=image.get("scale")

        fc = ee.FeatureCollection(image.reduceRegions(collection=roi,
                                                      reducer=reducer_choice,
                                                      scale=scale))

        fc = fc.map(lambda feature: feature.set("dataset_name",image.get("system:index")))
        return fc
    fc_out = imageCollection.map(reduceStats).flatten()
    return fc_out



## kernels insrtead of buffer
latest_radd_alert_confirmed_recent_area_km2_kernel = latest_radd_alert_confirmed_recent_area_km2.float().unmask().reduceNeighborhood(
    reducer=ee.Reducer.sum(),
  kernel=ee.Kernel.circle(radius= ee.Number(50), 
                            units= 'pixels', 
                            normalize= False)).reproject(radd.first().select(0).projection())

latest_radd_alert_confirmed_recent_area_km2_kernel = area_stats.set_scale_property_from_image(latest_radd_alert_confirmed_recent_area_km2_kernel,radd.first(),0,verbose=True)


Map.addLayer(latest_radd_alert_confirmed_recent_area_km2_kernel,
    {'min': 0, 'max': 20, 'palette': ['blue', 'orange']},
    'latest_radd_alert_confirmed_recent_area_km2_kernel - recent (i.e., filtered by date)', 1, 1)

latest_radd_alert_confirmed_recent_area_km2_kernel = latest_radd_alert_confirmed_recent_area_km2_kernel.setMulti(
    {"presence_only_flag":1})
import os
import ee

from modules.gee_initialize import initialize_ee

initialize_ee()

##### Alternative feature collection: create random polygons
####select random points inside administrative boundaries and buffer them 

# def create_random_points_in_polys(feature): #to tidy
#     """ creates random points within either a polygon or a feature collection NB relies upon some globals being set currently"""
#     return ee.FeatureCollection.randomPoints(region = feature.geometry(max_error), points = number_of_points, seed=seed, maxError=10)

# admin_boundaries = ee.FeatureCollection("FAO/GAUL_SIMPLIFIED_500m/2015/level2").filter(
#     ee.Filter.inList("ADM0_NAME",["Indonesia","Malaysia","Ghana"]))##.filter(ee.Filter.lt("Shape_Area",1000))

# random_collection = admin_boundaries.randomColumn(seed = seed).sort('random').limit(number_of_boundaries)

# if points_per_admin_boundary:
#     random_points = random_collection.map(create_random_points_in_polys).flatten()
# else:
#     random_points = create_random_points_in_polys(random_collection)

# random_buffers = random_points.map(lambda feature: feature.buffer(buffer_distance_meters,100)) ##buffer by distance in meters

# roi= random_buffers.map(lambda feature: feature.set(geo_id_column,(feature.get("system:index"))))
# # roi= random_buffers.map(set_geo_id_from_system_index) ## add surrogate "geo_id" for formatting

# #checks
# if debug: print ("number of admin regions",random_collection.size().getInfo())
# if debug: print ("number of countries",random_collection.aggregate_array("ADM0_NAME").distinct().getInfo())
# if debug: print ("number of buffers created",random_buffers.size().getInfo())
#     # geemap.ee_to_pandas(roi)###create random points and buffer them
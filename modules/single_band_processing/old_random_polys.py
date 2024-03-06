import ee

def create_random_points_in_polys(feature): #to tidy
    """ creates random points within either a polygon or a feature collection NB relies upon some globals being set currently"""
    return ee.FeatureCollection.randomPoints(region = feature.geometry(max_error), points = number_of_points, seed=seed, maxError=10)

def set_geo_id_from_system_index_for_fc (feature_collection,geo_id_column):
    def set_geo_id_from_system_index (feature):
        """sets geo_id column based on 'system:index' property"""
        return feature.set(geo_id_column,(feature.get("system:index")))
    feature_collection_w_geo_id = feature_collection.map(set_geo_id_from_system_index)
    return feature_collection_w_geo_id

def set_geo_id_from_system_index (feature):
    """sets geo_id column based on 'system:index' property"""
    return feature.set(geo_id_column,(feature.get("system:index")))
 



import ee
import pandas as pd


def remove_geometry_from_feature_collection(feature_collection):
    """ Define the function to remove geometry from features in a feature collection"""
    # Function to remove geometry from features
    def remove_geometry(feature):
        # Remove the geometry property
        feature = feature.setGeometry(None)
        return feature

    # Apply the function to remove geometry to the feature collection
    feature_collection_no_geometry = feature_collection.map(remove_geometry)
    return feature_collection_no_geometry


    
def collection_properties_to_df(collection, property_selection=None):
    """creates a pandas dataframe from feature collection properties. NB SLOW but functions >5000 rows (unlike geemap_to_df)"""
    nested_list = []
    
    if property_selection is None:
        collection_properties_list = collection.first().propertyNames().getInfo()
    else:
        collection_properties_list = property_selection 

    for property in collection_properties_list:
        nested_list.append(collection.aggregate_array(property).getInfo())

    nested_list_transposed = list(map(list, zip(*nested_list)))
    
    return pd.DataFrame(data=nested_list_transposed, columns=collection_properties_list)



# Compute centroids of each polygon
def get_centroid(feature, geo_id_column="Geo_id"):
    keepProperties = [geo_id_column];
    # Get the centroid of the feature's geometry.
    centroid = feature.geometry().centroid(1);
    #Return a new Feature, copying properties from the old Feature.
    return ee.Feature(centroid).copyProperties(feature, keepProperties);



def buffer_point_to_required_area(feature,area,area_unit):
    """buffers feature to get a given area (needs math library); area unit in 'ha' or 'km2' (the default)"""
    area = feature.get('REP_AREA')
    
    # buffer_size = get_radius_m_to_buffer_for_given_area(area,"km2")# should work but untested in this function
    
    buffer_size = (ee.Number(feature.get('REP_AREA')).divide(math.pi)).sqrt().multiply(1000) #calculating radius in metres from REP_AREA in km2
    
    return ee.Feature(feature).buffer(buffer_size,1);  ### buffering (incl., max error parameter should be 0m. But put as 1m anyhow - doesn't seem to make too much of a difference for speed)



def get_radius_m_to_buffer_to_required_area(area,area_unit="km2"):
    """gets radius in metres to buffer to get an area (needs math library); area unit ha or km2 (the default)"""
    if area_unit=="km2": unit_fix_factor =1000
    elif area_unit=="ha": unit_fix_factor =100
    radius = ee.Number(area).divide(math.pi).sqrt().multiply(unit_fix_factor)
    return radius

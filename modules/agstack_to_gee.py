import requests
import json
import geojson
import ee
import geopandas as gpd
from modules.area_stats import buffer_point_to_required_area # to handle point features

def geo_id_or_ids_to_feature_collection (all_geo_ids,geo_id_column, session,asset_registry_base,required_area,area_unit,debug=False):
    """creates a feature collection from agstack with single (string) or multiple geo_ids (list) as input. /
    NB feature collection has one feature if single input""" 

    multiple_inputs_boolean=true_if_list_false_if_string(all_geo_ids,debug)
    
    #if list of geo ids, loop over them and make a feature collection else just put as a single feature in a feature collection
    
    if multiple_inputs_boolean==True:
        roi = geo_id_list_to_feature_collection(all_geo_ids,geo_id_column, session,asset_registry_base,required_area,area_unit)    
        if debug: print ("Count of geo ids in list: ", len(all_geo_ids))
            
    elif multiple_inputs_boolean == False: 
        roi = ee.FeatureCollection(geo_id_to_feature(all_geo_ids,geo_id_column, session,asset_registry_base,required_area,area_unit))
        if debug: print ("Geo id input: ", all_geo_ids)
            
    else: 
        print("no ee.Object created: check input format")

    if debug: print ("Count of features in FeatureCollection: ", roi.size().getInfo())
    
    return roi



def true_if_list_false_if_string(python_object,debug=False): 
    if isinstance(python_object, list):
        boolean=True
        if debug: print ("input: list")
    elif isinstance(python_object, str):
        boolean=False
        if debug: print (python_object,"input: string")
    else:
        if debug: print (python_object," must be a single string or list of strings")
    return boolean
    

def geo_id_list_to_feature_collection(list_of_geo_ids,geo_id_column,session,asset_registry_base,required_area,area_unit):
    """Converts a list of geo_ids fron asset registry to a feature collection. "Geo_id" is setas a property for each feature)"""
    out_fc_list = []
    if isinstance(list_of_geo_ids, list):
        for geo_id in list_of_geo_ids:
            feature = geo_id_to_feature(geo_id,geo_id_column,session,asset_registry_base,required_area,area_unit)
            out_fc_list.append(feature)
    else:
        geo_id = list_of_geo_ids
        feature = geo_id_to_feature(geo_id)
        out_fc_list.append(feature)
    return ee.FeatureCollection(out_fc_list)




def geo_id_to_feature(geo_id, geo_id_column, session, asset_registry_base,required_area,area_unit):
    """converts geo_id fron asset registry into a feature with geo_id (or similar) set as a property"""
    
    res = session.get(asset_registry_base + f"/fetch-field/{geo_id}?s2_index=") # s2 indexes. Will need S2 cell token
    
    geo_json = res.json()['Geo JSON']
    
    coordinates = geo_json['geometry']['coordinates']
    
    if check_json_geometry_type(geo_json)=='Polygon':        
        poly_feature = ee.Feature(ee.Geometry.Polygon(coordinates),ee.Dictionary([geo_id_column,geo_id]))
        
    elif check_json_geometry_type(geo_json)=='Point':
        point_feature = ee.Feature(ee.Geometry.Point(coordinates),ee.Dictionary([geo_id_column,geo_id]))
        
        poly_feature = buffer_point_to_required_area(point_feature,required_area,area_unit)
        
    return poly_feature

# def geo_id_to_feature(geo_id, geo_id_column, session, asset_registry_base):
#     """converts geo_id fron asset registry into a feature with geo_id (or similar) set as a property"""
#     res = session.get(asset_registry_base + f"/fetch-field/{geo_id}?s2_index=") # s2 indexes. Will need S2 cell token
#     geo_json = res.json()['Geo JSON']['geometry']['coordinates']  
#     feature = ee.Feature(ee.Geometry.Polygon(geo_json),ee.Dictionary([geo_id_column,geo_id]))
#     return feature
    


def check_json_geometry_type(geojson_obj):
    if geojson_obj['type'] == 'Feature':
        geometry_type = geojson_obj['geometry']['type']
        if geometry_type == 'Point':
            return 'Point'
        elif geometry_type == 'Polygon':
            return 'Polygon'
        else:
            return 'Unknown Geometry Type'
    else:
        return 'Not a Feature'
        
# def json_to_feature_with_id(geo_json,geo_id,geo_id_column):
#     """converts json into a feature with a specified id column"""
#     return ee.Feature(ee.Geometry.Polygon(geo_json),ee.Dictionary([geo_id_column,geo_id]))



# def geo_id_to_json(geo_id,session,asset_registry_base):
#     """converts geo_id from asset registry into a json"""
#     res = session.get(asset_registry_base + f"/fetch-field/{geo_id}?s2_index=") # s2 index are indexes for which we need S2 cell token
#     geo_json = res.json()['Geo JSON']['geometry']['coordinates']
#     return geo_json

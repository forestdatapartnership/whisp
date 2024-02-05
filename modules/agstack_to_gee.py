import requests
import json
import geojson
import ee
import geopandas as gpd
from modules.area_stats import buffer_point_to_required_area # to handle point features


# functions for setting up session based on usr credentials
def start_agstack_session(email,password,user_registry_base,debug=False):
    """using session to store cookies that are persistent"""
    import requests
    session = requests.session()
    session.headers = headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    req_body = {'email': email, 'password': password}
    res = session.post(user_registry_base, json=req_body)
    if debug: print ("Cookies",session.cookies)
    if debug: print ("status code:", res.status_code)
    return session


def get_agstack_token(email, password, asset_registry_base='https://api-ar.agstack.org'):
    """
    Authenticate with the AgStack API and obtain a token.

    Parameters:
    - email: The email for authentication.
    - password: The password for authentication.
    - asset_registry_base: The base URL of the asset registry. Default is the AgStack API base URL.

    Returns:
    - token: The access token obtained after successful authentication.
    """
    # Define the authentication endpoint
    auth_url = f"{asset_registry_base}/login"

    # Make a POST request to authenticate and obtain the token
    response = requests.post(auth_url, json={'email': email, 'password': password})

    # Check if the authentication was successful
    if response.status_code == 200:
        # Extract the token from the response
        token = response.json()['access_token']
        return token
    else:
        print('Authentication failed. Status code:', response.status_code)
        print('Error message:', response.text)
        return None  # Return None to indicate failure



# def register_fc_and_set_geo_id(feature_col,geo_id_column,token,session,asset_registry_base,debug=True):
#     """mapped version of register_feature_and_set_geo_id, wokring on feature collections"""
#     fc_w_geo_id = feature_col.map(lambda feature_col: 
#                              register_feature_and_set_geo_id(
#                                  feature_col,
#                                  geo_id_column,
#                                  token
#                                  session,
#                                  asset_registry_base,
#                                  debug
#                              )
#                             )
#     return fc_w_geo_id


def register_feature_and_set_geo_id(feature,geo_id_column,token,session,asset_registry_base,debug=True):

    # Convert the polygon to WKT using the function
    geo_id = feature_to_geo_id(feature,token,session,asset_registry_base,debug=True)
    
    feature_w_geo_id_property = feature.set(geo_id_column,geo_id)
    return feature_w_geo_id_property
    


def feature_to_geo_id(feature, token=None, session=None, asset_registry_base="https://api-ar.agstack.org", debug=False):
    """
    Registers a field boundary with the ee geometry using the AgStack API.

    Parameters:
    - ee.Geometry(): earth engine geometry 
    - session: Optional parameter. If provided, the function will use the existing session for the request.

    Returns:
    - res: The Geo Id or matched Geo Ids for the registered field boundary.
    """
    wkt = feature_to_wkt(feature)
    
    geo_id = wkt_to_geo_id(wkt, token,session, asset_registry_base, debug)
    
    return geo_id


def geometry_to_geo_id(geometry,
                       token=None,
                       session=None, 
                       asset_registry_base="https://api-ar.agstack.org", 
                       debug=False):
    """
    Registers a field boundary with the ee geometry using the AgStack API.

    Parameters:
    - ee.Geometry(): earth engine geometry 
    - session: Optional parameter. If provided, the function will use the existing session for the request.

    Returns:
    - res: The Geo Id or matched Geo Ids for the registered field boundary.
    """
    
    wkt = geometry_to_wkt(geometry)

    
    geo_id = wkt_to_geo_id(wkt,token, session, asset_registry_base, debug)
    
    return geo_id


def wkt_to_geo_id(wkt, token=None, session=None, asset_registry_base="https://api-ar.agstack.org", debug=False):
    """
    Registers a field boundary with the given WKT using the AgStack API.

    Parameters:
    - wkt: The Well-Known Text (WKT) representation.
    - token: The authentication token (in bytes format) required by the API.
    - session: Optional parameter. If provided, the function will use the existing session for the request.

    Returns:
    - res: The Geo Id or matched Geo Ids for the registered field boundary.
    """

    # Define the request payload
    payload = {
        "wkt": wkt
    }

    # Make the POST request
    url = asset_registry_base + f"/register-field-boundary"
    if debug: print("url", url)
    
    headers = {'Authorization': f'Bearer {token}'} if token else None

    # Use provided session if available, otherwise create a new session
    if session:
        response = session.post(url, json=payload, headers=headers)
        if debug: print("using existing session")
    else:
        if debug: print("using individual request as no existing session set up, to set one up use: agstack_to_gee.start_agstack_session")
        response = requests.post(url, json=payload, headers=headers)

    # Process the response
    if response.status_code == 200:
        print("Field boundary registered successfully!")
        json_response = response.json()
        print("Response:", json_response)
        res = json_response['Geo Id']
    else:
        json_response = response.json()
        res = json_response.get("matched geo ids", None)

        if res: # if already matching use existing
            if debug: print("Warning:", json_response["message"])
            if debug: print("Failed to register field boundary. Status code:", response.status_code)
            if debug: print("Returning existing (registered) geo id for field")
        else:
            print("Failed to register field boundary. Status code:", response.status_code)
            print("Error message:", json_response)

    return res
    
# def wkt_to_geo_id(wkt, session=None,asset_registry_base="https://api-ar.agstack.org", debug=False):
#     """
#     Registers a field boundary with the given WKT using the AgStack API.

#     Parameters:
#     - wkt: The Well-Known Text (WKT) representation.
#     - session: Optional parameter. If provided, the function will use the existing session for the request.

#     Returns:
#     - res: The Geo Id or matched Geo Ids for the registered field boundary.
#     """

#     # Define the request payload
#     payload = {
#         "wkt": wkt
#     }

#     # Make the POST request
#     # url = asset_registry_base + f"/register-field-boundary"
#     url = "https://api-ar.agstack.org/register-field-boundary"

#     if debug: print ("url",url)
#     # Use provided session if available, otherwise create a new session
#     if session:
#         if debug: print ("using existing session")
#         response = session.post(url, json=payload)
#     else:
#         if debug: print ("using individual request as no existing session set up, to set one up use: agstack_to_gee.start_agstack_session")
#         response = requests.post(url, json=payload)

#     # Process the response
#     if response.status_code == 200:
#         print("Field boundary registered successfully!")
#         json = response.json()
#         print("Response:", json)
#         res = json['Geo Id']
#     else:
#         json = response.json()
#         res = json.get("matched geo ids", None)

#         if res: # if already matching use existing
#             if debug: print("Warning:", json["message"])
#             if debug: print("Failed to register field boundary. Status code:", response.status_code)
#             if debug: print("Returning existing (registered) geo id for field")
#         else:
#             print("Failed to register field boundary. Status code:", response.status_code)
#             print("Error message:", json)

#     return res


def feature_to_wkt(feature):
    """
    Convert an ee.Feature with a geometry to a WKT representation.

    Parameters:
    - feature: An ee.Feature with a geometry.

    Returns:
    - wkt: The WKT representation of the geometry.
    """
    
    # Extract the geometry of the feature (polygon)
    geometry = feature.geometry()

    wkt = geometry_to_wkt(geometry)
    
    return wkt

def geometry_to_wkt(geometry):
    """
    Convert an ee.Feature with a polygon geometry to a WKT representation.

    Parameters:
    - feature: An ee.Feature with a polygon geometry.

    Returns:
    - wkt: The WKT representation of the polygon geometry.
    """
     
    # Get the coordinates of the polygon as a nested list
    coordinates = geometry.coordinates().getInfo()[0]

    # Construct the WKT string
    wkt = 'POLYGON((' + ', '.join([f'{lon} {lat}' for lon, lat in coordinates]) + '))'

    return wkt
    

def geo_id_or_ids_to_feature_collection (all_geo_ids,
                                         geo_id_column, 
                                         session,asset_registry_base="https://api-ar.agstack.org",
                                         required_area=4,
                                         area_unit="ha",
                                         debug=False):
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
    

def geo_id_list_to_feature_collection(list_of_geo_ids,
                                      geo_id_column,
                                      session,
                                      asset_registry_base="https://api-ar.agstack.org",
                                      required_area=4,
                                      area_unit="ha"):
    """Converts a list of geo_ids fron asset registry to a feature collection. "Geo_id" is setas a property for each feature)"""
    out_fc_list = []
    if isinstance(list_of_geo_ids, list):
        for geo_id in list_of_geo_ids:
            feature = geo_id_to_feature(geo_id,geo_id_column,session,asset_registry_base,required_area=4,area_unit="ha")
            out_fc_list.append(feature)
    else:
        geo_id = list_of_geo_ids
        feature = geo_id_to_feature(geo_id)
        out_fc_list.append(feature)
    return ee.FeatureCollection(out_fc_list)




def geo_id_to_feature(geo_id,
                      geo_id_column,
                      session,
                      asset_registry_base="https://api-ar.agstack.org",
                      required_area=4,
                      area_unit="ha"):
    """converts geo_id fron asset registry into a feature with geo_id (or similar) set as a property"""
    
    res = session.get(asset_registry_base + f"/fetch-field/{geo_id}?s2_index=") # s2 indexes. Will need S2 cell token
    
    geo_json = res.json()['Geo JSON']
    
    coordinates = geo_json['geometry']['coordinates']
    
    if check_json_geometry_type(geo_json)=='Polygon':        
        feature = ee.Feature(ee.Geometry.Polygon(coordinates),ee.Dictionary([geo_id_column,geo_id]))
        
    elif check_json_geometry_type(geo_json)=='Point':
        point_feature = ee.Feature(ee.Geometry.Point(coordinates),ee.Dictionary([geo_id_column,geo_id]))
        
        feature = point_feature
        
        if debug: print("Point input")
        # feature = buffer_point_to_required_area(point_feature,required_area,area_unit) //
        # if debug: print("Buffering points into polygons of required area")
    
    return feature

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

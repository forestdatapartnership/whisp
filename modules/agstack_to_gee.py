import requests
import json
import geojson
import ee
import geemap
import os 
import geopandas as gpd
import shutil
from datetime import datetime
# from modules.utils import buffer_point_to_required_area # to handle point features

# A lot of these could be done with decorators instead of building up functions etc

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


def register_fc_and_set_geo_id(feature_col, geo_id_column, token, session, asset_registry_base, debug=False):
    """Version for feature collection of register_feature_and_set_geo_id"""
    # Initialize an empty list to store features
    feature_list = []

    # Convert the FeatureCollection to a list
    feature_collection_list = feature_col.toList(feature_col.size())
    
    total_iterations = feature_collection_list.size().getInfo()
    
    # loop over each feature in the list
    for i in range(total_iterations):
        feature = ee.Feature(feature_collection_list.get(i))
        
        # Apply register_feature_and_set_geo_id function to each feature
        feature_with_geo_id = register_feature_and_set_geo_id(
            feature,
            geo_id_column,
            token,
            session,
            asset_registry_base,
            debug
        )
    
        # Print the progress
        if debug: 
            # Calculate progress percentage
            progress = (i + 1) / total_iterations * 100

            print(f"Progress: {progress:.2f}% ({i + 1}/{total_iterations})", end='\r')

        # Append the feature to the list
        feature_list.append(feature_with_geo_id)

    # Create a new FeatureCollection from the list of features
    fc_w_geo_id = ee.FeatureCollection(feature_list)

    return fc_w_geo_id


def register_feature_and_set_geo_id(feature,geo_id_column,token,session,asset_registry_base,debug=True):
    """Registers a field boundary with the ee geometry using the AgStack API"""
    geo_id = feature_to_geo_id(feature,token,session,asset_registry_base,debug)
    
    feature_w_geo_id_property = feature.set(geo_id_column,geo_id)
    return feature_w_geo_id_property
    
# def register_feature_and_append_to_csv(feature,geo_id_column,csv_path,csv_name,token,session,asset_registry_base,debug=True):
#     """Registers a field boundary with the ee geometry using the AgStack API"""
#     geo_id = feature_to_geo_id(feature,token,session,asset_registry_base,debug)
#     system_index = feature.get('system:index').getInfo()
    
#     feature_w_geo_id_property = feature.set(geo_id_column,geo_id)
#     return feature_w_geo_id_property

# def write_to_csv_if_exists

def feature_to_geo_id(feature, token=None, session=None, asset_registry_base="https://api-ar.agstack.org", debug=False):
    """
    Registers a field boundary from ee.Feature using the AgStack API.

    Parameters:
    - ee.Feature(): earth engine geometry 
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
    
    headers = {'Authorization': f'Bearer {token}'} if token else None

    # Use provided session if available, otherwise create a new session
    if session:
        response = session.post(url, json=payload, headers=headers)
        # if debug: print("using existing session ")
    else:
        # if debug: print("using individual request as no existing session set up, to set one up use: agstack_to_gee.start_agstack_session ")
        response = requests.post(url, json=payload, headers=headers)
    
    # Process the response
    if response.status_code == 200:
        json_response = response.json()
        print("Response:", json_response)#['Geo Id'])
        res = json_response['Geo Id']
        print("Field boundary registered successfully!")
    else:
        json_response = response.json()
        res = json_response.get("matched geo ids", None)[0] # if already matching use existing
        
        if res: 
            # if debug: print("Warning:", json_response["message"])
            
            if debug: print("Matched existing field (failed to register). Status code:", response.status_code, "Using pre-existing geo id: ",res)
                
        else:
            print("Failed to register field boundary (no geo id returned). Status code:", response.status_code)
            print("Error message:", json_response)
    return res


def shapefile_to_ee_feature_collection(shapefile_path):
    """
    Convert a zipped shapefile to an Earth Engine FeatureCollection.
    NB Making this as existing Geemap function shp_to_ee wouldnt work.
    Args:
    - shapefile_path (str): Path to the shapefile (.zip) to be converted.
    - geo_id_column (str): Name of the column to be used as the GeoID.

    Returns:
    - ee.FeatureCollection: Earth Engine FeatureCollection created from the shapefile.
    """
    # Unzip the shapefile
    # with zipfile.ZipFile(shapefile_path, "r") as zip_ref:
    #     zip_ref.extractall("shapefile")

    # Load the shapefile into a GeoDataFrame
    gdf = gpd.read_file(shapefile_path)#"shapefile/test_ceo_all.shp")

    # Convert GeoDataFrame to GeoJSON
    geo_json = gdf.to_json()

    # Create a FeatureCollection from GeoJSON
    roi = ee.FeatureCollection(json.loads(geo_json))

    return roi


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
    wkt = coordinates_to_wkt(coordinates) 
    #'POLYGON((' + ', '.join([f'{lon} {lat}' for lon, lat in coordinates]) + '))'

    return wkt
    
def coordinates_to_wkt(coordinates):
    """client side coordinates list"""
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

import pandas as pd

def check_inputs_same_size(fc,df,override_checks=False):
    """ Throws an error if differentb sizes and override_checks not True"""
    # df = pd.read_csv(output_lookup_csv)
    df_size = len(df)
    
    fc_size = fc.size().getInfo()
    
    if df_size == fc_size:
        res= (f"Check passed: feature collection and table same size: {df_size} rows")
    else:
        print (f"Warning, different sized inputs: table rows ={df_size}, fc = {fc_size} \n Are they the same inputs? \n ")

        if override_checks:
            # print (message)
            res = (f" 'override_checks' set to 'True'; continuing processing non-matching inputs")
        else:     
            # print (message)
            raise ValueError (f" 'override_checks' set to 'False'; stopping processing with non-matching inputs")

    return print (res)


def create_csv_from_list(join_id_column,data_list, output_lookup_csv):
    # Create a pandas DataFrame from the list
    df = pd.DataFrame(data_list, columns=[join_id_column])

    # Write DataFrame to CSV file
    df.to_csv(output_lookup_csv, index=False)


def get_system_index_vals_w_missing_geo_ids(df,geo_id_column,join_id_column):
    """
    Extracts system:index values where Geo_id is not present (NaN).

    Args:
    - data: Input data in the form of pandas dataframe

    Returns:
    - List of system:index / join_id_column values where no Geo_id is present.
    """

    # Extract system:index values where Geo_id is NaN
    no_geo_id_values = df[df[geo_id_column].isna()][join_id_column].tolist()

    return no_geo_id_values

def filter_features_by_system_index(feature_col, system_indexes,join_id_column):
    # Create a list of filters
    filters = ee.Filter.inList(join_id_column, system_indexes)
    
    # Apply the filter to the feature collection
    filtered_features = feature_col.filter(filters)
    
    return ee.FeatureCollection(filtered_features)

def update_geo_id_in_csv(output_lookup_csv,system_index, geo_id_column,new_geo_id, join_id_column):
    """updates rows in csv based on match to the individual input system_index string value. Uses an overwrite (slow as adds time for each but works ok...batch would be quicker)"""
    # Read the CSV into a pandas DataFrame
    df = pd.read_csv(output_lookup_csv)
    # pd.dataframe(data)
    # Update the Geo_id for the corresponding system_index
    df.loc[df[join_id_column.replace(' ', '_')] == system_index, geo_id_column] = new_geo_id

    # Write the updated DataFrame back to the CSV
    df.to_csv(output_lookup_csv, index=False)


def register_feature_and_append_to_csv(feature,geo_id_column,output_lookup_csv,join_id_column,token,session,asset_registry_base,debug=True):
    """Registers a field boundary with the ee geometry using the AgStack API"""
    new_geo_id = feature_to_geo_id(feature,token,session,asset_registry_base,debug)
    
    system_index = feature.get(join_id_column).getInfo()
    
    update_geo_id_in_csv(output_lookup_csv,system_index,geo_id_column, new_geo_id,join_id_column)
    # feature_w_geo_id_property = feature.set(geo_id_column,geo_id)
    
    if debug: print(new_geo_id, "for" , join_id_column ," added to " ,output_lookup_csv, end='\r')


def copy_and_rename_csv(source_file, destination_folder,delete_source):
    """"copies and auto renames csv but with option to delete original"""
    # Get the base filename without extension
    file_name, file_extension = os.path.splitext(source_file)

    # Generate timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # New filename with timestamp suffix
    new_file_name = f"{file_name}_{timestamp}{file_extension}"

    # Destination path
    destination_path = os.path.join(destination_folder, new_file_name)

    try:
        # Copy the file
        shutil.copy(source_file, destination_path)
        # os.system(cp source_file destination_path)
        print(f"Backup file copied successfully to: {destination_path}")

        # Optionally, you may delete the original file
        if delete_source:
            
            os.remove(source_file)
            
            print(f"Original csv file deleted: {source_file}")

    except Exception as e:
        print(f"Error: {e}")

def csv_prep_and_fc_filtering(feature_col, geo_id_column, output_lookup_csv, join_id_column, override_checks=False, debug=False):
    """If csv exists, run checks to compare size to feature_col (fc) input. Then calculates geo ids left to run and filters accordingly. If csv doesnt exist makes one based on fc join_id_column""" 

    if os.path.exists(output_lookup_csv):
        # get list of system indexes 
        df = pd.read_csv(output_lookup_csv)
            
        check_inputs_same_size(fc=feature_col,df=df,override_checks=override_checks)       
                
        system_indexes = get_system_index_vals_w_missing_geo_ids(df,geo_id_column,join_id_column)
        
        filtered_features = filter_features_by_system_index(feature_col, system_indexes,join_id_column)

        fc = ee.FeatureCollection(filtered_features)

    else:
        data_list = feature_col.aggregate_array(join_id_column).getInfo()
        
        # make a csv with this list as first column
        create_csv_from_list(join_id_column,data_list, output_lookup_csv)

        fc = feature_col
        
    return fc
        
def register_fc_and_append_to_csv(feature_col, geo_id_column, output_lookup_csv, join_id_column, token, session, asset_registry_base, override_checks=False,remove_temp_csv=True, debug=False):
    """feature collection to geo ids stored in a csv, either as a lookup table to add to other datasets (e.g. feature collections). 
    If csv exists (e.g. a lookup or whisp results) this adds in missing geo ids (so if crashes can carry on where left)"""
    # Initialize an empty list to store features
    feature_list = []

    #checks and fc filtering
    fc = csv_prep_and_fc_filtering(feature_col, geo_id_column, output_lookup_csv, join_id_column, override_checks=override_checks, debug=debug)
    
    # Convert the FeatureCollection to a list
    feature_col_list = fc.toList(feature_col.size())

    total_iterations = feature_col_list.size().getInfo()
    
    if debug: print (f"Number without geo ids:{total_iterations}. \n Processing started...")
        
    # loop over each feature in the list
    for i in range(total_iterations):

        feature = ee.Feature(feature_col_list.get(i))
        
        try: 
            register_feature_and_append_to_csv(feature,geo_id_column,output_lookup_csv,join_id_column,token,session,asset_registry_base,debug)
        except KeyboardInterrupt:
            raise ValueError("KeyboardInterrupt")
                       
        except Exception as error:
            # handle the exception
            print("An exception occurred:", error) # An exception occurred: division by zero
            print(f"Skipping feature {join_id_column} {str(feature.get(join_id_column).getInfo())}")
        
        # Calculate progress percentage         
        progress = (i + 1) / total_iterations * 100

        print(f"Progress: {progress:.2f}% ({i + 1}/{total_iterations}) ", end='\r')
    
    # back up copy in csvs folder (currently hard coded)
    # option to delet original (e.g. if its a temp csv you want cleared)    
    copy_and_rename_csv(source_file=output_lookup_csv,destination_folder="csvs",delete_source=remove_temp_csv)
    
    return print("Done")


def add_geo_ids_to_feature_col_from_lookup_df(fc,df,join_id_column="system:index",geo_id_column="Geo_id",override_checks=False,remove_other_properties=False,
                                              debug=False):
    
    check_inputs_same_size(fc, df, override_checks) 
    
    df = df.loc[:, [join_id_column, geo_id_column]]
    
    df_cleaned =df.dropna().copy()

    if debug: print (f"dropping rows without values (NaN etc) from input table. Before: {len(df)} After: {len(df_cleaned)} \n processing...")
          
    out_fc = geemap.ee_join_table(ee_object=fc, data=df_cleaned, src_key=join_id_column, dst_key=join_id_column)
    
    joined_not_null = out_fc.filter(ee.Filter.neq(geo_id_column, None)).aggregate_array(geo_id_column).size().getInfo()

    if remove_other_properties:
        out_fc = out_fc.select([geo_id_column])
    
    if debug: print (f"Finished. \n New count of {geo_id_column} values in feature collection: {joined_not_null} (from total of {out_fc.size().getInfo()} features)")
    
    return out_fc
    

def add_geo_ids_to_feature_col_from_lookup_csv(fc,csv,join_id_column="system:index",geo_id_column="Geo_id",override_checks=False,remove_other_properties=False,debug=False):
    df = pd.read_csv(csv)
    return add_geo_ids_to_feature_col_from_lookup_df(fc,df,join_id_column,geo_id_column,override_checks=override_checks,remove_other_properties=remove_other_properties,debug=debug)

def add_empty_column_to_csv(csv_file, column_name):
    df = pd.read_csv(csv_file)

    # Check if the column exists in the DataFrame
    if column_name in df.columns:
        print(f"Column '{column_name}' already exists")
    else:
        # if doesn't exist add it
        df.insert(loc=1, column=column_name, value=None)
        print(f"Column '{column_name}' not found, adding it.")

    # Write the updated DataFrame back to the CSV file
    df.to_csv(csv_file, index=False)


def remove_column_from_csv(csv_file, column_name):
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file)

    # Check if the column exists in the DataFrame
    if column_name in df.columns:
        # Drop the specified column
        df = df.drop(columns=[column_name])
        print(f"Column '{column_name}' removed successfully from {csv_file}")
    else:
        print(f"Column '{column_name}' not found in {csv_file}.")

    # Write the updated DataFrame back to the CSV file
    df.to_csv(csv_file, index=False)
    return None 

def reorder_column(df, column_name, position):
    # Pop the column to remove it from the DataFrame
    column = df.pop(column_name)
    # Insert the column at the specified position
    df.insert(position, column_name, column)

def add_geo_ids_to_csv_from_lookup_df(
    input_csv,
    geo_id_lookup_df,
    join_id_column="system:index",
    geo_id_column="Geo_id",
    # override_checks=False, # needs implementing
    join_id_column_rename=True,
    overwrite=False,
    drop_geo=False,
    debug=False
    ):
    
    input_df = pd.read_csv(input_csv)
    
    # Check if the column exists in the DataFrame
    if geo_id_column in input_df.columns:
        print(f"Column '{geo_id_column}' already exists. No join carried out")
        sys.exit()
    elif join_id_column not in input_df.columns:
        print(f"Column '{join_id_column}' not present to carry out join")      
        sys.exit()  
    else:
        #carry out the join     
        input_df_w_geo_ids = input_df.merge(geo_id_lookup_df,on=join_id_column,how="left")
    
    reorder_column(df=input_df_w_geo_ids, column_name=geo_id_column, position=1)
    
    if drop_geo:
        if debug: print ("Dropping geometry column ('drop_geo' set to True)")
        input_df_w_geo_ids = input_df_w_geo_ids.drop(".geo",axis=1)   

    if overwrite:
        out_name = input_csv
        print ("new csv: ",  out_name)
    else:    
        out_name = f'copy_{input_csv}'
        print ("new csv: ",  out_name)
    
    
    
    input_df_w_geo_ids.to_csv(out_name,index=False)
   
    return None

def add_geo_ids_to_csv_from_lookup_csv(input_csv,
    geo_id_lookup_csv,
    join_id_column="system:index",
    geo_id_column="Geo_id",
    # override_checks=False, # needs implementing
    overwrite=False,
    drop_geo=False,
    debug=False
    ):

    #create a df from the csv 
    geo_id_lookup_df = pd.read_csv(geo_id_lookup_csv)
    #run preexisting function (NB could update with a decorator function)
    add_geo_ids_to_csv_from_lookup_df(
        input_csv,
        geo_id_lookup_df,
        join_id_column=join_id_column,
        geo_id_column=geo_id_column,
        # override_checks=False, # needs implementing
        overwrite=overwrite,
        drop_geo=drop_geo,
        debug=debug
    )

    return None   



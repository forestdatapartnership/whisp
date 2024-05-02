import base64
import io
import json
import requests
from datetime import datetime
import sys
import os
import pandas as pd
import geopandas as gpd
import geemap


# Assuming file_to_ceo.py is in the modules directory, we add the parameters directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'parameters'))

from config_ceo import ceo_email, ceo_password

def file_to_base64(file_path):
    # Ensure the path is to a valid file
    if not (file_path.endswith('.shp') or file_path.endswith('.zip')):
        raise ValueError("The provided path does not point to a .shp or .zip file.")

    buffer = io.BytesIO()

    with open(file_path, 'rb') as file:
        buffer.write(file.read())

    buffer.seek(0)
    base64_encoded = base64.b64encode(buffer.read()).decode('utf-8')

    # Append the required string to the beginning
    base64_encoded = 'data:application/zip;base64,' + base64_encoded

    return base64_encoded

def login(email, password, url):
    headers = {'Content-Type': 'application/json'}
    data = {
        "email": email,
        "password": password
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()  # Raises an error for bad HTTP status codes

        # Extracting the entire 'Set-Cookie' header
        ring_session = response.headers.get('Set-Cookie').split(';')[0]
        return ring_session
    except requests.RequestException as e:
        print(f'HTTP Request failed: {e}')
        return None

def send_project_creation_request(data, cookie):
    url = 'https://app.collect.earth/create-project'  # Replace with your actual API endpoint
    headers = {
        'Content-Type': 'application/json',
        'Cookie': cookie  # Include the cookie in the request headers
    }

    # Convert the data to JSON, if it's not already a JSON string
    if not isinstance(data, str):
        data = json.dumps(data)
        

    try:
        response = requests.post(url, headers=headers, data=data)
        

        
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
        return response.json()  # Return the JSON response if request is successful
    except requests.RequestException as e:
        print(f'HTTP Request failed: {e}')
        return None

def build_and_send_project_data(file_path):
    # Convert shapefile to base64
    try:
        plot_file_base64 = file_to_base64(file_path)
    except Exception as e:
        print(f"Error in converting shapefile to base64: {e}")
        return None

    # Login to get cookie
    cookie = login(ceo_email, ceo_password, "https://app.collect.earth/login")
    if cookie is None:
        print("Login failed.")
        return None

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    project_name = f"dpi_{timestamp}"

    # Build the data object
    data = {
    "institutionId": 3878,
    "projectTemplate": -1,
    "useTemplatePlots": False,
    "useTemplateWidgets": False,
    "imageryId": 1317,
    "projectImageryList": [
        1885
    ],
    "aoiFeatures": [],
    "aoiFileName": "",
    "description": "",
    "name": project_name,
    "privacyLevel": "users",
    "projectOptions": {
        "showGEEScript": False,
        "showPlotInformation": True,
        "collectConfidence": False,
        "autoLaunchGeoDash": True
    },
    "designSettings": {
        "userAssignment": {
            "userMethod": "none",
            "users": [],
            "percents": []
        },
        "qaqcAssignment": {
            "qaqcMethod": "none",
            "percent": 0,
            "smes": [],
            "timesToReview": 2
        },
        "sampleGeometries": {
            "points": True,
            "lines": True,
            "polygons": True
        }
    },
    "numPlots": "",
    "plotDistribution": "shp",
    "plotShape": "square",
    "plotSize": "",
    "plotSpacing": "",
    "shufflePlots": False,
    "sampleDistribution": "center",
    "samplesPerPlot": "1",
    "sampleResolution": "",
    "allowDrawnSamples": False,
    "surveyQuestions": {
        "0": {
            "question": "Forest 2020",
            "answers": {
                "0": {
                    "answer": "Yes",
                    "color": "#5eb273",
                    "hide": False
                },
                "1": {
                    "answer": "No",
                    "color": "#d99c2e",
                    "hide": False
                }

            },
            "parentQuestionId": -1,
            "parentAnswerIds": [],
            "dataType": "text",
            "hideQuestion": False,
            "componentType": "button",
            "cardOrder": 1
        }
    },
    "surveyRules": [],
    "plotFileName": "test_ceo.zip",
        "plotFileBase64": plot_file_base64
    }

    # Send project creation request
    response = send_project_creation_request(data, cookie)

    if response is not None:
        if 'projectId' in response:
            project_id = response['projectId']         
            publish_response = publish_project(project_id, cookie)
            print("Project creation request sent successfully.")
        else:
            raise ValueError("Response does not include 'projectId'.")
    else:
        print("Failed to send project creation request.")

    return project_id


def publish_project(project_id, cookie):
    headers = {
        'Content-Type': 'application/json',
        'Cookie': cookie  # Include the cookie in the request headers
    }
    try:        
        response = requests.post(f'https://app.collect.earth/publish-project?projectId={project_id}&clearSaved=true', headers=headers)
        response.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
    except requests.RequestException as e:
        print(f'HTTP Request failed: {e}')
        return None    

def get_ceo_url(file_path):
    project_id = build_and_send_project_data(file_path)

    url = "https://app.collect.earth/collection?projectId=" + str(project_id)
    return url


def whisp_stats_shapefile_and_ceo_url(roi, df, plot_id_column, out_shapefile):
    """
    Creates a CEO URL For viewing outputs and a shapefile.

    Parameters:
    - roi: feature collection of region(s) of interest (i.e input plots)
    - plot_id_column:  str, name for output column for quick reference integer index 
    - df: DataFrame, the DataFrame of stats from WHISP .
    - out_shapefile: str, name of the output shapefile.
    -

    Returns:
    - ceo_url: str, URL to the exported shapefile.
    """
    
    gdf = merge_and_reformat_to_gdf(roi, df, plot_id_column)

    ceo_url = gdf_to_shapefile_w_ceo_url(gdf, out_shapefile)

    return ceo_url




def merge_and_reformat_to_gdf(roi, df, plot_id_column):
    """
    Joins dataframe to roi and adds index etc

    Parameters:
    - roi: feature collection of region(s) of interest (i.e input plots)
    - df: DataFrame, from the whisp stats output
    - plot_id_column: name for output column for quick reference 

    Returns:
    - gdf: GeoDataFrame, the merged and reformatted DataFrame.
    """
    df = df.rename(columns=lambda x: x.replace(' ', '_'))
    
    # Merge the dataframes
    df = pd.merge(geemap.ee_to_gdf(roi),
                  df, 
                  left_on='Geo_id', 
                  right_on='Geo_id', 
                  how='inner')

    # Add a new column for plot ID
    df[plot_id_column] = df.index + 1
    df.set_index(plot_id_column, inplace=True)

    # Select columns from the merged dataframe
    ceo_cols = df.columns
    gdf = df[ceo_cols]

    return gdf

    
def gdf_to_shapefile_w_ceo_url(gdf, out_shapefile):
    """
    Exports GeoDataFrame to a shapefile and returns the CEOS URL.

    Parameters:
    - gdf: GeoDataFrame, the GeoDataFrame to be exported.
    - out_shapefile: str, name of the output shapefile.

    Returns:
    - ceo_url: str, URL to the exported shapefile.
    """
    # Truncate column names for shapefile export
    # ceo_cols_for_exporting = tidy_tables.truncate_strings_in_list(ceo_cols_for_exporting, 10)

    # # Set column names of GeoDataFrame
    # gdf.columns = ceo_cols_for_exporting
    gdf.rename(columns=lambda x: x[:10], inplace=True)  # Truncate column names for shapefile export
    
    # Create GeoDataFrame with geometry and CRS
    gdf = gpd.GeoDataFrame(gdf, geometry=gdf.geometry, crs="EPSG:4326")

    # Export GeoDataFrame to shapefile
    gdf.to_file(filename=out_shapefile, driver='ESRI Shapefile')

    # get collect earth online (ceo) url for viewing plots and stats (function from 'modules.file_to_ceo')
    ceo_url = get_ceo_url(out_shapefile)

    return ceo_url


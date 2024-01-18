import ee
import sys
import os
# from google.oauth2 import service_account

#for sepal instance
def initialize_ee():
    """Initializes Google Earth Engine with credentials located one level up from the script's directory."""
    try:
        # Check if EE is already initialized
        if not ee.data._initialized:
            # ee.Initialize()
            try:
                ee.Initialize() #cloud project update. Temp workaround for me (Andy)
            except: 
                print("searching for 'gee_cloud_project' in parameters/config_gee.py")
                sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'parameters'))
                from config_gee import gee_cloud_project
                ee.Initialize(project="ee-andyarnellgee")

            print("Earth Engine has been initialized with the specified credentials.")
    except Exception as e:
        print("An error occurred during Earth Engine initialization:", e)

#for WHISP App / local jupyter
# def initialize_ee():
#     """Initializes Google Earth Engine with credentials located one level up from the script's directory."""
#     try:
#         # Check if EE is already initialized
#         if not ee.data._initialized:
#             # Construct the path to the credentials file
#             current_directory = os.getcwd()
#             credentials_path = os.path.join(current_directory, 'credentials.json')

#             # Initialize EE with the credentials file
#             credentials = service_account.Credentials.from_service_account_file(credentials_path,
#                                                                                 scopes=['https://www.googleapis.com/auth/earthengine'])
#             ee.Initialize(credentials)
#             print("Earth Engine has been initialized with the specified credentials.")
#     except Exception as e:
#         print("An error occurred during Earth Engine initialization:", e)
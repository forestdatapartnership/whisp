import ee
import sys
import os

# from google.oauth2 import service_account

# for sepal instance
def initialize_ee():
    """Initializes Google Earth Engine with credentials located one level up from the script's directory."""
    try:
        # Check if EE is already initialized
        if not ee.data._initialized:
            # ee.Initialize()
            try:
                ee.Initialize()  # cloud project update.
            except:
                print(
                    "searching for user's gee cloud project name for account linked to sepal. i.e., a python file located here: 'parameters/config_gee.py', containing: gee_cloud_project 'insert_project_name_here' "
                )
                sys.path.append(
                    os.path.join(os.path.dirname(__file__), "..", "parameters")
                )
                from config_gee import gee_cloud_project

                ee.Initialize(project=gee_cloud_project)

            print("Earth Engine has been initialized with the specified credentials.")
    except Exception as e:
        print("An error occurred during Earth Engine initialization:", e)


# for WHISP App / local jupyter
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

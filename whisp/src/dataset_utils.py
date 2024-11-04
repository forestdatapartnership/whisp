import inspect
import re
import ee

ee.Authenticate()
ee.Initialize()

# For each function in functions, extract_asset_ids retrieves a list of asset IDs used within the function.
# Then, filter_functions_with_accessible_assets uses all(...) with is_asset_accessible(asset_id) for asset_id in asset_ids to check each asset.
# all(...) returns True only if every asset in asset_ids is accessible.
# If any single asset is inaccessible, all(...) returns False, and that function is skipped.


def is_asset_accessible(asset_id):
    """
    Checks if an asset with the specified ID is accessible.

    Args:
        asset_id (str): The ID of the asset to check.

    Returns:
        bool: True if the asset is accessible, False otherwise.
    """
    try:
        ee.data.getAsset(asset_id)  # This checks asset availability
        return True
    except ee.EEException as e:
        print(f"Asset {asset_id} is not accessible: {e}")
        return False


def extract_asset_ids(function):
    """
    Extracts asset IDs from the source code of a given function.

    Args:
        function: The function object from which to extract asset IDs.

    Returns:
        list: A list of asset IDs found in the function's source code.
    """
    # Get the source code of the function
    source_code = inspect.getsource(function)

    # Filter out commented lines
    uncommented_lines = [
        line for line in source_code.splitlines() if not line.strip().startswith("#")
    ]

    # Join uncommented lines back into a single string
    uncommented_code = "\n".join(uncommented_lines)

    # Regular expression to find asset IDs for FeatureCollection, Image, and ImageCollection
    asset_ids = re.findall(
        r'ee\.(FeatureCollection|Image|ImageCollection)\([\'"]([^\'"]+)[\'"]\)',
        uncommented_code,
    )

    # Extract just the asset ID part from the matched tuples
    return [asset_id[1] for asset_id in asset_ids]


def filter_functions_with_accessible_assets(functions):
    """
    Filters a list of functions to keep only those that have accessible assets.

    Args:
        functions (list): A list of function objects to check.

    Returns:
        list: A list of functions that have accessible assets.
    """
    valid_functions = []

    for function in functions:
        asset_ids = extract_asset_ids(function)

        # Check if all assets are accessible
        if all(is_asset_accessible(asset_id) for asset_id in asset_ids):
            valid_functions.append(function)

    return valid_functions


### code to run
# from whisp.src.dataset_utils import filter_functions_with_accessible_assets

# function_list = list_functions()

# # Filter functions to keep only those with accessible assets
# valid_functions = filter_functions_with_accessible_assets(function_list)

# # Print the names of valid functions
# print("Functions with accessible assets:")
# for func in valid_functions:
#     print(func.__name__)


# function_list = list_functions()

# # Filter functions to keep only those with accessible assets
# valid_functions = filter_functions_with_accessible_assets(function_list)

# # Print the names of valid functions
# print("Functions with accessible assets:")
# for func in valid_functions:
#     print(func.__name__)


#
# results = [func() for func in prep_functions]

# print("Functions ending with '_prep':", [func.__name__ for func in prep_functions])
# print("Results:", results)

# # server side
# def ee_image_checker(image):
#     """
#     Efficiently tests if the input is a valid ee.Image using ee.Algorithms.If.

#     Args:
#         image: An ee.Image object.

#     Returns:
#         bool: True if the input is a valid ee.Image, False otherwise.
#     """
#     try:
#         # Check if the input type is an Image using ee.Algorithms.If
#         return ee.Algorithms.If(ee.Algorithms.ObjectType(image) =='Image',
#             True,  # Returns True if it’s a valid image
#             False  # Returns False if it’s not an image
#         )
#     except Exception as e:
#         print(f"Image validation failed: {e}")
#         return False

# old version - client side


# # from here https://gis.stackexchange.com/questions/402736/how-do-i-filter-a-list-by-type-in-google-earth-engine
# def keep_valid_images(image_list):
#     """
#     Filters a list of Earth Engine objects, returning only valid ee.Image objects.

#     Args:
#         image_list: A list of Earth Engine objects (could be ee.Image, ee.Feature, etc.)

#     Returns:
#         list: A list of only valid ee.Image objects.
#     """
#     # First filter for items with 'system:bands' property (to keep only ee.Image objects)
#     images = ee.List(image_list).filter(ee.Filter.notNull(['system:bands'])).getInfo()

#     # Perform additional check on each item to confirm it's valid
#     valid_images = [ee.Image(img['id']) for img in images if is_image_valid(ee.Image(img['id']))]

#     return valid_images

# # Example usage
# def create_valid_image():
#     return ee.Image(1).rename("valid_image")
#     # 'COPERNICUS/S2_SR/20200726T083601_20200726T083603_T36TXM')


# # Create image objects to test
# valid_image = create_valid_image()
# invalid_image = create_invalid_image()

# # List of images to check
# # images_to_test = [creaf_descals_palm_prep(),valid_image, invalid_image]


# # Get the list of valid images
# valid_imgs = keep_valid_images(images_to_test)

# img_combined = ee.Image(1)

# for img in valid_imgs:
#     img_combined = img_combined.addBands(img)


# print (img_combined.bandNames().getInfo())


# def combine_datasets():
#     "Combines datasets into a single multiband image"
#     # Initialize an image with a constant band to start with
#     img_combined = ee.Image(1).rename(geometry_area_column) # becomes the area column after pixel area multiplication step below
#     # print (prep_functions)
#     images_to_test = [func() for func in prep_functions]

#     # Get the list of valid images
#     valid_imgs = keep_valid_images(images_to_test)

#     img_combined = ee.Image(1)

#     for img in valid_imgs:
#         img_combined = img_combined.addBands(img)

#     img_combined = img_combined.multiply(ee.Image.pixelArea()) # multiple all bands by pixel area

#     return img_combined

# print (combine_datasets().bandNames().getInfo())


# # Execute the function to list the functions and call them


# def combine_datasets():

#     "Combines datasets into a single multiband image"
#     img_combined = ee.Image(1).rename(geometry_area_column)
#     # images_to_test = [func() for func in list_functions()]

#     # Try to combine images
#     try:
#         # valid_imgs = keep_valid_images(images_to_test)
#         # for img in images_to_test:
#         #     img_combined = img_combined.addBands(img)
#         for img in  [func() for func in list_functions()]:
#             img_combined = img_combined.addBands(img)

#         img_combined = img_combined.multiply(ee.Image.pixelArea())

#     except Exception as e:
#         print(f"Error encountered: {e}")
#         print("Validating images due to the error...")
#         # images_to_test = [func() for func in list_functions()]
#         # # Trigger validation when an error occurs
#         # valid_imgs = keep_valid_images(images_to_test)  # Validate again


#         # # Retry combining images after validation
#         # img_combined = ee.Image(1).rename(geometry_area_column)

#         # for img in valid_imgs:
#         #     img_combined = img_combined.addBands(img)

#         # img_combined = img_combined.multiply(ee.Image.pixelArea())

#     return img_combined

# # print (combine_datasets().bandNames().getInfo())


# def combine_valid_datasets():
#     """Combines validated datasets into a single multiband image."""
#     img_combined = ee.Image(1).rename(geometry_area_column)

#     # Validate images
#     images_to_test = [func() for func in list_functions()]
#     valid_imgs = keep_valid_images(images_to_test)  # Validate images

#     # Retry combining images after validation
#     for img in valid_imgs:
#         img_combined = img_combined.addBands(img)

#     img_combined = img_combined.multiply(ee.Image.pixelArea())

#     return img_combined

# from googleapiclient.errors import HttpError


# def combine_valid_datasets():
#     """Combines datasets into a single multiband image, with fallback if assets are missing."""
#     img_combined = ee.Image(1).rename(geometry_area_column)

#     # #combine images directly
#     # for img in [func() for func in list_functions()]:
#     #     img_combined = img_combined.addBands(img)

#     #     img_combined = img_combined.multiply(ee.Image.pixelArea())


#     try:
#         # Attempt to combine images directly
#         for img in [func() for func in list_functions()]:
#             img_combined = img_combined.addBands(img)

#         img_combined = img_combined.multiply(ee.Image.pixelArea())

#     except HttpError as e:
#         # Check for the specific error message related to missing assets
#         if "Image asset" in str(e) and "not found" in str(e):
#             print(f"Specific error encountered: {e}")
#             print("Validating images due to the missing asset error...")
#             img_combined = combine_valid_datasets()  # Call the validation function

#         else:
#             # Re-raise any other errors that are not related to missing assets
#             raise e

#     return img_combined


# alternative suggested

# def combine_valid_datasets():
#     img_combined = ee.Image(1).rename(geometry_area_column)
#     valid_imgs = []
#     for func in list_functions():
#         try:
#             img = func()
#             # Check if the image can be loaded successfully
#             _ = img.getInfo()  # This triggers a load check
#             valid_imgs.append(img)
#         except ee.EEException as e:
#             print(f"Skipping image: {e}")
#         except Exception as e:
#             print(f"Unexpected error: {e}")

#     for img in valid_imgs:
#         img_combined = img_combined.addBands(img)

#     return img_combined

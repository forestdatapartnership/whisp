import base64
import ee
import math
import os
import pandas as pd

import importlib.resources as pkg_resources

from dotenv import load_dotenv
from pathlib import Path

from .logger import StdoutLogger


logger = StdoutLogger(__name__)


def get_example_data_path(filename):
    """
    Get the path to an example data file included in the package.

    Parameters:
    -----------
    filename : str
        The name of the example data file.

    Returns:
    --------
    str
        The path to the example data file.
    """
    return os.path.join("..", "tests", "fixtures", filename)


def load_env_vars() -> None:
    """Loads the environment variables required for testing the codebase.

    Returns
    -------
    out : None
    """

    all_dotenv_paths = [Path(__file__).parents[2] / ".env", Path.cwd() / ".env"]
    dotenv_loaded = False

    for dotenv_path in all_dotenv_paths:
        logger.logger.debug(f"dotenv_path: {dotenv_path}")
        if dotenv_path.exists():
            dotenv_loaded = load_dotenv(dotenv_path)
            break

    if not dotenv_loaded:
        raise DotEnvNotFoundError
    logger.logger.info(f"Loaded evironment variables from '{dotenv_path}'")


def init_ee() -> None:
    """Initialize earth engine according to the environment"""

    # only do the initialization if the credential are missing
    if not ee.data._credentials:

        # if in test env use the private key
        if "EE_PRIVATE_KEY" in os.environ:

            # key need to be decoded in a file
            content = base64.b64decode(os.environ["EE_PRIVATE_KEY"]).decode()
            with open("ee_private_key.json", "w") as f:
                f.write(content)

            # connection to the service account
            service_account = "test-sepal-ui@sepal-ui.iam.gserviceaccount.com"
            credentials = ee.ServiceAccountCredentials(
                service_account, "ee_private_key.json"
            )
            ee.Initialize(credentials)
            logger.logger.info(f"Used env var")

        # if in local env use the local user credential
        else:
            try:
                load_env_vars()
                logger.logger.info("Called 'ee.Initialize()'.")
                ee.Initialize(project=os.environ["PROJECT"])
            except ee.ee_exception.EEException:
                logger.logger.info("Called 'ee.Authenticate()'.")
                ee.Authenticate()
                ee.Initialize(project=os.environ["PROJECT"])


def clear_ee_credentials():

    path_to_creds = Path().home() / ".config" / "earthengine" / "credentials"
    if not path_to_creds.exists():
        logger.logger.error(
            f"GEE credentials file '{path_to_creds}' not found, could not de-authenticate."
        )
    else:
        path_to_creds.unlink()
        logger.logger.warning(f"GEE credentials file deleted.")


def remove_geometry_from_feature_collection(feature_collection):
    """Define the function to remove geometry from features in a feature collection"""
    # Function to remove geometry from features
    def remove_geometry(feature):
        # Remove the geometry property
        feature = feature.setGeometry(None)
        return feature

    # Apply the function to remove geometry to the feature collection
    feature_collection_no_geometry = feature_collection.map(remove_geometry)
    return feature_collection_no_geometry


# Compute centroids of each polygon including the external_id_column
def get_centroid(feature, external_id_column="external_id"):
    keepProperties = [external_id_column]
    # Get the centroid of the feature's geometry.
    centroid = feature.geometry().centroid(1)
    # Return a new Feature, copying properties from the old Feature.
    return ee.Feature(centroid).copyProperties(feature, keepProperties)


def buffer_point_to_required_area(feature, area, area_unit):
    """buffers feature to get a given area (needs math library); area unit in 'ha' or 'km2' (the default)"""
    area = feature.get("REP_AREA")

    # buffer_size = get_radius_m_to_buffer_for_given_area(area,"km2")# should work but untested in this function

    buffer_size = (
        (ee.Number(feature.get("REP_AREA")).divide(math.pi)).sqrt().multiply(1000)
    )  # calculating radius in metres from REP_AREA in km2

    return ee.Feature(feature).buffer(buffer_size, 1)
    ### buffering (incl., max error parameter should be 0m. But put as 1m anyhow - doesn't seem to make too much of a difference for speed)


def get_radius_m_to_buffer_to_required_area(area, area_unit="km2"):
    """gets radius in metres to buffer to get an area (needs math library); area unit ha or km2 (the default)"""
    if area_unit == "km2":
        unit_fix_factor = 1000
    elif area_unit == "ha":
        unit_fix_factor = 100
    radius = ee.Number(area).divide(math.pi).sqrt().multiply(unit_fix_factor)
    return radius


class DotEnvNotFoundError(FileNotFoundError):
    def __init__(self) -> None:
        super().__init__(
            "Running tests requires setting an appropriate '.env' in the root directory or in your current working "
            "directory. You may copy and edit the '.env.template' file from the root directory or from the README.",
        )

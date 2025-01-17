import ee
from google.oauth2 import service_account


def initialize_ee(credentials_path=None):
    """Initializes Google Earth Engine using the provided path or defaults to normal if no path is given."""
    try:
        if not ee.data._initialized:
            print(credentials_path)
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=["https://www.googleapis.com/auth/earthengine"],
                )
                ee.Initialize(credentials)
                print("EE initialized with credentials from:", credentials_path)
            else:
                ee.Initialize()
                print("EE initialized with default credentials.")
    except Exception as e:
        print("Error initializing EE:", e)


# Default to normal initialize if nobody calls whisp.initialize_ee.
try:
    if not ee.data._initialized:
        ee.Initialize()
        print("EE auto-initialized with default credentials.")
except Exception as e:
    print("Error in default EE initialization:", e)

from whisp.src.stats import (
    whisp_stats_ee_to_ee,
    whisp_stats_ee_to_df,
    whisp_stats_geojson_to_df,
    whisp_stats_geojson_to_ee,
    whisp_stats_ee_to_drive,
    whisp_stats_geojson_to_drive,
    whisp_formatted_stats_ee_to_df,  # uses lookup tables to create schemas for reformatting
    whisp_formatted_stats_geojson_to_df,  # uses lookup tables to create schemas for reformatting
    convert_iso3_to_iso2,  # temporary function to convert iso3 to iso2
)

# temporary parameters to be removed once isio3 to iso2 conversion server side is implemented
from whisp.parameters.config_runtime import iso3_country_column, iso2_country_column

from whisp.src.reformat import (
    validate_dataframe_using_lookups,
    validate_dataframe,
    create_schema_from_dataframe,
    load_schema_if_any_file_changed,
    # log_missing_columns,
)

from whisp.src.risk import whisp_risk

# from whisp.src.plot_generator import generate_plots_from_csv

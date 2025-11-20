import ee
from google.oauth2 import service_account


def initialize_ee(credentials_path=None, use_high_vol_endpoint=False):
    """Initializes Google Earth Engine using the provided path or defaults to normal if no path is given.
    Args:
    use_high_vol_endpoint: True/False to use high-volume endpoint via opt_url parameter (defaults to False).
    """
    try:
        if not ee.data._initialized:
            print(credentials_path)
            if credentials_path:
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=["https://www.googleapis.com/auth/earthengine"],
                )
                if use_high_vol_endpoint == False:
                    ee.Initialize(credentials)
                else:
                    ee.Initialize(
                        credentials,
                        opt_url="https://earthengine-highvolume.googleapis.com",
                    )
                print("EE initialized with credentials from:", credentials_path)
            else:
                if use_high_vol_endpoint == False:
                    ee.Initialize()
                else:
                    ee.Initialize(
                        opt_url="https://earthengine-highvolume.googleapis.com"
                    )
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

from openforis_whisp.datasets import combine_datasets, combine_custom_bands

from openforis_whisp.stats import (
    whisp_stats_ee_to_ee,
    whisp_stats_ee_to_df,
    whisp_stats_geojson_to_df,
    whisp_stats_geojson_to_ee,
    whisp_stats_geojson_to_geojson,
    whisp_stats_ee_to_drive,
    whisp_stats_geojson_to_drive,
    whisp_formatted_stats_ee_to_df,
    whisp_formatted_stats_ee_to_geojson,
    whisp_formatted_stats_geojson_to_df,
    whisp_formatted_stats_geojson_to_geojson,
    set_point_geometry_area_to_zero,
    reformat_geometry_type,
    convert_iso3_to_iso2,
)

from openforis_whisp.advanced_stats import (
    whisp_formatted_stats_geojson_to_df_fast,
)

# temporary parameters to be removed once isio3 to iso2 conversion server side is implemented
from openforis_whisp.parameters.config_runtime import (
    iso3_country_column,
    iso2_country_column,
)

from openforis_whisp.reformat import (
    validate_dataframe_using_lookups,
    validate_dataframe_using_lookups_flexible,
    validate_dataframe,
    create_schema_from_dataframe,
    load_schema_if_any_file_changed,
    format_stats_dataframe,
)

from openforis_whisp.data_conversion import (
    convert_ee_to_df,
    convert_geojson_to_ee,
    convert_df_to_geojson,
    convert_csv_to_geojson,
    convert_ee_to_geojson,
)

from openforis_whisp.risk import whisp_risk, detect_unit_type

from openforis_whisp.utils import (
    get_example_data_path,
    generate_test_polygons,  # to be deprecated
    generate_random_features,
    generate_random_points,
    generate_random_polygons,
)

from openforis_whisp.data_checks import (
    analyze_geojson,
    validate_geojson_constraints,
    suggest_processing_mode,
)

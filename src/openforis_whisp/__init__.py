import ee
from google.oauth2 import service_account


EE_HIGH_VOLUME_URL = "https://earthengine-highvolume.googleapis.com"


def _is_ee_initialized():
    """True if the Earth Engine client is initialized, on any earthengine-api version (#250)."""
    is_initialized = getattr(ee.data, "is_initialized", None)
    if is_initialized is not None:
        return bool(is_initialized())
    return bool(getattr(ee.data, "_initialized", False))


def initialize_ee(credentials_path=None, use_high_vol_endpoint=False):
    """
    Initialize Google Earth Engine, with a service-account key file if given, otherwise with the
    default credentials.

    Earth Engine may already be initialized (importing whisp tries the defaults), so whenever a key
    file or the high-volume endpoint is asked for it is set up again from scratch, so those
    settings always take effect and are checked straight away. Without a key file the current
    credentials and project are kept. With neither setting, an existing initialization is left as
    it is. Errors are raised rather than printed, so a credentials problem shows up at start-up.

    Args:
    credentials_path: path to a service-account JSON key file (optional).
    use_high_vol_endpoint: True to use the high-volume endpoint (defaults to False).
    """
    global _applied_key_path
    if _is_ee_initialized():
        if not (credentials_path or use_high_vol_endpoint):
            return
        # Already set up this way (e.g. a worker calling this for every job): leave it alone
        wanted_url = EE_HIGH_VOLUME_URL if use_high_vol_endpoint else _DEFAULT_EE_URL
        same_key = credentials_path is None or credentials_path == _applied_key_path
        if same_key and _current_ee_url() == wanted_url:
            return
    # Work out credentials and project before touching the live client, so a bad key path leaves
    # a working session as it was
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/earthengine"],
        )
        project = None  # the service account's own project applies
    else:
        credentials = _current_ee_credentials() or "persistent"
        project = _current_ee_project()  # default credentials usually need one
    url = EE_HIGH_VOLUME_URL if use_high_vol_endpoint else None

    # Start from a clean client: the new settings are then checked straight away (a key without
    # Earth Engine access fails here, not mid-analysis), a forked worker gets its own connection,
    # and asking for the standard endpoint really switches back to it
    _reset_ee()
    try:
        ee.Initialize(credentials, url=url, project=project)
    except Exception:
        _reset_ee()
        raise
    _applied_key_path = credentials_path
    if credentials_path:
        print("EE initialized with credentials from:", credentials_path)
    else:
        print("EE initialized with default credentials.")


_DEFAULT_EE_URL = getattr(
    ee.data, "DEFAULT_CLOUD_API_BASE_URL", "https://earthengine.googleapis.com"
)
_applied_key_path = None  # the key file initialize_ee last set Earth Engine up with


def _current_ee_url():
    """The Earth Engine endpoint in use now, if it can be read."""
    try:
        from ee import _state

        url = _state.get_state().cloud_api_base_url
    except Exception:  # earthengine-api before 1.6.12
        url = getattr(ee.data, "_cloud_api_base_url", None)
    return str(url).rstrip("/") if url else None


def _reset_ee():
    """
    Clear a failed initialization. A failed ee.Initialize() can leave the client looking
    initialized (with the wrong project), which would make later checks skip a real one.
    """
    try:
        ee.Reset()
    except Exception:
        pass


def _current_ee_project():
    """The Cloud project Earth Engine is set up with now, if any, so re-initializing keeps it."""
    try:
        from ee import _state

        project = _state.get_state().cloud_api_user_project
    except Exception:  # earthengine-api before 1.6.12
        project = getattr(ee.data, "_cloud_api_user_project", None)
    default = getattr(ee.data, "DEFAULT_CLOUD_API_USER_PROJECT", None)
    return None if not project or project == default else project


def _current_ee_credentials():
    """The credentials Earth Engine is using now, if any, so re-initializing keeps them."""
    if not _is_ee_initialized():
        return None
    try:
        from ee import _state

        return _state.get_state().credentials
    except Exception:  # earthengine-api before 1.6.12
        return getattr(ee.data, "_credentials", None)


# Default to normal initialize if nobody calls whisp.initialize_ee.
try:
    if not _is_ee_initialized():
        ee.Initialize()
        print("EE auto-initialized with default credentials.")
except Exception as e:
    _reset_ee()
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
    normalize_geojson_to_gdf,
    split_multipart_geojson,
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
    check_geojson_limits,
    screen_geojson,  # Backward compatibility alias
    suggest_processing_mode,
    validate_geojson_constraints,  # Backward compatibility alias
)

from openforis_whisp.asset_registry import (
    fetch_feature,
    fetch_collection_features,
    fetch_features_by_ids,
    fetch_to_dict,
    fetch_and_save,
    save_geojson,
)

from openforis_whisp.local_stats import (
    download_geotiff_for_feature,
    download_geotiffs_for_feature_collection,
    convert_geojson_to_ee_bbox_obscured,
    convert_geojson_to_ee_bbox,
    create_vrt_from_folder,
    exact_extract_in_chunks_parallel,
    extend_bbox,
    shift_bbox,
    generate_random_box_geometries,
    reformat_geojson_properties,
    delete_all_files_in_folder,
    delete_folder,
    whisp_stats_local,
    get_band_names_from_raster,
    rename_exactextract_columns,
)

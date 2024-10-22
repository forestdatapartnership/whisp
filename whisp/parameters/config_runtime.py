from pathlib import Path
from .config_directory import RESULTS_DIR, BACKUP_CSVS_DIR


debug = True  # get print messages or not (e.g. for debugging code etc) (True or False)

# unit choice ("ha" or "percent")
percent_or_ha = "ha"

out_directory = RESULTS_DIR


threshold_to_drive = (
    500  # default 500 - whisping anything over this many features will go to drive
)

# main outputs (main output format)
out_csv_name = "whisp_output_table.csv"  # set output name

out_csv = out_directory / out_csv_name  # set full path for output csv

out_shapefile_name = "shapefile_for_ceo.shp.zip"

out_shapefile = out_directory / out_shapefile_name


# src outputs formatting

# output column names
geometry_area_column = "Plot_area_ha"

stats_unit_type_column = "Unit"

country_column = "Country"

admin_1_column = "Admin_Level_1"

centroid_x_coord_column = "Centroid_lon"

centroid_y_coord_column = "Centroid_lat"

geo_id_column = "Geo_id"

geometry_type_column = "Geometry_type"

plot_id_column = "Plot_ID"

water_flag = "water_flag"

# ordering prefix columns: inserted before stats columns (plot metadata and stats unit type).
prefix_columns_list = [
    plot_id_column,
    geo_id_column,
    geometry_area_column,
    geometry_type_column,
    country_column,
    admin_1_column,
    centroid_x_coord_column,
    centroid_y_coord_column,
    stats_unit_type_column,
]


# reformatting numbers to decimal places (e.g. '%.3f' is 3 dp)
geometry_area_column_formatting = "%.3f"

stats_area_columns_formatting = "%.3f"

stats_percent_columns_formatting = "%.0f"


# do you want to keep system:index from input feature collection? NB it's useful for making joins after processing
keep_system_index = True


# do you keep other properties from input feature collection?
keep_original_properties = False


# lookup path - for dataset info
DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH = (
    Path(__file__).parent / "lookup_gee_datasets.csv"
)

# lookup path - for dataset info
DEFAULT_CONTEXT_LOOKUP_TABLE_PATH = (
    Path(__file__).parent / "lookup_context_and_metadata.csv"
)


# Temp output parameters

# #for storing backup/temp csv files
temp_csvs_folder_path = BACKUP_CSVS_DIR

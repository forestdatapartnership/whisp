from pathlib import Path

# unit choice ("ha" or "percent")
percent_or_ha = "ha"

# output column names
# The names need to align with whisp/parameters/lookup_context_and_metadata.csv
geometry_area_column = "Area"

stats_unit_type_column = "Unit"

iso3_country_column = "Country"

iso2_country_column = "ProducerCountry"

admin_1_column = "Admin_Level_1"

centroid_x_coord_column = "Centroid_lon"

centroid_y_coord_column = "Centroid_lat"

geo_id_column = "external_id"

geometry_type_column = "Geometry_type"

plot_id_column = "plotId"

water_flag = "In_waterbody"

geometry_column = "geo"  # geometry column name, stored as a string.

# reformatting numbers to decimal places (e.g. '%.3f' is 3 dp)
geometry_area_column_formatting = "%.3f"

stats_area_columns_formatting = "%.3f"

stats_percent_columns_formatting = "%.0f"

# lookup path - for dataset info
DEFAULT_GEE_DATASETS_LOOKUP_TABLE_PATH = (
    Path(__file__).parent / "lookup_gee_datasets.csv"
)

# lookup path - for dataset info
DEFAULT_CONTEXT_LOOKUP_TABLE_PATH = (
    Path(__file__).parent / "lookup_context_and_metadata.csv"
)

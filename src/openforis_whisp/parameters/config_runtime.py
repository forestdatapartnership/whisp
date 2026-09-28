import re
from datetime import datetime
from pathlib import Path

import pandas as pd

# output column names
# The names need to align with whisp/parameters/lookup_datasets.csv
geometry_area_column = "Area"  # Note: datasets.py defines this explicitly as "Area", to allow it to be a standalone script. iso2 country code. Default of "Area" aligns with the EU Traces online reporting platform.

stats_unit_type_column = "Unit"  # name of unit type column in the stats tabl

iso3_country_column = "Country"

iso2_country_column = "ProducerCountry"  # iso2 country code. Default of "ProducerCountry" aligns with the EU Traces online reporting platform.

admin_1_column = "Admin_Level_1"

centroid_x_coord_column = "Centroid_lon"

centroid_y_coord_column = "Centroid_lat"

external_id_column = "external_id"

geometry_type_column = "Geometry_type"

plot_id_column = "plotId"

water_flag = "In_waterbody"

geometry_column = "geo"  # geometry column name, stored as a string.

# reformatting numbers to decimal places (e.g. '%.3f' is 3 dp)
geometry_area_column_formatting = "%.3f"

stats_area_columns_formatting = "%.3f"

stats_percent_columns_formatting = "%.1f"

# lookup path - for dataset info (GEE datasets, context columns, and metadata)
DEFAULT_LOOKUP_TABLE_PATH = Path(__file__).parent / "lookup_datasets.csv"

# Per-year series whose prep functions run to the current year (see datasets.py). When the
# lookup is read, each is extended to this year by copying its newest row, so a new year needs
# no manual row in January. A real row in the CSV always wins.
YEAR_SERIES_TO_CURRENT_YEAR = (
    "MODIS_fire_",
    "GLAD-L_year_",
    "GLAD-S2_year_",
    "RADD_year_",
)

# Fixed when the package loads, like CURRENT_YEAR in datasets.py, so the lookup and the prep
# functions agree on the newest year for the whole session (even one running over New Year)
CURRENT_YEAR = datetime.now().year


def read_lookup_table(path=DEFAULT_LOOKUP_TABLE_PATH):
    """Read a lookup CSV, extending the per-year series above to the current year."""
    lookup = pd.read_csv(path)
    if "name" not in lookup.columns:
        return lookup  # e.g. a custom file without dataset rows: nothing to extend
    names = lookup["name"].astype(str)
    extra = []
    for prefix in YEAR_SERIES_TO_CURRENT_YEAR:
        rows = lookup[names.str.fullmatch(rf"{re.escape(prefix)}\d{{4}}")]
        if rows.empty:
            continue
        newest = rows.loc[rows["name"].str[-4:].astype(int).idxmax()]
        newest_year = int(newest["name"][-4:])
        for year in range(newest_year + 1, CURRENT_YEAR + 1):
            row = newest.copy()
            row["name"] = f"{prefix}{year}"
            if "order" in lookup.columns:
                row["order"] = newest["order"] + (year - newest_year)
            extra.append(row)
    if not extra:
        return lookup
    extra = pd.DataFrame(extra).astype(lookup.dtypes.to_dict())
    return pd.concat([lookup, extra], ignore_index=True)

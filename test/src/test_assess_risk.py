from pathlib import Path
import ee
from whisp.src.stats import whisp_stats_geojson_to_df

import pandas as pd


GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "geojson_example.geojson"
)


def test_whisp_stats_geojson_to_df() -> None:

    df_stats = whisp_stats_geojson_to_df(GEOJSON_EXAMPLE_FILEPATH)
    assert isinstance(df_stats, pd.DataFrame)
    assert len(df_stats) == 6
    print(df_stats)

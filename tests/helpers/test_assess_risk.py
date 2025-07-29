from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import whisp_risk

import pandas as pd


GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "geojson_example.geojson"
)


def test_whisp_stats_geojson_to_df() -> None:

    df_stats = whisp_formatted_stats_geojson_to_df(GEOJSON_EXAMPLE_FILEPATH)
    df_stats_with_risk = whisp_risk(df_stats)
    assert isinstance(df_stats_with_risk, pd.DataFrame)
    assert len(df_stats_with_risk) == 36
    print(df_stats_with_risk)

from pathlib import Path
from src.assess_risk import whisp_stats_from_geojson_roi

import pandas as pd


GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parent / "fixtures" / "geojson_example.geojson"
)


def test_whisp_stats_from_geojson_roi() -> None:

    df_stats = whisp_stats_from_geojson_roi(GEOJSON_EXAMPLE_FILEPATH)
    assert isinstance(df_stats, pd.DataFrame)
    assert len(df_stats) == 6

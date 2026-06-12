from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import whisp_risk

import pandas as pd


GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "geojson_example.geojson"
)


EXPECTED_RISK_COLS = ["risk_pcrop", "risk_acrop", "risk_cattle", "risk_timber"]
EXPECTED_RISK_VALUES = {"low", "high", "more_info_needed"}
EXPECTED_INDICATOR_COLS = [
    "Ind_01_treecover",
    "Ind_02_commodities",
    "Ind_03_disturbance_before_2020",
    "Ind_04_disturbance_after_2020",
    "Ind_12_pasture_2020",
]


def test_whisp_stats_geojson_to_df() -> None:

    df_stats = whisp_formatted_stats_geojson_to_df(GEOJSON_EXAMPLE_FILEPATH)
    df_stats_with_risk = whisp_risk(df_stats)
    assert isinstance(df_stats_with_risk, pd.DataFrame)
    assert len(df_stats_with_risk) == 50
    for col in EXPECTED_RISK_COLS:
        assert col in df_stats_with_risk.columns, f"missing risk column: {col}"
        assert set(df_stats_with_risk[col].dropna()).issubset(
            EXPECTED_RISK_VALUES
        ), f"unexpected values in {col}"
    for col in EXPECTED_INDICATOR_COLS:
        assert col in df_stats_with_risk.columns, f"missing indicator column: {col}"
    print(df_stats_with_risk)

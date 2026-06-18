from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import whisp_risk

import pandas as pd


GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "geojson_example.geojson"
)


EXPECTED_RISK_COLS = ["risk_pcrop", "risk_acrop", "risk_timber"]
EXPECTED_RISK_VALUES = {"low", "high", "more_info_needed"}
EXPECTED_INDICATOR_COLS = [
    "Ind_01_treecover",
    "Ind_02_commodities",
    "Ind_03_disturbance_before_2020",
    "Ind_04_disturbance_after_2020",
    "Ind_05_primary_2020",
    "Ind_06_nat_reg_forest_2020",
    "Ind_07a_planted_2020",
    "Ind_07b_plantation_2020",
    "Ind_08a_planted_after_2020",
    "Ind_08b_plantation_after_2020",
    "Ind_09_treecover_after_2020",
    "Ind_10_agri_after_2020",
    "Ind_11_logging_concession_before_2020",
    "Ind_12_other_land_2020",
    "Ind_13_other_land_after_2020",
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
    ind_cols = [c for c in df_stats_with_risk.columns if c.startswith("Ind_")]
    extra_cols = [c for c in ["primary_2025"] if c in df_stats_with_risk.columns]
    print(df_stats_with_risk[["risk_timber"] + ind_cols + extra_cols].to_string())
    print(
        "\nrisk_timber distribution:\n",
        df_stats_with_risk["risk_timber"].value_counts(),
    )

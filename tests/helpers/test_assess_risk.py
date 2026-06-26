from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import whisp_risk, add_risk_timber_col

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
    "Ind_08b_plantation_after_2020",
    "Ind_09_treecover_after_2020",
    "Ind_10_agri_after_2020",
    "Ind_12_other_land_2020",
    "Ind_13_other_land_after_2020",
    "Ind_14_primary_2025",
    "Ind_15_agriculture_2020",
    "Ind_16_plantation_presence_2025",
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


# Indicator column names used directly by add_risk_timber_col (yes/no per-plot signals).
_IND_1 = "Ind_01_treecover"
_IND_2 = "Ind_02_commodities"
_IND_5 = "Ind_05_primary_2020"
_IND_6 = "Ind_06_nat_reg_forest_2020"
_IND_7A = "Ind_07a_planted_2020"
_IND_7B = "Ind_07b_plantation_2020"
_IND_8B = "Ind_08b_plantation_after_2020"
_IND_9 = "Ind_09_treecover_after_2020"
_IND_10 = "Ind_10_agri_after_2020"
_IND_12 = "Ind_12_other_land_2020"
_IND_13 = "Ind_13_other_land_after_2020"
_IND_15 = "Ind_15_agriculture_2020"
_IND_16 = "Ind_16_plantation_presence_2025"
_PRIMARY_2025 = "primary_2025"

_TIMBER_INPUT_COLS = [
    _IND_1,
    _IND_2,
    _IND_5,
    _IND_6,
    _IND_7A,
    _IND_7B,
    _IND_8B,
    _IND_9,
    _IND_10,
    _IND_12,
    _IND_13,
    _IND_15,
    _IND_16,
    _PRIMARY_2025,
]


def _timber_row(label, expected_risk, expected_pathway, **overrides):
    """Build one synthetic plot. All Ind_/primary_2025 columns default to 'no'; override the
    ones the terminal needs. Returns a dict carrying the case label and expectations alongside
    the yes/no signal columns."""
    row = {col: "no" for col in _TIMBER_INPUT_COLS}
    row.update(overrides)
    row["_case"] = label
    row["_expected_risk"] = expected_risk
    row["_expected_pathway"] = expected_pathway
    return row


def test_timber_decision_tree_terminals() -> None:
    """Per-terminal check of add_risk_timber_col on a synthetic DataFrame (one row per terminal,
    NO GEE). Asserts both risk_timber and risk_timber_pathway at every terminal. This is the
    alignment guarantee that the map/viewer/diagram match the tree in risk.py."""
    cases = [
        # Rule 0: other land 2020 (terminal sub-branch, both legs).
        _timber_row(
            "rule0_other_land_stable",
            "low",
            "low: other land 2020 (stable)",
            **{_IND_12: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "rule0_other_land_changed",
            "more_info_needed",
            "more-info: other land 2020 changed",
            **{_IND_12: "yes", _IND_13: "no"},
        ),
        # Rule 1: agriculture / commodity 2020.
        _timber_row(
            "rule1_agriculture_2020",
            "low",
            "low: agriculture 2020",
            **{_IND_15: "yes"},
        ),
        # Rule 2: deforestation (treecover 2020 AND agriculture after 2020).
        _timber_row(
            "rule2_deforestation",
            "high",
            "high: deforestation",
            **{_IND_1: "yes", _IND_10: "yes"},
        ),
        # Rule 3: plantation 2020.
        _timber_row(
            "rule3_plantation_stable",  # stable via Ind_16 PRESENCE (not the gain)
            "low",
            "low: stable plantation",
            **{_IND_7B: "yes", _IND_16: "yes"},
        ),
        _timber_row(
            "rule3_plantation_no_2025",  # no presence, no other-land -> more info
            "more_info_needed",
            "more-info: plantation 2020, no 2025 state",
            **{_IND_7B: "yes"},
        ),
        _timber_row(
            "rule3_plantation_to_other_land",  # plantation 2020 -> other land 2025 (code 18 on the map)
            "low",
            "low: plantation 2020 -> other land",
            **{_IND_7B: "yes", _IND_13: "yes"},
        ),
        # Rule 4: regenerating-planted 2020.
        _timber_row(
            "rule4_regen_to_plantation",  # code 7 via the Ind_08b GAIN
            "high",
            "high: regen->plantation degradation",
            **{_IND_6: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            "rule4_regen_stayed",  # stayed forest (treecover 2020 + regen-2025, no new plantation)
            "low",
            "low: regen stayed forest",
            **{_IND_6: "yes", _IND_1: "yes", _IND_9: "yes"},
        ),
        _timber_row(
            "rule4_regen_to_other_land",
            "low",
            "low: regen 2020 -> other land",
            **{_IND_6: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "rule4_regen_more_info",
            "more_info_needed",
            "more-info: regen 2020, no 2025 state",
            **{_IND_6: "yes"},
        ),
        # Rule 5: primary 2020.
        _timber_row(
            "rule5_primary_still",  # strict primary_2025
            "low",
            "low: still primary",
            **{_IND_5: "yes", _PRIMARY_2025: "yes"},
        ),
        _timber_row(
            # MISSED-HIGH fix: primary_2025=yes (disturbance NOT detected) BUT a new plantation was gained ->
            # degradation HIGH, NOT masked as "still primary". Regression test for the `and not plantation_2025`
            # gate added to Rule 5. Without the fix this row read "low: still primary".
            "rule5_primary_still_but_new_plantation",
            "high",
            "high: primary->plantation degradation",
            **{_IND_5: "yes", _PRIMARY_2025: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            "rule5_primary_to_plantation",  # code 12 via the Ind_08b GAIN
            "high",
            "high: primary->plantation degradation",
            **{_IND_5: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            "rule5_primary_to_other_land",
            "low",
            "low: primary 2020 -> other land",
            **{_IND_5: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "rule5_primary_more_info",
            "more_info_needed",
            "more-info: primary 2020, no 2025 state",
            **{_IND_5: "yes"},
        ),
        # Rule 6: nothing recognised in 2020 (non-forest, no 2020 state) -> more info.
        _timber_row(
            "rule6_state_unknown",
            "more_info_needed",
            "more-info: 2020 state unknown",
        ),
    ]

    df = pd.DataFrame(cases)

    add_risk_timber_col(
        df=df,
        ind_1_name=_IND_1,
        ind_2_name=_IND_2,
        ind_5_name=_IND_5,
        ind_6_name=_IND_6,
        ind_7a_name=_IND_7A,
        ind_7b_name=_IND_7B,
        ind_8b_name=_IND_8B,
        ind_9_name=_IND_9,
        ind_10_name=_IND_10,
        ind_12_name=_IND_12,
        ind_13_name=_IND_13,
        ind_15_name=_IND_15,
        ind_16_name=_IND_16,
        primary_2025_name=_PRIMARY_2025,
    )

    print(
        "\nper-terminal results:\n",
        df[["_case", "risk_timber", "risk_timber_pathway"]].to_string(),
    )

    for _, row in df.iterrows():
        assert row["risk_timber"] == row["_expected_risk"], (
            f"{row['_case']}: risk_timber={row['risk_timber']!r} "
            f"expected {row['_expected_risk']!r}"
        )
        assert row["risk_timber_pathway"] == row["_expected_pathway"], (
            f"{row['_case']}: risk_timber_pathway={row['risk_timber_pathway']!r} "
            f"expected {row['_expected_pathway']!r}"
        )

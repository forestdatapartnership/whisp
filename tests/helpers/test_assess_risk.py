import itertools
from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import (
    whisp_risk,
    add_risk_timber_col,
    add_risk_pcrop_col,
    add_risk_acrop_col,
)
from openforis_whisp import decision_tree as _decision_tree
from openforis_whisp import pcrop_tree_export as _pcrop
from openforis_whisp import acrop_tree_export as _acrop

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
    "Ind_17_disturbance_after_2020_timber",
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
_IND_17 = "Ind_17_disturbance_after_2020_timber"
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
    _IND_17,
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
        # ---- non-forest half (forest_2020 = no) ----
        _timber_row(
            "nonforest_other_land_stable",
            "low",
            "low: other land 2020 (stable)",
            **{_IND_12: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "nonforest_other_land_changed",
            "more_info_needed",
            "more-info: other land 2020 changed",
            **{_IND_12: "yes"},
        ),
        _timber_row(
            "nonforest_agriculture_2020",
            "low",
            "low: agriculture 2020",
            **{_IND_15: "yes"},
        ),
        _timber_row(
            "nonforest_unclassified",  # incl. afforestation (non-forest 2020 -> new plantation)
            "more_info_needed",
            "more-info: non-forest 2020, unclassified",
        ),
        # ---- forest half (forest_2020 = yes) ----
        _timber_row(
            "forest_deforestation",
            "high",
            "high: deforestation",
            **{_IND_1: "yes", _IND_10: "yes"},
        ),
        _timber_row(
            # ROOT DELTA (exoneration closed): a plot that is BOTH forest-2020 (Ind_05) and pre-2020
            # agriculture (Ind_15), cleared to ag after 2020, is no longer exonerated (agriculture-2020 now
            # sits inside the non-forest half). It reaches the deforestation HIGH. Current tree read this
            # "low: agriculture 2020".
            "forest_and_ag2020_overlap_deforestation",
            "high",
            "high: deforestation",
            **{_IND_5: "yes", _IND_15: "yes", _IND_10: "yes"},
        ),
        # plantation-2020 branch: Ind_07b is NOT in the forest gate union, so these rows need Ind_01 to
        # route into the forest half.
        _timber_row(
            "plantation_stable",  # Ind_16 PRESENCE -> LOW
            "low",
            "low: stable plantation",
            **{_IND_1: "yes", _IND_7B: "yes", _IND_16: "yes"},
        ),
        _timber_row(
            "plantation_to_other_land",
            "low",
            "low: plantation 2020 -> other land",
            **{_IND_1: "yes", _IND_7B: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "plantation_no_2025",
            "more_info_needed",
            "more-info: plantation 2020, no 2025 state",
            **{_IND_1: "yes", _IND_7B: "yes"},
        ),
        # primary-2020 branch (checked BEFORE regen; plantation-2025 GAIN checked FIRST)
        _timber_row(
            "primary_to_plantation",
            "high",
            "high: primary->plantation degradation",
            **{_IND_5: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            # MISSED-HIGH still caught: still-primary but a NEW plantation -> HIGH, because plantation-2025
            # is checked first (no `and not plantation_2025` guard needed anymore).
            "primary_still_but_new_plantation",
            "high",
            "high: primary->plantation degradation",
            **{_IND_5: "yes", _PRIMARY_2025: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            "primary_still",
            "low",
            "low: still primary",
            **{_IND_5: "yes", _PRIMARY_2025: "yes"},
        ),
        _timber_row(
            # Ind_09_treecover_after_2020 DROPPED from the primary branch: a primary plot showing only canopy
            # but knocked out of still-primary by disturbance is no longer rescued to LOW; it lands on
            # more-info unless it converted to other land.
            "primary_disturbed_canopy_only",
            "more_info_needed",
            "more-info: primary 2020, no 2025 state",
            **{_IND_5: "yes", _IND_9: "yes"},
        ),
        _timber_row(
            "primary_to_other_land",
            "low",
            "low: primary 2020 -> other land",
            **{_IND_5: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "primary_more_info",
            "more_info_needed",
            "more-info: primary 2020, no 2025 state",
            **{_IND_5: "yes"},
        ),
        # regen-2020 branch (plantation-2025 GAIN checked FIRST)
        _timber_row(
            "regen_to_plantation",
            "high",
            "high: regen->plantation degradation",
            **{_IND_6: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            # no new plantation, no disturbance -> stayed-forest LOW. Ind_09 dropped, so a plain regen plot
            # (nothing adverse detected) is now LOW by absence of disturbance, not by canopy presence.
            "regen_stayed_forest",
            "low",
            "low: regen stayed forest",
            **{_IND_6: "yes"},
        ),
        _timber_row(
            "regen_to_other_land",
            "low",
            "low: regen 2020 -> other land",
            **{_IND_6: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # regen + DISTURBANCE (Ind_17), no plantation-gain, no other-land -> more-info. This is now the
            # ONLY route to the regen more-info leg (a no-disturbance regen plot is LOW, above).
            "regen_disturbed",
            "more_info_needed",
            "more-info: regen 2020, no 2025 state",
            **{_IND_6: "yes", _IND_17: "yes"},
        ),
        # forest-gate plot with no recognised class or change -> more info
        _timber_row(
            "forest_unclassified",  # treecover 2020 only, no class, no change
            "more_info_needed",
            "more-info: forest 2020, unclassified",
            **{_IND_1: "yes"},
        ),
        _timber_row(
            # treecover-only forest (no primary/regen/plantation class) that became other land: keeps the
            # other-land chance -> revived code 14 low, not more-info.
            "forest_unclassified_to_other_land",
            "low",
            "low: forest 2020 -> other land",
            **{_IND_1: "yes", _IND_13: "yes"},
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


# Crop indicator column names read by the flat crop-risk rules and their decision-tree mirrors.
_CROP_IND_1 = "Ind_01_treecover"
_CROP_IND_2 = "Ind_02_commodities"
_CROP_IND_3 = "Ind_03_disturbance_before_2020"
_CROP_IND_4 = "Ind_04_disturbance_after_2020"


def _crop_bools(spec, row):
    """Answer each tree question the way the JS / EE walkers do: a question is 'yes' when ANY of its
    q_to_columns indicators reads 'yes' on this plot."""
    return {q: any(row[c] == "yes" for c in cols) for q, cols in spec.q_to_columns.items()}


def test_pcrop_acrop_decision_tree_terminals() -> None:
    """Drift guard for the crop DISPLAY trees. Unlike timber (whose TIMBER_ROOT_TREE IS the
    computation and is guarded by test_timber_decision_tree_terminals), PCROP_SPEC / ACROP_SPEC are
    SEPARATE mirrors that only draw the Mermaid diagram and paint the map; the authority for the
    outcome is the flat rules add_risk_pcrop_col / add_risk_acrop_col in risk.py. This test walks
    each spec over EVERY input combination and asserts it reproduces the flat rule exactly, so the
    crop diagram/map can never silently diverge from risk.py. Offline, no Earth Engine.

    Perennial (pcrop) consults disturbances before AND after 2020 (Ind_03_disturbance_before_2020 +
    Ind_04_disturbance_after_2020); annual (acrop) DROPS the before-2020 disturbance because annual
    crops are not typically established under significant canopy. That structural divergence is
    intentional (documented in the README) and is exactly what these two specs encode."""
    # pcrop uses all four indicators -> 16 combinations
    for vals in itertools.product(["yes", "no"], repeat=4):
        row = dict(zip([_CROP_IND_1, _CROP_IND_2, _CROP_IND_3, _CROP_IND_4], vals))
        df = pd.DataFrame([row])
        add_risk_pcrop_col(df, _CROP_IND_1, _CROP_IND_2, _CROP_IND_3, _CROP_IND_4)
        authoritative = df.iloc[0]["risk_pcrop"]
        walked, _pathway = _decision_tree.eval_tree(
            _crop_bools(_pcrop.PCROP_SPEC, row), _pcrop.PCROP_SPEC
        )
        assert walked == authoritative, (
            f"pcrop tree drifted from add_risk_pcrop_col at {row}: "
            f"tree={walked!r} vs flat-rule={authoritative!r}"
        )

    # acrop ignores Ind_03_disturbance_before_2020 -> 8 combinations
    for vals in itertools.product(["yes", "no"], repeat=3):
        row = dict(zip([_CROP_IND_1, _CROP_IND_2, _CROP_IND_4], vals))
        df = pd.DataFrame([row])
        add_risk_acrop_col(df, _CROP_IND_1, _CROP_IND_2, _CROP_IND_4)
        authoritative = df.iloc[0]["risk_acrop"]
        walked, _pathway = _decision_tree.eval_tree(
            _crop_bools(_acrop.ACROP_SPEC, row), _acrop.ACROP_SPEC
        )
        assert walked == authoritative, (
            f"acrop tree drifted from add_risk_acrop_col at {row}: "
            f"tree={walked!r} vs flat-rule={authoritative!r}"
        )

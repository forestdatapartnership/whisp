import itertools
import re
from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import (
    whisp_risk,
    add_risk_timber_col,
    add_risk_pcrop_col,
    add_risk_acrop_col,
    lookup_gee_datasets_df,
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
    "Ind_07_planted_plantations_2020",
    "Ind_08_planted_plantations_after_2020",
    "Ind_09_treecover_after_2020",
    "Ind_10_agri_after_2020",
    "Ind_11_logging_concession_before_2020",
    "Ind_12_other_land_2020",
    "Ind_15_cropland_2020",
    "Ind_20_planted_forest_2020",
    "Ind_21_plantation_forest_2020",
    "Ind_22_plantation_forest_gain_after_2020",
]


def _period_columns_below_their_years(df):
    """Names of before/after 2020 columns that are smaller than one of their year columns.

    A combined column covers its year columns (e.g. GLAD-L_before_2020 covers GLAD-L_year_2017 to
    GLAD-L_year_2020, TMF_def_after_2020 covers TMF_def_2021 onwards), so it can never be smaller
    than any one of them. Columns without year columns (e.g. PRODES, DETER) are skipped.
    """
    below = []
    for col in df.columns:
        match = re.fullmatch(r"(.+)_(before|after)_2020", col)
        if not match:
            continue
        prefix, period = match.groups()
        year_cols = []
        for other in df.columns:
            year_match = re.fullmatch(re.escape(prefix) + r"(_year)?_(\d{4})", other)
            if not year_match:
                continue
            is_before = int(year_match.group(2)) <= 2020
            if is_before == (period == "before"):
                year_cols.append(other)
        if year_cols and (df[year_cols].max(axis=1) - df[col] > 0.01).any():
            below.append(col)
    return below


def test_whisp_stats_geojson_to_df() -> None:

    df_stats = whisp_formatted_stats_geojson_to_df(GEOJSON_EXAMPLE_FILEPATH)
    df_stats_with_risk = whisp_risk(df_stats)
    assert isinstance(df_stats_with_risk, pd.DataFrame)
    assert len(df_stats_with_risk) == 50
    # Regression guard (GLAD-L homogeneous-collection crash): the GLAD-L alert bands must compute
    # over the whole fixture - including the South America plots that trip the UpdResult mosaic -
    # and be emitted as numeric columns.
    for col in ("GLAD-L_after_2020", "GLAD-L_before_2020"):
        assert col in df_stats_with_risk.columns
        assert pd.api.types.is_numeric_dtype(df_stats_with_risk[col])
    # Regression guard (GLAD-L year join, fixed in #287): joining the yearly layers so that one
    # year's missing data drops the other years' alerts makes a combined column smaller than its
    # year columns.
    below = _period_columns_below_their_years(df_stats_with_risk)
    assert not below, f"smaller than one of their year columns: {below}"
    for col in EXPECTED_RISK_COLS:
        assert col in df_stats_with_risk.columns, f"missing risk column: {col}"
        assert set(df_stats_with_risk[col].dropna()).issubset(
            EXPECTED_RISK_VALUES
        ), f"unexpected values in {col}"
    for col in EXPECTED_INDICATOR_COLS:
        assert col in df_stats_with_risk.columns, f"missing indicator column: {col}"
    ind_cols = [c for c in df_stats_with_risk.columns if c.startswith("Ind_")]
    extra_cols = ["risk_timber_pathway"]
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
_IND_7A = "Ind_20_planted_forest_2020"
_IND_7B = "Ind_21_plantation_forest_2020"
_IND_8B = "Ind_22_plantation_forest_gain_after_2020"
_IND_9 = "Ind_09_treecover_after_2020"
_IND_10 = "Ind_10_agri_after_2020"
_IND_12 = "Ind_12_other_land_2020"
_IND_13 = "Ind_13_other_land_after_2020"
_IND_15 = "Ind_15_cropland_2020"
_IND_16 = "Ind_16_plantation_presence_2025"
_IND_4 = "Ind_04_disturbance_after_2020"
_PRIMARY_2025 = "primary_2025"
_IND_18 = "Ind_18_regen_planted_2025"
_REGEN_STILL = "regen_planted_still_2025"
_PLANTATION_FOREST = "plantation_forest_2020"

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
    _IND_4,
    _PRIMARY_2025,
    _IND_18,
    _REGEN_STILL,
    _PLANTATION_FOREST,
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
        # plantation-2020 branch: Ind_21 opens the forest gate only through the derived
        # plantation_forest_2020 (no agriculture in 2020); with tree cover it is in the forest half anyway.
        _timber_row(
            "plantation_stable",  # Ind_16 PRESENCE -> LOW
            "low",
            "low: still plantation forest",
            **{_IND_1: "yes", _IND_7B: "yes", _IND_16: "yes"},
        ),
        _timber_row(
            "plantation_only_opens_the_gate",
            "low",
            "low: still plantation forest",
            **{_PLANTATION_FOREST: "yes", _IND_7B: "yes", _IND_16: "yes"},
        ),
        _timber_row(
            "plantation_without_the_derived_column_stays_non_forest",
            "more_info_needed",
            "more-info: non-forest 2020, unclassified",
            **{_IND_7B: "yes", _IND_16: "yes"},
        ),
        _timber_row(
            "plantation_to_other_land",
            "low",
            "low: plantation forest 2020 -> other land",
            **{_IND_1: "yes", _IND_7B: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "plantation_no_2025",
            "more_info_needed",
            "more-info: plantation forest 2020, no later state",
            **{_IND_1: "yes", _IND_7B: "yes"},
        ),
        # primary-2020 branch (checked BEFORE regen; plantation-2025 GAIN checked FIRST)
        _timber_row(
            "primary_to_plantation",
            "high",
            "high: primary -> plantation forest (degradation)",
            **{_IND_5: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            # MISSED-HIGH still caught: still-primary but a NEW plantation -> HIGH, because plantation-2025
            # is checked first (no `and not plantation_2025` guard needed anymore).
            "primary_still_but_new_plantation",
            "high",
            "high: primary -> plantation forest (degradation)",
            **{_IND_5: "yes", _PRIMARY_2025: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            "primary_still",
            "low",
            "low: still primary",
            **{_IND_5: "yes", _PRIMARY_2025: "yes"},
        ),
        # 2026-11-03: on the primary leg DISTURBANCE after 2020 (Ind_04, the crop decision trees' set) is asked right
        # after the plantation gain, and a disturbed primary plot is MORE INFO: disturbance is not itself a
        # non-compliant end state, but it must never read still primary -> LOW. The old LOW exits (became
        # regenerating / planted, became other land) are gone: ForTy V2 still calls much cleared primary
        # forest "primary" or "regenerating". HIGH still needs agriculture or a plantation gain.
        _timber_row(
            "primary_disturbed",
            "more_info_needed",
            "more-info: primary 2020, disturbed after 2020",
            **{_IND_5: "yes", _IND_4: "yes"},
        ),
        _timber_row(
            # formerly LOW (code 20 / 16): disturbance now wins over a regen or other-land 2025 state
            "primary_disturbed_then_regen_or_other_land",
            "more_info_needed",
            "more-info: primary 2020, disturbed after 2020",
            **{_IND_5: "yes", _IND_4: "yes", _IND_18: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # the order inside the leg: a plantation gain is still read as degradation first
            "primary_disturbed_and_new_plantation",
            "high",
            "high: primary -> plantation forest (degradation)",
            **{_IND_5: "yes", _IND_4: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            # agriculture after 2020 is asked before the forest classes, so this stays deforestation
            "primary_disturbed_and_agriculture",
            "high",
            "high: deforestation",
            **{_IND_5: "yes", _IND_4: "yes", _IND_10: "yes"},
        ),
        _timber_row(
            # a disturbed plot can never read still primary: the tree asks disturbance before primary_2025
            "primary_disturbed_even_if_still_primary",
            "more_info_needed",
            "more-info: primary 2020, disturbed after 2020",
            **{_IND_5: "yes", _IND_4: "yes", _PRIMARY_2025: "yes"},
        ),
        _timber_row(
            # still primary = no with no disturbance: not reachable through whisp_risk today (still primary
            # is not disturbed and not converted), kept for when planted forest is split out
            "primary_changed_to_regen_without_disturbance",
            "more_info_needed",
            "more-info: primary 2020, changed without disturbance",
            **{_IND_5: "yes", _IND_18: "yes"},
        ),
        _timber_row(
            # other land 2025 is no longer asked on the primary leg (was LOW, code 16)
            "primary_changed_to_other_land_without_disturbance",
            "more_info_needed",
            "more-info: primary 2020, changed without disturbance",
            **{_IND_5: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # Ind_09_treecover_after_2020 stays out of the primary branch: canopy alone rescues nothing
            "primary_changed_canopy_only",
            "more_info_needed",
            "more-info: primary 2020, changed without disturbance",
            **{_IND_5: "yes", _IND_9: "yes"},
        ),
        _timber_row(
            "primary_more_info",
            "more_info_needed",
            "more-info: primary 2020, changed without disturbance",
            **{_IND_5: "yes"},
        ),
        # regen-2020 branch (plantation-2025 GAIN checked FIRST)
        _timber_row(
            "regen_to_plantation",
            "high",
            "high: regenerating -> plantation forest (degradation)",
            **{_IND_6: "yes", _IND_8B: "yes"},
        ),
        _timber_row(
            # no new plantation and still regenerating / planted in 2025 (the derived column: regen 2020
            # AND NOT Ind_04) -> stayed-forest LOW, by absence of disturbance, not by canopy presence.
            "regen_stayed_forest",
            "low",
            "low: regen stayed forest",
            **{_IND_6: "yes", _REGEN_STILL: "yes"},
        ),
        _timber_row(
            # 2026-07-29 planned diagram: the still-forest question comes BEFORE other land, so a regen plot
            # that is still forest AND also flags other land 2025 reads stayed-forest (code 3), not 17.
            "regen_still_forest_and_other_land",
            "low",
            "low: regen stayed forest",
            **{_IND_6: "yes", _REGEN_STILL: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            "regen_to_other_land",
            "low",
            "low: regen 2020 -> other land",
            **{_IND_6: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # regen + DISTURBANCE (Ind_04), no plantation-gain, no other-land -> more-info. This is now the
            # ONLY route to the regen more-info leg (a no-disturbance regen plot is LOW, above).
            "regen_disturbed",
            "more_info_needed",
            "more-info: regen 2020, no later state",
            **{_IND_6: "yes", _IND_4: "yes"},
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
        _timber_row(
            # CAVEAT-28 FIX (plantation checked LAST): a genuine primary-2020 plot that a broad plantation
            # layer ALSO flags (Ind_21), with a NEW plantation gain (Ind_22). Under the old plantation-first
            # order this was masked as "low: still plantation forest"; now primary wins the overlap and the
            # degradation HIGH fires. Locks in the reorder.
            "primary_and_plantation2020_overlap_degradation",
            "high",
            "high: primary -> plantation forest (degradation)",
            **{
                _IND_1: "yes",
                _IND_5: "yes",
                _IND_7B: "yes",
                _IND_16: "yes",
                _IND_8B: "yes",
            },
        ),
    ]

    df = pd.DataFrame(cases)

    add_risk_timber_col(
        df=df,
        ind_1_name=_IND_1,
        ind_2_name=_IND_2,
        ind_5_name=_IND_5,
        ind_6_name=_IND_6,
        ind_20_name=_IND_7A,
        ind_21_name=_IND_7B,
        ind_22_name=_IND_8B,
        ind_9_name=_IND_9,
        ind_10_name=_IND_10,
        ind_12_name=_IND_12,
        ind_13_name=_IND_13,
        ind_15_name=_IND_15,
        ind_16_name=_IND_16,
        plantation_forest_2020_name=_PLANTATION_FOREST,
        primary_2025_name=_PRIMARY_2025,
        ind_18_name=_IND_18,
        regen_planted_still_2025_name=_REGEN_STILL,
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


def _synthetic_plot(**hectares):
    """One 10 ha plot with every global dataset column at 0 ha except the ones given."""
    lut = lookup_gee_datasets_df
    names = lut[
        (lut["exclude_from_output"] != 1)
        & lut["ISO2_code"].isna()
        & (lut["theme"] != "context_and_metadata")
    ]["name"]
    row = {name: 0.0 for name in names}
    row.update({"Area": 10.0, "Unit": "ha"})
    row.update(hectares)
    return row


def test_timber_primary_leg_through_whisp_risk() -> None:
    """The primary leg end to end through whisp_risk (no GEE): the lookup rows behind Ind_04 and Ind_22,
    the derived primary_2025 (not disturbed, not converted into plantation forest) and the tree. Covers
    the case a ForTy-only "still primary" rule got wrong (cleared forest ForTy still calls primary) and
    primary that only TMF calls primary."""
    cases = [
        # (label, hectares, risk_timber, pathway)
        (
            "disturbed_by_hansen_loss",
            {
                "ForTy_v2_primary_2020": 8.0,
                "ForTy_v2_primary_2025": 8.0,
                "GFC_loss_after_2020": 2.0,
            },
            "more_info_needed",
            "more-info: primary 2020, disturbed after 2020",
        ),
        (
            "disturbed_by_tmf",
            {"ForTy_v2_primary_2020": 8.0, "TMF_def_after_2020": 2.0},
            "more_info_needed",
            "more-info: primary 2020, disturbed after 2020",
        ),
        (
            "converted_into_plantation_forest",
            {
                "ForTy_v2_primary_2020": 8.0,
                "ForTy_v2_plantation_forest_gain_after_2020": 3.0,
            },
            "high",
            "high: primary -> plantation forest (degradation)",
        ),
        (
            "still_primary",
            {"ForTy_v2_primary_2020": 8.0, "ForTy_v2_primary_2025": 8.0},
            "low",
            "low: still primary",
        ),
        (
            "primary_only_per_tmf",
            {"TMF_undist": 8.0, "ForTy_v2_regen_planted_latest": 8.0},
            "low",
            "low: still primary",
        ),
        (
            # an alert layer counts too: timber reads the same disturbance as the crop decision trees
            "disturbed_by_radd_alert",
            {"ForTy_v2_primary_2020": 8.0, "RADD_after_2020": 2.0},
            "more_info_needed",
            "more-info: primary 2020, disturbed after 2020",
        ),
        (
            # below the 10 % threshold: 0.5 ha of a 10 ha plot is not "disturbed"
            "small_disturbance",
            {"ForTy_v2_primary_2020": 8.0, "GFC_loss_after_2020": 0.5},
            "low",
            "low: still primary",
        ),
    ]
    df = pd.DataFrame([_synthetic_plot(**ha) for _, ha, _, _ in cases])
    out = whisp_risk(df)
    for i, (label, _, risk, pathway) in enumerate(cases):
        got = (out["risk_timber"].iloc[i], out["risk_timber_pathway"].iloc[i])
        assert got == (risk, pathway), f"{label}: got {got}, expected {(risk, pathway)}"
    assert "Ind_19_primary_change_after_2020" not in out.columns


def test_plantation_opens_the_forest_gate_unless_agriculture_in_2020() -> None:
    cases = [
        # (label, hectares, risk_timber, pathway)
        (
            "plantation_forest_stable",
            {"IIASA_plantation_2020": 8.0, "ForTy_v2_plantation_latest": 8.0},
            "low",
            "low: still plantation forest",
        ),
        (
            "plantation_forest_to_cropland",
            {"IIASA_plantation_2020": 8.0, "ESRI_crop_gain_after_2020": 3.0},
            "high",
            "high: deforestation",
        ),
        (
            # oil palm that a plantation forest map also maps: stays agriculture, no deforestation high
            "oil_palm_mapped_as_plantation",
            {
                "IIASA_plantation_2020": 8.0,
                "Oil_palm_Descals": 8.0,
                "FDaP_tree_crop_gain_after_2020": 3.0,
            },
            "low",
            "low: agriculture 2020",
        ),
    ]
    df = pd.DataFrame([_synthetic_plot(**ha) for _, ha, _, _ in cases])
    out = whisp_risk(df)
    for i, (label, _, risk, pathway) in enumerate(cases):
        got = (out["risk_timber"].iloc[i], out["risk_timber_pathway"].iloc[i])
        assert got == (risk, pathway), f"{label}: got {got}, expected {(risk, pathway)}"


def test_timber_derived_columns_keep_each_row_when_index_labels_repeat() -> None:
    # results joined together can share index labels; each plot must keep its own 2025 states
    cases = [
        ({"ForTy_v2_primary_2020": 8.0}, "low"),
        (
            {"ForTy_v2_primary_2020": 8.0, "GFC_loss_after_2020": 2.0},
            "more_info_needed",
        ),
        (
            {"ForTy_v2_nat_reg_2020": 8.0, "ForTy_v2_regen_planted_latest": 8.0},
            "low",
        ),
        ({"ForTy_v2_nat_reg_2020": 8.0}, "more_info_needed"),
    ]
    df = pd.DataFrame([_synthetic_plot(**ha) for ha, _ in cases], index=[0, 0, 1, 1])
    out = whisp_risk(df, include_working_columns=True)
    assert list(out["risk_timber"]) == [risk for _, risk in cases]
    assert list(out["primary_2025"]) == ["yes", "no", "no", "no"]


_WORKING_COLS = [
    _IND_13,
    _IND_16,
    _IND_18,
    _PRIMARY_2025,
    _REGEN_STILL,
    _PLANTATION_FOREST,
]


def test_released_timber_indicators_kept_and_working_columns_hidden() -> None:
    # Ind_07, Ind_08 and Ind_11 stay in the output for existing users (Ind_07 = planted or plantation 2020,
    # Ind_08 has no dataset so stays "no"); the decision tree's working columns are left out unless asked for
    cases = [
        {"ForTy_v2_planted_2020": 8.0},
        {"ForTy_v2_plantation_2020": 8.0},
        {"GFW_logging_before_2020": 8.0},
        {},
    ]
    df = pd.DataFrame([_synthetic_plot(**ha) for ha in cases])
    out = whisp_risk(df)
    assert list(out["Ind_07_planted_plantations_2020"]) == ["yes", "yes", "no", "no"]
    assert list(out["Ind_08_planted_plantations_after_2020"]) == ["no"] * 4
    assert list(out["Ind_11_logging_concession_before_2020"]) == [
        "no",
        "no",
        "yes",
        "no",
    ]
    assert not [c for c in _WORKING_COLS if c in out.columns]
    assert set(c for c in out.columns if c.startswith("Ind_")) == set(
        EXPECTED_INDICATOR_COLS
    )
    full = whisp_risk(df, include_working_columns=True)
    assert all(c in full.columns for c in _WORKING_COLS)
    assert list(full["risk_timber"]) == list(out["risk_timber"])


# Crop indicator column names read by the flat crop-risk rules and their decision-tree mirrors.
_CROP_IND_1 = "Ind_01_treecover"
_CROP_IND_2 = "Ind_02_commodities"
_CROP_IND_3 = "Ind_03_disturbance_before_2020"
_CROP_IND_4 = "Ind_04_disturbance_after_2020"


def _crop_bools(spec, row):
    """Answer each tree question the way the JS / EE walkers do: a question is 'yes' when ANY of its
    q_to_columns indicators reads 'yes' on this plot."""
    return {
        q: any(row[c] == "yes" for c in cols) for q, cols in spec.q_to_columns.items()
    }


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

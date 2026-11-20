import itertools
import json
import re
from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import (
    whisp_risk,
    add_risk_timber_col,
    add_risk_pcrop_col,
    add_risk_acrop_col,
    lookup_gee_datasets_df,
    DECISION_TREES,
    decision_tree_recipe,
)

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
    "Ind_13_other_land_gain_after_2020",
    "Ind_15_cropland_2020",
    "Ind_14_planted_forest_2020",
    "Ind_16_plantation_forest_2020",
    "Ind_17_plantation_forest_gain_after_2020",
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
    print(df_stats_with_risk)


# Indicator column names used directly by add_risk_timber_col (yes/no per-plot signals).
_IND_1 = "Ind_01_treecover"
_IND_2 = "Ind_02_commodities"
_IND_5 = "Ind_05_primary_2020"
_IND_6 = "Ind_06_nat_reg_forest_2020"
_IND_14 = "Ind_14_planted_forest_2020"
_IND_16 = "Ind_16_plantation_forest_2020"
_IND_17 = "Ind_17_plantation_forest_gain_after_2020"
_IND_9 = "Ind_09_treecover_after_2020"
_IND_10 = "Ind_10_agri_after_2020"
_IND_12 = "Ind_12_other_land_2020"
_IND_13 = "Ind_13_other_land_gain_after_2020"
_IND_15 = "Ind_15_cropland_2020"
_IND_4 = "Ind_04_disturbance_after_2020"

_TIMBER_INPUT_COLS = [
    _IND_1,
    _IND_2,
    _IND_5,
    _IND_6,
    _IND_14,
    _IND_16,
    _IND_17,
    _IND_9,
    _IND_10,
    _IND_12,
    _IND_13,
    _IND_15,
    _IND_4,
]


def _timber_row(label, expected_risk, expected_pathway, **overrides):
    """Build one synthetic plot. All Ind_ columns default to 'no'; override the
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
    NO GEE). Asserts both risk_timber and risk_timber_pathway at every terminal."""
    cases = [
        # ---- non-forest half (forest_2020 = no) ----
        _timber_row(
            "nonforest_other_land_stable",
            "low",
            "low: other land 2020 (stable)",
            **{_IND_12: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # still other land is worked out from other land 2020, so "changed" is not reached
            "nonforest_other_land_without_later_other_land",
            "low",
            "low: other land 2020 (stable)",
            **{_IND_12: "yes"},
        ),
        _timber_row(
            "nonforest_agriculture_2020",
            "low",
            "low: agriculture 2020",
            **{_IND_15: "yes"},
        ),
        _timber_row(
            "nonforest_unclassified",  # incl. afforestation (non-forest 2020 -> new plantation forest)
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
            # a plot that is both forest in 2020 (Ind_05) and agriculture in 2020 (Ind_15), cleared for
            # agriculture after 2020, reaches the deforestation high (agriculture in 2020 is asked only in the
            # non-forest half)
            "forest_and_ag2020_overlap_deforestation",
            "high",
            "high: deforestation",
            **{_IND_5: "yes", _IND_15: "yes", _IND_10: "yes"},
        ),
        # plantation-2020 branch: Ind_16 opens the forest gate like the other forest classes, read as it is
        _timber_row(
            "plantation_stable",
            "low",
            "low: plantation forest 2020",
            **{_IND_1: "yes", _IND_16: "yes"},
        ),
        _timber_row(
            "plantation_only_opens_the_gate",
            "low",
            "low: plantation forest 2020",
            **{_IND_16: "yes"},
        ),
        _timber_row(
            # agriculture mapped in 2020 too does not hold plantation forest out of the forest half
            "plantation_with_agriculture_2020_opens_the_gate",
            "low",
            "low: plantation forest 2020",
            **{_IND_16: "yes", _IND_2: "yes", _IND_15: "yes"},
        ),
        _timber_row(
            # other land is not asked on the plantation forest branch: low either way
            "plantation_to_other_land",
            "low",
            "low: plantation forest 2020",
            **{_IND_1: "yes", _IND_16: "yes", _IND_13: "yes"},
        ),
        # primary-2020 branch (checked BEFORE regen; plantation-2025 GAIN checked FIRST)
        _timber_row(
            "primary_to_plantation",
            "high",
            "high: primary -> plantation forest (degradation)",
            **{_IND_5: "yes", _IND_17: "yes"},
        ),
        # on the primary leg, disturbance after 2020 (Ind_04) gives more info: disturbance alone is not an end
        # state, but a disturbed plot must not read still primary (low); high still needs agriculture or a
        # plantation forest gain
        _timber_row(
            "primary_disturbed",
            "more_info_needed",
            "more-info: primary 2020, disturbed after 2020",
            **{_IND_5: "yes", _IND_4: "yes"},
        ),
        _timber_row(
            # became other land is asked before disturbance: a disturbance explained by a new mine or
            # built-up land reads low
            "primary_disturbed_then_other_land",
            "low",
            "low: primary 2020 -> other land",
            **{_IND_5: "yes", _IND_4: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # the order inside the leg: a plantation gain is still read as degradation first
            "primary_disturbed_and_new_plantation",
            "high",
            "high: primary -> plantation forest (degradation)",
            **{_IND_5: "yes", _IND_4: "yes", _IND_17: "yes"},
        ),
        _timber_row(
            # agriculture after 2020 is asked before the forest classes, so this stays deforestation
            "primary_disturbed_and_agriculture",
            "high",
            "high: deforestation",
            **{_IND_5: "yes", _IND_4: "yes", _IND_10: "yes"},
        ),
        _timber_row(
            # no gain and no disturbance: still primary (a change to regrowth or planted forest is not mapped)
            "primary_no_change",
            "low",
            "low: still primary",
            **{_IND_5: "yes"},
        ),
        _timber_row(
            # became other land on the primary leg: low (outside the EUDR, as one example)
            "primary_to_other_land",
            "low",
            "low: primary 2020 -> other land",
            **{_IND_5: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # Ind_09_treecover_after_2020 stays out of the primary branch: canopy alone changes nothing
            "primary_canopy_only",
            "low",
            "low: still primary",
            **{_IND_5: "yes", _IND_9: "yes"},
        ),
        # regen-2020 branch (plantation-2025 GAIN checked FIRST)
        _timber_row(
            "regen_to_plantation",
            "high",
            "high: regenerating -> plantation forest (degradation)",
            **{_IND_6: "yes", _IND_17: "yes"},
        ),
        _timber_row(
            # no plantation forest gain -> low
            "regen_stayed_forest",
            "low",
            "low: regen or planted forest 2020",
            **{_IND_6: "yes"},
        ),
        _timber_row(
            # other land is not asked on the regenerating branch: low either way
            "regen_to_other_land",
            "low",
            "low: regen or planted forest 2020",
            **{_IND_6: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # disturbance is not asked on the regenerating branch: harvest and regrowth, or replanting, are
            # neither deforestation nor degradation there, so with no gain the plot is low
            "regen_disturbed",
            "low",
            "low: regen or planted forest 2020",
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
            # tree cover only (no forest type mapped): more info, even if it became other land
            "forest_unclassified_to_other_land",
            "more_info_needed",
            "more-info: forest 2020, unclassified",
            **{_IND_1: "yes", _IND_13: "yes"},
        ),
        _timber_row(
            # a primary plot that a plantation forest map also covers (Ind_16), with a plantation forest gain
            # (Ind_17): primary is asked first, so the degradation high fires
            "primary_and_plantation2020_overlap_degradation",
            "high",
            "high: primary -> plantation forest (degradation)",
            **{
                _IND_1: "yes",
                _IND_5: "yes",
                _IND_16: "yes",
                _IND_17: "yes",
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
        ind_14_name=_IND_14,
        ind_16_name=_IND_16,
        ind_17_name=_IND_17,
        ind_10_name=_IND_10,
        ind_12_name=_IND_12,
        ind_13_name=_IND_13,
        ind_15_name=_IND_15,
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


# The EUDR transition table, as one example framework the timber decision tree approximates: the state in 2020
# (rows) against the state after 2020 (columns). f = free of deforestation and degradation, o = outside the
# regulation's scope (both low); D = deforestation, G = degradation (both high).
_EUDR_STATES = [
    "primary",
    "naturally regenerating",
    "planted",
    "plantation",
    "other wooded land",
    "agriculture",
    "other land",
]
_EUDR_TABLE = {
    "primary": "ffGGGDo",
    "naturally regenerating": "offGGDo",
    "planted": "offffDo",
    "plantation": "offffDo",
    "other wooded land": "ooooooo",
    "agriculture": "ooooooo",
    "other land": "ooooooo",
}
# each 2020 state as datasets on a synthetic 10 ha plot (there is no other wooded land map yet)
_EUDR_STATE_2020 = {
    "primary": {"ForTy_v2_primary_2020": 8.0},
    "naturally regenerating": {"ForTy_v2_nat_reg_2020": 8.0},
    "planted": {"ForTy_v2_planted_2020": 8.0},
    "plantation": {"ForTy_v2_plantation_2020": 8.0},
    "other wooded land": {},
    "agriculture": {"GLAD_cropland_2020": 8.0},
    "other land": {"ESRI_other_land_2020": 8.0},
}
# what the data show of each change after 2020: forest that is cut and grows back, cleared and replanted, thinned to
# scrub or cleared for a road or a small mine shows as forest loss (there is no planted forest gain or other wooded
# land map yet, and other land gain counts only when it covers most of the plot)
_LOSS = {"GFC_loss_after_2020": 3.0}
_EUDR_CHANGE = {
    "primary": {},
    "naturally regenerating": _LOSS,
    "planted": _LOSS,
    "plantation": {"ForTy_v2_plantation_forest_gain_after_2020": 3.0},
    "other wooded land": _LOSS,
    "agriculture": {"GLAD_crop_gain_after_2020": 3.0},
    "other land": _LOSS,
}
# cells where the timber decision tree gives another outcome, on purpose or until the data exist
_EUDR_DIFFERENCES = {
    # disturbed primary forest asks for a closer look
    ("primary", "naturally regenerating"): "more_info_needed",
    ("primary", "other land"): "more_info_needed",
    # no planted forest gain or other wooded land map yet
    ("primary", "planted"): "more_info_needed",
    ("primary", "other wooded land"): "more_info_needed",
    ("naturally regenerating", "other wooded land"): "low",
    **{("other wooded land", after): "more_info_needed" for after in _EUDR_STATES},
}


def test_timber_decision_tree_against_the_eudr_transition_table() -> None:
    """Every cell of the EUDR transition table on a synthetic plot (no GEE). The timber outcome matches the
    table (f or o -> low, deforestation or degradation -> high) except the listed differences, which are
    pinned so that closing a gap, or opening a new one, shows up here."""
    cells, rows = [], []
    for before in _EUDR_STATES:
        for after, code in zip(_EUDR_STATES, _EUDR_TABLE[before]):
            change = _EUDR_CHANGE[after]
            if after == "plantation" and before in ("planted", "plantation"):
                # the plantation forest gain map only counts primary or naturally regenerating forest in 2020
                change = {}
            rows.append(_synthetic_plot(**_EUDR_STATE_2020[before], **change))
            cells.append((before, after, "high" if code in "DG" else "low"))
    out = whisp_risk(pd.DataFrame(rows))
    for i, (before, after, table) in enumerate(cells):
        expected = _EUDR_DIFFERENCES.get((before, after), table)
        got = out["risk_timber"].iloc[i]
        assert (
            got == expected
        ), f"{before} -> {after}: got {got}, expected {expected} (table: {table})"


def test_timber_outcome_keeps_each_row_when_index_labels_repeat() -> None:
    # results joined together can share index labels; each plot must keep its own outcome
    cases = [
        ({"ForTy_v2_primary_2020": 8.0}, "low"),
        (
            {"ForTy_v2_primary_2020": 8.0, "GFC_loss_after_2020": 2.0},
            "more_info_needed",
        ),
        (
            {
                "ForTy_v2_nat_reg_2020": 8.0,
                "ForTy_v2_plantation_forest_gain_after_2020": 3.0,
            },
            "high",
        ),
        ({"ForTy_v2_nat_reg_2020": 8.0}, "low"),
    ]
    df = pd.DataFrame([_synthetic_plot(**ha) for ha, _ in cases], index=[0, 0, 1, 1])
    out = whisp_risk(df)
    assert list(out["risk_timber"]) == [risk for _, risk in cases]


def test_released_timber_indicators_kept() -> None:
    # Ind_07, Ind_08 and Ind_11 stay in the output for existing users (Ind_07 = planted or plantation forest in
    # 2020, Ind_08 has no dataset so stays "no")
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
    assert set(c for c in out.columns if c.startswith("Ind_")) == set(
        EXPECTED_INDICATOR_COLS
    )


# ---- decision_tree_recipe() and the DECISION_TREES data behind it stay in step with risk.py: every end point has
# its own code (none retired), every question has a label and columns, every column read is a described indicator,
# and a walker that reads only the recipe gives the same outcome as risk.py for every yes/no combination.
def _questions(node):
    if "q" not in node:
        return set()
    return {node["q"]} | _questions(node["yes"]) | _questions(node["no"])


def _end_points(node):
    if "q" not in node:
        return [node["pathway"]]
    return _end_points(node["yes"]) + _end_points(node["no"])


def test_every_end_point_has_its_own_code():
    for name, spec in DECISION_TREES.items():
        ends = _end_points(spec["tree"])
        assert len(ends) == len(set(ends)), f"{name}: an end point appears twice"
        assert set(ends) == set(
            spec["codes"]
        ), f"{name}: codes differ from the end points"
        codes = list(spec["codes"].values())
        assert len(codes) == len(set(codes)), f"{name}: a code is used twice"
        assert not set(codes) & set(
            spec["retired_codes"]
        ), f"{name}: a retired code is used again"


def test_every_question_has_a_label_and_columns():
    for name, spec in DECISION_TREES.items():
        for q in _questions(spec["tree"]):
            assert spec["labels"].get(q), f"{name}: no label for {q}"
            assert spec["columns"].get(q), f"{name}: no columns for {q}"


def _walk_recipe(node, row):
    # what an app would do with the recipe: at each question, yes when any of its columns is "yes"
    while "question" in node:
        node = (
            node["yes"]
            if any(row.get(c) == "yes" for c in node["columns"])
            else node["no"]
        )
    return node


def _recipe_columns(node):
    if "question" not in node:
        return set()
    return (
        set(node["columns"])
        | _recipe_columns(node["yes"])
        | _recipe_columns(node["no"])
    )


def _recipe_ids(node):
    if "question" not in node:
        return [node["id"]]
    return [node["id"]] + _recipe_ids(node["yes"]) + _recipe_ids(node["no"])


def test_recipe_ids_unique_and_columns_described():
    recipe = json.loads(json.dumps(decision_tree_recipe()))
    for name, tree in recipe["decision_trees"].items():
        ids = _recipe_ids(tree["tree"])
        assert len(ids) == len(set(ids)), f"{name}: an id is used twice"
        for col in _recipe_columns(tree["tree"]):
            assert name in recipe["indicators"][col]["read_by"], (name, col)


def test_recipe_rebuilds_every_outcome():
    """A walker that reads only decision_tree_recipe() (the copy apps can use instead of hard-coding the
    decision trees) gives the same risk as risk.py, and for timber the same pathway, for every yes/no
    combination of the indicators each decision tree reads."""
    trees = json.loads(json.dumps(decision_tree_recipe()))["decision_trees"]

    cols = sorted(_recipe_columns(trees["timber"]["tree"]))
    df = pd.DataFrame(
        [dict(zip(cols, v)) for v in itertools.product(["yes", "no"], repeat=len(cols))]
    )
    out = add_risk_timber_col(
        df=df.copy(),
        ind_1_name="Ind_01_treecover",
        ind_2_name="Ind_02_commodities",
        ind_5_name="Ind_05_primary_2020",
        ind_6_name="Ind_06_nat_reg_forest_2020",
        ind_14_name="Ind_14_planted_forest_2020",
        ind_16_name="Ind_16_plantation_forest_2020",
        ind_17_name="Ind_17_plantation_forest_gain_after_2020",
        ind_10_name="Ind_10_agri_after_2020",
        ind_12_name="Ind_12_other_land_2020",
        ind_13_name="Ind_13_other_land_gain_after_2020",
        ind_15_name="Ind_15_cropland_2020",
        ind_4_name="Ind_04_disturbance_after_2020",
    )
    assert len(df) == 2 ** len(cols)
    for i, row in df.iterrows():
        leaf = _walk_recipe(trees["timber"]["tree"], row.to_dict())
        assert (leaf["risk"], leaf["pathway"]) == (
            out.loc[i, "risk_timber"],
            out.loc[i, "risk_timber_pathway"],
        ), dict(row)

    for name, add_col, inds in (
        (
            "pcrop",
            add_risk_pcrop_col,
            [
                "Ind_01_treecover",
                "Ind_02_commodities",
                "Ind_03_disturbance_before_2020",
                "Ind_04_disturbance_after_2020",
            ],
        ),
        (
            "acrop",
            add_risk_acrop_col,
            ["Ind_01_treecover", "Ind_02_commodities", "Ind_04_disturbance_after_2020"],
        ),
    ):
        assert _recipe_columns(trees[name]["tree"]) == set(inds)
        for vals in itertools.product(["yes", "no"], repeat=len(inds)):
            row = dict(zip(inds, vals))
            df = pd.DataFrame([row])
            add_col(df, *inds)
            assert (
                _walk_recipe(trees[name]["tree"], row)["risk"]
                == df.iloc[0][trees[name]["risk_column"]]
            ), (name, row)


# ---- Timber and crop outcomes on the 50 standard test plots (the GeoJSON fixture above), so that any change to the
# decision trees shows up. Their zonal statistics are saved (dataset columns only, no geometry), so the test runs
# offline and live data updates cannot change it; the expected_* columns hold their outcomes. A dataset added to
# the lookup after the snapshot reads 0 here, so a new dataset alone does not fail the test; a changed rule,
# threshold or indicator does. After a deliberate change, rewrite the expected_* columns with
# `python tests/helpers/test_assess_risk.py` (with the code to test on PYTHONPATH) and list the changed plots under
# "Changes to results:" in the PR.
_SNAPSHOT = Path(__file__).parents[1] / "fixtures" / "timber_snapshot.csv"
_SNAPSHOT_NATIONAL_CODES = ["br", "ci", "co", "cm"]
_SNAPSHOT_OUTCOMES = ["risk_pcrop", "risk_acrop", "risk_timber"]


def _snapshot_outcomes():
    df = pd.read_csv(_SNAPSHOT)
    df = df.drop(columns=["expected_" + c for c in _SNAPSHOT_OUTCOMES])
    lut = lookup_gee_datasets_df
    for name in lut[lut["theme"] != "context_and_metadata"]["name"]:
        if name not in df.columns:
            df[name] = 0.0
    out = whisp_risk(df, national_codes=_SNAPSHOT_NATIONAL_CODES)
    return out[["external_id"] + _SNAPSHOT_OUTCOMES]


def test_timber_and_crop_outcomes_match_the_snapshot() -> None:
    got = _snapshot_outcomes().set_index("external_id")
    expected = pd.read_csv(_SNAPSHOT).set_index("external_id")
    changed = [
        f"{eid} {col}: {row['expected_' + col]} -> {got.loc[eid, col]}"
        for eid, row in expected.iterrows()
        for col in _SNAPSHOT_OUTCOMES
        if str(got.loc[eid, col]) != str(row["expected_" + col])
    ]
    assert not changed, (
        f"{len(changed)} outcome(s) changed on the snapshot plots. If deliberate, rewrite the expected_* columns "
        "(python tests/helpers/test_assess_risk.py) and list them under Changes to results in the PR:\n"
        + "\n".join(changed[:30])
    )


if __name__ == "__main__":
    # rewrite the snapshot's expected_* columns; the other columns keep their text
    got = _snapshot_outcomes().set_index("external_id")
    df = pd.read_csv(_SNAPSHOT, dtype=str, keep_default_na=False)
    for col in _SNAPSHOT_OUTCOMES:
        df["expected_" + col] = [str(got.loc[eid, col]) for eid in df["external_id"]]
    df.to_csv(_SNAPSHOT, index=False, lineterminator="\n")
    print("rewrote", _SNAPSHOT)

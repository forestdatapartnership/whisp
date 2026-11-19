"""The timber decision tree's display copies stay in step with risk.TIMBER_ROOT_TREE: the code table covers exactly
the tree's leaves, every question has its label / node kind / id / result columns, and decision_tree_recipe()
rebuilds every outcome. No Earth Engine requests.
"""

import itertools
import json

import pandas as pd

from openforis_whisp import timber_tree_export as tx
from openforis_whisp.risk import (
    TIMBER_ROOT_TREE,
    add_risk_acrop_col,
    add_risk_pcrop_col,
    add_risk_timber_col,
    decision_tree_recipe,
)


def _questions(node):
    if "q" not in node:
        return set()
    return {node["q"]} | _questions(node["yes"]) | _questions(node["no"])


QUESTIONS = _questions(TIMBER_ROOT_TREE)


def test_code_table_matches_the_tree_leaves():
    summary = tx.selfcheck()
    assert (
        not set(summary["codes"]) & tx.RETIRED_CODES
    ), "a retired code is emitted again"
    assert tx.RETIRED_CODES <= set(
        tx.CODE_COLOUR
    ), "retired codes keep their palette colour"


def test_every_question_has_its_metadata():
    for name, table in [
        ("Q_LABEL", tx.Q_LABEL),
        ("Q_KIND", tx.Q_KIND),
        ("_ABBREV", tx._ABBREV),
        ("Q_TO_COLUMNS", tx.Q_TO_COLUMNS),
        ("_Q_TO_LAYER", tx._Q_TO_LAYER),
    ]:
        assert (
            set(table) == QUESTIONS
        ), f"{name} differs from the tree's questions: {sorted(set(table) ^ QUESTIONS)}"


def test_diagram_and_viewer_walk_generate():
    assert "Disturbed after 2020?" in tx.to_mermaid()
    assert "disturbed_after_2020" in tx.js_module_source()


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


def test_recipe_rebuilds_every_outcome():
    """A walker that reads only decision_tree_recipe() (the copy apps can use instead of hard-coding the
    decision trees) gives the same risk as risk.py, and for timber the same pathway, for every yes/no
    combination of the indicators each decision tree reads."""
    trees = json.loads(json.dumps(decision_tree_recipe()))["decision_trees"]

    cols = sorted(_recipe_columns(trees["timber"]["tree"]))
    df = pd.DataFrame(
        [dict(zip(cols, v)) for v in itertools.product(["yes", "no"], repeat=len(cols))]
    )
    for unread in (
        "Ind_09_treecover_after_2020",
        "primary_2025",
        "regen_planted_still_2025",
    ):
        df[unread] = "no"
    out = add_risk_timber_col(
        df=df.copy(),
        ind_1_name="Ind_01_treecover",
        ind_2_name="Ind_02_commodities",
        ind_5_name="Ind_05_primary_2020",
        ind_6_name="Ind_06_nat_reg_forest_2020",
        ind_14_name="Ind_14_planted_forest_2020",
        ind_16_name="Ind_16_plantation_forest_2020",
        ind_17_name="Ind_17_plantation_forest_gain_after_2020",
        ind_9_name="Ind_09_treecover_after_2020",
        ind_10_name="Ind_10_agri_after_2020",
        ind_12_name="Ind_12_other_land_2020",
        ind_13_name="Ind_13_other_land_gain_after_2020",
        ind_15_name="Ind_15_cropland_2020",
        ind_4_name="Ind_04_disturbance_after_2020",
        primary_2025_name="primary_2025",
        regen_planted_still_2025_name="regen_planted_still_2025",
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

"""decision_tree_recipe() and the DECISION_TREES data behind it stay in step with risk.py: every end point has its own
code (none retired), every question has a label and columns, every column read is a described indicator, and a walker
that reads only the recipe gives the same outcome as risk.py for every yes/no combination. No Earth Engine requests.
"""

import itertools
import json

import pandas as pd

from openforis_whisp.risk import (
    DECISION_TREES,
    add_risk_acrop_col,
    add_risk_pcrop_col,
    add_risk_timber_col,
    decision_tree_recipe,
)


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

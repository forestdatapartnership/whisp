"""The timber decision tree's display copies stay in step with risk.TIMBER_ROOT_TREE: the code table covers exactly
the tree's leaves, every question has its label / node kind / id / result columns / drill-down layer, and
the map builder answers exactly the tree's questions. No Earth Engine requests (the map images are lazy).
"""

from openforis_whisp import timber_map
from openforis_whisp import timber_tree_export as tx
from openforis_whisp.risk import TIMBER_ROOT_TREE


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


def test_map_builder_answers_exactly_the_tree_questions():
    qimg = timber_map.build_timber_question_images()
    assert set(qimg) == QUESTIONS, sorted(set(qimg) ^ QUESTIONS)

import inspect
from types import SimpleNamespace

from openforis_whisp import pathway_viewer
from openforis_whisp.risk import whisp_risk


def test_slider_defaults_follow_whisp_risk():
    spec = SimpleNamespace(
        q_to_columns={
            "treecover": ["Ind_01_treecover"],
            "commodity": ["Ind_02_commodities"],
            "before": ["Ind_03_disturbance_before_2020"],
        }
    )
    params = inspect.signature(whisp_risk).parameters
    for ind in pathway_viewer._indicators(spec):
        assert ind["default"] == params[ind["param"]].default


def test_outcome_nodes_carry_their_route_and_questions_their_datasets():
    from openforis_whisp.pcrop_tree_export import PCROP_SPEC

    q_tiles = {q: "q-url" for q in PCROP_SPEC.q_to_columns}
    code_tiles = {c: "code-url" for c in PCROP_SPEC.pathway_to_code.values()}
    nodes = pathway_viewer.node_tiles_by_id(PCROP_SPEC, q_tiles, code_tiles)
    assert nodes["term_2"]["route"] == ["TC2020", "COMM2020", "term_2"]
    assert nodes["COMM2020"]["q"] == "commodities_2020"

    layers = {"Ind_02_commodities": [{"name": "Cocoa_ETH"}]}
    assert pathway_viewer._input_groups(PCROP_SPEC, layers) == [
        {
            "q": "commodities_2020",
            "question": "Commodity / agriculture in 2020?",
            "indicator": "Ind_02_commodities",
            "layers": [{"name": "Cocoa_ETH"}],
        }
    ]

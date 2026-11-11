import inspect
import json
import re
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


def test_timber_sliders_send_only_thresholds_whisp_risk_accepts():
    from openforis_whisp.serve_viewer import _clean_thresholds
    from openforis_whisp.timber_tree_export import TIMBER_SPEC

    params = inspect.signature(whisp_risk).parameters
    sliders = pathway_viewer._indicators(TIMBER_SPEC)
    assert len({s["param"] for s in sliders}) == len(sliders) > 0
    for s in sliders:
        assert s["default"] == params[s["param"]].default
    sent = {s["param"]: s["default"] for s in sliders}
    sent.update({"ind_14_pcent_threshold": 5, "national_codes": "xx"})
    assert _clean_thresholds(sent) == {s["param"]: float(s["default"]) for s in sliders}


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


def test_page_built_without_earth_engine_still_analyses_plots_and_shows_their_route():
    from openforis_whisp import crop_map

    html = crop_map.build_crop_viewer("acrop", national_codes=["br"])
    cfg = json.loads(re.search(r"const CFG = (\{.*?\});\n", html).group(1))
    assert cfg["tiles"] == {} and cfg["nodeTiles"] == {}
    assert cfg["nationalCodes"] == ["br"] and cfg["apiKey"] == ""
    assert cfg["routes"]["1"] == ["TC2020", "term_1"]
    assert cfg["builtWith"]["version"] and cfg["builtWith"]["date"]


def test_timber_page_built_without_earth_engine_uses_every_national_dataset():
    from openforis_whisp import timber_map
    from openforis_whisp.risk import lookup_gee_datasets_df

    html = timber_map.build_timber_viewer()
    cfg = json.loads(re.search(r"const CFG = (\{.*?\});\n", html).group(1))
    codes = lookup_gee_datasets_df["ISO2_code"].dropna().astype(str).str.strip()
    assert set(cfg["nationalCodes"]) == {c.lower() for c in codes if c}
    assert cfg["tiles"] == {} and cfg["countries"] == []
    assert 'const RISK_COL = "risk_timber"' in html


def test_a_derived_question_lists_the_indicator_it_adds():
    from openforis_whisp.timber_tree_export import TIMBER_SPEC

    ind = "Ind_21_plantation_forest_2020"
    layers = {
        "plantation_forest_2020": {"indicator": ind, "layers": [{"name": "ForTy"}]}
    }
    (group,) = pathway_viewer._input_groups(TIMBER_SPEC, layers)
    assert group["q"] == "forest_2020" and group["indicator"] == ind

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

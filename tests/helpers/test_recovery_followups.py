"""
Follow-ups to the lazy broken-dataset handling (#262): whisp_risk says when a dataset left out of
the run feeds a risk tree, legacy mode recovers only images it built, and a formatting error in
concurrent mode is reported rather than triggering a full rerun.
"""

import json
import logging
from pathlib import Path

import ee
import pytest

from openforis_whisp import advanced_stats, datasets, risk, stats
from openforis_whisp.data_conversion import convert_geojson_to_ee

GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "geojson_example.geojson"
)


def g_alive_prep():
    return ee.Image(1).rename("Alive_dataset")


def g_dead_prep():
    return ee.Image("projects/does-not-exist/assets/whisp-test-dead-asset").rename(
        "Dead_dataset"
    )


@pytest.fixture
def three_plots(tmp_path):
    data = json.loads(GEOJSON_EXAMPLE_FILEPATH.read_text())
    data["features"] = data["features"][:3]
    path = tmp_path / "three_plots.geojson"
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def whisp_log(caplog):
    """The whisp logger doesn't propagate, so attach caplog's handler to it directly."""
    whisp_logger = logging.getLogger("whisp")
    whisp_logger.addHandler(caplog.handler)
    yield caplog
    whisp_logger.removeHandler(caplog.handler)


def test_risk_inputs_left_out_maps_short_names_to_the_risk_trees_they_feed():
    affected = risk.risk_inputs_left_out(
        ["ESA_fire", "RADD_after_2020", "admin_code", "dead"]
    )
    assert affected == {"RADD_after_2020": ["pcrop", "acrop", "timber"]}


def test_whisp_risk_notes_when_it_works_without_a_dataset_that_feeds_it(whisp_log):
    import pandas as pd

    meta = {"whisp_version": "x", "unavailable_datasets": ["RADD_after_2020"]}
    df = pd.DataFrame({"whisp_processing_metadata": [meta, meta]})
    noted = risk._note_risk_inputs_left_out(df)
    assert noted["whisp_processing_metadata"].iloc[1]["risk_computed_without"] == [
        "RADD_after_2020"
    ]
    assert any("RADD_after_2020" in r.getMessage() for r in whisp_log.records)

    quiet = pd.DataFrame(
        {"whisp_processing_metadata": [{"unavailable_datasets": ["ESA_fire"]}]}
    )
    assert (
        "risk_computed_without"
        not in risk._note_risk_inputs_left_out(quiet)["whisp_processing_metadata"].iloc[
            0
        ]
    )


def test_legacy_mode_recovers_an_image_it_built(three_plots, monkeypatch):
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_dead_prep],
    )
    df = stats.whisp_stats_ee_to_df(convert_geojson_to_ee(str(three_plots)))
    assert len(df) == 3
    assert any(c.startswith("Alive_dataset") for c in df.columns)
    assert not any(c.startswith("Dead_dataset") for c in df.columns)


def test_legacy_mode_never_rebuilds_a_passed_in_image(three_plots, monkeypatch):
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_dead_prep],
    )
    image = datasets.combine_datasets(include_context_bands=False)
    with pytest.raises(RuntimeError, match="auto_recovery=True"):
        stats.whisp_stats_ee_to_df(
            convert_geojson_to_ee(str(three_plots)), whisp_image=image
        )


def test_concurrent_formatting_error_is_reported_not_rerun(three_plots, monkeypatch):
    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *a, **k: None)
    monkeypatch.setattr(
        datasets, "list_functions", lambda national_codes=None: [g_alive_prep]
    )

    def broken_format(*a, **k):
        raise ValueError("formatting went wrong")

    from openforis_whisp import reformat

    monkeypatch.setattr(reformat, "format_stats_dataframe", broken_format)
    with pytest.raises(ValueError, match="formatting went wrong"):
        advanced_stats.whisp_stats_geojson_to_df_concurrent(three_plots)

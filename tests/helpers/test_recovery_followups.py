"""
Follow-ups to the lazy broken-dataset handling (#262): whisp_risk says when a dataset left out of
the run feeds a risk tree, the FeatureCollection path (whisp_stats_ee_to_df) recovers only images it built, and a formatting error in
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
    # On this branch the timber tree reads disturbance after 2020 through its own
    # Ind_17 getter (TMF, GLAD-L and the national layers), so RADD feeds only the
    # crop trees here; on main it feeds all three.
    assert affected == {"RADD_after_2020": ["pcrop", "acrop"]}


def test_rerunning_risk_replaces_a_stale_risk_inputs_unavailable_note():
    import pandas as pd

    # A previous run noted RADD_after_2020; rerun with a lookup where it no longer feeds any tree
    # (as a different national_codes filter or a custom lookup could do): the old note must go
    stale = {
        "unavailable_datasets": ["RADD_after_2020"],
        "risk_inputs_unavailable": ["RADD_after_2020"],
    }
    df = pd.DataFrame(
        {"whisp_processing_metadata": [stale, {"unavailable_datasets": []}]}
    )
    lookup = risk.lookup_gee_datasets_df.copy()
    lookup.loc[lookup["name"] == "RADD_after_2020", list(risk._RISK_FLAGS)] = 0
    rerun = risk._note_risk_inputs_left_out(df, lookup)
    assert "risk_inputs_unavailable" not in rerun["whisp_processing_metadata"].iloc[0]
    assert rerun["whisp_processing_metadata"].iloc[0]["unavailable_datasets"] == [
        "RADD_after_2020"
    ]
    # and with the normal lookup the note comes back, recomputed rather than carried over
    again = risk._note_risk_inputs_left_out(rerun)
    assert again["whisp_processing_metadata"].iloc[0]["risk_inputs_unavailable"] == [
        "RADD_after_2020"
    ]
    assert "risk_inputs_unavailable" not in again["whisp_processing_metadata"].iloc[1]


def test_whisp_risk_notes_when_it_works_without_a_dataset_that_feeds_it(whisp_log):
    import pandas as pd

    meta = {"whisp_version": "x", "unavailable_datasets": ["RADD_after_2020"]}
    df = pd.DataFrame({"whisp_processing_metadata": [meta, meta]})
    noted = risk._note_risk_inputs_left_out(df)
    assert noted["whisp_processing_metadata"].iloc[1]["risk_inputs_unavailable"] == [
        "RADD_after_2020"
    ]
    assert any("RADD_after_2020" in r.getMessage() for r in whisp_log.records)

    quiet = pd.DataFrame(
        {"whisp_processing_metadata": [{"unavailable_datasets": ["ESA_fire"]}]}
    )
    assert (
        "risk_inputs_unavailable"
        not in risk._note_risk_inputs_left_out(quiet)["whisp_processing_metadata"].iloc[
            0
        ]
    )


def test_ee_to_df_recovers_an_image_it_built(three_plots, monkeypatch):
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_dead_prep],
    )
    df = stats.whisp_stats_ee_to_df(convert_geojson_to_ee(str(three_plots)))
    assert len(df) == 3
    assert any(c.startswith("Alive_dataset") for c in df.columns)
    assert not any(c.startswith("Dead_dataset") for c in df.columns)


def test_ee_to_df_never_rebuilds_a_passed_in_image(three_plots, monkeypatch):
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


def test_rows_from_different_runs_are_each_checked():
    import pandas as pd

    clean = {"whisp_version": "x"}
    dropped = {"whisp_version": "x", "unavailable_datasets": ["RADD_after_2020"]}
    df = pd.DataFrame({"whisp_processing_metadata": [clean, dropped, str(dropped)]})
    noted = risk._note_risk_inputs_left_out(df)["whisp_processing_metadata"]
    assert "risk_inputs_unavailable" not in noted.iloc[0]
    assert noted.iloc[1]["risk_inputs_unavailable"] == ["RADD_after_2020"]
    assert noted.iloc[2]["risk_inputs_unavailable"] == ["RADD_after_2020"]


def test_national_datasets_only_count_when_their_country_is_included():
    from openforis_whisp.reformat import filter_lookup_by_country_codes

    lookup = risk.lookup_gee_datasets_df
    national = lookup[lookup["ISO2_code"].notna() & (lookup["use_for_risk_pcrop"] == 1)]
    row = national.iloc[0]
    short = datasets.unavailable_dataset_names([row["corresponding_variable"]])[0]
    without_country = filter_lookup_by_country_codes(lookup, "ISO2_code", None)
    with_country = filter_lookup_by_country_codes(
        lookup, "ISO2_code", [row["ISO2_code"].lower()]
    )
    assert risk.risk_inputs_left_out([short], without_country) == {}
    assert short in risk.risk_inputs_left_out([short], with_country)


def test_dataset_short_names_are_never_blank():
    names = datasets.unavailable_dataset_names(
        ["g_glad_gfc_10pc_prep", "g_fdap_forest_prep"]
    )
    assert names == ["GFC_TC_2020", "Forest_FDaP"]

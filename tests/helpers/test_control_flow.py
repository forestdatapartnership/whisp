"""
A Celery soft time limit, or anything else the caller raises to stop a run, must come straight
out of Whisp: not retried, not turned into a failed batch, not swallowed (#235).
"""

import json
import time

import ee
import pytest

from openforis_whisp import advanced_stats, control_flow, datasets

from test_lazy_asset_recovery import GEOJSON_EXAMPLE_FILEPATH, g_alive_prep


class FakeTimeLimit(Exception):
    """Stands in for celery.exceptions.SoftTimeLimitExceeded, which subclasses Exception."""


@pytest.fixture
def stop_signal(monkeypatch):
    # celery isn't installed here, so add a look-alike to the tuple the handlers consult
    monkeypatch.setattr(
        control_flow, "PROPAGATE", control_flow.PROPAGATE + (FakeTimeLimit,)
    )
    return FakeTimeLimit


@pytest.fixture
def three_plots(tmp_path):
    data = json.loads(GEOJSON_EXAMPLE_FILEPATH.read_text())
    data["features"] = data["features"][:3]
    path = tmp_path / "three_plots.geojson"
    path.write_text(json.dumps(data))
    return path


def test_propagate_includes_celery_soft_time_limit_when_celery_is_installed():
    assert KeyboardInterrupt in control_flow.PROPAGATE
    assert SystemExit in control_flow.PROPAGATE
    try:
        from celery.exceptions import SoftTimeLimitExceeded
    except ImportError:
        return
    assert SoftTimeLimitExceeded in control_flow.PROPAGATE


def test_dataset_check_does_not_retry_a_stop_signal(stop_signal):
    calls = []

    def probe(img):
        calls.append(1)
        raise stop_signal("soft time limit")

    with pytest.raises(stop_signal):
        datasets._find_broken_images(
            [("alive", g_alive_prep())], probe=probe, skip_other_errors=True
        )
    assert calls == [1]  # not tried a second time, not treated as "fine"


@pytest.mark.parametrize("mode", ["sequential", "concurrent"])
def test_a_stop_signal_during_a_run_comes_straight_out(
    three_plots, monkeypatch, stop_signal, mode
):
    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *a, **k: None)
    monkeypatch.setattr(
        datasets, "list_functions", lambda national_codes=None: [g_alive_prep]
    )

    def stopped(*a, **k):
        raise stop_signal("soft time limit")

    monkeypatch.setattr(advanced_stats, "convert_ee_to_df", stopped)
    started = time.time()
    with pytest.raises(stop_signal):
        getattr(advanced_stats, f"whisp_stats_geojson_to_df_{mode}")(three_plots)
    # the batch retry loop would otherwise sleep and retry, then raise a RuntimeError
    assert time.time() - started < 30


def test_an_ordinary_error_is_still_retried_then_reported(three_plots, monkeypatch):
    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *a, **k: None)
    monkeypatch.setattr(
        datasets, "list_functions", lambda national_codes=None: [g_alive_prep]
    )
    attempts = []

    def flaky(*a, **k):
        attempts.append(1)
        raise ee.EEException("Computation timed out.")

    monkeypatch.setattr(advanced_stats, "convert_ee_to_df", flaky)
    with pytest.raises(Exception) as excinfo:
        advanced_stats.whisp_stats_geojson_to_df_concurrent(three_plots)
    assert not isinstance(excinfo.value, FakeTimeLimit)
    assert len(attempts) > 1  # the normal retry path is untouched

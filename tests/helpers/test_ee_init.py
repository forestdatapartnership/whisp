"""
Earth Engine initialization on earthengine-api 1.6.12+ (#250), where the client moved its state
out of ee.data: whisp must tell reliably whether EE is initialized, apply an explicitly requested
key file or endpoint, raise (not print) failures, and read which endpoint is in use.

ee.Initialize is replaced with a recorder here, so these tests never change the real session.
"""

import logging

import ee
import pytest

import openforis_whisp as whisp
from openforis_whisp import advanced_stats, utils


@pytest.fixture
def init_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(ee, "Initialize", lambda *a, **k: calls.append((a, k)))
    return calls


@pytest.mark.parametrize("state", [True, False])
def test_is_ee_initialized_follows_the_client(monkeypatch, state):
    monkeypatch.setattr(ee.data, "is_initialized", lambda: state)
    assert whisp._is_ee_initialized() is state


def test_explicit_high_volume_request_reinitializes(monkeypatch, init_calls):
    monkeypatch.setattr(ee.data, "is_initialized", lambda: True)
    monkeypatch.setattr(whisp, "_current_ee_project", lambda: "my-project")
    whisp.initialize_ee(use_high_vol_endpoint=True)
    assert init_calls == [
        ((), {"url": whisp.EE_HIGH_VOLUME_URL, "project": "my-project"})
    ]


def test_no_settings_leaves_an_existing_initialization_alone(monkeypatch, init_calls):
    monkeypatch.setattr(ee.data, "is_initialized", lambda: True)
    whisp.initialize_ee()
    assert init_calls == []


def test_failed_initialization_raises_and_resets(monkeypatch):
    resets = []

    def failing_initialize(*a, **k):
        raise ee.EEException("ee.Initialize: no project found.")

    monkeypatch.setattr(ee.data, "is_initialized", lambda: False)
    monkeypatch.setattr(ee, "Initialize", failing_initialize)
    monkeypatch.setattr(ee, "Reset", lambda: resets.append(1))
    with pytest.raises(ee.EEException, match="no project found"):
        whisp.initialize_ee()
    assert resets == [1]


def test_init_ee_skips_when_already_initialized(monkeypatch):
    def must_not_run():
        raise AssertionError("init_ee should not reload .env when EE is initialized")

    monkeypatch.setattr(ee.data, "is_initialized", lambda: True)
    monkeypatch.setattr(utils, "load_env_vars", must_not_run)
    utils.init_ee()


def test_endpoint_check_reads_the_live_client_state():
    # conftest initializes on the standard endpoint
    assert advanced_stats.check_ee_endpoint("standard") is True
    assert advanced_stats.check_ee_endpoint("high-volume") is False


def test_wrong_endpoint_warns_once_instead_of_failing(monkeypatch, caplog):
    monkeypatch.setattr(advanced_stats, "check_ee_endpoint", lambda *a: False)
    monkeypatch.setattr(advanced_stats, "_endpoint_warned", set())
    with caplog.at_level(logging.WARNING):
        advanced_stats.validate_ee_endpoint("high-volume", raise_error=False)
        advanced_stats.validate_ee_endpoint("high-volume", raise_error=False)
    warnings = [r for r in caplog.records if "HIGH-VOLUME" in r.getMessage()]
    assert len(warnings) == 1

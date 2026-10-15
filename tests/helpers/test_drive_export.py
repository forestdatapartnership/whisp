"""
The Drive export names the CSV and folder as asked (dated by default) and says which Earth
Engine project the task runs under (#278). Earth Engine is faked, so nothing is exported.
"""

import re

import pytest

from openforis_whisp import stats


class FakeTask:
    id = "FAKETASKID"

    def start(self):
        pass


@pytest.fixture
def export_calls(monkeypatch):
    calls = []

    def fake_to_drive(**kwargs):
        calls.append(kwargs)
        return FakeTask()

    monkeypatch.setattr(stats.ee.batch.Export.table, "toDrive", fake_to_drive)
    monkeypatch.setattr(stats, "whisp_stats_ee_to_ee", lambda *a, **k: "collection")
    monkeypatch.setattr(stats, "_ee_project_in_use", lambda: "my-sepal-project")
    return calls


def test_default_name_is_dated_and_in_drive_root(export_calls, capsys):
    task = stats.whisp_stats_ee_to_drive("fc")

    (call,) = export_calls
    assert re.fullmatch(r"whisp_output_\d{8}_\d{4}", call["fileNamePrefix"])
    assert call["description"] == call["fileNamePrefix"]
    assert call["folder"] is None
    assert task.id == "FAKETASKID"
    out = capsys.readouterr().out
    assert "Earth Engine project: my-sepal-project" in out
    assert "FAKETASKID" in out


def test_file_name_and_folder_are_used(export_calls, capsys):
    stats.whisp_stats_ee_to_drive(
        "fc", file_name="cameroon hexagons (run 1).csv", folder="whisp_runs"
    )

    (call,) = export_calls
    assert call["fileNamePrefix"] == "cameroon hexagons (run 1)"
    assert call["folder"] == "whisp_runs"
    # task names only allow letters, numbers, spaces and .,:;_-
    assert call["description"] == "cameroon hexagons _run 1_"
    assert "'whisp_runs/cameroon hexagons (run 1).csv'" in capsys.readouterr().out

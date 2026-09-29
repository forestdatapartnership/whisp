"""
whisp_formatted_stats_geojson_to_df_legacy and mode="legacy" are deprecated (#196): they must
warn in a way users actually see, keep working until the removal version, and then be gone.
"""

import sys
import warnings
from pathlib import Path

import pandas as pd
import pytest
from packaging.version import parse as parse_version

from openforis_whisp import stats

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

REMOVAL_VERSION = "3.0.0b1"


@pytest.fixture
def stubbed_pipeline(monkeypatch):
    """Skip Earth Engine: the legacy path is only a GeoJSON front door to the ee_to_df pipeline."""
    monkeypatch.setattr(stats, "convert_geojson_to_ee", lambda path: "fc")
    monkeypatch.setattr(
        stats,
        "whisp_formatted_stats_ee_to_df",
        lambda fc, *a, **k: pd.DataFrame({"plotId": ["1"]}),
    )
    monkeypatch.setattr(
        "openforis_whisp.advanced_stats.validate_ee_endpoint", lambda *a, **k: None
    )


def test_mode_legacy_warns_once_at_the_call_site_and_still_runs(stubbed_pipeline):
    with pytest.warns(FutureWarning, match="mode='legacy'") as record:
        df = stats.whisp_formatted_stats_geojson_to_df("plots.geojson", mode="legacy")
    assert len(df) == 1
    future = [w for w in record if issubclass(w.category, FutureWarning)]
    assert len(future) == 1  # the wrapper warns; the inner function stays quiet
    assert (
        Path(future[0].filename).name == Path(__file__).name
    )  # stacklevel points at the caller


def test_direct_legacy_call_warns(stubbed_pipeline):
    with pytest.warns(
        FutureWarning, match="whisp_formatted_stats_geojson_to_df_legacy"
    ):
        stats.whisp_formatted_stats_geojson_to_df_legacy("plots.geojson")


def test_the_warning_is_not_one_this_package_silences(stubbed_pipeline):
    # advanced_stats.py filters DeprecationWarning at import, so the deprecation must use a
    # category that survives the package's own filters
    import openforis_whisp.advanced_stats  # noqa: F401  (applies its warning filters)

    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("default")
        stats.whisp_formatted_stats_geojson_to_df("plots.geojson", mode="legacy")
    assert any(issubclass(w.category, FutureWarning) for w in seen)


def test_legacy_entry_point_is_removed_once_the_removal_version_is_reached():
    pyproject = Path(__file__).parent.parent.parent / "pyproject.toml"
    with open(pyproject, "rb") as f:
        version = tomllib.load(f)["tool"]["poetry"]["version"]
    if parse_version(version) >= parse_version(REMOVAL_VERSION):
        assert not hasattr(stats, "whisp_formatted_stats_geojson_to_df_legacy"), (
            f"openforis-whisp is at {version}: remove the legacy entry point, mode='legacy' "
            "and this test (#196)"
        )

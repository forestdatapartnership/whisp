"""Tests for openforis_whisp.asset_registry using mocked HTTP responses."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from openforis_whisp.asset_registry import (
    fetch_and_save,
    fetch_collection_features,
    fetch_feature,
    fetch_features_by_ids,
    fetch_to_dict,
    save_geojson,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_FEATURE = {
    "type": "Feature",
    "id": "plot-001",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
    },
    "properties": {"name": "Test plot", "created_at": "2026-01-01T00:00:00Z"},
}

SAMPLE_FEATURE_2 = {
    "type": "Feature",
    "id": "plot-002",
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]],
    },
    "properties": {"name": "Test plot 2"},
}


def _mock_response(json_data, status_code=200):
    """Create a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_feature(mock_get):
    """fetch_feature returns a single GeoJSON Feature dict."""
    mock_get.return_value = _mock_response(SAMPLE_FEATURE)

    result = fetch_feature("cat1", "plots", "plot-001")

    assert result["type"] == "Feature"
    assert result["id"] == "plot-001"
    assert "geometry" in result
    assert "properties" in result
    mock_get.assert_called_once()


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_collection_features_single_page(mock_get):
    """fetch_collection_features returns a FeatureCollection from a single page."""
    page = {"type": "FeatureCollection", "features": [SAMPLE_FEATURE, SAMPLE_FEATURE_2]}
    mock_get.return_value = _mock_response(page)

    result = fetch_collection_features("cat1", "plots", limit=100)

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 2


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_collection_features_pagination(mock_get):
    """fetch_collection_features auto-paginates across multiple pages."""
    page1 = {"type": "FeatureCollection", "features": [SAMPLE_FEATURE]}
    page2 = {"type": "FeatureCollection", "features": [SAMPLE_FEATURE_2]}
    page3 = {"type": "FeatureCollection", "features": []}

    mock_get.side_effect = [
        _mock_response(page1),
        _mock_response(page2),
        _mock_response(page3),
    ]

    result = fetch_collection_features("cat1", "plots", limit=1)

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 2
    assert mock_get.call_count == 3  # page1(1 feat) + page2(1 feat) + page3(empty)


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_collection_features_max_features(mock_get):
    """max_features caps the total number of features returned."""
    page = {"type": "FeatureCollection", "features": [SAMPLE_FEATURE, SAMPLE_FEATURE_2]}
    mock_get.return_value = _mock_response(page)

    result = fetch_collection_features("cat1", "plots", limit=100, max_features=1)

    assert len(result["features"]) == 1


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_features_by_ids(mock_get):
    """fetch_features_by_ids assembles a FeatureCollection from individual requests."""
    mock_get.side_effect = [
        _mock_response(SAMPLE_FEATURE),
        _mock_response(SAMPLE_FEATURE_2),
    ]

    result = fetch_features_by_ids("cat1", "plots", ["plot-001", "plot-002"])

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 2
    assert result["features"][0]["id"] == "plot-001"
    assert result["features"][1]["id"] == "plot-002"
    assert mock_get.call_count == 2


def test_save_geojson(tmp_path):
    """save_geojson writes valid GeoJSON to disk."""
    fc = {"type": "FeatureCollection", "features": [SAMPLE_FEATURE]}
    out = tmp_path / "test_output.geojson"

    returned_path = save_geojson(fc, out)

    assert returned_path == out
    assert out.exists()
    with open(out, encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["type"] == "FeatureCollection"
    assert len(loaded["features"]) == 1


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_and_save_by_ids(mock_get, tmp_path):
    """fetch_and_save with item_ids fetches by ID and saves to file."""
    mock_get.side_effect = [
        _mock_response(SAMPLE_FEATURE),
        _mock_response(SAMPLE_FEATURE_2),
    ]

    out = tmp_path / "registry_output.geojson"
    filepath = fetch_and_save(
        "cat1",
        "plots",
        item_ids=["plot-001", "plot-002"],
        output_path=out,
    )

    assert filepath == out
    assert filepath.exists()
    with open(filepath, encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["type"] == "FeatureCollection"
    assert len(loaded["features"]) == 2


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_and_save_temp_file(mock_get):
    """fetch_and_save creates a temp file when output_path is None."""
    page = {"type": "FeatureCollection", "features": [SAMPLE_FEATURE]}
    mock_get.return_value = _mock_response(page)

    filepath = fetch_and_save("cat1", "plots")

    assert filepath.exists()
    assert filepath.suffix == ".geojson"
    # Clean up temp file
    filepath.unlink(missing_ok=True)


@patch("openforis_whisp.asset_registry.requests.get")
def test_fetch_to_dict(mock_get):
    """fetch_to_dict returns a GeoJSON FeatureCollection dict."""
    page = {"type": "FeatureCollection", "features": [SAMPLE_FEATURE]}
    mock_get.return_value = _mock_response(page)

    result = fetch_to_dict("cat1", "plots")

    assert result["type"] == "FeatureCollection"
    assert len(result["features"]) == 1

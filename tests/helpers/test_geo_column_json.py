"""Regression tests for #323: the geo column is valid JSON across modes."""

import json
from pathlib import Path
from typing import Any

import ee
import pandas as pd
import pytest
from ee import table_converter

from openforis_whisp import advanced_stats, data_conversion, reformat, stats


@pytest.fixture(scope="session", autouse=True)
def _setup_and_teardown_ee_session() -> None:
    """Override session-wide EE initialization for offline unit tests."""
    return


def test_convert_ee_to_df_serializes_geo_dicts_as_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """ee.data.computeFeatures(fileFormat='PANDAS_DATAFRAME') yields Python dicts in 'geo'."""
    features: list[dict[str, Any]] = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-74.1, 4.6]},
            "properties": {"plotId": "1", "Area_sum": 0.0},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[0.0, 0.0], [0.0, 1.0], [1.0, 1.0], [1.0, 0.0], [0.0, 0.0]],
                ],
            },
            "properties": {"plotId": "2", "Area_sum": 10000.0},
        },
    ]

    def fake_compute_features(params: dict[str, Any]) -> pd.DataFrame:
        assert params.get("fileFormat") == "PANDAS_DATAFRAME"
        converter = table_converter.from_file_format("PANDAS_DATAFRAME")
        assert converter is not None
        return converter.do_conversion(iter(features))

    monkeypatch.setattr(ee.data, "computeFeatures", fake_compute_features)

    dummy_fc = object.__new__(ee.FeatureCollection)
    df = data_conversion.convert_ee_to_df(dummy_fc)

    assert list(df["plotId"]) == ["1", "2"]
    for raw_geo, expected_feat in zip(df["geo"], features, strict=True):
        assert isinstance(raw_geo, str)
        assert "'" not in raw_geo
        assert json.loads(raw_geo) == expected_feat["geometry"]


def test_validate_dataframe_coerces_geo_dicts_to_valid_json() -> None:
    """Pandera schema validation must not coerce dict geometries to single-quoted Python repr."""
    point_geom: dict[str, Any] = {"type": "Point", "coordinates": [36.87, -0.99]}
    df_raw = pd.DataFrame(
        {
            "plotId": ["1"],
            "geo": [point_geom],
            "Area": [0.0],
            "Geometry_type": ["Point"],
            "Centroid_lon": [36.87],
            "Centroid_lat": [-0.99],
            "Unit": ["ha"],
            "In_waterbody": [False],
        },
    )

    validated = reformat.validate_dataframe_using_lookups_flexible(df_raw)
    geo_val = validated["geo"].iloc[0]

    assert isinstance(geo_val, str)
    assert "'" not in geo_val
    assert json.loads(geo_val) == point_geom


@pytest.mark.parametrize("mode", ["concurrent", "sequential"])
def test_formatted_stats_concurrent_and_sequential_geo_column_is_valid_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    """whisp_formatted_stats_geojson_to_df produces valid JSON in 'geo' in both modes."""
    geom: dict[str, Any] = {"type": "Point", "coordinates": [16.35, 3.17]}
    fc_dict: dict[str, Any] = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": "plot-1"},
                "geometry": geom,
            },
        ],
    }
    geojson_path = tmp_path / "points.geojson"
    geojson_path.write_text(json.dumps(fc_dict), encoding="utf-8")

    class _DummyCollection(ee.FeatureCollection):
        def __init__(self) -> None:
            pass

        def map(self, _func: Any) -> "_DummyCollection":
            return self

        def select(self, _cols: Any) -> "_DummyCollection":
            return self

    class _DummyImage(ee.Image):
        def __init__(self) -> None:
            pass

        def reduceRegions(self, **_kwargs: Any) -> _DummyCollection:  # noqa: N802
            return _DummyCollection()

    class _DummyReducer:
        def combine(self, *_args: Any, **_kwargs: Any) -> "_DummyReducer":
            return self

    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *_a, **_k: None)
    monkeypatch.setattr(
        advanced_stats,
        "convert_batch_to_ee",
        lambda _b: _DummyCollection(),
    )
    monkeypatch.setattr(
        advanced_stats,
        "convert_geojson_to_ee",
        lambda *_a, **_k: _DummyCollection(),
    )
    monkeypatch.setattr(
        advanced_stats.ee.Reducer,
        "sum",
        staticmethod(_DummyReducer),
    )
    monkeypatch.setattr(
        advanced_stats.ee.Reducer,
        "mode",
        staticmethod(lambda *_a, **_k: _DummyReducer()),
    )

    def fake_compute_features(_params: dict[str, Any]) -> pd.DataFrame:
        converter = table_converter.from_file_format("PANDAS_DATAFRAME")
        assert converter is not None
        return converter.do_conversion(
            iter(
                [
                    {
                        "type": "Feature",
                        "geometry": geom,
                        "properties": {
                            "plotId": "1",
                            "Area_sum": 0.0,
                            "In_waterbody_sum": 0.0,
                            "admin_code_mode": 0,
                        },
                    },
                ],
            ),
        )

    monkeypatch.setattr(ee.data, "computeFeatures", fake_compute_features)

    df = stats.whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=geojson_path,
        whisp_image=_DummyImage(),
        mode=mode,
    )

    geo_val = df["geo"].iloc[0]
    assert isinstance(geo_val, str)
    assert "'" not in geo_val
    assert json.loads(geo_val) == geom

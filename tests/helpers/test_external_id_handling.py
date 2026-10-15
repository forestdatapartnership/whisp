"""
External ids: a missing column stops the run, empty values warn and stay empty (not the
text 'None'), and split_multipart_geojson records its counts for the processing metadata
only when something was split. No Earth Engine calls.
"""

import json
import logging

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from openforis_whisp.data_conversion import (
    check_external_id_column,
    external_id_to_str,
    read_split_metadata,
    split_multipart_geojson,
)


@pytest.fixture
def gdf():
    return gpd.GeoDataFrame(
        {"farm_id": ["A", None, "C", None]}, geometry=[Point(0, 0)] * 4, crs=4326
    )


def test_missing_column_raises_with_available_columns(gdf):
    with pytest.raises(ValueError, match="'plot_code' not found.*farm_id"):
        check_external_id_column(gdf, "plot_code")


def test_empty_values_warn_with_count(gdf, caplog):
    # a plain logger: the whisp logger has its own handlers and does not propagate to caplog
    with caplog.at_level(logging.WARNING, logger="test_external_id"):
        check_external_id_column(gdf, "farm_id", logging.getLogger("test_external_id"))
    assert "2 of 4 features" in caplog.text
    assert "plotId" in caplog.text


def test_complete_column_is_silent(gdf, caplog):
    with caplog.at_level(logging.WARNING, logger="test_external_id"):
        check_external_id_column(
            gdf.dropna(), "farm_id", logging.getLogger("test_external_id")
        )
    assert caplog.text == ""


def test_no_column_given_is_a_no_op(gdf):
    check_external_id_column(gdf, None)


def test_external_id_to_str_keeps_missing_values_missing():
    out = external_id_to_str(pd.Series(["A", None, 7, float("nan")]))
    assert list(out[[0, 2]]) == ["A", "7"]
    assert out.isna().tolist() == [False, True, False, True]
    # and never the strings a plain astype(str) would produce
    assert "None" not in out.values and "nan" not in out.values


def _fc(*geoms):
    return {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"id": i}, "geometry": g}
            for i, g in enumerate(geoms, start=1)
        ],
    }


SQUARE = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]}
TWO_SQUARES = {
    "type": "MultiPolygon",
    "coordinates": [SQUARE["coordinates"], [[[2, 2], [3, 2], [3, 3], [2, 3], [2, 2]]]],
}


def test_split_records_counts_only_when_something_was_split(tmp_path):
    split = split_multipart_geojson(_fc(SQUARE, TWO_SQUARES))
    assert split["whisp_split"] == {
        "features_received": 2,
        "multipart_features_split": 1,
        "single_parts_produced": 3,
    }
    # written at the top of the file, found without parsing the features
    path = tmp_path / "split.geojson"
    path.write_text(json.dumps(split), encoding="utf-8")
    assert read_split_metadata(path) == split["whisp_split"]

    untouched = split_multipart_geojson(_fc(SQUARE, SQUARE))
    assert "whisp_split" not in untouched
    path2 = tmp_path / "plain.geojson"
    path2.write_text(json.dumps(untouched), encoding="utf-8")
    assert read_split_metadata(path2) is None


def test_read_split_metadata_tolerates_missing_or_odd_files(tmp_path):
    assert read_split_metadata(tmp_path / "does_not_exist.geojson") is None
    assert read_split_metadata(None) is None
    bad = tmp_path / "bad.geojson"
    bad.write_text("not json at all", encoding="utf-8")
    assert read_split_metadata(bad) is None

"""Keep the precision used to determine indicators and risk."""

import io
import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import openforis_whisp as whisp
from openforis_whisp import advanced_stats, local_stats
from openforis_whisp.data_conversion import convert_df_to_geojson
from openforis_whisp.reformat import (
    create_schema_from_dataframe,
    format_stats_dataframe,
    validate_dataframe_using_lookups_flexible,
)
from openforis_whisp.risk import add_indicator_column, whisp_risk
from openforis_whisp.stats import set_point_geometry_area_to_zero


def _risk_options():
    options = {f"ind_{i}_input_columns": [] for i in range(1, 18)}
    options["ind_1_input_columns"] = ["EUFO_2020"]
    options["ind_4_input_columns"] = ["DIST_after_2020"]
    return options


def _raw_stats():
    return pd.DataFrame(
        [{"plotId": "1", "Area_sum": 100.0, "EUFO_2020_sum": 10.1,
          "DIST_after_2020_sum": 20.0}]
    )


def test_rounding_before_risk_changes_indicator_and_risk():
    precise = format_stats_dataframe(_raw_stats())
    rounded = format_stats_dataframe(_raw_stats(), decimal_places=3)
    precise = whisp_risk(precise, **_risk_options())
    rounded = whisp_risk(rounded, **_risk_options())
    assert precise.loc[0, "EUFO_2020"] == pytest.approx(0.00101)
    assert precise.loc[0, "Ind_01_treecover"] == "yes"
    assert precise.loc[0, "risk_pcrop"] == "high"
    assert precise.loc[0, "risk_acrop"] == "high"
    assert rounded.loc[0, "Ind_01_treecover"] == "no"
    assert rounded.loc[0, "risk_pcrop"] == "low"


@pytest.mark.parametrize("threshold", [10, 50])
@pytest.mark.parametrize("unit", ["ha", "percent"])
def test_thresholds_are_strict_for_hectares_and_percent(threshold, unit):
    if unit == "ha":
        values = [threshold / 100 - 1e-7, threshold / 100, threshold / 100 + 1e-7]
    else:
        values = [threshold - 1e-5, threshold, threshold + 1e-5]
    df = pd.DataFrame({"Area": [1.0] * 3, "cover": values})
    out = add_indicator_column(df, ["cover"], threshold, "indicator", unit_type=unit)
    assert out["indicator"].tolist() == ["no", "no", "yes"]


def test_small_polygon_is_not_treated_as_a_point():
    raw = pd.DataFrame([
        {"plotId": "1", "Geometry_type": "Polygon", "Area_sum": 4.0, "EUFO_2020_sum": 0.1},
        {"plotId": "2", "Geometry_type": "Point", "Area_sum": 100.0, "EUFO_2020_sum": 0.1},
        {"plotId": "3", "Geometry_type": "Point", "Area_sum": 100.0, "EUFO_2020_sum": 0.0},
    ])
    out = set_point_geometry_area_to_zero(format_stats_dataframe(raw, decimal_places=None))
    out = add_indicator_column(out, ["EUFO_2020"], 10, "indicator", unit_type="ha")
    assert out.loc[0, "Area"] > 0
    assert out["indicator"].tolist() == ["no", "yes", "no"]


def test_real_schema_and_csv_geojson_preserve_precision(tmp_path):
    stats = format_stats_dataframe(_raw_stats(), decimal_places=None)
    validated = validate_dataframe_using_lookups_flexible(stats)
    assert str(validated["EUFO_2020"].dtype) == "float32"
    assert validated.loc[0, "EUFO_2020"] == pytest.approx(0.00101, rel=1e-6)
    result = whisp_risk(validated, **_risk_options())
    assert result.loc[0, "Ind_01_treecover"] == "yes"
    assert result.loc[0, "risk_pcrop"] == "high"
    data = validated[["Area", "EUFO_2020", "DIST_after_2020", "Unit"]]
    read_back = pd.read_csv(io.StringIO(data.to_csv(index=False)))
    first = whisp_risk(read_back, **_risk_options())
    second = whisp_risk(first, **_risk_options())
    assert first.loc[0, "risk_pcrop"] == "high"
    assert second.loc[0, "risk_pcrop"] == "high"

    data = data.copy()
    data["geo"] = json.dumps({"type": "Point", "coordinates": [-47, -20]})
    path = tmp_path / "result.geojson"
    convert_df_to_geojson(data, str(path))
    feature = json.loads(path.read_text())["features"][0]
    assert feature["properties"]["EUFO_2020"] == pytest.approx(0.00101, rel=1e-6)


def test_custom_float32_schema_remains_float32():
    lookup = pd.DataFrame([
        {"name": "custom", "col_type": "float32", "is_nullable": 1, "is_required": 1}
    ])
    schema = create_schema_from_dataframe(lookup)
    out = schema(pd.DataFrame({"custom": [0.5]}))
    assert str(out["custom"].dtype) == "float32"


@pytest.mark.parametrize("mode", ["sequential", "concurrent", "local"])
@pytest.mark.parametrize("unit", ["ha", "percent"])
def test_default_precision_in_all_processing_modes(tmp_path, mode, unit):
    path = tmp_path / "plot.geojson"
    path.write_text(json.dumps({"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {}, "geometry": {"type": "Polygon", "coordinates": [[
            [-47.008, -20.479], [-47.007, -20.479], [-47.007, -20.478],
            [-47.008, -20.478], [-47.008, -20.479]
        ]]}}
    ]}))
    image = MagicMock(name="image")
    image.bandNames.return_value.getInfo.return_value = ["Area", "EUFO_2020", "DIST_after_2020"]
    ee_result = _raw_stats()
    raster_result = ee_result.drop(columns="plotId")
    with patch.object(advanced_stats, "validate_ee_endpoint"), \
         patch.object(advanced_stats, "combine_datasets", return_value=image), \
         patch.object(advanced_stats, "convert_geojson_to_ee", return_value=MagicMock()), \
         patch.object(advanced_stats, "convert_batch_to_ee", return_value=MagicMock()), \
         patch.object(advanced_stats, "convert_ee_to_df", return_value=ee_result.copy()), \
         patch.object(advanced_stats, "process_ee_batch", return_value=ee_result.copy()), \
         patch.object(advanced_stats.ee, "Reducer", MagicMock()), \
         patch.object(local_stats, "validate_ee_endpoint"), \
         patch.object(local_stats, "convert_geojson_to_ee_bbox_obscured", return_value=MagicMock()), \
         patch.object(local_stats, "download_geotiffs_for_feature_collection", return_value=["mock.tif"]), \
         patch.object(local_stats, "create_vrt_from_folder", return_value="mock.vrt"), \
         patch.object(local_stats, "exact_extract_in_chunks_parallel", return_value=raster_result.copy()):
        kwargs = {"mode": mode, "unit_type": unit}
        if mode == "local":
            kwargs.update(whisp_image=image, output_dir=str(tmp_path))
        stats = whisp.whisp_formatted_stats_geojson_to_df(path, **kwargs)
        result = whisp_risk(stats, **_risk_options())
        expected = 0.00101 if unit == "ha" else 10.1
        assert result.loc[0, "EUFO_2020"] == pytest.approx(expected, rel=1e-6)
        assert result.loc[0, "Ind_01_treecover"] == "yes"
        assert result.loc[0, "risk_pcrop"] == "high"


def test_larger_plot_keeps_the_same_classification():
    raw = pd.DataFrame([{"plotId": "1", "Area_sum": 100000.0,
                         "EUFO_2020_sum": 20000.0, "DIST_after_2020_sum": 20000.0}])
    precise = whisp_risk(format_stats_dataframe(raw), **_risk_options())
    rounded = whisp_risk(format_stats_dataframe(raw, decimal_places=3), **_risk_options())
    assert precise.loc[0, "Ind_01_treecover"] == rounded.loc[0, "Ind_01_treecover"] == "yes"
    assert precise.loc[0, "risk_pcrop"] == rounded.loc[0, "risk_pcrop"] == "high"


@pytest.mark.parametrize("unit, expected", [("ha", 0.00101), ("percent", 10.1)])
def test_get_stats_feature_keeps_numeric_values(unit, expected):
    from openforis_whisp import stats as stats_module

    class FakeNumber(float):
        def divide(self, value):
            return FakeNumber(self / value)

        def multiply(self, value):
            return FakeNumber(self * value)

    class FakeDictionary(dict):
        def map(self, func):
            return FakeDictionary({k: func(k, v) for k, v in self.items()})

        def set(self, key, value):
            return FakeDictionary({**self, key: value})

        def combine(self, other):
            return FakeDictionary({**self, **other})

    feature = MagicMock()
    feature.set.side_effect = lambda properties: properties
    image = MagicMock()
    image.reduceRegion.return_value = FakeDictionary({"Area": 100.0, "EUFO_2020": 10.1})
    with patch.object(stats_module.ee, "Number", side_effect=FakeNumber), \
         patch.object(stats_module.ee, "Dictionary", side_effect=FakeDictionary), \
         patch.object(stats_module.ee, "Reducer", MagicMock()), \
         patch.object(stats_module.ee, "Algorithms", MagicMock()), \
         patch.object(stats_module, "get_type_and_location", return_value=FakeDictionary()):
        stats_module.ee.Algorithms.If.side_effect = lambda condition, a, b: a if condition else b
        result = stats_module.get_stats_feature(feature, image, unit_type=unit)
    assert isinstance(result["EUFO_2020"], float)
    assert result["EUFO_2020"] == pytest.approx(expected)

"""
Broken datasets are dealt with lazily: building the whisp image makes no Earth Engine calls, and a
dataset that fails during processing is dropped so the run carries on without it.

The dead dataset is simulated by pointing a stand-in prep function at an asset id that does not
exist, so no real asset is created or deleted.
"""

import json
import logging
import time
from pathlib import Path

import ee
import pytest

from openforis_whisp import advanced_stats, datasets
from openforis_whisp.datasets import (
    combine_datasets,
    combine_datasets_without_broken,
    is_dataset_error,
)

GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "geojson_example.geojson"
)
DEAD_ASSET = "projects/does-not-exist/assets/whisp-test-dead-asset"


def g_alive_prep():
    """Stand-in dataset that always loads."""
    return ee.Image(1).rename("Alive_dataset")


def g_dead_prep():
    """Stand-in dataset whose asset has been deleted or moved."""
    return ee.Image(DEAD_ASSET).rename("Dead_dataset")


@pytest.fixture
def ee_calls(monkeypatch):
    """Records every request the Earth Engine client sends to the server."""
    calls = []
    original = ee.data._execute_cloud_call

    def recording(call, *args, **kwargs):
        calls.append(getattr(call, "uri", repr(call)))
        return original(call, *args, **kwargs)

    monkeypatch.setattr(ee.data, "_execute_cloud_call", recording)
    return calls


@pytest.fixture
def stack_with_dead_dataset(monkeypatch):
    """Swap the dataset list for one alive and one dead dataset; count rebuilds and retry sleeps."""
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_dead_prep],
    )
    rebuilds = []
    original_rebuild = advanced_stats.combine_datasets_without_broken

    def counting_rebuild(*args, **kwargs):
        rebuilds.append(1)
        return original_rebuild(*args, **kwargs)

    monkeypatch.setattr(
        advanced_stats, "combine_datasets_without_broken", counting_rebuild
    )
    spy = _TimeSpy()
    monkeypatch.setattr(advanced_stats, "time", spy)
    return rebuilds, spy.sleeps


class _TimeSpy:
    """Stands in for the time module inside advanced_stats, recording its retry sleeps."""

    def __init__(self):
        self.sleeps = []

    def sleep(self, seconds):
        self.sleeps.append(seconds)

    def __getattr__(self, name):
        return getattr(time, name)


@pytest.fixture
def three_plots(tmp_path):
    data = json.loads(GEOJSON_EXAMPLE_FILEPATH.read_text())
    data["features"] = data["features"][:3]
    path = tmp_path / "three_plots.geojson"
    path.write_text(json.dumps(data))
    return path


@pytest.mark.parametrize(
    "message",
    [
        "Image.load: Image asset 'projects/x/assets/y' not found (does not exist or caller does not have access).",
        "ImageCollection.load: ImageCollection asset 'projects/x/assets/y' not found (does not exist or caller does not have access).",
        "Image.select: Pattern 'Map' did not match any bands.",
        "Expected a homogeneous image collection, but an image with incompatible bands was encountered.",
        "Image.load: Asset 'projects/x/assets/y' does not exist or doesn't allow this operation.",
    ],
)
def test_is_dataset_error_true_for_broken_dataset_messages(message):
    assert is_dataset_error(ee.EEException(message))


@pytest.mark.parametrize(
    "message",
    [
        "Quota exceeded.",
        "Too many concurrent aggregations.",
        "Computation timed out.",
        "User memory limit exceeded.",
        "Request payload size exceeds the limit: 10485760 bytes.",
        "Deadline exceeded",
        "Earth Engine capacity exceeded.",
        "An internal server error has occurred.",
        "Geometry.polygon: Invalid geometry.",
    ],
)
def test_is_dataset_error_false_for_passing_problems(message):
    assert not is_dataset_error(ee.EEException(message))


def test_is_dataset_error_false_for_non_earth_engine_errors():
    assert not is_dataset_error(ValueError("Image.load: asset not found"))


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"national_codes": ["br", "co", "ci", "cm"]},
        {"include_context_bands": False},
    ],
)
def test_combine_datasets_makes_no_earth_engine_calls(ee_calls, kwargs):
    combine_datasets(**kwargs)
    assert ee_calls == []


@pytest.mark.parametrize("flag", ["auto_recovery", "validate_bands"])
def test_up_front_check_is_one_call_when_all_datasets_load(ee_calls, flag):
    combine_datasets(national_codes=["br", "co", "ci", "cm"], **{flag: True})
    assert len(ee_calls) == 1


def test_auto_recovery_leaves_out_broken_dataset_before_image_is_returned(monkeypatch):
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_dead_prep],
    )
    image = combine_datasets(auto_recovery=True, include_context_bands=False)
    assert image.bandNames().getInfo() == [
        datasets.geometry_area_column,
        "Alive_dataset",
    ]


def test_combine_datasets_without_broken_drops_only_the_broken_dataset(monkeypatch):
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_dead_prep],
    )
    image, dropped = combine_datasets_without_broken(include_context_bands=False)
    assert dropped == ["g_dead_prep"]
    assert image.bandNames().getInfo() == [
        datasets.geometry_area_column,
        "Alive_dataset",
    ]


def test_rebuild_raises_original_error_when_nothing_is_broken(monkeypatch):
    monkeypatch.setattr(
        datasets, "list_functions", lambda national_codes=None: [g_alive_prep]
    )
    error = ee.EEException("Image.reduceRegions: some other problem")
    with pytest.raises(ee.EEException) as raised:
        advanced_stats._rebuild_without_broken(
            error, None, False, logging.getLogger("test")
        )
    assert raised.value is error


def _assert_ran_without_dead_dataset(df, n_rows, rebuilds, sleeps):
    assert len(df) == n_rows
    assert any(c.startswith("Alive_dataset") for c in df.columns)
    assert not any(c.startswith("Dead_dataset") for c in df.columns)
    assert len(rebuilds) == 1
    assert sleeps == []


def test_sequential_recovers_from_dead_dataset(three_plots, stack_with_dead_dataset):
    rebuilds, sleeps = stack_with_dead_dataset
    df = advanced_stats.whisp_stats_geojson_to_df_sequential(three_plots)
    _assert_ran_without_dead_dataset(df, 3, rebuilds, sleeps)


def test_concurrent_recovers_from_dead_dataset(
    three_plots, stack_with_dead_dataset, monkeypatch
):
    # The concurrent path insists on the high-volume endpoint; a few plots are fine on the standard one
    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *a, **k: None)
    rebuilds, sleeps = stack_with_dead_dataset
    df = advanced_stats.whisp_stats_geojson_to_df_concurrent(
        three_plots, batch_size=1, max_concurrent=3
    )
    _assert_ran_without_dead_dataset(df, 3, rebuilds, sleeps)


def test_bespoke_image_built_with_auto_recovery_runs_with_dead_dataset(
    three_plots, stack_with_dead_dataset
):
    # The notebook flow: build with auto_recovery=True, add custom bands, pass the image in
    rebuilds, sleeps = stack_with_dead_dataset
    image = combine_datasets(auto_recovery=True).addBands(ee.Image(1).rename("My_band"))
    df = advanced_stats.whisp_stats_geojson_to_df_sequential(
        three_plots, whisp_image=image, custom_bands=["My_band"]
    )
    assert len(df) == 3
    assert any(c.startswith("My_band") for c in df.columns)
    assert any(c.startswith("Alive_dataset") for c in df.columns)
    assert not any(c.startswith("Dead_dataset") for c in df.columns)
    assert rebuilds == []  # cleaned up front, so the run itself never had to rebuild


@pytest.mark.parametrize("mode", ["sequential", "concurrent"])
@pytest.mark.parametrize("custom", [False, True])
def test_passed_in_image_is_never_rebuilt(
    three_plots, stack_with_dead_dataset, monkeypatch, mode, custom
):
    # Whisp can't know what a passed-in image contains, so it stops with guidance instead
    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *a, **k: None)
    rebuilds, _ = stack_with_dead_dataset
    image = combine_datasets()
    kwargs = {}
    if custom:
        image = image.addBands(ee.Image(1).rename("My_band"))
        kwargs["custom_bands"] = ["My_band"]
    run = getattr(advanced_stats, f"whisp_stats_geojson_to_df_{mode}")
    with pytest.raises(RuntimeError, match="auto_recovery=True") as raised:
        run(three_plots, whisp_image=image, **kwargs)
    assert isinstance(raised.value.__cause__, ee.EEException)
    assert rebuilds == []


def test_unavailable_datasets_only_in_metadata_when_something_was_dropped(
    three_plots, stack_with_dead_dataset, monkeypatch
):
    df = advanced_stats.whisp_formatted_stats_geojson_to_df_sequential(three_plots)
    assert df["whisp_processing_metadata"].iloc[0]["unavailable_datasets"] == ["dead"]

    monkeypatch.setattr(
        datasets, "list_functions", lambda national_codes=None: [g_alive_prep]
    )
    df = advanced_stats.whisp_formatted_stats_geojson_to_df_sequential(three_plots)
    assert "unavailable_datasets" not in df["whisp_processing_metadata"].iloc[0]


def test_unavailable_dataset_names_use_column_prefixes():
    assert datasets.unavailable_dataset_names(
        ["g_esa_fire_prep", "g_modis_fire_prep", "g_gaul_admin_code", "g_dead_prep"]
    ) == ["ESA_fire", "MODIS_fire", "admin_code", "dead"]


def test_concurrent_many_batches_stops_early_then_reruns_once(
    tmp_path, stack_with_dead_dataset, monkeypatch
):
    # 12 batches, at most 2 in Earth Engine at once: the first dataset error should stop the rest
    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *a, **k: None)
    data = json.loads(GEOJSON_EXAMPLE_FILEPATH.read_text())
    data["features"] = data["features"][:12]
    path = tmp_path / "twelve_plots.geojson"
    path.write_text(json.dumps(data))
    calls = []
    original = advanced_stats.process_ee_batch

    def counting(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(advanced_stats, "process_ee_batch", counting)
    rebuilds, sleeps = stack_with_dead_dataset
    df = advanced_stats.whisp_stats_geojson_to_df_concurrent(
        path, batch_size=1, max_concurrent=2
    )
    assert len(df) == 12
    assert len(rebuilds) == 1
    assert sleeps == []
    assert (
        len(calls) <= 4 + 12
    )  # a few in flight when the first error hit, then the rerun


def test_too_many_broken_datasets_raise_instead_of_dropping_them(monkeypatch):
    def g_dead_two_prep():
        return ee.Image(DEAD_ASSET + "-2").rename("Dead_two")

    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_dead_prep, g_dead_two_prep],
    )
    with pytest.raises(RuntimeError, match="access or connection problem"):
        combine_datasets_without_broken(include_context_bands=False)


class _FakeImage:
    """Enough of an ee.Image for process_ee_batch: reduceRegions just returns a token."""

    def reduceRegions(self, **kwargs):
        return "token"


def _batch_frame():
    import pandas as pd

    return pd.DataFrame({"plotId": ["1"], "Area_sum": [1.0]})


def _failing_then_ok(monkeypatch, error):
    attempts = []

    def fake_convert(results):
        attempts.append(1)
        if len(attempts) == 1:
            raise error
        return _batch_frame()

    monkeypatch.setattr(advanced_stats, "convert_ee_to_df", fake_convert)
    return attempts


def test_unknown_earth_engine_error_is_still_retried(monkeypatch):
    spy = _TimeSpy()
    monkeypatch.setattr(advanced_stats, "time", spy)
    attempts = _failing_then_ok(monkeypatch, ee.EEException("Deadline exceeded"))
    df = advanced_stats.process_ee_batch("fc", _FakeImage(), None, 0, max_retries=3)
    assert len(df) == 1 and len(attempts) == 2 and len(spy.sleeps) == 1


def test_dataset_error_is_not_retried(monkeypatch):
    spy = _TimeSpy()
    monkeypatch.setattr(advanced_stats, "time", spy)
    error = ee.EEException(
        "Image.load: Asset 'x' does not exist or doesn't allow this operation."
    )
    attempts = _failing_then_ok(monkeypatch, error)
    with pytest.raises(ee.EEException):
        advanced_stats.process_ee_batch("fc", _FakeImage(), None, 0, max_retries=3)
    assert len(attempts) == 1 and spy.sleeps == []


def test_memory_error_keeps_its_message(monkeypatch):
    monkeypatch.setattr(advanced_stats, "time", _TimeSpy())

    def always_fails(results):
        raise ee.EEException("User memory limit exceeded.")

    monkeypatch.setattr(advanced_stats, "convert_ee_to_df", always_fails)
    with pytest.raises(Exception, match="User memory limit exceeded"):
        advanced_stats.process_ee_batch("fc", _FakeImage(), None, 0, max_retries=2)


def test_quota_error_keeps_its_message(monkeypatch):
    monkeypatch.setattr(advanced_stats, "time", _TimeSpy())

    def always_fails(results):
        raise ee.EEException("Quota exceeded for project x.")

    monkeypatch.setattr(advanced_stats, "convert_ee_to_df", always_fails)
    with pytest.raises(RuntimeError, match="Quota exceeded for project x"):
        advanced_stats.process_ee_batch("fc", _FakeImage(), None, 0, max_retries=2)


def test_convert_ee_to_df_passes_earth_engine_errors_through(monkeypatch):
    from openforis_whisp import data_conversion

    error = ee.EEException("Image.load: Asset 'x' does not exist.")

    def raiser(kwargs):
        raise error

    monkeypatch.setattr(ee.data, "computeFeatures", raiser)
    with pytest.raises(ee.EEException) as raised:
        data_conversion.convert_ee_to_df(ee.FeatureCollection([]))
    assert raised.value is error


def test_modis_fire_next_year_builds_as_empty_band_not_an_error(monkeypatch):
    """
    From 1 January the fire layer asks for a year MODIS has not published yet. That year must come
    out as an all-masked band (same as no fire) rather than raising, and must not hide real data.
    """
    next_year = datasets.CURRENT_YEAR + 1
    monkeypatch.setattr(datasets, "CURRENT_YEAR", next_year)
    image = datasets.g_modis_fire_prep()

    future_band, past_band = f"MODIS_fire_{next_year}", f"MODIS_fire_{next_year - 2}"
    assert image.bandNames().getInfo()[-1] == future_band

    # Mato Grosso, on the arc of deforestation, where MODIS maps burns every year
    region = ee.Geometry.Rectangle([-56, -12, -54, -10])
    burned_pixels = (
        image.select([future_band, past_band])
        .reduceRegion(ee.Reducer.count(), region, 500)
        .getInfo()
    )
    assert burned_pixels[future_band] == 0
    assert burned_pixels[past_band] > 0


def g_pixel_bad_prep():
    """Stand-in dataset that passes a band check but fails when pixels are computed (#248)."""
    mixed = ee.ImageCollection([ee.Image(1).rename("a"), ee.Image(2).rename("b")])
    return mixed.mosaic().rename("Pixel_bad")


def test_pixel_check_finds_what_the_band_check_misses(three_plots, monkeypatch):
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_pixel_bad_prep],
    )
    _, dropped = combine_datasets_without_broken(include_context_bands=False)
    assert dropped == []  # bands look fine

    region = ee.FeatureCollection(
        [ee.Feature(ee.Geometry.Rectangle([-56, -12, -55.99, -11.99]))]
    )
    image, dropped = combine_datasets_without_broken(
        include_context_bands=False, probe_region=region
    )
    assert dropped == ["g_pixel_bad_prep"]
    assert image.bandNames().getInfo() == [
        datasets.geometry_area_column,
        "Alive_dataset",
    ]


@pytest.mark.parametrize("mode", ["sequential", "concurrent"])
def test_runs_recover_from_a_pixel_level_error(three_plots, monkeypatch, mode):
    monkeypatch.setattr(advanced_stats, "validate_ee_endpoint", lambda *a, **k: None)
    monkeypatch.setattr(
        datasets,
        "list_functions",
        lambda national_codes=None: [g_alive_prep, g_pixel_bad_prep],
    )
    df = getattr(advanced_stats, f"whisp_stats_geojson_to_df_{mode}")(three_plots)
    assert len(df) == 3
    assert any(c.startswith("Alive_dataset") for c in df.columns)
    assert not any(c.startswith("Pixel_bad") for c in df.columns)
    assert df.attrs["whisp_unavailable_datasets"] == ["pixel_bad"]

"""
Broken datasets are dealt with lazily: building the whisp image makes no Earth Engine calls, and a
dataset that fails during processing is dropped so the run carries on without it.

The dead dataset is simulated by pointing a stand-in prep function at an asset id that does not
exist, so no real asset is created or deleted.
"""

import json
import logging
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
    sleeps = []
    monkeypatch.setattr(advanced_stats.time, "sleep", lambda s: sleeps.append(s))
    return rebuilds, sleeps


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
        {"auto_recovery": True},
        {"national_codes": ["br", "co", "ci", "cm"]},
        {"include_context_bands": False},
    ],
)
def test_combine_datasets_makes_no_earth_engine_calls(ee_calls, kwargs):
    combine_datasets(**kwargs)
    assert ee_calls == []


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
            error, None, False, None, logging.getLogger("test")
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


def test_supplied_image_with_custom_bands_is_not_replaced(
    three_plots, stack_with_dead_dataset
):
    image = combine_datasets()
    with pytest.raises(RuntimeError, match="validate_bands=True"):
        advanced_stats.whisp_stats_geojson_to_df_sequential(
            three_plots, whisp_image=image, custom_bands={"My_band": {}}
        )


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

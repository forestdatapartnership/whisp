"""
The per-year series whose prep functions run to the current year (MODIS fire, GLAD-L, GLAD-S2,
RADD) get their lookup rows extended to the current year automatically, so a new year's column
isn't dropped from the output until someone adds a row by hand.
"""

import datetime as dt

import pytest

from openforis_whisp import datasets
from openforis_whisp.parameters import config_runtime
from openforis_whisp.parameters.config_runtime import (
    YEAR_SERIES_TO_CURRENT_YEAR,
    read_lookup_table,
)

PREP_FOR_SERIES = {
    "MODIS_fire_": "g_modis_fire_prep",
    "GLAD-L_year_": "g_glad_l_year_prep",
    "GLAD-S2_year_": "g_glad_s2_year_prep",
    "RADD_year_": "g_radd_year_prep",
}


def _pretend_year(monkeypatch, year):
    """Make both the lookup and the prep functions think it is `year`."""

    class _FixedDatetime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(year, 1, 1)

    monkeypatch.setattr(config_runtime, "datetime", _FixedDatetime)
    monkeypatch.setattr(datasets, "CURRENT_YEAR", year)
    monkeypatch.setattr(datasets, "CURRENT_YEAR_2DIGIT", year % 100)


def test_every_listed_series_reaches_this_year():
    names = set(read_lookup_table()["name"])
    this_year = dt.datetime.now().year
    for prefix in YEAR_SERIES_TO_CURRENT_YEAR:
        assert f"{prefix}{this_year}" in names


def test_future_years_are_copied_from_the_newest_row(monkeypatch):
    today = read_lookup_table()
    _pretend_year(monkeypatch, 2029)
    future = read_lookup_table()

    assert list(future.dtypes) == list(today.dtypes)
    newest = today[today["name"] == "MODIS_fire_2026"].iloc[0]
    added = future[future["name"] == "MODIS_fire_2029"].iloc[0]
    assert added["order"] == newest["order"] + 3
    for col in today.columns.drop(["name", "order"]):
        assert (added[col] == newest[col]) or (
            added[col] != added[col] and newest[col] != newest[col]
        )


def test_series_not_in_the_list_are_left_alone(monkeypatch):
    # GFC and TMF end at a fixed product year, so they must not grow empty new-year columns
    _pretend_year(monkeypatch, 2029)
    names = set(read_lookup_table()["name"])
    assert "GFC_loss_year_2029" not in names
    assert "TMF_def_2029" not in names


def test_real_rows_are_not_duplicated():
    lookup = read_lookup_table()
    assert not lookup["name"].duplicated().any()


@pytest.mark.parametrize("prefix,prep", sorted(PREP_FOR_SERIES.items()))
def test_every_band_a_prep_emits_next_year_has_a_lookup_row(monkeypatch, prefix, prep):
    # Live check: build the prep as if it were next year and compare its band names to the lookup
    next_year = dt.datetime.now().year + 1
    _pretend_year(monkeypatch, next_year)
    bands = getattr(datasets, prep)().bandNames().getInfo()
    names = set(read_lookup_table()["name"])
    assert f"{prefix}{next_year}" in bands
    assert [b for b in bands if b not in names] == []

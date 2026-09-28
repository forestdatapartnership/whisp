"""
The per-year series whose prep functions run to the current year (MODIS fire, GLAD-L, GLAD-S2,
RADD) get their lookup rows extended to the current year automatically, so a new year's column
isn't dropped from the output until someone adds a row by hand.
"""

import pandas as pd
import pytest

from openforis_whisp import datasets
from openforis_whisp.parameters import config_runtime
from openforis_whisp.parameters.config_runtime import (
    DEFAULT_LOOKUP_TABLE_PATH,
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
    monkeypatch.setattr(config_runtime, "CURRENT_YEAR", year)
    monkeypatch.setattr(datasets, "CURRENT_YEAR", year)
    monkeypatch.setattr(datasets, "CURRENT_YEAR_2DIGIT", year % 100)


def _newest_real_row(prefix):
    raw = pd.read_csv(DEFAULT_LOOKUP_TABLE_PATH)
    rows = raw[raw["name"].str.fullmatch(prefix + r"\d{4}")]
    return rows.loc[rows["name"].str[-4:].astype(int).idxmax()]


def test_every_listed_series_reaches_this_year():
    names = set(read_lookup_table()["name"])
    for prefix in YEAR_SERIES_TO_CURRENT_YEAR:
        assert f"{prefix}{config_runtime.CURRENT_YEAR}" in names


def test_future_years_are_copied_from_the_newest_real_row(monkeypatch):
    today = read_lookup_table()
    newest = _newest_real_row("MODIS_fire_")
    newest_year = int(newest["name"][-4:])
    _pretend_year(monkeypatch, newest_year + 3)
    future = read_lookup_table()

    assert list(future.dtypes) == list(today.dtypes)
    added = future[future["name"] == f"MODIS_fire_{newest_year + 3}"].iloc[0]
    assert added["order"] == newest["order"] + 3
    for col in future.columns.drop(["name", "order"]):
        same = added[col] == newest[col]
        both_missing = pd.isna(added[col]) and pd.isna(newest[col])
        assert same or both_missing, col


def test_series_not_in_the_list_are_left_alone(monkeypatch):
    # GFC and TMF end at a fixed product year, so they must not grow empty new-year columns
    _pretend_year(monkeypatch, 2029)
    names = set(read_lookup_table()["name"])
    assert "GFC_loss_year_2029" not in names
    assert "TMF_def_2029" not in names


def test_real_rows_are_not_duplicated():
    assert not read_lookup_table()["name"].duplicated().any()


def test_custom_lookup_files_without_order_or_with_blank_names(tmp_path, monkeypatch):
    _pretend_year(monkeypatch, 2029)
    no_order = tmp_path / "no_order.csv"
    no_order.write_text("name,theme\nMODIS_fire_2026,disturbance_after\n")
    assert "MODIS_fire_2029" in set(read_lookup_table(no_order)["name"])

    blank_name = tmp_path / "blank_name.csv"
    blank_name.write_text("name,order\n,1\nMODIS_fire_2026,5\n")
    assert "MODIS_fire_2029" in set(read_lookup_table(blank_name)["name"])

    no_name = tmp_path / "no_name.csv"
    no_name.write_text("theme,order\ntreecover,1\n")
    assert len(read_lookup_table(no_name)) == 1


@pytest.mark.parametrize("prefix,prep", sorted(PREP_FOR_SERIES.items()))
def test_bands_and_lookup_rows_match_next_year(monkeypatch, prefix, prep):
    # Live check: build the prep as if it were next year and compare its bands with the lookup
    next_year = config_runtime.CURRENT_YEAR + 1
    _pretend_year(monkeypatch, next_year)
    bands = getattr(datasets, prep)().bandNames().getInfo()
    series_rows = {
        n
        for n in read_lookup_table()["name"]
        if pd.Series([n]).str.fullmatch(prefix + r"\d{4}")[0]
    }
    assert f"{prefix}{next_year}" in bands
    assert set(bands) == series_rows

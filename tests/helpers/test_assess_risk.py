import re
from pathlib import Path

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df
from openforis_whisp.risk import whisp_risk

import pandas as pd


GEOJSON_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "geojson_example.geojson"
)


def _period_columns_below_their_years(df):
    """Names of before/after 2020 columns that are smaller than one of their year columns.

    A combined column covers its year columns (e.g. GLAD-L_before_2020 covers GLAD-L_year_2017 to
    GLAD-L_year_2020, TMF_def_after_2020 covers TMF_def_2021 onwards), so it can never be smaller
    than any one of them. Columns without year columns (e.g. PRODES, DETER) are skipped.
    """
    below = []
    for col in df.columns:
        match = re.fullmatch(r"(.+)_(before|after)_2020", col)
        if not match:
            continue
        prefix, period = match.groups()
        year_cols = []
        for other in df.columns:
            year_match = re.fullmatch(re.escape(prefix) + r"(_year)?_(\d{4})", other)
            if not year_match:
                continue
            is_before = int(year_match.group(2)) <= 2020
            if is_before == (period == "before"):
                year_cols.append(other)
        if year_cols and (df[year_cols].max(axis=1) - df[col] > 0.01).any():
            below.append(col)
    return below


def test_whisp_stats_geojson_to_df() -> None:

    df_stats = whisp_formatted_stats_geojson_to_df(GEOJSON_EXAMPLE_FILEPATH)
    df_stats_with_risk = whisp_risk(df_stats)
    assert isinstance(df_stats_with_risk, pd.DataFrame)
    assert len(df_stats_with_risk) == 50
    # Regression guard (GLAD-L homogeneous-collection crash): the GLAD-L alert bands must compute
    # over the whole fixture - including the South America plots that trip the UpdResult mosaic -
    # and be emitted as numeric columns.
    for col in ("GLAD-L_after_2020", "GLAD-L_before_2020"):
        assert col in df_stats_with_risk.columns
        assert pd.api.types.is_numeric_dtype(df_stats_with_risk[col])
    # Regression guard (GLAD-L year join, fixed in #287): joining the yearly layers so that one
    # year's missing data drops the other years' alerts makes a combined column smaller than its
    # year columns.
    below = _period_columns_below_their_years(df_stats_with_risk)
    assert not below, f"smaller than one of their year columns: {below}"
    print(df_stats_with_risk)

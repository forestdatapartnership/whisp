import pandas as pd

from openforis_whisp.risk import (
    _dedupe_keep_order,
    get_cols_ind_02_commodities,
    lookup_gee_datasets_df,
)


def test_dedupe_keep_order():
    assert _dedupe_keep_order(["a", "b", "b", "c", "a"]) == ["a", "b", "c"]
    assert _dedupe_keep_order([]) == []


def test_ind_02_commodity_union_not_empty():
    # Regression guard: an inverted dedup once made the Ind_02_commodities union
    # always empty, so commodities never fired. The union of the perennial and
    # annual commodity datasets must be non-empty and keep every unique dataset.
    pcrop = get_cols_ind_02_commodities(
        lookup_gee_datasets_df, risk_col="use_for_risk_pcrop"
    )
    acrop = get_cols_ind_02_commodities(
        lookup_gee_datasets_df, risk_col="use_for_risk_acrop"
    )
    union = _dedupe_keep_order(pcrop + acrop)
    assert len(union) > 0
    assert len(union) == len(set(pcrop + acrop))


def test_pcrop_acrop_held_identical():
    # The perennial/annual crop split is not yet wired: whisp_risk builds Ind_02_commodities
    # as the union of pcrop + acrop and feeds the same indicator to both crop trees. To keep
    # the LUT honest (so anyone rebuilding a tree from a single column matches the canonical
    # result), use_for_risk_pcrop and use_for_risk_acrop are held identical. Lock that here so
    # the two columns cannot silently drift apart before the split is genuinely implemented.
    pcrop_flag = (
        pd.to_numeric(lookup_gee_datasets_df["use_for_risk_pcrop"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    acrop_flag = (
        pd.to_numeric(lookup_gee_datasets_df["use_for_risk_acrop"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    assert (pcrop_flag == acrop_flag).all()

    # Consequently the two commodity input sets are identical (each == the Ind_02 union).
    pcrop = get_cols_ind_02_commodities(
        lookup_gee_datasets_df, risk_col="use_for_risk_pcrop"
    )
    acrop = get_cols_ind_02_commodities(
        lookup_gee_datasets_df, risk_col="use_for_risk_acrop"
    )
    assert pcrop == acrop
    assert _dedupe_keep_order(pcrop + acrop) == pcrop

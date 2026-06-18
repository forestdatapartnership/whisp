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

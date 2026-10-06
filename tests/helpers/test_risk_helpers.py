import pandas as pd

import inspect

from openforis_whisp.risk import (
    _dedupe_keep_order,
    add_indicator_column,
    get_cols_ind_02_commodities,
    lookup_gee_datasets_df,
    whisp_risk,
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


def test_ind_03_default_threshold_is_50():
    # Ind_03_disturbance_before_2020 leads to a low risk outcome in the perennial crop tree,
    # so it uses a higher bar than the other indicators: the disturbance must cover more than
    # half the plot. Pin the defaults so neither drifts silently.
    defaults = inspect.signature(whisp_risk).parameters
    assert defaults["ind_3_pcent_threshold"].default == 50
    for i in (1, 2, 4):
        assert defaults[f"ind_{i}_pcent_threshold"].default == 10


def test_ind_03_threshold_is_per_dataset_not_summed():
    # Any single disturbance dataset must exceed the threshold on its own; two datasets at 30%
    # each do not add up to a "yes". Points (Area == 0) count on any non-zero value.
    df = pd.DataFrame(
        {
            "Area": [1.0, 1.0, 1.0, 0.0],
            "TMF_def_before_2020": [0.05, 0.3, 0.6, 0.01],
            "GFC_loss_before_2020": [0.0, 0.3, 0.0, 0.0],
        }
    )
    cols = ["TMF_def_before_2020", "GFC_loss_before_2020"]
    out = add_indicator_column(df.copy(), cols, 50, "Ind_03", unit_type="ha")
    assert out["Ind_03"].tolist() == ["no", "no", "yes", "yes"]
    out = add_indicator_column(df.copy(), cols, 10, "Ind_03", unit_type="ha")
    assert out["Ind_03"].tolist() == ["no", "yes", "yes", "yes"]

"""ForTy 2020 forest-typology bands: present-but-unused (verdict-neutral) contract.

These bands are emitted as output columns but must NOT feed any risk indicator.
The tests are pure-Python (no Earth Engine): they check the shipped lookup table
and the risk-column getters, guarding against anyone silently wiring ForTy into risk.
"""

import openforis_whisp.datasets as datasets
from openforis_whisp.risk import (
    lookup_gee_datasets_df,
    get_cols_ind_01_treecover,
    get_cols_ind_02_commodities,
    get_cols_ind_05_primary_2020,
    get_cols_ind_06_nat_reg_2020,
    get_cols_ind_07_planted_2020,
)

# name -> prep function expected in datasets.py (the CSV/code contract)
FORTY_EXPECTED = {
    "ForTy_primary_2020": "g_forty_primary_2020_prep",
    "ForTy_nat_reg_2020": "g_forty_nat_reg_2020_prep",
    "ForTy_planted_2020": "g_forty_planted_2020_prep",
    "ForTy_plantation_2020": "g_forty_plantation_2020_prep",
    "ForTy_tree_crops_2020": "g_forty_tree_crops_2020_prep",
    "ForTy_forest_2020": "g_forty_forest_2020_prep",
}


def _forty_rows(df):
    return df[df["name"].astype(str).str.startswith("ForTy_")]


def test_forty_lookup_rows_are_risk_neutral():
    """Every ForTy band ticks no risk pathway yet is still emitted."""
    rows = _forty_rows(lookup_gee_datasets_df)
    assert not rows.empty, "no ForTy_ rows found in the lookup table"
    for col in ("use_for_risk_pcrop", "use_for_risk_acrop", "use_for_risk_timber"):
        assert (rows[col] == 0).all(), (
            f"ForTy rows must have {col}==0 (present-but-unused); "
            f"offenders: {list(rows.loc[rows[col] != 0, 'name'])}"
        )
    assert (
        rows["exclude_from_output"] == 0
    ).all(), "ForTy rows must be emitted (exclude_from_output==0)"


def test_forty_bands_excluded_from_every_indicator():
    """No ForTy band is selected by any risk-indicator getter."""
    df = lookup_gee_datasets_df
    selected = []
    for risk_col in ("use_for_risk_pcrop", "use_for_risk_acrop"):
        selected += get_cols_ind_01_treecover(df, risk_col=risk_col)
        selected += get_cols_ind_02_commodities(df, risk_col=risk_col)
    selected += get_cols_ind_05_primary_2020(df)
    selected += get_cols_ind_06_nat_reg_2020(df)
    selected += get_cols_ind_07_planted_2020(df)
    forty = sorted(c for c in selected if str(c).startswith("ForTy_"))
    assert forty == [], f"ForTy bands must not feed any indicator, got: {forty}"


def test_forty_prep_functions_present_and_named():
    """Each ForTy lookup row maps to a real prep function of the expected name."""
    rows = _forty_rows(lookup_gee_datasets_df)
    names = set(rows["name"])
    missing = set(FORTY_EXPECTED) - names
    assert not missing, f"missing ForTy lookup rows: {sorted(missing)}"

    mapping = dict(zip(rows["name"], rows["corresponding_variable"]))
    for name, fn in FORTY_EXPECTED.items():
        assert (
            mapping.get(name) == fn
        ), f"{name} corresponding_variable is {mapping.get(name)!r}, expected {fn!r}"
        assert hasattr(datasets, fn) and callable(
            getattr(datasets, fn)
        ), f"prep function {fn} not found/callable in datasets.py"

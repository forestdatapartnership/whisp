"""ForTy 2020 forest-typology bands: emitted as output columns, neutral for the CROP trees.

On main these bands are present-but-unused everywhere. On this branch the timber tree deliberately
reads four of them (primary, naturally regenerating, planted, plantation) through theme_timber, so the
timber-neutrality assertion from main is dropped here; the crop trees must still never see them.
The tests are pure-Python (no Earth Engine): they check the shipped lookup table and the crop
risk-column getters.
"""

import openforis_whisp.datasets as datasets
from openforis_whisp.risk import (
    lookup_gee_datasets_df,
    get_cols_ind_01_treecover,
    get_cols_ind_02_commodities,
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


def test_forty_lookup_rows_are_crop_risk_neutral():
    """Every ForTy band ticks no crop risk pathway yet is still emitted (timber may use them)."""
    rows = _forty_rows(lookup_gee_datasets_df)
    assert not rows.empty, "no ForTy_ rows found in the lookup table"
    for col in ("use_for_risk_pcrop", "use_for_risk_acrop"):
        assert (rows[col] == 0).all(), (
            f"ForTy rows must have {col}==0 (present-but-unused); "
            f"offenders: {list(rows.loc[rows[col] != 0, 'name'])}"
        )
    assert (
        rows["exclude_from_output"] == 0
    ).all(), "ForTy rows must be emitted (exclude_from_output==0)"


def test_forty_bands_excluded_from_crop_indicators():
    """No ForTy band is selected by a crop risk-indicator getter."""
    df = lookup_gee_datasets_df
    selected = []
    for risk_col in ("use_for_risk_pcrop", "use_for_risk_acrop"):
        selected += get_cols_ind_01_treecover(df, risk_col=risk_col)
        selected += get_cols_ind_02_commodities(df, risk_col=risk_col)
    forty = sorted(c for c in selected if str(c).startswith("ForTy_"))
    assert forty == [], f"ForTy bands must not feed a crop indicator, got: {forty}"


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

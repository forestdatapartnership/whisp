"""Timber and crop outcomes on 75 saved test plots, so that any change to the decision trees shows up.

The inputs are zonal statistics saved from a 564-plot test run (dataset columns only, no geometry), chosen to cover
every timber end point those plots reach and every crop outcome pair. The test runs whisp_risk on them offline (no
GEE) and compares the outcomes with the expected file. A dataset added to the lookup after the snapshot reads 0 here,
so a new dataset alone does not fail the test; a changed rule, threshold or indicator does.

After a deliberate change, rewrite the expected outcomes with

    python tests/helpers/test_timber_snapshot.py

(with the code to test on PYTHONPATH) and list the changed plots under "Changes to results:" in the PR.
"""
from pathlib import Path

import pandas as pd

from openforis_whisp.risk import whisp_risk, lookup_gee_datasets_df

FIXTURES = Path(__file__).parents[1] / "fixtures"
INPUTS = FIXTURES / "timber_snapshot_inputs.csv"
EXPECTED = FIXTURES / "timber_snapshot_expected.csv"
NATIONAL_CODES = ["br", "ci", "co", "cm"]
OUTCOMES = ["risk_pcrop", "risk_acrop", "risk_timber", "risk_timber_pathway"]


def _snapshot_outcomes():
    df = pd.read_csv(INPUTS)
    lut = lookup_gee_datasets_df
    for name in lut[lut["theme"] != "context_and_metadata"]["name"]:
        if name not in df.columns:
            df[name] = 0.0
    out = whisp_risk(df, national_codes=NATIONAL_CODES)
    return out[["external_id"] + OUTCOMES]


def test_timber_and_crop_outcomes_match_the_snapshot() -> None:
    got = _snapshot_outcomes().set_index("external_id")
    expected = pd.read_csv(EXPECTED).set_index("external_id")
    changed = [
        f"{eid} {col}: {row[col]} -> {got.loc[eid, col]}"
        for eid, row in expected.iterrows()
        for col in OUTCOMES
        if str(got.loc[eid, col]) != str(row[col])
    ]
    assert not changed, (
        f"{len(changed)} outcome(s) changed on the snapshot plots. If deliberate, rewrite the expected file "
        "(python tests/helpers/test_timber_snapshot.py) and list them under Changes to results in the PR:\n"
        + "\n".join(changed[:30])
    )


if __name__ == "__main__":
    _snapshot_outcomes().to_csv(EXPECTED, index=False)
    print("rewrote", EXPECTED)

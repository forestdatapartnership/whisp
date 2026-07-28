"""Rule semantics for the timber_map combine-flexibility fold (combine_bools).

combine_bools is the pure-python mirror of the Earth-Engine _combine_pairs fold, sharing the exact same
formulas, so it is the testable surface for the "how are the datasets combined" knob (any / all / k /
prop) without needing Earth Engine. presence[i] = source i fired here; cover[i] = source i covers here.
"""

import pytest

from openforis_whisp.timber_map import combine_bools


def test_any_is_or():
    assert combine_bools([0, 0, 0]) is False
    assert combine_bools([0, 1, 0]) is True
    assert combine_bools([]) is False  # empty pool -> no firing


def test_k_agreement_counts_firing_sources():
    assert combine_bools([1, 1, 0], rule="k", k=2) is True
    assert combine_bools([1, 0, 0], rule="k", k=2) is False
    assert combine_bools([1, 1, 0], rule="k", k=3) is False
    # k = 1 collapses to "any"
    assert combine_bools([0, 1, 0], rule="k", k=1) is True


def test_all_requires_every_covering_source():
    # all present and all cover -> True
    assert combine_bools([1, 1, 1], cover=[1, 1, 1], rule="all") is True
    # one covering source does not fire -> False
    assert combine_bools([1, 1, 0], cover=[1, 1, 1], rule="all") is False
    # a non-covering source is ignored: only the covering one must fire
    assert combine_bools([1, 0], cover=[1, 0], rule="all") is True
    # nothing covers -> False (not vacuously true)
    assert combine_bools([0, 0], cover=[0, 0], rule="all") is False


def test_prop_is_fair_across_uneven_coverage():
    # 2 of 4 covering fire -> 0.5 meets the 0.5 threshold
    assert (
        combine_bools([1, 1, 0, 0], cover=[1, 1, 1, 1], rule="prop", prop=0.5) is True
    )
    # 1 of 4 -> 0.25 fails
    assert (
        combine_bools([1, 0, 0, 0], cover=[1, 1, 1, 1], rule="prop", prop=0.5) is False
    )
    # the fairness case: 1 of 1 covering fires -> 1.0, passes where a raw k>=2 could not
    assert combine_bools([1], cover=[1], rule="prop", prop=0.5) is True
    # no coverage -> False (no division by zero)
    assert combine_bools([0], cover=[0], rule="prop", prop=0.5) is False


def test_defaults_all_covered_when_cover_omitted():
    # omitting cover treats every source as covering, so prop uses the full pool
    assert combine_bools([1, 0], rule="prop", prop=0.5) is True  # 1/2 = 0.5
    assert combine_bools([1, 0, 0], rule="prop", prop=0.5) is False  # 1/3 < 0.5


def test_unknown_rule_raises():
    with pytest.raises(ValueError):
        combine_bools([1], rule="majority")

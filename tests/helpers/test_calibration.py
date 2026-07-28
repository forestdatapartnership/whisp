"""Pure-python calibration helpers: the map-to-plot aggregation rule and the outcome-agreement scorer.

The Earth Engine part (plot_class_areas) is not covered here (needs EE); these test the decision logic that
defines the target outcome and scores a threshold set against it.
"""

from openforis_whisp.calibration import (
    aggregate_outcome,
    agreement,
    outcomes_from_class_areas,
)


def test_precedence_high_first_by_absolute_area():
    # 1 ha of high on a 10 ha plot flags HIGH even though most of it is low (precautionary precedence).
    assert (
        aggregate_outcome(low_ha=8, more_ha=1, high_ha=1.0, plot_area_ha=10) == "high"
    )
    # a high area below the absolute floor does not fire high.
    assert (
        aggregate_outcome(
            low_ha=9.3, more_ha=0.5, high_ha=0.2, plot_area_ha=10, high_min_ha=0.5
        )
        == "low"
    )


def test_high_floor_is_absolute_not_percent():
    # 0.6 ha of high flags a big plot HIGH even though that is < 1% of it (a small clearing is not diluted).
    assert (
        aggregate_outcome(
            low_ha=99, more_ha=0, high_ha=0.6, plot_area_ha=100, high_min_ha=0.5
        )
        == "high"
    )


def test_more_info_by_percent_after_high_fails():
    assert (
        aggregate_outcome(
            low_ha=6, more_ha=3, high_ha=0.1, plot_area_ha=10, more_min_pct=10
        )
        == "more_info_needed"
    )
    assert (
        aggregate_outcome(
            low_ha=9, more_ha=0.5, high_ha=0.1, plot_area_ha=10, more_min_pct=10
        )
        == "low"
    )


def test_no_data_routes_to_more_info_never_low():
    # only 20% of the plot carries any class -> cannot clear it -> more-info, not low.
    assert (
        aggregate_outcome(
            low_ha=1.5, more_ha=0.5, high_ha=0, plot_area_ha=10, min_coverage_pct=50
        )
        == "more_info_needed"
    )


def test_degenerate_area_is_error():
    assert aggregate_outcome(0, 0, 0, plot_area_ha=0) == "error"


def test_outcomes_from_class_areas_maps_all():
    areas = {
        "a": {"low_ha": 8, "more_ha": 1, "high_ha": 1.0, "plot_area_ha": 10},
        "b": {"low_ha": 9.3, "more_ha": 0.5, "high_ha": 0.2, "plot_area_ha": 10},
    }
    out = outcomes_from_class_areas(areas, high_min_ha=0.5)
    assert out == {"a": "high", "b": "low"}


def test_agreement_confusion_and_pct():
    target = {"a": "high", "b": "low", "c": "more_info_needed", "d": "high"}
    predicted = {"a": "high", "b": "low", "c": "high", "d": "low"}
    sc = agreement(target, predicted)
    assert sc["n"] == 4 and sc["aligned"] == 2 and sc["pct_aligned"] == 50.0
    assert sc["confusion"]["high"]["low"] == 1  # one high target predicted low
    assert sc["confusion"]["more_info_needed"]["high"] == 1

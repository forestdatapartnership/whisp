import pandas as pd

from openforis_whisp.reformat import format_stats_dataframe


def _stats(water_m2, area_m2):
    # per-plot sums as they come from Earth Engine (square metres), with the water band's _sum and _mode
    n = len(water_m2)
    return pd.DataFrame(
        {
            "plotId": [str(i + 1) for i in range(n)],
            "Area_sum": area_m2,
            "In_waterbody_sum": water_m2,
            "In_waterbody_mode": [1] * n,
            "EUFO_2020_sum": [0.0] * n,
        }
    )


def test_water_flag_is_true_only_when_more_than_half_the_plot_is_water():
    area = 1_000_000.0  # 100 ha
    df = format_stats_dataframe(
        _stats([70_000.0, 500_000.0, 700_000.0, 0.0], [area] * 4)
    )
    assert list(df["In_waterbody"]) == [False, False, True, False]
    assert "In_waterbody_sum" not in df.columns


def test_water_flag_is_the_same_in_percent_units():
    df = format_stats_dataframe(
        _stats([70_000.0, 700_000.0], [1_000_000.0] * 2), unit_type="percent"
    )
    assert list(df["In_waterbody"]) == [False, True]


def test_a_point_is_flagged_when_it_falls_on_water():
    # a point covers one pixel: its area and water are that pixel's, or it has no area at all
    df = format_stats_dataframe(_stats([100.0, 0.0, 100.0], [100.0, 100.0, 0.0]))
    assert list(df["In_waterbody"]) == [True, False, True]

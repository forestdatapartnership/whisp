from pathlib import Path

import pytest

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df


POINTS_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "points_example.geojson"
)


@pytest.mark.parametrize("mode", ["concurrent", "local"])
def test_points_get_the_values_under_them(mode):
    # Three points, each well inside its class: Lake Victoria, forest in Kalimantan (EUFO_2020)
    # and an oil palm plantation in Riau (Oil_palm_Descals). A point has no area, so it needs its
    # own handling: local mode used to give a point 0 for every dataset, and in the other modes a
    # point was never flagged as in water (#313).
    df = whisp_formatted_stats_geojson_to_df(
        POINTS_EXAMPLE_FILEPATH, external_id_column="name", mode=mode
    ).set_index("external_id")
    assert (df["Area"] == 0).all()
    assert df.loc["lake", "In_waterbody"]
    assert not df.loc["forest", "In_waterbody"]
    assert not df.loc["oil_palm", "In_waterbody"]
    assert df.loc["forest", "EUFO_2020"] > 0
    assert df.loc["oil_palm", "Oil_palm_Descals"] > 0

import ast
import json
from pathlib import Path

import pandas as pd
from shapely.geometry import shape

from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df


POINTS_EXAMPLE_FILEPATH = (
    Path(__file__).parents[1] / "fixtures" / "points_example.geojson"
)

# a small square (about 1 ha) in Cote d'Ivoire, so the run also covers a polygon and a national dataset
CI_SQUARE = {
    "type": "Feature",
    "properties": {"name": "ci_square"},
    "geometry": {
        "type": "Polygon",
        "coordinates": [
            [
                [-6.6205, 5.7795],
                [-6.6195, 5.7795],
                [-6.6195, 5.7805],
                [-6.6205, 5.7805],
                [-6.6205, 5.7795],
            ]
        ],
    },
}


def test_points_and_a_polygon_give_the_same_results_in_concurrent_and_local_mode(
    tmp_path, capfd
):
    # Two points, each well inside its class: Lake Victoria and forest in Kalimantan (EUFO_2020),
    # plus the small square above. A point has no area, so it needs its own handling: local mode
    # used to give a point 0 for every dataset, and in the other modes a point was never flagged
    # as in water (#313). One run per mode keeps the test short.
    fc = json.loads(POINTS_EXAMPLE_FILEPATH.read_text())
    fc["features"].append(CI_SQUARE)
    path = tmp_path / "points_and_square.geojson"
    path.write_text(json.dumps(fc))

    results = {}
    for mode in ("concurrent", "local"):
        df = whisp_formatted_stats_geojson_to_df(
            str(path), external_id_column="name", national_codes=["ci"], mode=mode
        ).set_index("external_id")
        assert (df.loc[["lake", "forest"], "Area"] == 0).all()
        assert df.loc["lake", "In_waterbody"]
        assert not df.loc["forest", "In_waterbody"]
        assert df.loc["forest", "EUFO_2020"] > 0
        assert df.loc["ci_square", "Area"] > 0
        assert "nCI_Forest_bnetd_2020" in df.columns
        results[mode] = df.sort_index()

    # local mode's worker processes don't try to start Earth Engine (#322)
    printed = "".join(capfd.readouterr())
    assert "Error in default EE initialization" not in printed
    assert "EE auto-initialized" not in printed

    # local mode gives the same results as concurrent, plot ids included
    concurrent, local = results["concurrent"], results["local"]
    assert list(local.index) == list(concurrent.index)
    for col in concurrent.columns:
        if col == "whisp_processing_metadata":
            continue
        a, b = concurrent[col], local[col]
        if col == "geo":
            # the same shapes, written as Python dicts by concurrent mode and as JSON by local mode
            same = [
                shape(ast.literal_eval(x)).equals_exact(
                    shape(ast.literal_eval(y)), 1e-9
                )
                for x, y in zip(a, b)
            ]
            assert all(same), col
            continue
        if pd.api.types.is_numeric_dtype(a) and a.dtype != bool:
            assert ((a.fillna(0) - b.fillna(0)).abs() <= 0.01).all(), col
        else:
            assert (a.astype(str) == b.astype(str)).all(), col

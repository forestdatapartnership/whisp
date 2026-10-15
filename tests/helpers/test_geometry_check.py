import json

import geopandas as gpd
import pytest
from shapely.geometry import Polygon

from openforis_whisp.data_conversion import check_geometries_usable
from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

SQUARE = Polygon([(0, 0), (0, 0.001), (0.001, 0.001), (0.001, 0), (0, 0)])


def _gdf(geoms, ids=None):
    data = {"geometry": geoms}
    if ids is not None:
        data["farm"] = ids
    return gpd.GeoDataFrame(data, crs="EPSG:4326")


def test_all_usable_passes():
    check_geometries_usable(_gdf([SQUARE, SQUARE]))


def test_null_and_empty_are_named_by_plot_id_and_external_id():
    gdf = _gdf([SQUARE, None, Polygon(), SQUARE], ids=["a", "b", "c", "d"])
    with pytest.raises(ValueError) as err:
        check_geometries_usable(gdf, external_id_column="farm")
    msg = str(err.value)
    assert "2 of 4 features" in msg
    assert "plotId 2, 3" in msg
    assert "external_id b, c" in msg


def test_long_lists_are_cut_short():
    gdf = _gdf([None] * 15)
    with pytest.raises(ValueError, match="and 5 more"):
        check_geometries_usable(gdf)


@pytest.mark.parametrize("mode", ["sequential", "concurrent"])
def test_entry_point_fails_before_processing(tmp_path, mode):
    # a feature with empty coordinates, as the API can receive
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"farm": "ok-1"},
                "geometry": json.loads(gpd.GeoSeries([SQUARE]).to_json())["features"][
                    0
                ]["geometry"],
            },
            {
                "type": "Feature",
                "properties": {"farm": "bad-2"},
                "geometry": {"type": "Polygon", "coordinates": []},
            },
        ],
    }
    path = tmp_path / "plots.geojson"
    path.write_text(json.dumps(fc))
    with pytest.raises(ValueError, match=r"plotId 2 \(external_id bad-2\)"):
        whisp_formatted_stats_geojson_to_df(
            str(path), external_id_column="farm", mode=mode
        )

import geopandas as gpd
import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import Point, box

from openforis_whisp.local_stats import _points_to_raster_cells


def test_a_point_becomes_the_raster_cell_it_falls_in(tmp_path):
    # a 10 x 10 grid of 0.1-degree cells from (0, 1) down to (1, 0)
    raster = tmp_path / "grid.tif"
    transform = from_origin(0.0, 1.0, 0.1, 0.1)
    with rasterio.open(
        raster,
        "w",
        driver="GTiff",
        height=10,
        width=10,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write(np.ones((1, 10, 10), dtype="uint8"))

    plot = box(0.2, 0.2, 0.4, 0.4)
    gdf = gpd.GeoDataFrame(geometry=[Point(0.25, 0.75), plot], crs="EPSG:4326")
    out = _points_to_raster_cells(gdf, str(raster))

    assert out.geometry.iloc[0].geom_type == "Polygon"
    assert out.geometry.iloc[0].bounds == pytest.approx((0.2, 0.7, 0.3, 0.8))
    assert out.geometry.iloc[1].equals(plot)
    assert gdf.geometry.iloc[0].geom_type == "Point"  # the input is left alone

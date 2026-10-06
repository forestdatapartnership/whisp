"""Tests for split_multipart_geojson and the analyze_geojson geometry changes."""

from __future__ import annotations

from openforis_whisp.data_conversion import split_multipart_geojson
from openforis_whisp.data_checks import analyze_geojson


def _square(ox, oy, z=None):
    """A closed unit-square ring at offset (ox, oy), optionally 3D."""
    pts = [(ox, oy), (ox, oy + 1), (ox + 1, oy + 1), (ox + 1, oy), (ox, oy)]
    if z is None:
        return [[list(p) for p in pts]]
    return [[[x, y, z] for (x, y) in pts]]


# ---------------------------------------------------------------------------
# split_multipart_geojson
# ---------------------------------------------------------------------------


def test_multipolygon_split_duplicates_external_id_with_no_part_index():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"external_id": "A", "crop": "cocoa"},
                "geometry": {"type": "Polygon", "coordinates": _square(0, 0)},
            },
            {
                "type": "Feature",
                "properties": {"external_id": "B", "crop": "coffee"},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [_square(2, 2), _square(4, 4), _square(6, 6)],
                },
            },
        ],
    }
    out = split_multipart_geojson(fc)
    feats = out["features"]

    # Polygon A stays 1, MultiPolygon B becomes 3 -> 4 single-part features
    assert len(feats) == 4
    b_parts = [f for f in feats if f["properties"]["external_id"] == "B"]
    assert len(b_parts) == 3
    assert all(f["geometry"]["type"] == "Polygon" for f in b_parts)
    # Attributes are duplicated identically, with no disambiguating part_index
    assert all(
        f["properties"] == {"external_id": "B", "crop": "coffee"} for f in b_parts
    )


def test_geometrycollection_is_decomposed_recursively():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"external_id": "C"},
                "geometry": {
                    "type": "GeometryCollection",
                    "geometries": [
                        {"type": "Polygon", "coordinates": _square(0, 0)},
                        {
                            "type": "MultiPolygon",
                            "coordinates": [_square(1, 1), _square(2, 2)],
                        },
                    ],
                },
            }
        ],
    }
    out = split_multipart_geojson(fc)
    feats = out["features"]

    # Polygon + MultiPolygon(2 parts) -> 3 single polygons, all inheriting external_id
    assert len(feats) == 3
    assert all(f["geometry"]["type"] == "Polygon" for f in feats)
    assert all(f["properties"]["external_id"] == "C" for f in feats)


def test_z_coordinates_are_stripped():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "Polygon", "coordinates": _square(0, 0, z=100)},
            }
        ],
    }
    out = split_multipart_geojson(fc)
    ring = out["features"][0]["geometry"]["coordinates"][0]
    assert all(len(coord) == 2 for coord in ring)


def test_part_index_is_opt_in():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"external_id": "B"},
                "geometry": {
                    "type": "MultiPolygon",
                    "coordinates": [_square(0, 0), _square(2, 2)],
                },
            }
        ],
    }
    out = split_multipart_geojson(fc, add_part_index=True)
    assert [f["properties"]["part_index"] for f in out["features"]] == [1, 2]


def test_single_part_input_is_unchanged_count():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"external_id": "A"},
                "geometry": {"type": "Polygon", "coordinates": _square(0, 0)},
            }
        ],
    }
    out = split_multipart_geojson(fc)
    assert len(out["features"]) == 1


# ---------------------------------------------------------------------------
# analyze_geojson geometry handling
# ---------------------------------------------------------------------------


def test_analyze_counts_and_flags_geometrycollection():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "Polygon", "coordinates": _square(0, 0)},
            },
            {
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "GeometryCollection", "geometries": []},
            },
        ],
    }
    res = analyze_geojson(fc, metrics=["count", "geometry_types"])
    # GeometryCollection is counted (not silently dropped) and flagged
    assert res["count"] == 2
    assert res["geometry_types"].get("GeometryCollection") == 1
    assert "geometrycollection_warning" in res


def test_analyze_fast_check_omits_area_when_not_requested():
    fc = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "Polygon", "coordinates": _square(0, 0)},
            }
        ],
    }
    fast = analyze_geojson(fc, metrics=["count", "geometry_types"])
    assert set(fast) >= {"count", "geometry_types"}
    assert "max_area_ha" not in fast and "max_vertices" not in fast

    # When requested, area/vertex metrics are computed
    full = analyze_geojson(fc, metrics=["max_area_ha", "max_vertices"])
    assert full["max_area_ha"] > 0
    assert full["max_vertices"] == 5  # closed unit-square ring

"""RADD alerts come from the latest weekly release of each region.

The RADD collection keeps every weekly release, and alerts that are not confirmed are dropped from
later releases. A mosaic of every release brings those alerts back (in central Kalimantan, more than
half of the RADD alerts after 2020), so only the latest release may be used.
"""

import ee

from openforis_whisp.datasets import g_radd_after_2020_prep

# central Kalimantan, where earlier weekly releases hold many alerts that were dropped later
BOX = [113.5, 0.5, 113.55, 0.55]


def test_radd_after_2020_only_has_alerts_from_the_latest_release():
    latest = ee.Image(
        ee.ImageCollection("projects/radar-wur/raddalert/v1")
        .filter(ee.Filter.stringStartsWith("system:index", "asia_"))
        .sort("system:time_end", False)
        .first()
    )
    radd = g_radd_after_2020_prep().unmask(0)
    in_latest = latest.select("Date").gte(21000).unmask(0)
    counts = (
        ee.Image.cat([radd.rename("radd"), radd.And(in_latest.Not()).rename("extra")])
        .reduceRegion(
            ee.Reducer.sum(), ee.Geometry.Rectangle(BOX), scale=10, maxPixels=1e9
        )
        .getInfo()
    )
    assert counts["radd"] > 0
    assert counts["extra"] == 0

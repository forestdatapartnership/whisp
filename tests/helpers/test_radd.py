"""RADD_after_2020 only counts confirmed alerts from RADD's latest weekly copy.

RADD publishes a fresh copy of all its alerts every week and drops the ones it can't confirm,
so merging every copy (as Whisp used to) also counts alerts RADD has dropped. In central
Kalimantan that was more than half of the RADD alerts after 2020.
"""

import ee

from openforis_whisp.datasets import g_radd_after_2020_prep

# central Kalimantan, where older weekly copies hold many alerts that were dropped later
BOX = [113.5, 0.5, 113.55, 0.55]


def test_radd_after_2020_only_has_confirmed_alerts_from_the_latest_copy():
    latest = ee.Image(
        ee.ImageCollection("projects/radar-wur/raddalert/v1")
        .filter(ee.Filter.stringStartsWith("system:index", "asia_"))
        .sort("system:time_end", False)
        .first()
    )
    confirmed = (
        latest.select("Alert").eq(3).And(latest.select("Date").gte(21000)).unmask(0)
    )
    radd = g_radd_after_2020_prep().unmask(0)
    counts = (
        ee.Image.cat([radd.rename("radd"), radd.And(confirmed.Not()).rename("extra")])
        .reduceRegion(
            ee.Reducer.sum(), ee.Geometry.Rectangle(BOX), scale=10, maxPixels=1e9
        )
        .getInfo()
    )
    assert counts["radd"] > 0
    assert counts["extra"] == 0

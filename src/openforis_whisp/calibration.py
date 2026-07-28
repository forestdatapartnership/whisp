"""Calibration helpers: map-to-plot aggregation, per-plot zonal areas, and outcome scoring.

INTERNAL / not yet part of the published API. The combined map is useful internally, so this packages the
map-first calibration logic (see temp_dev_notes/design_explorations/map_first_threshold_calibration_plan.md)
as small, testable functions:

  * ``aggregate_outcome``          - the map-to-plot rule: collapse a plot's low / more-info / high AREAS to a
    single outcome by precedence (high -> more-info -> low), with the HIGH trigger a minimum ABSOLUTE area
    (so a large plot cannot dilute a small clearing) and low map-coverage routed to more-info-needed (never
    auto-low). This rule is where the human / EUDR judgement lives; it defines the TARGET outcome the risk.py
    per-indicator thresholds are then tuned to reproduce.
  * ``plot_class_areas``           - per-plot area (ha) of each 3-class outcome, from a combined-map outcome
    image (1 = low / 2 = more-info / 3 = high), via ``reduceRegions`` at a FIXED scale (default 30 m, so the
    percentages are honest and not off the zoomed tile pyramid).
  * ``outcomes_from_class_areas``  - apply ``aggregate_outcome`` over a whole set to get the per-plot target.
  * ``agreement``                  - confusion matrix + share-aligned between two per-plot outcome mappings
    (e.g. the tabular risk column vs the map-aggregated target).

WHISP is a non-authoritative exploration / due-diligence tool: these are outcome SIGNALS (low / more-info /
high), not a legal determination (EUDR is one example framework this can inform, not its focus).
"""

from __future__ import annotations

import ee

OUTCOMES = ("low", "more_info_needed", "high")
_CLASS_TO_OUTCOME = {1: "low", 2: "more_info_needed", 3: "high"}


def aggregate_outcome(
    low_ha,
    more_ha,
    high_ha,
    plot_area_ha,
    high_min_ha=0.5,
    more_min_pct=10.0,
    min_coverage_pct=50.0,
):
    """Collapse a plot's per-class map AREAS to one outcome (the TARGET the table is tuned to reproduce).

    Precedence is high -> more-info -> low, precautionary for a deforestation-risk screen. A plot the map
    barely covers is more-info-needed (never auto-low): you cannot clear a plot you cannot see.

    Parameters
    ----------
    low_ha, more_ha, high_ha : float
        Area (ha) of low / more-info / high map pixels inside the plot.
    plot_area_ha : float
        Total plot area (ha).
    high_min_ha : float
        Minimum absolute high-risk area (ha) that flags the plot HIGH. An ABSOLUTE area (not a flat percent)
        so a large plot cannot dilute a small clearing below the bar.
    more_min_pct : float
        Percent of the plot area of more-info pixels that (after high fails) yields MORE-INFO.
    min_coverage_pct : float
        Minimum percent of the plot that must carry ANY map class; below this -> more-info-needed (no-data).

    Returns
    -------
    str : one of "low" / "more_info_needed" / "high", or "error" for a degenerate (non-positive) area.
    """
    if plot_area_ha is None or plot_area_ha <= 0:
        return "error"
    covered = (low_ha or 0.0) + (more_ha or 0.0) + (high_ha or 0.0)
    if 100.0 * covered / plot_area_ha < min_coverage_pct:
        return "more_info_needed"
    if (high_ha or 0.0) >= high_min_ha:
        return "high"
    if 100.0 * (more_ha or 0.0) / plot_area_ha >= more_min_pct:
        return "more_info_needed"
    return "low"


def plot_class_areas(outcome_image, features, scale=30, id_prop="external_id"):
    """Per-plot area (ha) of each 3-class outcome from a combined-map outcome image.

    ``outcome_image``: an ``ee.Image`` with 1 = low / 2 = more-info / 3 = high (e.g. from
    ``crop_map.collapse_to_outcome3`` / ``timber_map.collapse_to_outcome3``), typically ``selfMask``'d so
    unclassified pixels do not count (that gap is the no-data the aggregation rule routes to more-info).
    ``features``: an ``ee.FeatureCollection`` of plots, each carrying ``id_prop``.

    Returns a dict ``{plot_id: {"low_ha", "more_ha", "high_ha", "plot_area_ha"}}`` (pulled via ``getInfo``,
    so use it on a modest set, batch large sets). Area is summed by class at ``scale`` (default 30 m).
    """
    feats = features.map(
        lambda f: f.set("plot_area_ha", f.geometry().area(1).divide(1e4))
    )
    area_ha = ee.Image.pixelArea().divide(1e4)
    stacked = area_ha.addBands(outcome_image.toInt().rename("cls"))
    reduced = stacked.reduceRegions(
        collection=feats,
        reducer=ee.Reducer.sum().group(groupField=1, groupName="cls"),
        scale=scale,
    )
    out = {}
    for ft in reduced.getInfo()["features"]:
        props = ft["properties"]
        by_cls = {1: 0.0, 2: 0.0, 3: 0.0}
        for g in props.get("groups", []):
            c = int(g.get("cls", 0))
            if c in by_cls:
                by_cls[c] += float(g.get("sum", 0.0) or 0.0)
        out[props.get(id_prop)] = {
            "low_ha": by_cls[1],
            "more_ha": by_cls[2],
            "high_ha": by_cls[3],
            "plot_area_ha": float(
                props.get("plot_area_ha") or (by_cls[1] + by_cls[2] + by_cls[3])
            ),
        }
    return out


def outcomes_from_class_areas(areas, **rule):
    """Apply ``aggregate_outcome`` over a ``plot_class_areas`` dict; return ``{plot_id: outcome}``.

    ``**rule`` passes through ``high_min_ha`` / ``more_min_pct`` / ``min_coverage_pct``.
    """
    return {
        pid: aggregate_outcome(
            a["low_ha"], a["more_ha"], a["high_ha"], a["plot_area_ha"], **rule
        )
        for pid, a in areas.items()
    }


def agreement(target, predicted):
    """Confusion matrix + share-aligned between two ``{plot_id: outcome}`` mappings.

    ``target`` is the aiming point (e.g. the map-aggregated outcome), ``predicted`` is what a threshold set
    produced (e.g. the tabular ``risk_*`` column). Only plot ids present in both are scored.
    """
    keys = [k for k in target if k in predicted]
    conf = {t: {p: 0 for p in OUTCOMES} for t in OUTCOMES}
    aligned = 0
    for k in keys:
        t, p = target[k], predicted[k]
        if t in conf and p in conf[t]:
            conf[t][p] += 1
        if t == p:
            aligned += 1
    n = len(keys)
    per_class = {}
    for t in OUTCOMES:
        row = sum(conf[t].values())
        col = sum(conf[o][t] for o in OUTCOMES)
        tp = conf[t][t]
        per_class[t] = {
            "recall": round(tp / row, 3) if row else None,
            "precision": round(tp / col, 3) if col else None,
            "n": row,
        }
    return {
        "n": n,
        "aligned": aligned,
        "pct_aligned": round(100.0 * aligned / n, 1) if n else 0.0,
        "confusion": conf,
        "per_class": per_class,
    }

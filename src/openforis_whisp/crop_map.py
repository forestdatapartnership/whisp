"""Earth-Engine-side crop "combined map": the per-pixel pathway-code image plus the collapsed
low / more-info / high outcome image, for the perennial-crop (pcrop) and annual-crop (acrop) trees.

This is the crop analogue of ``timber_map``, kept SPEC-DRIVEN so ONE builder covers both crops: pass
``pcrop_tree_export.PCROP_SPEC`` (with ``risk_col="use_for_risk_pcrop"``) or ``acrop_tree_export.ACROP_SPEC``
(``"use_for_risk_acrop"``). The crop trees are far simpler than timber: every tree question maps to exactly
ONE WHISP indicator's presence OR-union (no derived combinations like timber's forest_2020 / primary_2025 /
agriculture_2020, and no multi-source count node). So the qimg is a direct getter-to-union map.

SINGLE-SOURCE INTENT
--------------------
The per-question images are built from the SAME ``risk.get_cols_ind_*`` getters that
``risk.add_risk_pcrop_col`` / ``add_risk_acrop_col`` read (via the spec's ``q_to_columns``), and the pathway
precedence comes from walking the spec's tree with ``decision_tree.eval_tree_ee``. So the crop map cannot
drift from the crop risk logic, exactly as ``timber_map`` cannot drift from ``add_risk_timber_col``.

PER-PIXEL vs PER-PLOT: the tabular crop outcome uses a per-indicator PERCENT threshold on plot coverage; a
pixel has no percent, so this map is a per-pixel PRESENCE union (a pixel is 1 or 0). Treat it as a per-pixel
presence approximation of the plot-level outcome, not a pixel-exact reproduction. WHISP is a
non-authoritative exploration tool, so these are outcome SIGNALS per pixel, not a legal determination (EUDR
is one example framework, not the focus).
"""

from __future__ import annotations

import ee

from . import datasets
from . import decision_tree as _dt
from .reformat import filter_lookup_by_country_codes
from .risk import (
    lookup_gee_datasets_df,
    get_cols_ind_01_treecover,
    get_cols_ind_02_commodities,
    get_cols_ind_03_dist_before_2020,
    get_cols_ind_04_dist_after_2020,
)

# 3-class outcome colours (green low / amber more-info / red high), matching timber_map.OUTCOME_COLOUR.
OUTCOME_COLOUR = {1: "41ab5d", 2: "f08c00", 3: "e31a1c"}
OUTCOME_NAMES = {1: "low", 2: "more_info_needed", 3: "high"}

# WHISP indicator column name -> the getter that returns its lookup dataset names. The crop trees only use
# these four indicators (Ind_01 treecover, Ind_02 commodities, Ind_03 disturbance-before, Ind_04
# disturbance-after); pcrop uses all four, acrop drops Ind_03_disturbance_before_2020.
_GETTERS = {
    "Ind_01_treecover": get_cols_ind_01_treecover,
    "Ind_02_commodities": get_cols_ind_02_commodities,
    "Ind_03_disturbance_before_2020": get_cols_ind_03_dist_before_2020,
    "Ind_04_disturbance_after_2020": get_cols_ind_04_dist_after_2020,
}


# --------------------------------------------------------------------------------------------------
# Lookup-name -> ee.Image resolvers (same behaviour as timber_map's band_img / union: a row becomes a
# 0/1 presence image via its corresponding_variable + datasets.py prep, global 0-fill so a limited
# footprint member does not mask the union). Kept local so crop_map is independent of timber_map.
# --------------------------------------------------------------------------------------------------
def _band_image(row):
    fn = getattr(datasets, str(row["corresponding_variable"]), None)
    if fn is None:
        return None
    try:
        return ee.Image(fn()).select(0).gt(0).unmask(0, False)
    except Exception:  # noqa: BLE001 - one unbuildable dataset must not abort the union
        return None


def _union(lookup_df, names):
    rows = lookup_df[lookup_df["name"].isin(list(names))]
    imgs = [img for _, row in rows.iterrows() if (img := _band_image(row)) is not None]
    if not imgs:
        return ee.Image(0)
    out = imgs[0]
    for img in imgs[1:]:
        out = out.Or(img)
    return out


def _getter_names(ind_col, lut, risk_col):
    """Dataset names for one WHISP indicator column, passing risk_col to getters that accept it."""
    fn = _GETTERS[ind_col]
    try:
        return list(fn(lut, risk_col=risk_col))
    except TypeError:
        return list(fn(lut))


# --------------------------------------------------------------------------------------------------
# 1. The qimg builder: one ee.Image per tree question, each an OR-union of its indicator's datasets.
# --------------------------------------------------------------------------------------------------
def build_crop_question_images(
    spec, risk_col, region=None, national_codes=None, lookup_df=None
):
    """Return {question_name: ee.Image} for the crop ``spec``'s tree questions.

    ``spec``: PCROP_SPEC or ACROP_SPEC. ``risk_col``: "use_for_risk_pcrop" or "use_for_risk_acrop".
    Each question's image is the OR-union of the presence images of the datasets its indicator(s) feed on,
    chosen by the same get_cols_ind_* getters add_risk_pcrop_col / add_risk_acrop_col use.
    """
    lut = lookup_gee_datasets_df if lookup_df is None else lookup_df
    lut = filter_lookup_by_country_codes(
        lut, filter_col="ISO2_code", national_codes=national_codes
    )
    qimg = {}
    for q, ind_cols in spec.q_to_columns.items():
        names = []
        for ind_col in ind_cols:
            names += _getter_names(ind_col, lut, risk_col)
        qimg[q] = _union(lut, names)
    return qimg


# --------------------------------------------------------------------------------------------------
# 2. The pathway-code image: walk the crop tree over the qimg via the single-source generic walker.
# --------------------------------------------------------------------------------------------------
def build_crop_pathway_image(
    spec, risk_col, region=None, national_codes=None, lookup_df=None, clip=True
):
    """Per-pixel crop pathway-code ee.Image (codes per ``spec.pathway_to_code``)."""
    qimg = build_crop_question_images(spec, risk_col, region, national_codes, lookup_df)
    img = _dt.eval_tree_ee(qimg, ee.Image, spec)
    if region is not None and clip:
        img = img.clip(region)
    return img


# --------------------------------------------------------------------------------------------------
# 3. Collapse to the 3-class low / more-info / high outcome image (code sets from the spec's tree).
# --------------------------------------------------------------------------------------------------
def collapse_to_outcome3(spec, pathway_image):
    cc = _dt.class_codes(spec)  # {"low": [...], "more": [...], "high": [...]}

    def _mask(image, codes):
        m = ee.Image(0)
        for c in codes:
            m = m.Or(image.eq(c))
        return m

    low = _mask(pathway_image, cc["low"])
    more = _mask(pathway_image, cc["more"])
    high = _mask(pathway_image, cc["high"])
    out = ee.Image(0).where(low, 1).where(more, 2).where(high, 3)
    return out.selfMask()


# --------------------------------------------------------------------------------------------------
# 4. Palette + legend, from the spec (mirrors timber_map.timber_map_palette).
# --------------------------------------------------------------------------------------------------
def crop_map_palette(spec):
    code_names = _dt.code_names(spec)
    code_colour = spec.code_colour
    emitted = sorted(code_names)
    lo, hi = emitted[0], emitted[-1]
    pathway_palette = [code_colour.get(c, "000000") for c in range(lo, hi + 1)]
    return {
        "code_names": dict(code_names),
        "code_colour": {c: code_colour[c] for c in emitted},
        "pathway_vis": {"min": lo, "max": hi, "palette": pathway_palette},
        "class_codes": _dt.class_codes(spec),
        "outcome_names": dict(OUTCOME_NAMES),
        "outcome_colour": dict(OUTCOME_COLOUR),
        "outcome_vis": {
            "min": 1,
            "max": 3,
            "palette": [OUTCOME_COLOUR[1], OUTCOME_COLOUR[2], OUTCOME_COLOUR[3]],
        },
    }


# --------------------------------------------------------------------------------------------------
# 5. The headline "combined map": both images + palette in one call.
# --------------------------------------------------------------------------------------------------
def build_crop_combined_map(
    spec, risk_col, region=None, national_codes=None, lookup_df=None, clip=True
):
    """Build the crop combined map: pathway-code image + 3-class outcome image + palette / legend."""
    pathway = build_crop_pathway_image(
        spec, risk_col, region, national_codes, lookup_df, clip
    )
    outcome = collapse_to_outcome3(spec, pathway)
    return {
        "pathway_image": pathway,
        "outcome_image": outcome,
        "palette": crop_map_palette(spec),
    }


def map_id_tile_url(image, vis_params: dict) -> str:
    """Resolve (image, vis) to a signed XYZ tile-template URL via getMapId (needs EE initialized)."""
    return ee.Image(image).getMapId(vis_params)["tile_fetcher"].url_format

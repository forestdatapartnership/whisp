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

from . import decision_tree as _dt
from . import map_layers as _ml
from .map_layers import (  # noqa: F401 - kept as names of this module too
    INPUT_COLOURS,
    OUTCOME_COLOUR,
    OUTCOME_NAMES,
    map_id_tile_url,
)
from .reformat import filter_lookup_by_country_codes
from .risk import (
    lookup_gee_datasets_df,
    get_cols_ind_01_treecover,
    get_cols_ind_02_commodities,
    get_cols_ind_03_dist_before_2020,
    get_cols_ind_04_dist_after_2020,
)

# WHISP indicator column name -> the getter that returns its lookup dataset names. The crop trees only use
# these four indicators (Ind_01 treecover, Ind_02 commodities, Ind_03 disturbance-before, Ind_04
# disturbance-after); pcrop uses all four, acrop drops Ind_03_disturbance_before_2020.
_GETTERS = {
    "Ind_01_treecover": get_cols_ind_01_treecover,
    "Ind_02_commodities": get_cols_ind_02_commodities,
    "Ind_03_disturbance_before_2020": get_cols_ind_03_dist_before_2020,
    "Ind_04_disturbance_after_2020": get_cols_ind_04_dist_after_2020,
}


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
        qimg[q] = _ml.union(lut, names)
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
# 3. The 3-class low / more-info / high outcome image and 4. the palette + legend: one copy each in
#    map_layers, shared with timber_map, so the crop and timber maps cannot drift apart.
# --------------------------------------------------------------------------------------------------
def collapse_to_outcome3(spec, pathway_image):
    """1 = low / 2 = more info / 3 = high from a pathway-code image (``map_layers.outcome3``)."""
    return _ml.outcome3(spec, pathway_image)


def crop_map_palette(spec):
    """Palette and legend for the crop tree's images (``map_layers.palette``)."""
    return _ml.palette(spec)


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


# --------------------------------------------------------------------------------------------------
# 6. Input dataset layers: each dataset the tree reads, on its own, for switching on and off in the viewer.
# --------------------------------------------------------------------------------------------------
def crop_input_layers(
    spec, risk_col, national_codes=None, lookup_df=None, max_workers=16
):
    """Tile layers for every dataset the crop ``spec``'s tree reads, to view the inputs one at a time.

    Returns {indicator_column: [{"name", "iso2", "colour", "url"}]} in tree order, global datasets
    first (``map_layers.input_layers``). The datasets come from the same getters as the question images.
    Needs Earth Engine initialised.
    """
    lut = lookup_gee_datasets_df if lookup_df is None else lookup_df
    lut = filter_lookup_by_country_codes(
        lut, filter_col="ISO2_code", national_codes=national_codes
    )
    names = {}
    for ind_cols in spec.q_to_columns.values():
        for ind_col in ind_cols:
            if ind_col not in names:
                names[ind_col] = _getter_names(ind_col, lut, risk_col)
    return _ml.input_layers(names, lut, max_workers=max_workers)


# --------------------------------------------------------------------------------------------------
# 7. The whole viewer page in one call (what the notebook and notebooks/build_crop_viewers.py run).
# --------------------------------------------------------------------------------------------------
BRAZIL_BOOKMARKS = [
    ["Coffee (Minas Gerais)", -21.0, -46.0, 8],
    ["Cocoa (southern Bahia)", -15.0, -39.3, 9],
    ["Oil palm (Para)", -2.3, -48.6, 9],
    ["Soy frontier (Mato Grosso)", -12.5, -55.5, 8],
    ["Cocoa (Cote d Ivoire)", 6.7, -5.5, 9],
]


def build_crop_viewer(
    tree="pcrop",
    ee_project=None,
    national_codes=None,
    api_key=None,
    input_datasets=True,
    bookmarks=None,
    center=(-12, -52),
    zoom=4,
):
    """Build the crop pathway viewer page and return its HTML.

    ``tree``: "pcrop" (perennial crops) or "acrop" (annual crops).
    ``ee_project``: Earth Engine project for the map layers. None builds a page without map layers (the
    tree, a base map and plot analysis), which never expires; with a project, the layers stop loading
    about a day after building.
    ``national_codes``: national datasets for the map and for analysing plots, e.g. ["br"].
    ``api_key``: a Whisp API key to build into the page; anyone with the file can read it, so by default
    leave it out and whoever opens the page pastes their own.
    ``input_datasets``: include each input dataset as its own on/off layer (needs ``ee_project``).
    """
    from . import acrop_tree_export, pcrop_tree_export
    from . import pathway_viewer as pv

    spec, risk_use_col, risk_col, title = {
        "pcrop": (
            pcrop_tree_export.PCROP_SPEC,
            "use_for_risk_pcrop",
            "risk_pcrop",
            "Perennial-crop pathway viewer",
        ),
        "acrop": (
            acrop_tree_export.ACROP_SPEC,
            "use_for_risk_acrop",
            "risk_acrop",
            "Annual-crop pathway viewer",
        ),
    }[tree]
    national_codes = list(national_codes or [])
    palette = crop_map_palette(spec)
    layers, input_layers = {}, None
    if ee_project:
        _ml.init_ee(ee_project)
        qimg = build_crop_question_images(
            spec, risk_use_col, national_codes=national_codes
        )
        pathway = _dt.eval_tree_ee(qimg, ee.Image, spec)
        layers = _ml.viewer_layers(spec, qimg, pathway, palette)
        if input_datasets:
            input_layers = crop_input_layers(
                spec, risk_use_col, national_codes=national_codes
            )
    where = " + national layers (%s)" % ", ".join(national_codes).upper()
    return pv.build_pathway_viewer_html(
        spec,
        layers.get("tiles"),
        palette,
        title,
        subtitle="%s tree%s%s"
        % (
            risk_col,
            " + combined risk map" if layers.get("tiles") else "",
            where if national_codes else "",
        ),
        bookmarks=BRAZIL_BOOKMARKS if bookmarks is None else bookmarks,
        backgrounds=layers.get("backgrounds"),
        risk_col=risk_col,
        center=center,
        zoom=zoom,
        node_tiles=layers.get("node_tiles"),
        input_layers=input_layers,
        national_codes=national_codes,
        api_key=api_key,
        countries=layers.get("countries"),
    )

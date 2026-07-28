"""Earth-Engine-side timber "combined map": the per-pixel pathway-code image plus the collapsed
low / more-info / high outcome image, single-sourced with ``risk.add_risk_timber_col``.

WHAT THIS PRODUCES
------------------
Two raw ``ee.Image`` layers, map-library-agnostic (no geemap / folium dependency):

  * the per-pixel TIMBER PATHWAY-CODE image (which decision-tree terminal a pixel reaches, coded per
    ``timber_tree_export.PATHWAY_TO_CODE``), and
  * the collapsed 3-class OUTCOME image (1 = low, 2 = more-info-needed, 3 = high), the pixel analogue of
    ``risk_timber`` (WHISP is a non-authoritative exploration tool, so this is an outcome SIGNAL per
    pixel, not a legal determination; EUDR is one example of a framework it can inform, not the focus).

plus the palette and legend, taken straight from ``timber_tree_export``.

SINGLE-SOURCE INTENT (the whole point of this module)
-----------------------------------------------------
The precedence of the map comes from ``timber_tree_export.eval_tree_ee``, which walks the ONE source of
truth ``risk.TIMBER_ROOT_TREE`` (proven identical to the pandas ``risk._eval_timber_tree`` on all 4096
question combinations), so the map's terminal ordering cannot drift from the tabular timber outcome and
cannot re-introduce the old exoneration-before-deforestation bug.

The step BEFORE the walk, assembling the 12 tree-question ``ee.Image`` layers (the ``qimg``), is what
used to be hand-copied in the notebook viewer and four gitignored scripts. Here it is derived from the
SAME ``risk.get_cols_ind_*`` getters that ``risk.add_risk_timber_col`` feeds its ``yes_locals`` from, so
which datasets union into each indicator is chosen identically to the table. The three derived
combinations (``forest_2020``, ``primary_2025``, ``agriculture_2020``) mirror ``add_risk_timber_col``'s
``yes_locals`` line for line. If a timber-tree indicator gains a dataset or changes a union, the map
follows automatically, the same way the Mermaid diagram and JS walk already do via
``timber_tree_export``.

This module is TIMBER-only: it walks ``risk.TIMBER_ROOT_TREE``. The perennial-crop and annual-crop risk
trees (pcrop / acrop) are separate and are not covered here.

PER-PIXEL vs PER-PLOT
---------------------
The tabular timber outcome uses a per-indicator PERCENT threshold on a plot's coverage. A single pixel
has no percent coverage, so the map is a per-pixel PRESENCE union (a pixel is 1 or 0). The ``k``
parameter (1 = OR-union / any source; 2 = agreement / at least two sources) is the pixel analogue of
that plot threshold on the primary and agriculture counts, not the same knob. Treat the map as a
per-pixel presence approximation of the plot-level outcome, not a pixel-exact reproduction of the table.

USAGE
-----
    import ee
    import openforis_whisp as whisp
    from openforis_whisp import timber_map as tmap

    whisp.initialize_ee()
    brazil = ee.FeatureCollection("FAO/GAUL/2015/level0").filter(
        ee.Filter.eq("ADM0_NAME", "Brazil"))

    combined = tmap.build_timber_combined_map(region=brazil, national_codes=["br"], k=1)
    pathway = combined["pathway_image"]     # per-pixel pathway-code ee.Image
    outcome = combined["outcome_image"]     # 1 = low / 2 = more-info / 3 = high ee.Image
    pal = combined["palette"]

    # any mapping stack can consume the raw images, e.g. geemap:
    #   m.addLayer(pathway, pal["pathway_vis"], "timber pathway code")
    #   m.addLayer(outcome, pal["outcome_vis"], "timber outcome signal")

NOTE ON national_codes: the reference notebook / scripts built the Brazil map on the FULL global lookup
(so every ``nBR_`` MapBiomas layer was included). To reproduce that, pass the matching national codes
(e.g. ``national_codes=["br"]``). With ``national_codes=None`` every country-specific (``nXX_``) dataset
is dropped, so the Brazil-only plantation presence / gain and other-land layers fall away and their
branches collapse to more-info, exactly as ``add_risk_timber_col`` documents for outside-Brazil.
"""

from __future__ import annotations

import ee

from . import datasets
from . import timber_tree_export as tx
from .reformat import filter_lookup_by_country_codes
from .risk import (
    lookup_gee_datasets_df,
    get_cols_ind_01_treecover,
    get_cols_ind_02_commodities,
    get_cols_ind_05_primary_2020,
    get_cols_ind_06_nat_reg_2020,
    get_cols_ind_07a_planted_2020,
    get_cols_ind_07b_plantation_2020,
    get_cols_ind_08b_plantation_after_2020,
    get_cols_ind_10_agri_after_2020,
    get_cols_ind_12_other_land_2020,
    get_cols_ind_13_other_land_after_2020,
    get_cols_ind_15_agriculture_2020,
    get_cols_ind_16_plantation_presence_2025,
    get_cols_ind_17_disturbance_after_2020_timber,
)

# 3-class outcome colours (green low / amber more-info / red high). A display choice carried over
# unchanged from the notebook viewer's _verdict3 palette; hex with no leading '#', matching the
# Earth-Engine palette convention used throughout timber_tree_export.CODE_COLOUR.
OUTCOME_COLOUR = {1: "41ab5d", 2: "f08c00", 3: "e31a1c"}
OUTCOME_NAMES = {1: "low", 2: "more_info_needed", 3: "high"}


# --------------------------------------------------------------------------------------------------
# 1. Lookup-name -> ee.Image resolvers.  These are the notebook / gitignored-script band_img / union /
#    count helpers, promoted into the package verbatim in behaviour.  A lookup row becomes a 0/1
#    presence image by reading its corresponding_variable and calling the matching datasets.py *_prep
#    getter, with a global 0-fill (unmask(0, False)) so a limited-footprint member does not mask the
#    union.  Kept private: callers work with whole indicator images, not single dataset rows.
# --------------------------------------------------------------------------------------------------
def _band_image(row):
    """Return a lookup row's dataset as a 0/1 presence ``ee.Image`` (global 0-fill), or None.

    Mirrors the ``band_img`` helper in ``notebooks/timber_pathway_viewer.ipynb`` and the
    ``temp_dev_notes/risk_maps/`` scripts: resolve ``corresponding_variable`` to a ``datasets.py``
    ``*_prep`` function, select band 0, threshold ``> 0``, and 0-fill globally so a national-only member
    does not mask the union outside its footprint.
    """
    fn = getattr(datasets, str(row["corresponding_variable"]), None)
    if fn is None:
        return None
    try:
        # select by INDEX (not name) so a renamed band still resolves;
        # unmask(0, False) 0-fills globally.
        return ee.Image(fn()).select(0).gt(0).unmask(0, False)
    except Exception:  # noqa: BLE001 - one unbuildable dataset must not abort the union
        return None


def _row_images(lookup_df, names):
    """The 0/1 presence images for every lookup row whose ``name`` is in ``names`` (lookup order)."""
    name_set = list(names)
    rows = lookup_df[lookup_df["name"].isin(name_set)]
    imgs = []
    for _, row in rows.iterrows():
        img = _band_image(row)
        if img is not None:
            imgs.append(img)
    return imgs


def _union(lookup_df, names):
    """OR-union of the presence images for ``names`` (``ee.Image(0)`` if the pool is empty)."""
    imgs = _row_images(lookup_df, names)
    if not imgs:
        return ee.Image(0)
    out = imgs[0]
    for img in imgs[1:]:
        out = out.Or(img)
    return out


def _count(lookup_df, names):
    """Per-pixel COUNT (sum of 0/1 presence images) over ``names``; each source contributes one vote."""
    imgs = _row_images(lookup_df, names)
    out = ee.Image(0)
    for img in imgs:
        out = out.add(img)
    return out


# --------------------------------------------------------------------------------------------------
# 1b. Combine flexibility (the risk-menu "how are the datasets combined" knob).  Generalises the two
#     existing folds (_union == "any", _count().gte(k) == "k") to a small rule set, and adds the
#     COVER-aware rules that need each dataset's extent mask kept (not just its 0-filled presence):
#       any   : at least one source fires                                   (OR; the default)
#       all   : every COVERING source fires                                 (strict; needs cover)
#       k     : at least k sources fire                                     (agreement count)
#       prop  : firing / covering >= prop, over the sources that cover here (fair under uneven extents)
#     "prop" is the proportion-of-covering-inputs measure from the risk-menu design: a 1-of-1 pixel
#     scores 1.0, a 2-of-4 pixel 0.5, so thin-coverage areas are not penalised the way a raw count is.
#     Composite datasets (e.g. EUFO / JRC GFC2020, itself a fusion of TMF + Hansen + ~35 layers) are NOT
#     independent votes here; weight or drop their ingredients before a k / prop fold to avoid double-counting.
# --------------------------------------------------------------------------------------------------
def _band_presence_cover(row):
    """Return the ``(presence, cover)`` 0/1 ``ee.Image`` pair for a lookup row, or None.

    ``presence`` is 1 where the dataset fires (band ``> 0``), 0 elsewhere (global 0-fill), i.e. exactly
    what ``_band_image`` returns. ``cover`` is 1 where the dataset HAS DATA at all (its extent mask), 0
    elsewhere. Keeping ``cover`` rather than 0-filling it away is what makes the ``all`` and ``prop``
    rules honest across datasets of differing footprint.
    """
    fn = getattr(datasets, str(row["corresponding_variable"]), None)
    if fn is None:
        return None
    try:
        band = ee.Image(fn()).select(0)
        presence = band.gt(0).unmask(0, False)
        cover = band.mask().gt(0).unmask(0, False)
        return presence, cover
    except Exception:  # noqa: BLE001 - one unbuildable dataset must not abort the fold
        return None


def _presence_cover_images(lookup_df, names):
    """The ``(presence, cover)`` image pairs for every lookup row whose ``name`` is in ``names``."""
    rows = lookup_df[lookup_df["name"].isin(list(names))]
    pairs = []
    for _, row in rows.iterrows():
        pc = _band_presence_cover(row)
        if pc is not None:
            pairs.append(pc)
    return pairs


def _combine_pairs(pairs, rule="any", k=2, prop=0.5):
    """Fold ``(presence, cover)`` image pairs into a 0/1 ``ee.Image`` per the combine ``rule``.

    See the combine note above for the rule semantics. Returns ``ee.Image(0)`` if the pool is empty.
    ``combine_bools`` is the pure-python mirror (same formulas), tested in place of this EE path.
    """
    if not pairs:
        return ee.Image(0)
    firing = pairs[0][0]
    covern = pairs[0][1]
    for pres, cov in pairs[1:]:
        firing = firing.add(pres)
        covern = covern.add(cov)
    if rule == "any":
        return firing.gt(0)
    if rule == "all":
        return covern.gt(0).And(firing.gte(covern))
    if rule == "k":
        return firing.gte(int(k))
    if rule == "prop":
        return covern.gt(0).And(firing.divide(covern.max(1)).gte(float(prop)))
    raise ValueError("unknown combine rule: %r (use any / all / k / prop)" % rule)


def _combine(lookup_df, names, rule="any", k=2, prop=0.5):
    """Build the ``(presence, cover)`` pool for ``names`` and fold it with ``_combine_pairs``."""
    return _combine_pairs(
        _presence_cover_images(lookup_df, names), rule=rule, k=k, prop=prop
    )


def combine_bools(presence, cover=None, rule="any", k=2, prop=0.5):
    """Pure-python mirror of ``_combine_pairs`` for ONE plot / pixel (for testing and tabular use).

    ``presence`` / ``cover`` are iterables of 0/1 (a source fires / a source covers here); ``cover``
    defaults to all-covered. Returns a bool with the SAME rule semantics as the Earth Engine fold, so a
    table or the interactive demo can reproduce the map's combine outcome exactly.
    """
    presence = [1 if p else 0 for p in presence]
    cover = [1] * len(presence) if cover is None else [1 if c else 0 for c in cover]
    firing = sum(presence)
    covern = sum(cover)
    if rule == "any":
        return firing > 0
    if rule == "all":
        return covern > 0 and firing >= covern
    if rule == "k":
        return firing >= int(k)
    if rule == "prop":
        return covern > 0 and (firing / covern) >= float(prop)
    raise ValueError("unknown combine rule: %r (use any / all / k / prop)" % rule)


# --------------------------------------------------------------------------------------------------
# 2. The qimg builder: the 12 tree-question ee.Images, assembled from the SAME get_cols_ind_* getters
#    add_risk_timber_col uses, so the map's per-indicator pools are chosen identically to the table.
#    The derived combinations mirror add_risk_timber_col's yes_locals line for line.
# --------------------------------------------------------------------------------------------------
def build_timber_question_images(
    region=None,
    national_codes: list[str] | None = None,
    k: int = 1,
    thresholds: dict | None = None,
    combine: dict | None = None,
    lookup_df=None,
):
    """Build the dict of 12 tree-question ``ee.Image`` layers (the ``qimg``) consumed by the walker.

    The keys are exactly the question names in ``risk.TIMBER_ROOT_TREE`` (and ``timber_tree_export``),
    and each value is the ``ee.Image`` analogue of the matching entry in ``add_risk_timber_col``'s
    ``yes_locals``. Exposed (rather than kept private) so a table-vs-map agreement check can inspect the
    per-question inputs.

    Parameters
    ----------
    region : ee.Geometry | ee.FeatureCollection, optional
        Only recorded for the caller's convenience; the question images are unbounded here and clipping
        happens in ``build_timber_pathway_image``. Accepted so the signatures line up.
    national_codes : list[str], optional
        ISO2 country codes whose national (``nXX_``) datasets to include, exactly as in ``whisp_risk``.
        None drops all country-specific datasets (see the module note).
    k : int
        1 = OR-union (any single source fires the node); 2 = agreement (at least two sources). Applies
        ONLY to the primary-2020 and agriculture-after-2020 COUNTS, the pixel analogue of the plot-level
        percent threshold. Every other question is a plain presence union.
    thresholds : dict, optional
        Accepted for signature parity with ``whisp_risk``'s per-indicator percent thresholds. NOT applied
        at pixel scale: a pixel has no percent coverage, so each member is a binary presence (``> 0``)
        and ``k`` is the pixel-level agreement knob instead. Reserved for a future coverage-fraction map.
    lookup_df : pd.DataFrame, optional
        Lookup table to build from; defaults to ``risk.lookup_gee_datasets_df``.

    Returns
    -------
    dict
        ``{question_name: ee.Image}`` for the 12 questions in ``risk.TIMBER_ROOT_TREE``.
    """
    if not isinstance(k, int) or k < 1:
        raise ValueError("k must be an integer >= 1 (1 = OR-union, 2 = agreement).")

    lut = lookup_gee_datasets_df if lookup_df is None else lookup_df
    # Include only global + requested-country datasets, the same filter whisp_risk applies. The
    # get_cols_ind_* getters re-apply exclude_from_output != 1 and the theme / use_for_risk_* flags.
    lut = filter_lookup_by_country_codes(
        lut, filter_col="ISO2_code", national_codes=national_codes
    )

    # --- per-indicator dataset-name pools, from the SAME getters add_risk_timber_col feeds -----------
    # Ind_01_treecover (treecover gate; pcrop pool, mirroring risk.py's gate).
    n_ind01 = get_cols_ind_01_treecover(lut)
    # Ind_02_commodities uses the pcrop pool, matching the notebook viewer / scripts. pcrop and acrop
    # commodity sets are held identical on main, so this equals the pcrop+acrop union whisp_risk forms.
    n_ind02 = get_cols_ind_02_commodities(lut, risk_col="use_for_risk_pcrop")
    n_ind05 = get_cols_ind_05_primary_2020(lut)  # Ind_05_primary_2020
    n_ind06 = get_cols_ind_06_nat_reg_2020(lut)  # Ind_06_nat_reg_forest_2020
    n_ind07a = get_cols_ind_07a_planted_2020(lut)  # Ind_07a_planted_2020
    n_ind07b = get_cols_ind_07b_plantation_2020(lut)  # Ind_07b_plantation_2020
    # Ind_08b_plantation_after_2020 (plantation GAIN = degradation).
    n_ind08b = get_cols_ind_08b_plantation_after_2020(lut)
    n_ind10 = get_cols_ind_10_agri_after_2020(lut)  # Ind_10_agri_after_2020
    n_ind12 = get_cols_ind_12_other_land_2020(lut)  # Ind_12_other_land_2020
    n_ind13 = get_cols_ind_13_other_land_after_2020(lut)  # Ind_13_other_land_after_2020
    n_ind15 = get_cols_ind_15_agriculture_2020(lut)  # Ind_15_agriculture_2020
    # Ind_16_plantation_presence_2025 (multi-year silviculture PRESENCE = stability).
    n_ind16 = get_cols_ind_16_plantation_presence_2025(lut)
    # Ind_17_disturbance_after_2020_timber (gates the derived primary_2025).
    n_ind17 = get_cols_ind_17_disturbance_after_2020_timber(lut)

    # --- indicator ee.Images (unions), and the two COUNT pools that carry the k agreement knob --------
    tc20 = _union(lut, n_ind01)  # Ind_01_treecover
    # Ind_02_commodities OR Ind_15_agriculture_2020 (the Rule-1 agriculture-2020 pool).
    ag_2020 = _union(lut, list(n_ind02) + list(n_ind15))
    # Ind_06_nat_reg_forest_2020 OR Ind_07a_planted_2020 (plain, no "and not primary" carve-out).
    regen_planted_2020 = _union(lut, list(n_ind06) + list(n_ind07a))
    plantation_2020 = _union(lut, n_ind07b)  # Ind_07b_plantation_2020
    # Ind_08b_plantation_after_2020 (plantation GAIN = degradation).
    plantation_gain_2025 = _union(lut, n_ind08b)
    # Ind_16_plantation_presence_2025 (stable-plantation PRESENCE).
    plantation_presence_2025 = _union(lut, n_ind16)
    other_land_2025 = _union(lut, n_ind13)  # Ind_13_other_land_after_2020
    other_land_2020 = _union(lut, n_ind12)  # Ind_12_other_land_2020
    combine = combine or {}

    def _folded(names, key, default):
        """Per-indicator fold: apply the ``combine[key]`` rule if supplied, else the ``default`` callable.

        ``combine`` maps an indicator id (e.g. ``"Ind_05"``, ``"Ind_10"``, ``"Ind_17"``) to a rule spec
        ``{"rule": "any"|"all"|"k"|"prop", "k": int, "prop": float}``. With ``combine`` empty this is a
        no-op and the historical behaviour is preserved exactly.
        """
        spec = combine.get(key)
        if not spec:
            return default()
        return _combine(
            lut,
            names,
            rule=spec.get("rule", "any"),
            k=int(spec.get("k", 2)),
            prop=float(spec.get("prop", 0.5)),
        )

    # Ind_17_disturbance_after_2020_timber (default OR-union; combine["Ind_17"] can require agreement).
    disturbance_2025 = _folded(n_ind17, "Ind_17", lambda: _union(lut, n_ind17))

    # Primary-2020 and agriculture-after-2020 default to the per-source vote COUNT thresholded at k
    # (k = 1 -> any single source; k = 2 -> agreement), or the combine[...] rule when supplied. These two
    # multi-source nodes are where agreement / proportion matters most (see the combine note above).
    primary_2020 = _folded(n_ind05, "Ind_05", lambda: _count(lut, n_ind05).gte(k))
    agriculture_2025 = _folded(n_ind10, "Ind_10", lambda: _count(lut, n_ind10).gte(k))

    # --- the 12 tree questions, mirroring add_risk_timber_col's yes_locals in ee.Image form ----------
    qimg = {
        # forest_2020 = treecover(Ind_01) OR primary(Ind_05) OR regen/planted(Ind_06 or Ind_07a);
        # Ind_07b_plantation_2020 is deliberately excluded, matching risk.py.
        "forest_2020": tc20.Or(primary_2020).Or(regen_planted_2020),
        "other_land_2020": other_land_2020,  # Ind_12_other_land_2020
        "other_land_2025": other_land_2025,  # Ind_13_other_land_after_2020
        # agriculture_2020 = Ind_02_commodities OR Ind_15_agriculture_2020.
        "agriculture_2020": ag_2020,
        "agriculture_2025": agriculture_2025,  # Ind_10_agri_after_2020 (deforestation)
        "plantation_2020": plantation_2020,  # Ind_07b_plantation_2020
        "plantation_presence_2025": plantation_presence_2025,  # Ind_16 (stable-plantation PRESENCE)
        "primary_2020": primary_2020,  # Ind_05_primary_2020
        "plantation_2025": plantation_gain_2025,  # Ind_08b_plantation_after_2020 (GAIN = degradation)
        # primary_2025 derived STRICT = Ind_05_primary_2020 AND NOT Ind_17_disturbance_after_2020_timber.
        "primary_2025": primary_2020.And(disturbance_2025.Not()),
        "disturbance_2025": disturbance_2025,  # Ind_17_disturbance_after_2020_timber
        # regen_planted_2020 = plain Ind_06 OR Ind_07a, NO "and not primary" carve-out: the tree checks
        # primary BEFORE regen, so ORDER (not exclusion) resolves a primary+regen overlap.
        "regen_planted_2020": regen_planted_2020,
    }
    return qimg


# --------------------------------------------------------------------------------------------------
# 3. The pathway-code image: walk risk.TIMBER_ROOT_TREE over the qimg via the single-source walker.
# --------------------------------------------------------------------------------------------------
def build_timber_pathway_image(
    region=None,
    national_codes: list[str] | None = None,
    k: int = 1,
    thresholds: dict | None = None,
    combine: dict | None = None,
    lookup_df=None,
    clip: bool = True,
):
    """Return the per-pixel timber pathway-code ``ee.Image`` (codes per ``tx.PATHWAY_TO_CODE``).

    The 12 tree-question images are assembled from the SAME ``get_cols_ind_*`` getters and derived
    combinations as ``add_risk_timber_col``, then walked by ``timber_tree_export.eval_tree_ee``, so the
    per-pixel precedence IS ``risk.TIMBER_ROOT_TREE``'s precedence by construction.

    Parameters mirror ``build_timber_question_images``; ``clip`` (default True) clips the result to
    ``region`` when one is given (pass ``clip=False`` to keep the image unbounded, e.g. for an export
    that supplies its own region). See the module docstring for the ``k`` / ``thresholds`` / national
    semantics.
    """
    qimg = build_timber_question_images(
        region=region,
        national_codes=national_codes,
        k=k,
        thresholds=thresholds,
        combine=combine,
        lookup_df=lookup_df,
    )
    pathway_image = tx.eval_tree_ee(qimg, ee.Image)
    if region is not None and clip:
        pathway_image = pathway_image.clip(region)
    return pathway_image


# --------------------------------------------------------------------------------------------------
# 4. Collapse the pathway-code image to the 3-class low / more-info / high outcome image.
#    This is the notebook viewer's _verdict3, using tx.class_codes() so the code sets come from the tree.
# --------------------------------------------------------------------------------------------------
def collapse_to_outcome3(pathway_image):
    """Collapse a pathway-code image to 1 = low / 2 = more-info-needed / 3 = high (``selfMask``'d).

    The low / more / high code sets come from ``timber_tree_export.class_codes()`` (derived from
    ``risk.TIMBER_ROOT_TREE``), so this stays in step with the tree automatically. Pixels reaching no
    coded terminal are masked out. Copies the notebook viewer's ``_verdict3`` logic exactly.
    """
    class_codes = tx.class_codes()  # {"low": [...], "more": [...], "high": [...]}

    def _codes_mask(image, codes):
        mask = ee.Image(0)
        for code in codes:
            mask = mask.Or(image.eq(code))
        return mask

    low = _codes_mask(pathway_image, class_codes["low"])
    more = _codes_mask(pathway_image, class_codes["more"])
    high = _codes_mask(pathway_image, class_codes["high"])

    out = ee.Image(0)
    out = out.where(low, 1)
    out = out.where(more, 2)
    out = out.where(high, 3)
    return out.selfMask()


# --------------------------------------------------------------------------------------------------
# 5. Palette + legend, straight from timber_tree_export.
# --------------------------------------------------------------------------------------------------
def timber_map_palette():
    """Return the palette + legend for both images, derived from ``timber_tree_export``.

    Returns
    -------
    dict with keys:
        ``code_names``     : {code: pathway label} for every code the tree emits (the pathway legend).
        ``code_colour``    : {code: hex} for the emitted codes (from ``tx.CODE_COLOUR``, no '#').
        ``pathway_vis``    : {"min", "max", "palette"} ready for a single ``addLayer`` on the pathway
                             image (the palette is contiguous over integer codes ``min``..``max``).
        ``class_codes``    : {"low"/"more"/"high": [codes]} (from ``tx.class_codes()``).
        ``outcome_names``  : {1: "low", 2: "more_info_needed", 3: "high"}.
        ``outcome_colour`` : {1/2/3: hex} for the 3-class outcome image (no '#').
        ``outcome_vis``    : {"min": 1, "max": 3, "palette": [...]} for the outcome image.
    """
    code_names = tx.code_names()  # {code: label}, only codes the tree actually reaches
    emitted = sorted(code_names)
    min_code, max_code = emitted[0], emitted[-1]
    # Contiguous palette over every integer min..max so a single addLayer(min, max, palette) is correct.
    # tx.CODE_COLOUR carries a swatch for every integer in that span (including retired codes), so no gaps.
    pathway_palette = [
        tx.CODE_COLOUR.get(c, "000000") for c in range(min_code, max_code + 1)
    ]

    return {
        "code_names": dict(code_names),
        "code_colour": {c: tx.CODE_COLOUR[c] for c in emitted},
        "pathway_vis": {"min": min_code, "max": max_code, "palette": pathway_palette},
        "class_codes": tx.class_codes(),
        "outcome_names": dict(OUTCOME_NAMES),
        "outcome_colour": dict(OUTCOME_COLOUR),
        "outcome_vis": {
            "min": 1,
            "max": 3,
            "palette": [OUTCOME_COLOUR[1], OUTCOME_COLOUR[2], OUTCOME_COLOUR[3]],
        },
    }


# --------------------------------------------------------------------------------------------------
# 6. The headline "combined map" convenience: both images + palette in one call.
# --------------------------------------------------------------------------------------------------
def build_timber_combined_map(
    region=None,
    national_codes: list[str] | None = None,
    k: int = 1,
    thresholds: dict | None = None,
    combine: dict | None = None,
    lookup_df=None,
    clip: bool = True,
):
    """Build the timber "combined map": pathway-code image + 3-class outcome image + palette / legend.

    Returns
    -------
    dict with keys:
        ``pathway_image`` : per-pixel pathway-code ``ee.Image`` (``build_timber_pathway_image``).
        ``outcome_image`` : 1 = low / 2 = more-info / 3 = high ``ee.Image`` (``collapse_to_outcome3``).
        ``palette``       : the palette / legend dict (``timber_map_palette``).

    Both images are raw ``ee.Image`` objects; any mapping stack (geemap, folium, an export, a notebook
    ``getMapId``) can consume them. See ``map_id_tile_url`` for an optional signed-tile-template helper.
    """
    pathway_image = build_timber_pathway_image(
        region=region,
        national_codes=national_codes,
        k=k,
        thresholds=thresholds,
        combine=combine,
        lookup_df=lookup_df,
        clip=clip,
    )
    outcome_image = collapse_to_outcome3(pathway_image)
    return {
        "pathway_image": pathway_image,
        "outcome_image": outcome_image,
        "palette": timber_map_palette(),
    }


# --------------------------------------------------------------------------------------------------
# 7. Optional convenience: resolve an ee.Image + vis to a signed XYZ tile template.  Uses only ee
#    (no geemap / folium), so the core builders keep no interactive-mapping dependency.
# --------------------------------------------------------------------------------------------------
def map_id_tile_url(image, vis_params: dict) -> str:
    """Resolve ``(image, vis_params)`` to a signed XYZ tile-template URL via ``getMapId``.

    Optional helper for embedding a layer as an XYZ tile source (Leaflet / folium / a static viewer).
    Uses only ``ee`` (``getMapId(...)["tile_fetcher"].url_format``); no interactive-mapping library is
    imported. The returned template is SIGNED and EXPIRES (typically within a day), so resolve it shortly
    before use. Requires Earth Engine to be initialized.
    """
    return ee.Image(image).getMapId(vis_params)["tile_fetcher"].url_format

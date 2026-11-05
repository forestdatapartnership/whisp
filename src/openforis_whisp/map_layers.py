"""Earth Engine pieces shared by the pathway viewers' map builders (``crop_map`` and ``timber_map``).

One copy of each, so the crop and timber maps cannot drift apart:

* a lookup row as a 0/1 presence image, and the OR-union or count of several (``band_image``, ``union``,
  ``count``), the same way for every tree;
* the low / more-info / high outcome image and the palette for any tree spec (``outcome3``, ``palette``);
* signed tile links, one at a time or many in parallel (``map_id_tile_url``, ``tile_urls``);
* the input dataset layers behind each tree question (``input_layers``);
* what the page needs around the map: Sentinel-2 medians, a land mask from Whisp's own water mask (the
  data behind the ``In_waterbody`` column) and a bounding box per country for the country menu.

Map layers are per-pixel PRESENCE approximations of the plot-level outcome (a pixel has no percent
coverage). WHISP is a non-authoritative exploration tool, so they show outcome signals, not a legal
determination.
"""

from __future__ import annotations

import logging
import unicodedata
from concurrent.futures import ThreadPoolExecutor

import ee

from . import datasets
from . import decision_tree as _dt

# 3-class outcome colours (green low / amber more-info / red high), hex with no leading '#'
OUTCOME_COLOUR = {1: "41ab5d", 2: "f08c00", 3: "e31a1c"}
OUTCOME_NAMES = {1: "low", 2: "more_info_needed", 3: "high"}

# Distinct within an indicator's group (the largest, Ind_02_commodities with Brazil on, has 15), starting
# away from the question layers' blue (2b6cb0).
INPUT_COLOURS = [
    "f0027f",
    "33a02c",
    "e31a1c",
    "ff7f00",
    "6a3d9a",
    "b15928",
    "a6cee3",
    "b2df8a",
    "fb9a99",
    "fdbf6f",
    "cab2d6",
    "ffff99",
    "17becf",
    "bcbd22",
    "8c564b",
    "e377c2",
]

GAUL_COUNTRIES = "projects/sat-io/open-datasets/FAO/GAUL/GAUL_2024_L0"


def init_ee(project):
    """Initialise Earth Engine on ``project``, signing in first if this machine has not yet."""
    try:
        ee.Initialize(project=project)
    except Exception:  # noqa: BLE001 - not signed in yet on this machine
        ee.Authenticate()
        ee.Initialize(project=project)


def band_image(row):
    """A lookup row's dataset as a 0/1 presence ``ee.Image``, or None if it cannot be built.

    Resolves ``corresponding_variable`` to its ``datasets.py`` ``*_prep`` function, takes band 0 (by index,
    so a renamed band still resolves), thresholds ``> 0`` and 0-fills globally, so a national dataset does
    not mask a union outside its own country.
    """
    fn = getattr(datasets, str(row["corresponding_variable"]), None)
    if fn is None:
        return None
    try:
        return ee.Image(fn()).select(0).gt(0).unmask(0, False)
    except Exception:  # noqa: BLE001 - one unbuildable dataset must not stop the rest
        return None


def row_images(lookup_df, names):
    """The presence images for every lookup row whose ``name`` is in ``names`` (lookup order)."""
    rows = lookup_df[lookup_df["name"].isin(list(names))]
    imgs = [band_image(row) for _, row in rows.iterrows()]
    return [img for img in imgs if img is not None]


def union(lookup_df, names):
    """OR-union of the presence images for ``names`` (``ee.Image(0)`` if there are none)."""
    imgs = row_images(lookup_df, names)
    if not imgs:
        return ee.Image(0)
    out = imgs[0]
    for img in imgs[1:]:
        out = out.Or(img)
    return out


def count(lookup_df, names):
    """Per-pixel count of the sources in ``names`` that are present (each source one vote)."""
    out = ee.Image(0)
    for img in row_images(lookup_df, names):
        out = out.add(img)
    return out


def outcome3(spec, pathway_image):
    """1 = low / 2 = more info / 3 = high from a pathway-code image, with the code sets from ``spec``'s
    tree; pixels on no coded outcome are masked."""
    cc = _dt.class_codes(spec)  # {"low": [...], "more": [...], "high": [...]}

    def _mask(codes):
        m = ee.Image(0)
        for c in codes:
            m = m.Or(pathway_image.eq(c))
        return m

    out = ee.Image(0).where(_mask(cc["low"]), 1)
    out = out.where(_mask(cc["more"]), 2).where(_mask(cc["high"]), 3)
    return out.selfMask()


def palette(spec):
    """Palette and legend for a tree's pathway-code and outcome images.

    Returns a dict: ``code_names`` {code: label} and ``code_colour`` {code: hex} for the codes the tree
    emits, ``pathway_vis`` (one contiguous palette over min..max code), ``class_codes``, and
    ``outcome_names`` / ``outcome_colour`` / ``outcome_vis`` for the 1 / 2 / 3 outcome image.
    """
    code_names = _dt.code_names(spec)
    code_colour = spec.code_colour
    emitted = sorted(code_names)
    lo, hi = emitted[0], emitted[-1]
    return {
        "code_names": dict(code_names),
        "code_colour": {c: code_colour[c] for c in emitted},
        "pathway_vis": {
            "min": lo,
            "max": hi,
            "palette": [code_colour.get(c, "000000") for c in range(lo, hi + 1)],
        },
        "class_codes": _dt.class_codes(spec),
        "outcome_names": dict(OUTCOME_NAMES),
        "outcome_colour": dict(OUTCOME_COLOUR),
        "outcome_vis": {
            "min": 1,
            "max": 3,
            "palette": [OUTCOME_COLOUR[1], OUTCOME_COLOUR[2], OUTCOME_COLOUR[3]],
        },
    }


def map_id_tile_url(image, vis_params: dict) -> str:
    """Resolve ``(image, vis_params)`` to a signed XYZ tile link via ``getMapId``.

    The link expires (about a day), so resolve it shortly before use. Needs Earth Engine initialised.
    """
    return ee.Image(image).getMapId(vis_params)["tile_fetcher"].url_format


def tile_urls(layers, max_workers=16):
    """Resolve {key: (image, vis_params)} to {key: tile link} in parallel.

    Each link is one round trip to Earth Engine, so many at once is much faster. A layer whose link
    cannot be made is left out with a warning, so one broken dataset does not stop the page.
    """

    def _one(item):
        key, (image, vis) = item
        try:
            return key, map_id_tile_url(image, vis)
        except Exception as e:  # noqa: BLE001
            logging.getLogger("whisp").warning("Map layer %s left out: %s", key, e)
            return key, None

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        return {k: url for k, url in ex.map(_one, layers.items()) if url}


def input_layers(names_by_column, lookup_df, max_workers=16):
    """Tile layers for each dataset behind a tree's indicators, to switch on one at a time in the viewer.

    ``names_by_column``: {column: dataset names}, in tree order, from the same getters the tree's question
    images use (so the list cannot drift from the map). Returns {column: [{"name", "iso2", "colour",
    "url"}]}, global datasets first (``iso2`` is "" for those). Needs Earth Engine initialised.
    """
    jobs = []
    for col, names in names_by_column.items():
        rows = lookup_df[lookup_df["name"].isin(list(names))]
        rows = rows.assign(iso2=rows["ISO2_code"].fillna("").astype(str))
        rows = rows.sort_values("iso2", kind="stable")
        for i, (_, row) in enumerate(rows.iterrows()):
            jobs.append((col, row, INPUT_COLOURS[i % len(INPUT_COLOURS)]))

    def _resolve(job):
        col, row, colour = job
        img = band_image(row)
        if img is None:
            return None
        try:
            url = map_id_tile_url(
                img.selfMask(), {"min": 1, "max": 1, "palette": [colour]}
            )
        except Exception as e:  # noqa: BLE001 - one broken dataset must not stop the others
            logging.getLogger("whisp").warning(
                "Input layer %s left out: %s", row["name"], e
            )
            return None
        return {"name": row["name"], "iso2": row["iso2"], "colour": colour, "url": url}

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        resolved = list(ex.map(_resolve, jobs))
    out = {col: [] for col in names_by_column}
    for (col, _, _), layer in zip(jobs, resolved):
        if layer is not None:
            out[col].append(layer)
    return out


def viewer_layers(spec, qimg, pathway, palette, agree_pathway=None, max_workers=16):
    """Every map layer of a viewer page, resolved in parallel, with water masked out (``land_mask``).

    ``qimg``: {question: presence image} and ``pathway``: the pathway-code image walked from it, for
    ``spec``'s tree. ``agree_pathway``: optional second pathway image (e.g. at least two sources agree),
    shown as its own outcome and pathway layers. Returns the keyword arguments
    ``pathway_viewer.build_pathway_viewer_html`` takes for the map: ``tiles`` (outcome, pathway and their
    ``_agree`` versions), ``node_tiles`` (one layer per box in the tree: a question's pixels or an
    outcome's pixels), ``backgrounds`` (Sentinel-2 2020 and 2025) and ``countries`` (the country menu).
    """
    from . import pathway_viewer as pv

    land = land_mask()
    pathways = {"": pathway.updateMask(land)}
    if agree_pathway is not None:
        pathways["_agree"] = agree_pathway.updateMask(land)
    jobs = {}
    for suffix, pw in pathways.items():
        jobs["outcome" + suffix] = (outcome3(spec, pw), palette["outcome_vis"])
        jobs["pathway" + suffix] = (pw.selfMask(), palette["pathway_vis"])
    for q, img in qimg.items():
        jobs["q:" + q] = (
            img.selfMask().updateMask(land),
            {"min": 1, "max": 1, "palette": ["2b6cb0"]},
        )
    for c in palette["code_names"]:
        jobs["c:%d" % c] = (
            pathways[""].eq(c).selfMask(),
            {"min": 1, "max": 1, "palette": [palette["code_colour"][c]]},
        )
    s2_vis = {"bands": ["B4", "B3", "B2"], "min": 0, "max": 3000}
    jobs["s2:2020"] = (s2_median("2020-01-01", "2020-12-31"), s2_vis)
    jobs["s2:2025"] = (s2_median("2025-01-01", "2025-12-31"), s2_vis)

    # the country boxes take about half a minute: fetch them while the tile links resolve
    pool = ThreadPoolExecutor(max_workers=1)
    boxes = pool.submit(country_boxes)
    urls = tile_urls(jobs, max_workers=max_workers)
    try:
        countries = boxes.result(timeout=300)
    except Exception as e:  # noqa: BLE001 - the page works without the menu
        logging.getLogger("whisp").warning("Country menu left out: %s", e)
        countries = []
    pool.shutdown(wait=False)

    def _pick(prefix):
        return {k[len(prefix) :]: u for k, u in urls.items() if k.startswith(prefix)}

    q_tiles, code_tiles = _pick("q:"), {int(c): u for c, u in _pick("c:").items()}
    return {
        "tiles": {
            k: u for k, u in urls.items() if k.startswith(("outcome", "pathway"))
        },
        "node_tiles": pv.node_tiles_by_id(spec, q_tiles, code_tiles, palette),
        "backgrounds": _pick("s2:"),
        "countries": countries,
    }


def s2_median(start, end):
    """Sentinel-2 surface reflectance median, scenes under 20% cloud (a ground reference under the map)."""
    return (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
        .median()
    )


def land_mask():
    """1 on land and 0 on water, from Whisp's own water mask (the data behind the ``In_waterbody``
    column), so the map hides the same water the plot-level flag uses."""
    return ee.Image(datasets.g_water_mask_prep()).unmask(0).eq(0)


def country_boxes():
    """[[name, south, west, north, east]] for every country, sorted by name, for the page's country menu.

    From GAUL 2024, the boundaries Whisp's country columns come from. GAUL splits some countries into
    several pieces, which are merged here by name. Takes about half a minute, so run it alongside the
    tile links.
    """
    fc = ee.FeatureCollection(GAUL_COUNTRIES)
    rows = fc.map(
        lambda f: ee.Feature(
            None,
            {
                "n": f.get("gaul0_name"),
                "r": f.geometry().bounds(1000).coordinates().get(0),
            },
        )
    ).getInfo()["features"]
    box = {}
    for p in (r["properties"] for r in rows):
        if p.get("n") and p.get("r"):
            xs, ys = [pt[0] for pt in p["r"]], [pt[1] for pt in p["r"]]
            b = box.setdefault(p["n"], [90.0, 180.0, -90.0, -180.0])
            b[:] = [
                min(b[0], min(ys)),
                min(b[1], min(xs)),
                max(b[2], max(ys)),
                max(b[3], max(xs)),
            ]

    def _key(name):
        return (
            unicodedata.normalize("NFKD", name)
            .encode("ascii", "ignore")
            .decode()
            .lower()
        )

    return [[n] + [round(v, 3) for v in box[n]] for n in sorted(box, key=_key)]

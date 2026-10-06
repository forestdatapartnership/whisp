"""Build the perennial-crop (pcrop) and annual-crop (acrop) pathway viewers against MAIN's data.

Same recipe as temp_dev_notes/viewer_and_maps/build_crop_viewers.py, but:
  * run on a checkout whose risk.py / datasets.py / lookup are main's (the viewer modules
    decision_tree, pcrop_tree_export, acrop_tree_export, crop_map, pathway_viewer, serve_viewer are
    additive and copied in from feat/timber-tree-rejig; they only need main's four crop getters),
  * Brazil national layers ON (national_codes=["br"]), Brazil-centred bookmarks,
  * output next to the deck, in temp_dev_notes/presentations/viewers/.

Run:
    source .venv/Scripts/activate
    python temp_dev_notes/presentations/viewers/build_crop_viewers_main.py [pcrop|acrop|both]

Serve one for working draw-and-analyze (separate terminal):
    python -m openforis_whisp.serve_viewer temp_dev_notes/presentations/viewers/pcrop_pathway_viewer.html 8788
    then open http://127.0.0.1:8788/ in Chrome or Edge.

The Earth Engine tile tokens embedded in the HTML expire after about a day: re-run before the talk.
"""
import os
import sys
import pathlib

import ee

from openforis_whisp import crop_map as cm
from openforis_whisp import decision_tree as dt
from openforis_whisp import pathway_viewer as pv
from openforis_whisp import pcrop_tree_export as pc
from openforis_whisp import acrop_tree_export as ac

EE_PROJECT = os.environ.get("EE_PROJECT", "ee-andyarnellgee")
OUT_DIR = pathlib.Path(__file__).resolve().parent
NATIONAL = [
    "br"
]  # Brazil national layers on top of the global ones; [] for global only

BOOKMARKS = [
    ["Coffee (Minas Gerais)", -21.0, -46.0, 8],
    ["Cocoa (southern Bahia)", -15.0, -39.3, 9],
    ["Oil palm (Para)", -2.3, -48.6, 9],
    ["Soy frontier (Mato Grosso)", -12.5, -55.5, 8],
    ["Cocoa (Cote d Ivoire)", 6.7, -5.5, 9],
]

VIEWERS = {
    "pcrop": (
        pc.PCROP_SPEC,
        "use_for_risk_pcrop",
        "risk_pcrop",
        "Perennial-crop pathway viewer",
        "risk_pcrop tree + combined risk map (global + Brazil national layers)",
    ),
    "acrop": (
        ac.ACROP_SPEC,
        "use_for_risk_acrop",
        "risk_acrop",
        "Annual-crop pathway viewer",
        "risk_acrop tree + combined risk map (global + Brazil national layers)",
    ),
}


def _s2_median(start, end):
    return (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterDate(start, end)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", 20))
        .median()
    )


def build(which=("pcrop", "acrop")):
    ee.Initialize(project=EE_PROJECT)
    land = ee.ImageCollection("ESA/WorldCover/v200").mosaic().gt(0)
    s2_vis = {"bands": ["B4", "B3", "B2"], "min": 0, "max": 3000}
    backgrounds = {
        "2020": cm.map_id_tile_url(_s2_median("2020-01-01", "2020-12-31"), s2_vis),
        "2025": cm.map_id_tile_url(_s2_median("2025-01-01", "2025-12-31"), s2_vis),
    }
    out = []
    for name in which:
        spec, risk_use_col, risk_col, title, subtitle = VIEWERS[name]
        qimg = cm.build_crop_question_images(
            spec, risk_use_col, national_codes=NATIONAL
        )
        pathway = dt.eval_tree_ee(qimg, ee.Image, spec).updateMask(land)
        outcome = cm.collapse_to_outcome3(spec, pathway)
        palette = cm.crop_map_palette(spec)
        tiles = {
            "outcome": cm.map_id_tile_url(outcome, palette["outcome_vis"]),
            "pathway": cm.map_id_tile_url(pathway.selfMask(), palette["pathway_vis"]),
        }
        # one layer per tree node: a question's presence pixels, or a pathway code's pixels
        q_tiles = {
            q: cm.map_id_tile_url(
                img.selfMask().updateMask(land),
                {"min": 1, "max": 1, "palette": ["2b6cb0"]},
            )
            for q, img in qimg.items()
        }
        code_tiles = {
            c: cm.map_id_tile_url(
                pathway.eq(c).selfMask(),
                {"min": 1, "max": 1, "palette": [palette["code_colour"][c]]},
            )
            for c in palette["code_names"]
        }
        node_tiles = pv.node_tiles_by_id(spec, q_tiles, code_tiles, palette)
        html = pv.build_pathway_viewer_html(
            spec,
            tiles,
            palette,
            title,
            subtitle=subtitle,
            bookmarks=BOOKMARKS,
            backgrounds=backgrounds,
            risk_col=risk_col,
            center=(-12, -52),
            zoom=4,
            node_tiles=node_tiles,
        )
        path = OUT_DIR / f"{name}_pathway_viewer.html"
        path.write_text(html, encoding="utf-8")
        print(f"wrote {path} ({len(html)} chars)")
        out.append(path)
    return out


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "both"
    build(("pcrop", "acrop") if arg == "both" else (arg,))

"""Build the perennial-crop (pcrop) and annual-crop (acrop) pathway viewers (the script version of
notebooks/crop_pathway_viewer.ipynb; both call crop_map.build_crop_viewer).

Run:
    source .venv/Scripts/activate
    python notebooks/build_crop_viewers.py [pcrop|acrop|both]

Serve one for draw-and-analyse with the local package (separate terminal):
    python -m openforis_whisp.serve_viewer notebooks/pcrop_pathway_viewer.html 8788
    then open http://127.0.0.1:8788/ in Chrome or Edge.

The Earth Engine map layers stop loading about a day after building: re-run before sharing.
"""
import os
import sys
import pathlib

from openforis_whisp import crop_map as cm

EE_PROJECT = os.environ.get("EE_PROJECT", "ee-andyarnellgee")
OUT_DIR = pathlib.Path(__file__).resolve().parent
NATIONAL = [
    "br"
]  # Brazil national layers on top of the global ones; [] for global only


def build(which=("pcrop", "acrop")):
    out = []
    for name in which:
        html = cm.build_crop_viewer(
            name, ee_project=EE_PROJECT, national_codes=NATIONAL
        )
        path = OUT_DIR / f"{name}_pathway_viewer.html"
        path.write_text(html, encoding="utf-8")
        print(f"wrote {path} ({len(html)} chars)")
        out.append(path)
    return out


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else "both"
    build(("pcrop", "acrop") if arg == "both" else (arg,))

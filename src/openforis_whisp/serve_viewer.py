"""Serve a pathway-viewer HTML in a REAL browser with WORKING draw-and-analyze (package backend).

Generic local-serve proxy for the pathway viewers (timber / pcrop / acrop). It serves the given HTML at
``/`` and runs WHISP on any polygon the viewer POSTs to ``/api/submit/geojson`` (same-origin, so no CORS
and no API key: it runs the installed ``openforis_whisp`` package on your Earth Engine project). This is
the same idea as the timber notebook's local-serve cell, generalized so any viewer can use it, and it is
why a drawn plot can be analyzed (a plain file opened directly has no backend). ``/api/info`` says which
code that is, and ``/api/relay/...`` passes calls on to the live Whisp API with the key the page sends, so
the page can also analyse with the API (or both, to compare) without the browser's CORS block.
``/api/pick?lat=..&lon=..`` reads the page's own map layers at one spot (the pathway, each question of the
decision tree and the datasets there), for the viewer's pixel picker.

Usage (run in a SEPARATE terminal, not the one running other tools):

    source .venv/Scripts/activate
    python -m openforis_whisp.serve_viewer pcrop_pathway_viewer.html 8788 [ee_project]

(without ``ee_project``: EE_PROJECT or PROJECT from the environment, else your Earth Engine default)

then open  http://127.0.0.1:8788/  in Chrome or Edge (use 127.0.0.1, NOT the VS Code preview). Draw a
polygon and it is analyzed; the viewer reads its own risk column (risk_pcrop / risk_acrop / risk_timber)
and colours the plot by its pathway via the single-source JS tree walk.
"""

from __future__ import annotations

import inspect
import json
import re
import sys
import tempfile
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .pathway_viewer import WHISP_API_BASE, package_info

# the viewers the pixel picker knows how to rebuild the map for, by the page's risk column
PICK_TREES = {
    "risk_timber": "timber",
    "risk_pcrop": "pcrop",
    "risk_acrop": "acrop",
}


def _pick_layers(tree, national_codes):
    """The page's per-pixel map, rebuilt the same way as the viewer: (one image with the pathway code as
    "code" and each tree question as "q_<question>", the palette, the question names)."""
    import ee

    if tree == "timber":
        from . import timber_tree_export as tx
        from .timber_map import build_timber_question_images, timber_map_palette

        qimg = build_timber_question_images(national_codes=national_codes)
        pathway, palette = tx.eval_tree_ee(qimg, ee.Image), timber_map_palette()
    else:
        from . import acrop_tree_export, pcrop_tree_export
        from .crop_map import build_crop_question_images, crop_map_palette

        export = {"pcrop": pcrop_tree_export, "acrop": acrop_tree_export}[tree]
        spec = export.PCROP_SPEC if tree == "pcrop" else export.ACROP_SPEC
        qimg = build_crop_question_images(
            spec, "use_for_risk_" + tree, national_codes=national_codes
        )
        pathway, palette = export.eval_tree_ee(qimg, ee.Image), crop_map_palette(spec)
    bands = [pathway.rename("code")]
    bands += [ee.Image(img).rename("q_" + q) for q, img in qimg.items()]
    return ee.Image.cat(bands), palette, list(qimg)


def _clean_thresholds(t):
    """The threshold arguments from the page that whisp_risk accepts, each clamped to 0-100."""
    from .risk import whisp_risk

    accepted = inspect.signature(whisp_risk).parameters
    out = {}
    for k, v in (t or {}).items():
        if re.match(r"^ind_\d+[a-z]?_pcent_threshold$", str(k)) and k in accepted:
            try:
                out[k] = max(0.0, min(100.0, float(v)))
            except (TypeError, ValueError):
                pass
    return out


def _relay(method, path, body, api_key):
    """Pass one call on to the live Whisp API (server to server, so the browser's CORS rule does not
    apply) with the key the page sent; returns (status, response bytes)."""
    req = urllib.request.Request(
        WHISP_API_BASE + path,
        data=body if method == "POST" else None,
        method=method,
        headers={"x-api-key": api_key or "", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def serve(html_path, port=8788, project=None, national_codes=None):
    """Serve ``html_path`` at http://127.0.0.1:port/ and analyze drawn polygons with the WHISP package.

    ``project``: the Earth Engine project to run on; by default EE_PROJECT or PROJECT from the
    environment, else your Earth Engine default project.
    ``national_codes``: national datasets to include; by default the ones the page's map was built with.
    """
    import os

    import ee
    import openforis_whisp as whisp

    project = project or os.environ.get("EE_PROJECT") or os.environ.get("PROJECT")
    try:
        if project:
            ee.Initialize(project=project)
        else:
            ee.Initialize()
    except ee.EEException as e:
        raise SystemExit(
            "Earth Engine needs a project: add it after the port (python -m openforis_whisp.serve_viewer "
            "page.html 8788 your-ee-project) or set EE_PROJECT. (%s)" % e
        )
    html = open(html_path, encoding="utf-8").read()
    html_bytes = html.encode("utf-8")
    if national_codes is None:
        # analyse with the same national layers as the map, so a plot and the pixels under it agree
        m = re.search(r'"nationalCodes":(\[[^\]]*\])', html)
        national_codes = json.loads(m.group(1)) if m else None
    package = package_info()
    package["national_codes"] = national_codes or []
    m = re.search(r'const RISK_COL = "([a-z_]+)"', html)
    pick_tree = PICK_TREES.get(m.group(1) if m else "")
    # the page offers its pixel picker when this is true
    package["pick"] = bool(pick_tree)

    _img_cache = (
        {}
    )  # the WHISP multiband image, built once and reused for every drawn polygon
    _stats_cache = (
        {}
    )  # token -> raw stats DataFrame (Earth Engine run once, reused for cheap re-thresholding)
    _order = []  # token insertion order, so the cache stays memory-bounded

    def _whisp_image():
        if "img" not in _img_cache:
            print("  [package] building the whisp image once (a few seconds)...")
            _img_cache["img"] = whisp.combine_datasets(
                national_codes=national_codes, auto_recovery=True
            )
        return _img_cache["img"]

    def _run_stats(fc):
        feats = fc.get("features", [])
        for i, ft in enumerate(feats):
            ft.setdefault("properties", {})
            ft["properties"]["external_id"] = ft["properties"].get("external_id") or (
                "drawn_%d" % (i + 1)
            )
        with tempfile.NamedTemporaryFile("w", suffix=".geojson", delete=False) as f:
            json.dump({"type": "FeatureCollection", "features": feats}, f)
            in_path = f.name
        return whisp.whisp_formatted_stats_geojson_to_df(
            input_geojson_filepath=in_path,
            external_id_column="external_id",
            national_codes=national_codes,
            unit_type="ha",
            whisp_image=_whisp_image(),
            mode="sequential",
        )

    def _apply_risk(stats_df, thresholds):
        # whisp_risk adds the Ind_* + risk_pcrop / risk_acrop / risk_timber columns; re-runnable cheaply
        # (pandas, no Earth Engine) at different thresholds. Single source: no tree logic re-implemented here.
        df = whisp.whisp_risk(
            stats_df.copy(),
            national_codes=national_codes,
            include_working_columns=True,  # the page walks the timber decision tree from them
            **_clean_thresholds(thresholds),
        )
        out_path = tempfile.NamedTemporaryFile(
            "w", suffix=".geojson", delete=False
        ).name
        whisp.convert_df_to_geojson(df, out_path, geo_column="geo")
        return json.load(open(out_path, encoding="utf-8"))

    def _cache_stats(stats_df):
        token = uuid.uuid4().hex
        _stats_cache[token] = stats_df
        _order.append(token)
        while len(_order) > 50:
            _stats_cache.pop(_order.pop(0), None)
        return token

    # the map layers and the whisp image in one image, built once for every pick
    _pick_cache = {}

    def _pick(lat, lon):
        if "img" not in _pick_cache:
            print("  [pick] building the map layers once (a few seconds)...")
            img, palette, questions = _pick_layers(pick_tree, national_codes)
            # the datasets too: each band of the whisp image is the dataset's area in the pixel, so > 0 = there
            _pick_cache.update(
                img=img.addBands(_whisp_image()), palette=palette, questions=questions
            )
        palette = _pick_cache["palette"]
        vals = (
            _pick_cache["img"]
            .reduceRegion(ee.Reducer.first(), ee.Geometry.Point([lon, lat]), 10)
            .getInfo()
        )
        code = None if vals.get("code") is None else int(vals["code"])
        names = palette["code_names"]
        outcome = next(
            (k for k, codes in palette["class_codes"].items() if code in codes), None
        )
        return {
            "lat": lat,
            "lon": lon,
            "code": code,
            "pathway": names.get(code, names.get(str(code), "")),
            "outcome": {"low": "low", "more": "more_info_needed", "high": "high"}.get(
                outcome
            ),
            "questions": {
                q: bool(vals.get("q_" + q)) for q in _pick_cache["questions"]
            },
            "present": sorted(
                k
                for k, v in vals.items()
                if v and k != "code" and not k.startswith("q_")
            ),
        }

    class Handler(BaseHTTPRequestHandler):
        def _send(self, code, body, ctype="application/json"):
            b = body if isinstance(body, bytes) else body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path in ("/", "") or path.startswith("/index"):
                self._send(200, html_bytes, "text/html; charset=utf-8")
            elif (
                path == "/api/info"
            ):  # tells the page it is served here, and by which code
                self._send(200, json.dumps(package))
            elif path == "/api/pick" and pick_tree:
                query = parse_qs(urlparse(self.path).query)
                try:
                    lat, lon = float(query["lat"][0]), float(query["lon"][0])
                    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                        raise ValueError("lat / lon out of range")
                    self._send(200, json.dumps(_pick(lat, lon)))
                except Exception as e:  # noqa: BLE001 - surface the error to the page
                    self._send(500, json.dumps({"error": repr(e)}))
            elif path.startswith("/api/relay/"):
                self._send(
                    *_relay(
                        "GET",
                        path[len("/api/relay") :],
                        None,
                        self.headers.get("x-api-key"),
                    )
                )
            else:
                self._send(404, b"{}")

        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(n) if n else b"{}"
            path = self.path.rstrip("/")
            try:
                if path.startswith("/api/relay/"):
                    self._send(
                        *_relay(
                            "POST",
                            path[len("/api/relay") :],
                            body,
                            self.headers.get("x-api-key"),
                        )
                    )
                elif path.endswith("/submit/geojson"):
                    fc = json.loads(body or b"{}")
                    stats_df = _run_stats(fc)
                    token = _cache_stats(stats_df)
                    self._send(
                        200,
                        json.dumps(
                            {
                                "token": token,
                                "data": _apply_risk(stats_df, {}),
                                "package": package,
                            }
                        ),
                    )
                elif path.endswith("/reclassify"):
                    req = json.loads(body or b"{}")
                    stats_df = _stats_cache.get(req.get("token"))
                    if stats_df is None:
                        self._send(
                            404,
                            json.dumps(
                                {"error": "unknown or expired token; redraw the plot"}
                            ),
                        )
                        return
                    self._send(
                        200,
                        json.dumps(
                            {"data": _apply_risk(stats_df, req.get("thresholds"))}
                        ),
                    )
                else:
                    self._send(404, b"{}")
            except Exception as e:  # noqa: BLE001 - surface the error to the browser, do not crash the server
                self._send(500, json.dumps({"error": repr(e)}))

        def log_message(self, *args):  # keep the console quiet
            pass

    print(
        "Serving %s at http://127.0.0.1:%d/  (open in Chrome/Edge; use 127.0.0.1, NOT the VS Code preview)"
        % (html_path, port)
    )
    print("  analysing with openforis-whisp %s" % package)
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(
            "usage: python -m openforis_whisp.serve_viewer <page.html> [port] [ee_project]"
        )
    port_arg = int(sys.argv[2]) if len(sys.argv) > 2 else 8788
    project_arg = sys.argv[3] if len(sys.argv) > 3 else None
    serve(sys.argv[1], port_arg, project=project_arg)

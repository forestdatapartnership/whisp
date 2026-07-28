"""Serve a pathway-viewer HTML in a REAL browser with WORKING draw-and-analyze (package backend).

Generic local-serve proxy for the pathway viewers (timber / pcrop / acrop). It serves the given HTML at
``/`` and runs WHISP on any polygon the viewer POSTs to ``/api/submit/geojson`` (same-origin, so no CORS
and no API key: it runs the installed ``openforis_whisp`` package on your Earth Engine project). This is
the same idea as the timber notebook's local-serve cell, generalized so any viewer can use it, and it is
why a drawn plot can be analyzed (a plain file opened directly has no backend).

Usage (run in a SEPARATE terminal, not the one running other tools):

    source .venv/Scripts/activate
    python -m openforis_whisp.serve_viewer C:/tmp/pcrop_pathway_viewer.html 8788

then open  http://127.0.0.1:8788/  in Chrome or Edge (use 127.0.0.1, NOT the VS Code preview). Draw a
polygon and it is analyzed; the viewer reads its own risk column (risk_pcrop / risk_acrop / risk_timber)
and colours the plot by its pathway via the single-source JS tree walk.
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def serve(html_path, port=8788, project="ee-andyarnellgee", national_codes=None):
    """Serve ``html_path`` at http://127.0.0.1:port/ and analyze drawn polygons with the WHISP package."""
    import ee
    import openforis_whisp as whisp

    ee.Initialize(project=project)
    html_bytes = open(html_path, encoding="utf-8").read().encode("utf-8")

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

    def _clean_thresholds(t):
        out = {}
        for k, v in (t or {}).items():
            if re.match(r"^ind_\d+_pcent_threshold$", str(k)):
                try:
                    out[k] = max(0.0, min(100.0, float(v)))
                except (TypeError, ValueError):
                    pass
        return out

    def _apply_risk(stats_df, thresholds):
        # whisp_risk adds the Ind_* + risk_pcrop / risk_acrop / risk_timber columns; re-runnable cheaply
        # (pandas, no Earth Engine) at different thresholds. Single source: no tree logic re-implemented here.
        df = whisp.whisp_risk(
            stats_df.copy(),
            national_codes=national_codes,
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
            else:
                self._send(404, b"{}")

        def do_POST(self):  # noqa: N802
            n = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(n) if n else b"{}"
            path = self.path.rstrip("/")
            try:
                if path.endswith("/submit/geojson"):
                    fc = json.loads(body or b"{}")
                    stats_df = _run_stats(fc)
                    token = _cache_stats(stats_df)
                    self._send(
                        200,
                        json.dumps({"token": token, "data": _apply_risk(stats_df, {})}),
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
    ThreadingHTTPServer(("127.0.0.1", port), Handler).serve_forever()


if __name__ == "__main__":
    html = sys.argv[1] if len(sys.argv) > 1 else "C:/tmp/pcrop_pathway_viewer.html"
    port_arg = int(sys.argv[2]) if len(sys.argv) > 2 else 8788
    project_arg = sys.argv[3] if len(sys.argv) > 3 else "ee-andyarnellgee"
    serve(html, port_arg, project=project_arg)

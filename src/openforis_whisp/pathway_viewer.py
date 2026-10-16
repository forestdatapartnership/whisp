"""Shared, spec-driven pathway VIEWER builder: a self-contained HTML page with the decision-tree diagram
(left) and the combined risk map (right), for ANY WHISP tree spec.

Reusable shell behind the per-commodity viewers. Given a ``DecisionTreeSpec`` (so the Mermaid diagram and
the client-side tree walk come from the single-source ``decision_tree``) plus already-resolved map tile
URLs + palette (from ``timber_map`` or ``crop_map``), it returns one self-contained HTML string.

Features
--------
* decision-tree diagram (left); click a question node to map that indicator's pixels, an outcome node to
  map the pixels on that pathway (``node_tiles`` from ``node_tiles_by_id``), click again or Esc to clear;
  the rest of the tree dims, an outcome's route lights up and a banner on the map names what is shown;
* the input datasets behind each question, each its own on/off layer (``input_layers``), for looking only;
* combined risk map (right) with a real Leaflet layer control (each layer an on/off checkbox; outcome and
  pathway either/or) + a legend of the one layer in view (bottom left, clear of the controls) + a
  risk-opacity slider + Sentinel-2 background layers + a map/satellite base;
* draw a polygon (Leaflet.draw) -> analyze through a same-origin ``/api`` (the notebook's local proxy /
  ``serve_viewer``); the drawn plot is coloured by its risk and a result panel shows a tick / cross per
  indicator and the outcome + pathway (following the timber viewer);
* per-indicator THRESHOLD SLIDERS (starting at whisp_risk's defaults) that RE-CLASSIFY the drawn plot live: the proxy caches the
  plot's stats once (the slow Earth Engine step) and re-runs the real ``whisp_risk`` with the new thresholds
  on each change (fast pandas, no Earth Engine), so the impact of changing thresholds is visible instantly.

WHISP is a non-authoritative exploration tool, so the map shows outcome SIGNALS (low / more-info / high),
not a legal determination (EUDR is one example framework it can inform, not the focus).
"""

from __future__ import annotations

import datetime
import inspect
import json
import pathlib
import re
import subprocess

from . import decision_tree as _dt

# the live Whisp API; a page outside whisp.openforis.org can only call it from the browser once the API
# allows that site (CORS), so when served locally the calls go through serve_viewer's relay instead
WHISP_API_BASE = "https://whisp.openforis.org/api"


def package_info():
    """Which openforis_whisp code is running: its version and, when run from a git checkout, the branch,
    commit and whether src has uncommitted changes. The installed version number alone can be stale
    (it is read from the install record, not from the code on the path)."""
    src = pathlib.Path(__file__).resolve().parent
    info = {"version": None, "branch": None, "commit": None, "changed": False}
    pyproject = src.parent.parent / "pyproject.toml"
    if pyproject.exists():
        m = re.search(
            r'^version\s*=\s*"([^"]+)"', pyproject.read_text(encoding="utf-8"), re.M
        )
        info["version"] = m.group(1) if m else None

        def git(*args):
            return subprocess.run(
                ["git", "-C", str(src)] + list(args),
                capture_output=True,
                text=True,
                timeout=10,
            ).stdout.strip()

        try:
            info["branch"] = git("rev-parse", "--abbrev-ref", "HEAD") or None
            info["commit"] = git("rev-parse", "--short", "HEAD") or None
            info["changed"] = bool(git("status", "--porcelain", "--", str(src)))
        except (OSError, subprocess.SubprocessError):
            pass
    if not info["version"]:
        from importlib.metadata import version

        info["version"] = version("openforis-whisp")
    return info


def _routes(spec):
    """Pathway code -> node ids from the top of the tree to its outcome box, to light up a plot's route."""
    out = {}

    def walk(node, route):
        route = route + [node["id"]]
        if "q" not in node:
            out[node["code"]] = route
            return
        walk(node["yes"], route)
        walk(node["no"], route)

    walk(_dt.annotate(spec), [])
    return out


def _legend_rows(palette):
    out_names = {1: "Low", 2: "More info", 3: "High"}
    oc = palette["outcome_colour"]
    outcome = "".join(
        "<div class='row'><i style='background:#%s'></i>%s</div>"
        % (oc[k], out_names[k])
        for k in (1, 2, 3)
    )
    # pathways grouped under the outcome they lead to
    cn, cc = palette["code_names"], palette["code_colour"]
    pathway = ""
    for cls, name in (("low", "Low"), ("more", "More info"), ("high", "High")):
        codes = [c for c in palette["class_codes"][cls] if c in cn]
        if codes:
            pathway += "<div class='grp'>%s</div>" % name
            pathway += "".join(
                "<div class='row'><i style='background:#%s'></i><b>%d</b> %s</div>"
                % (cc[c], c, cn[c].split(": ", 1)[-1])
                for c in codes
            )
    return outcome, pathway


# outcome box outlines, as in the trees' low / more / high classDefs
_OUTCOME_STROKE = {"low": "16a34a", "more": "c56f00", "high": "b91c1c"}


def _route_styles(spec, palette):
    """Mermaid lines filling each outcome box with its pathway's map colour, outlined in its outcome's
    colour, so a box in the tree matches its colour on the pathway map. Declared after the trees' own
    classDefs, so these win."""
    lines = []
    for node in _dt.iter_nodes(_dt.annotate(spec)):
        if "q" in node or node["code"] not in palette["code_colour"]:
            continue
        fill = palette["code_colour"][node["code"]]
        cls = next(k for k, v in palette["class_codes"].items() if node["code"] in v)
        r, g, b = (int(fill[i : i + 2], 16) for i in (0, 2, 4))
        text = "111" if 0.299 * r + 0.587 * g + 0.114 * b > 140 else "fff"
        lines.append(
            "  classDef route_%d fill:#%s,stroke:#%s,stroke-width:4px,color:#%s;"
            % (node["code"], fill, _OUTCOME_STROKE[cls], text)
        )
        lines.append("  class %s route_%d" % (node["id"], node["code"]))
    return "\n".join(lines) + "\n"


def _default_thresholds():
    """whisp_risk's own default threshold for each indicator, so the viewer starts from the same values."""
    from .risk import whisp_risk

    return {
        name: p.default
        for name, p in inspect.signature(whisp_risk).parameters.items()
        if re.fullmatch(r"ind_\d+_pcent_threshold", name)
    }


def _indicators(spec):
    """Ordered unique [{label, param, default}] for the tree's indicators; param is the whisp_risk
    threshold argument and default its default value in whisp_risk."""
    defaults = _default_thresholds()
    seen, out = set(), []
    for cols in spec.q_to_columns.values():
        for c in cols:
            if c in seen:
                continue
            seen.add(c)
            m = re.match(r"Ind_0*(\d+)", c)
            if m:
                param = "ind_%d_pcent_threshold" % int(m.group(1))
                out.append(
                    {"label": c, "param": param, "default": defaults.get(param, 10)}
                )
    return out


def node_tiles_by_id(spec, question_tiles=None, code_tiles=None, palette=None):
    """Map each Mermaid node id to the tile url shown when the node is clicked.

    ``question_tiles``: {question_name: xyz_url} (that question's presence pixels).
    ``code_tiles``: {code: xyz_url} (pixels on that pathway). Nodes with no url are not clickable.
    """
    out = {}
    anno = _dt.annotate(spec)
    cc = (palette or {}).get("code_colour", {}) if palette else {}
    cn = (palette or {}).get("code_names", {}) if palette else {}

    # an outcome also carries its route (node ids from the top of the tree), so the viewer can light it up
    def walk(node, route):
        route = route + [node["id"]]
        if "q" not in node:
            code = node["code"]
            url = (code_tiles or {}).get(code)
            if url:
                out[node["id"]] = {
                    "url": url,
                    "label": "%s (code %d)"
                    % (cn.get(code, node.get("pathway", "")), code),
                    "colour": "#" + cc.get(code, "888888"),
                    "kind": "outcome",
                    "route": route,
                }
            return
        q = node["q"]
        url = (question_tiles or {}).get(q)
        if url:
            out[node["id"]] = {
                "url": url,
                "label": re.sub(r"<[^>]+>", " ", node.get("label", q))
                .replace("  ", " ")
                .strip(),
                "colour": "#2b6cb0",
                "kind": "question",
                "q": q,
            }
        walk(node["yes"], route)
        walk(node["no"], route)

    walk(anno, [])
    return out


def _input_groups(spec, input_layers):
    """Ordered [{q, question, indicator, layers}] for the input datasets panel, one per tree question."""
    out = []
    for q, cols in spec.q_to_columns.items():
        question = re.sub(r"<[^>]+>", "", spec.q_label.get(q, q).split("<br")[0])
        for c in cols:
            if (input_layers or {}).get(c):
                out.append(
                    {
                        "q": q,
                        "question": question.strip(),
                        "indicator": c,
                        "layers": input_layers[c],
                    }
                )
    return out


def build_pathway_viewer_html(
    spec,
    tiles,
    palette,
    title,
    subtitle="",
    bookmarks=None,
    backgrounds=None,
    risk_col="",
    center=(4, 12),
    zoom=3,
    node_tiles=None,
    layer_min_zoom=8,
    input_layers=None,
    national_codes=None,
    api_key=None,
):
    """Return a self-contained viewer HTML string. See the module docstring for the feature list.

    ``tiles``: {"outcome", "pathway"} tile urls, or None for a page without map layers (tree, base map and
    plot analysis only, which never expires).
    ``input_layers``: {indicator_column: [{"name", "iso2", "colour", "url"}]} (``crop_map.crop_input_layers``),
    shown as an on/off list of the datasets behind each question.
    ``national_codes``: the national layers the map was built with; plots are analysed with the same ones.
    ``api_key``: a Whisp API key to build into the page. Anyone with the file can read it, so by default
    leave it out and whoever opens the page pastes their own.
    """
    outcome_leg, pathway_leg = _legend_rows(palette)
    built = package_info()
    built["date"] = datetime.date.today().isoformat()
    cfg = json.dumps(
        {
            "tiles": tiles or {},
            "apiBase": WHISP_API_BASE,
            "apiKey": api_key or "",
            "builtWith": built,
            "routes": _routes(spec),
            "backgrounds": backgrounds or {},
            "bookmarks": bookmarks or [],
            "center": list(center),
            "zoom": zoom,
            "indicators": _indicators(spec),
            "nodeTiles": node_tiles or {},
            "layerMinZoom": layer_min_zoom,
            "inputGroups": _input_groups(spec, input_layers),
            "nationalCodes": list(national_codes or []),
        },
        separators=(",", ":"),
    )
    repl = {
        "__TITLE__": title,
        "__SUBTITLE__": subtitle,
        "__OUTCOME_LEG__": outcome_leg,
        "__PATHWAY_LEG__": pathway_leg,
        "__CFG__": cfg,
        "__MERMAID_JSON__": json.dumps(
            _dt.to_mermaid(spec) + _route_styles(spec, palette)
        ),
        "__JS_MODULE__": _dt.js_module_source(spec),
        "__STORY_FN__": spec.js_story_fn,
        "__RISK_COL__": risk_col,
        "__CODE_COLOUR__": json.dumps(palette["code_colour"], separators=(",", ":")),
        "__CODE_NAMES__": json.dumps(palette["code_names"], separators=(",", ":")),
    }
    html = _TEMPLATE
    for k, v in repl.items():
        html = html.replace(k, v)
    return html


_TEMPLATE = r"""<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>__TITLE__</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet-draw@1.0.4/dist/leaflet.draw.js"></script>
<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
<style>
  :root{--ink:#17201b;--ink2:#4a544d;--line:#dce1d7;--accent:#33547e;--low:#238b45;--more:#d98600;--high:#ce2e27;
    --sans:-apple-system,"Segoe UI",Roboto,Arial,sans-serif;--mono:ui-monospace,Consolas,monospace}
  *{box-sizing:border-box} html,body{margin:0;height:100%}
  body{font-family:var(--sans);color:var(--ink);display:flex;flex-direction:column;height:100vh}
  header{padding:.5rem 1rem;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:.55rem 1rem;flex-wrap:wrap}
  header h1{font-size:1.02rem;margin:0;letter-spacing:-.01em} header .sub{font-size:.78rem;color:var(--ink2)} header .grow{flex:1}
  #drawStatus{font-size:.74rem;color:var(--accent)}
  .upl{font-family:var(--mono);font-size:.72rem;color:#fff;background:var(--accent);border-radius:7px;padding:.28rem .55rem;cursor:pointer} .upl input{display:none}
  .bm{font-family:var(--mono);font-size:.72rem;color:var(--ink2)} .bm button{font:inherit;font-family:var(--mono);font-size:.72rem;background:none;border:0;color:var(--accent);cursor:pointer;padding:0 .3rem}
  main{flex:1;display:flex;min-height:0}
  #tree{width:44%;min-width:270px;overflow:auto;border-right:1px solid var(--line);background:#fafbf9;padding:.8rem}
  #splitter{flex:0 0 5px;cursor:col-resize;background:#e5e7eb}
  #mapwrap{flex:1;position:relative} #map{position:absolute;inset:0}
  #mapMsgs{position:absolute;top:10px;left:50%;transform:translateX(-50%);z-index:1000;display:flex;flex-direction:column;align-items:center;gap:6px;width:max-content;max-width:min(560px,62%);pointer-events:none}
  #mapMsgs>div{pointer-events:auto}
  #zoomMsg{background:#fffbe6;border:1px solid #c9a227;border-radius:6px;padding:6px 10px;font-size:12px;display:none}
  #loadMsg{position:absolute;left:50%;transform:translateX(-50%);bottom:14px;z-index:1000;background:#eef5ff;border:1px solid #6b8fc7;border-radius:6px;padding:6px 10px;font-size:12px;display:none}
  #selBanner{display:none;align-items:center;gap:9px;background:#1e3a8a;color:#fff;border-radius:8px;padding:7px 10px 7px 12px;font-size:13px;line-height:1.3;box-shadow:0 2px 10px rgba(0,0,0,.35)}
  #selBanner i{width:16px;height:16px;border-radius:3px;border:2px solid #fff;flex:none}
  #selBanner small{display:block;font-size:11px;opacity:.85}
  #selBanner button{font:inherit;font-size:12px;font-weight:600;background:#fff;color:#1e3a8a;border:0;border-radius:5px;padding:3px 9px;cursor:pointer;flex:none}
  .mermaid{font-size:13px}
  #result{display:none;margin-top:.8rem;border-top:1px solid var(--line);padding-top:.7rem}
  #result h3{margin:0 0 .4rem;font-size:.9rem}
  .badge{display:inline-block;font-family:var(--mono);font-size:.8rem;font-weight:700;color:#fff;border-radius:6px;padding:.2rem .55rem}
  .badge.low{background:var(--low)} .badge.more{background:var(--more)} .badge.high{background:var(--high)}
  #pathwayTxt{font-size:.8rem;color:var(--ink2);margin:.35rem 0 .5rem}
  #pkgInfo{font-size:.7rem;color:var(--ink2);margin:-.2rem 0 .5rem;font-family:var(--mono)}
  .ind{display:flex;align-items:center;gap:.4rem;font-size:.78rem;padding:.12rem 0}
  .ind .mk{font-weight:700;width:1.1rem;text-align:center} .ind .mk.y{color:var(--low)} .ind .mk.n{color:#b7bdb4}
  .ind code{font-family:var(--mono);font-size:.74rem}
  #sliders{margin-top:.7rem} #sliders h4,#stats h4,#compare h4{margin:.2rem 0 .3rem;font-size:.72rem;letter-spacing:.06em;text-transform:uppercase;color:var(--ink2);font-family:var(--mono)}
  #stats{margin-top:.75rem;border-top:1px solid var(--line);padding-top:.55rem}
  .sld{display:grid;grid-template-columns:1fr 92px 3ch;gap:.4rem;align-items:center;font-size:.72rem;padding:.16rem 0}
  .sld code{font-family:var(--mono);font-size:.7rem} .sld input{accent-color:var(--accent);width:100%} .sld .v{font-family:var(--mono);text-align:right}
  /* bottom left: the layer list, opacity and input datasets stack top right, so the legend never covers them */
  #legend{position:absolute;bottom:22px;left:12px;z-index:1000;background:#fff;border:1px solid #999;border-radius:6px;padding:7px 9px;font-size:11px;max-width:300px;max-height:45%;overflow:auto}
  #legInputs{border-top:1px solid #e5e7eb;margin-top:.35rem;padding-top:.3rem;color:#4b5563}
  #legInputs .strip{display:flex;flex-wrap:wrap;gap:2px;margin-bottom:2px} #legInputs .strip i{width:10px;height:10px;border:1px solid #777}
  #legend b{font-family:var(--mono);color:#555;min-width:1.3ch;display:inline-block}
  #legend .row{display:flex;align-items:center;gap:5px;padding:1px 0}
  #legend .row i{width:13px;height:13px;border-radius:3px;border:1px solid #777;flex:none}
  #legend h4{margin:.3rem 0 .2rem;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#777;font-family:var(--mono)}
  #legend .grp{font-weight:700;margin:.25rem 0 .05rem}
  .note{font-size:.72rem;color:var(--ink2);padding:.3rem 1rem;background:#fbf4e6;border-top:1px solid var(--line)}
  .note b{color:var(--ink)}
  .leaflet-control-layers{font-size:12px}
  #diagram g.node.clickable{cursor:pointer} #diagram g.node.clickable:hover rect,#diagram g.node.clickable:hover polygon,#diagram g.node.clickable:hover path{filter:brightness(0.93)}
  /* selected node: the rest of the tree dims, the node gets a thick blue outline and glow, an outcome's route lights up */
  #diagram g.node,#diagram .edgePaths path,#diagram .edgeLabels>.edgeLabel{transition:opacity .15s}
  #diagram.has-active g.node:not(.onroute),#diagram.has-active .edgePaths path:not(.onroute),#diagram.has-active .edgeLabels>.edgeLabel:not(.onroute){opacity:.22}
  #diagram g.node.active rect,#diagram g.node.active polygon,#diagram g.node.active path{stroke:#1d4ed8!important;stroke-width:6px!important}
  #diagram g.node.active{filter:drop-shadow(0 0 8px rgba(29,78,216,.9))}
  #diagram g.node.onroute:not(.active) rect,#diagram g.node.onroute:not(.active) polygon,#diagram g.node.onroute:not(.active) path{stroke:#1d4ed8!important;stroke-width:3px!important}
  #diagram .edgePaths path.onroute{stroke:#1d4ed8!important;stroke-width:4px!important}
  #diagram .edgeLabels>.edgeLabel.onroute span,#diagram .edgeLabels>.edgeLabel.onroute p{font-weight:700;color:#1d4ed8}
  #nodeLeg,#legInputs{display:none}
  .inbox{background:#fff;padding:5px 8px;font:11px sans-serif;width:255px;max-height:42vh;overflow:auto}
  .inbox summary{cursor:pointer;line-height:1.75}
  .inbox>details>summary{font-weight:700;font-size:12px}
  .inbox .ingrp{margin:.1rem 0 .1rem 2px} .inbox .ingrp>summary{font-weight:600}
  .inbox .ingrp.flash{background:#dbeafe;border-radius:4px}
  .inbox .muted{color:#6b7280;font-weight:400}
  .inbox .inhelp{color:#4b5563;margin:.1rem 0 .3rem;line-height:1.35}
  .inbox .inname{font-family:var(--mono);font-size:10px;color:#6b7280;padding-left:18px}
  .inbox label{display:flex;align-items:center;gap:4px;line-height:1.6;padding-left:14px;cursor:pointer}
  .inbox label:hover{background:#f3f4f6} .inbox label:has(input:checked){font-weight:700}
  .inbox label i{width:10px;height:10px;border:1px solid #777;flex:none}
  .inbox label .nm{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
  .inbox .iso{font-family:var(--mono);font-size:9px;color:#fff;background:#6b7280;border-radius:3px;padding:0 3px;flex:none}
  .inbox button{font:inherit;font-size:10px;cursor:pointer}
  #clickHint{font-size:.72rem;color:var(--ink2);margin:0 0 .4rem}
  #anaBox{font-size:.74rem;color:var(--ink2)} #anaBox select,#anaBox button,#anaBox input{font:inherit;font-size:.72rem}
  #keyIn{display:none;width:17em} #anaWhy{font-size:.72rem;color:#9a3412}
  /* everything about plots (upload or draw, how to analyse, run, status) in one box, apart from the rest */
  #plotBox{display:flex;flex-wrap:wrap;align-items:center;gap:.3rem .6rem;border:1px solid #b9c6d8;background:#f4f7fb;border-radius:8px;padding:.35rem .65rem;font-size:.74rem;max-width:62rem}
  #plotBox>b{font-size:.8rem;color:var(--ink)} #plotBox .or{color:var(--ink2)}
  #plotBox #anaWhy,#plotBox #drawStatus{flex-basis:100%} #plotBox #anaWhy:empty,#plotBox #drawStatus:empty{display:none}
  #runBtn{font-weight:700;color:#fff;background:var(--accent);border:0;border-radius:5px;padding:.15rem .6rem;cursor:pointer} #runBtn:disabled{opacity:.4;cursor:default}
  #anaBox.off select{opacity:.5}
  #expMsg{background:#fef2f2;border:1px solid #dc2626;border-radius:6px;padding:6px 10px;font-size:12px;display:none}
  #compare{margin:.5rem 0;font-size:.74rem} #compare table{border-collapse:collapse;width:100%}
  #compare td,#compare th{border-bottom:1px solid var(--line);padding:2px 4px;text-align:left}
  #compare tr.diff td{background:#fff3c4;font-weight:700} #compare .src{font-size:.68rem;color:var(--ink2)}
  #sliders.off{opacity:.45;pointer-events:none}
</style></head><body>
<header>
  <h1>__TITLE__</h1><span class="sub">__SUBTITLE__</span>
  <span class="grow"></span>
  <div id="plotBox">
    <b>Plots</b>
    <label class="upl">upload GeoJSON<input type="file" id="geojsonFile" accept=".geojson,.json,application/geo+json"></label>
    <span class="or">or draw one on the map</span>
    <span id="anaBox">analyse with <select id="anaMode" disabled><option>checking&hellip;</option></select>
      <button id="runBtn" type="button" disabled title="analyse the plots on the map again with this option">Run</button>
      <button id="keyBtn" type="button" title="paste a Whisp API key (kept in this browser only)" style="display:none">API key</button><input id="keyIn" type="password" placeholder="paste your Whisp API key, then Enter" autocomplete="off"></span>
    <div id="anaWhy"></div>
    <div id="drawStatus"></div>
  </div>
  <span class="bm" id="bm">go to:</span>
</header>
<main>
  <div id="tree">
    <p id="clickHint">Click a question or an outcome in the tree to show it on the map; click it again, or press Esc, to go back.</p>
    <pre class="mermaid" id="diagram"></pre>
    <div id="result">
      <h3>Drawn plot</h3>
      <div><span class="badge" id="riskBadge"></span></div>
      <div id="pathwayTxt"></div>
      <div id="pkgInfo"></div>
      <div id="indList"></div>
      <div id="compare"></div>
      <div id="sliders"><h4>Indicator thresholds (%)</h4><div id="sliderRows"></div></div>
      <div id="stats"></div>
    </div>
  </div>
  <div id="splitter"></div>
  <div id="mapwrap"><div id="map"></div><div id="mapMsgs"><div id="selBanner"></div><div id="zoomMsg"></div><div id="expMsg"></div></div><div id="loadMsg">Loading map layer&hellip;</div>
    <div id="legend"><div id="legOutcome"><h4>Outcome</h4>__OUTCOME_LEG__</div><div id="legPathway"><h4>Pathways</h4>__PATHWAY_LEG__</div><div id="nodeLeg"></div><div id="legInputs"></div></div>
  </div>
</main>
<div class="note"><span id="zoomNote"><b>Zoom in for realistic areas.</b> When zoomed out, a tile-pyramiding artefact inflates disturbance pixels, so some categories (e.g. high risk) appear to cover far larger areas than they actually do; zoom in for a truthful view. </span>Draw a polygon (tools top-left of the map) to test a plot, then move the threshold sliders to see the impact. WHISP is a non-authoritative exploration tool: outcomes are signals, not a legal determination. <span id="builtNote"></span></div>
<script>
__JS_MODULE__
const CFG = __CFG__;
const MERMAID_SRC = __MERMAID_JSON__;
const RISK_COL = "__RISK_COL__";
const CODE_COLOUR = __CODE_COLOUR__;
const CODE_NAMES = __CODE_NAMES__;
let WHISP_API_BASE = "/api";

// --- decision-tree diagram (textContent so <br/> in labels is not HTML-parsed away) ---
const _diag = document.getElementById('diagram'); _diag.textContent = MERMAID_SRC;
mermaid.initialize({ startOnLoad:false, flowchart:{ useMaxWidth:true, htmlLabels:true } });
mermaid.run({ nodes:[_diag] }).then(attachNodeClicks).catch(e => console.warn('diagram', e));
if(!Object.keys(CFG.nodeTiles||{}).length) document.getElementById('clickHint').style.display='none';

// --- map + layers (native control => reliable on/off; z-index keeps risk over Sentinel-2 over base) ---
const map = L.map('map', { center: CFG.center, zoom: CFG.zoom });
// panes guarantee stacking regardless of toggle order: base(200) < Sentinel-2(250) < risk(350) < drawn polygon(overlayPane 400)
map.createPane('s2bg'); map.getPane('s2bg').style.zIndex = 250;
map.createPane('risk'); map.getPane('risk').style.zIndex = 350;
const osm = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}', { maxZoom:19, attribution:'&copy; Esri' });
const sat = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', { maxZoom:19, attribution:'Esri' });
// risk and node layers are computed by Earth Engine on request: zoomed out they take minutes per tile,
// so they are only drawn from LAYER_MIN_ZOOM in, with a message saying so
const LAYER_MIN_ZOOM = CFG.layerMinZoom || 8;
let layerOpacity = 0.85;  // set by the opacity slider, used by every layer added later too
let plotsColoured = false;  // set once a plot result has coloured a drawn plot by its pathway
function esc(s){ return String(s==null?'':s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
// a page built without Earth Engine has no map layers (tree, base map and plot analysis only)
const riskLayers = {
  outcome: CFG.tiles.outcome ? L.tileLayer(CFG.tiles.outcome, { pane:'risk', opacity:layerOpacity, minZoom:LAYER_MIN_ZOOM }) : null,
  pathway: CFG.tiles.pathway ? L.tileLayer(CFG.tiles.pathway, { pane:'risk', opacity:layerOpacity, minZoom:LAYER_MIN_ZOOM }) : null
};
function shown(l){ return !!l && map.hasLayer(l); }
const s2layers = {};
Object.entries(CFG.backgrounds || {}).forEach(([label,url]) => { s2layers[label] = watchLoading(L.tileLayer(url, { pane:'s2bg', maxZoom:19 })); });
osm.addTo(map); if(riskLayers.outcome) riskLayers.outcome.addTo(map);
// on-map messages: zoom in to see the layers; loading while their tiles arrive; expired once no tile loads
let _loading=0, _slowTimer=null, _tilesOk=0, _tilesBad=0;
function watchLoading(layer){
  layer.on('tileloadstart', ()=>{
    _loading++; const lm=document.getElementById('loadMsg'); lm.textContent='Loading map layer…'; lm.style.display='block';
    clearTimeout(_slowTimer); _slowTimer=setTimeout(()=>{ if(_loading) lm.textContent='Still loading… zooming in further usually helps'; }, 15000);
  });
  const done=()=>{ _loading=Math.max(0,_loading-1); if(!_loading){ clearTimeout(_slowTimer); document.getElementById('loadMsg').style.display='none'; } };
  layer.on('tileload', () => { _tilesOk++; done(); });
  layer.on('tileerror', () => {
    _tilesBad++; done();
    // Earth Engine tile links stop working about a day after the page is built
    if(_tilesBad>=4 && !_tilesOk){
      const m=document.getElementById('expMsg'); m.style.display='block';
      m.textContent='The map layers have expired (this page was built on '+CFG.builtWith.date+'). Ask for a rebuild; the tree and plot analysis still work.';
    }
  });
  layer.on('remove', ()=>{ _loading=0; document.getElementById('loadMsg').style.display='none'; });
  return layer;
}
[riskLayers.outcome, riskLayers.pathway].forEach(l => { if(l) watchLoading(l); });
function updateZoomMsg(){
  const m=document.getElementById('zoomMsg'); const live=[riskLayers.outcome, riskLayers.pathway, nodeLayer].some(shown);
  if(live && map.getZoom() < LAYER_MIN_ZOOM){
    m.innerHTML='Zoom in to see the map layer <button id="zoomBtn">Zoom in</button>';
    document.getElementById('zoomBtn').onclick=()=>map.setZoom(LAYER_MIN_ZOOM);
    m.style.display='block';
  }
  else m.style.display='none';
}
map.on('zoomend overlayadd overlayremove', updateZoomMsg);
const overlays = {};
if(riskLayers.outcome) overlays['Risk – outcome'] = riskLayers.outcome;
if(riskLayers.pathway) overlays['Risk – pathway'] = riskLayers.pathway;
Object.keys(s2layers).forEach(label => { overlays['Sentinel-2 ' + label] = s2layers[label]; });
L.control.layers({ 'Map': osm, 'Satellite': sat }, overlays, { collapsed:false, position:'topright' }).addTo(map);
// opacity slider for the risk, node and input layers (see Sentinel-2 / base through them)
const OpCtl = L.Control.extend({ options:{position:'topright'}, onAdd:function(){
  const d=L.DomUtil.create('div','leaflet-bar'); d.style.cssText='background:#fff;padding:4px 7px;font:11px sans-serif';
  d.innerHTML='layer opacity <input type="range" min="0" max="100" value="85" style="width:88px;vertical-align:middle">';
  L.DomEvent.disableClickPropagation(d);
  d.querySelector('input').addEventListener('input', e=>{ layerOpacity=e.target.value/100;
    [riskLayers.outcome, riskLayers.pathway, nodeLayer].concat(Object.values(inputLayers)).forEach(l => { if(l) l.setOpacity(layerOpacity); }); });
  return d; }});
if(riskLayers.outcome) map.addControl(new OpCtl());

// --- input datasets: every dataset the tree reads, each its own on/off layer (for looking only, never changes the outcome).
// One dataset draws in a second or two at any zoom, so unlike the combined layers these have no zoom limit.
map.createPane('inputs'); map.getPane('inputs').style.zIndex = 380;
const INPUT_GROUPS = CFG.inputGroups || [];
const inputDefs = {}, inputLayers = {};
INPUT_GROUPS.forEach(g => g.layers.forEach(d => { inputDefs[d.name]=d; }));
function setInput(name, on){
  if(!inputLayers[name]) inputLayers[name]=watchLoading(L.tileLayer(inputDefs[name].url, { pane:'inputs', opacity:layerOpacity }));
  const l=inputLayers[name]; if(on){ if(!map.hasLayer(l)) l.addTo(map); } else if(map.hasLayer(l)) map.removeLayer(l);
}
function inputsOn(){ return Object.keys(inputLayers).filter(n => map.hasLayer(inputLayers[n])); }
function afterInputChange(){
  document.querySelectorAll('#inBox input.inparent').forEach(p => {
    const kids=Array.from(p.closest('details').querySelectorAll('input[data-ds]')); const on=kids.filter(k => k.checked).length;
    p.checked = on>0 && on===kids.length; p.indeterminate = on>0 && on<kids.length;
  });
  const n=inputsOn().length; const c=document.getElementById('inCount'); if(c) c.textContent = n ? n+' on' : '';
  updateLegend();
}
if(INPUT_GROUPS.length){
  const InCtl = L.Control.extend({ options:{position:'topright'}, onAdd:function(){
    const d=L.DomUtil.create('div','leaflet-bar inbox'); d.id='inBox';
    d.innerHTML="<details id='inDetails'><summary>Input datasets <span class='muted'>("+Object.keys(inputDefs).length+")</span> <span id='inCount' class='muted'></span></summary>"
      +"<div class='inhelp'>The datasets behind each question in the tree. Switch any on to see where it maps; this does not change the outcome. <button id='inOff' type='button'>all off</button></div>"
      +INPUT_GROUPS.map(g => "<details class='ingrp' data-q='"+esc(g.q)+"'><summary><input type='checkbox' class='inparent' title='switch all in this group'> "+esc(g.question)+" <span class='muted'>("+g.layers.length+")</span></summary>"
        +"<div class='inname'>"+esc(g.indicator)+"</div>"
        +g.layers.map(ds => "<label title='"+esc(ds.name)+"'><input type='checkbox' data-ds='"+esc(ds.name)+"'><i style='background:#"+esc(ds.colour)+"'></i><span class='nm'>"+esc(ds.name)+"</span>"+(ds.iso2?"<span class='iso'>"+esc(ds.iso2)+"</span>":"")+"</label>").join('')
        +"</details>").join('')
      +"</details>";
    L.DomEvent.disableClickPropagation(d); L.DomEvent.disableScrollPropagation(d);
    d.querySelectorAll('input[data-ds]').forEach(cb => cb.addEventListener('change', e => { setInput(e.target.dataset.ds, e.target.checked); afterInputChange(); }));
    d.querySelectorAll('input.inparent').forEach(p => p.addEventListener('change', e => {
      e.target.closest('details').querySelectorAll('input[data-ds]').forEach(cb => { cb.checked=e.target.checked; setInput(cb.dataset.ds, cb.checked); });
      afterInputChange();
    }));
    d.querySelector('#inOff').addEventListener('click', () => { d.querySelectorAll('input[data-ds]').forEach(cb => { if(cb.checked){ cb.checked=false; setInput(cb.dataset.ds, false); } }); afterInputChange(); });
    return d; }});
  map.addControl(new InCtl());
}
// open the datasets behind a question (when its node is clicked) so they are one tick away
function revealInputs(q){
  const grp=document.querySelector("#inBox .ingrp[data-q='"+q+"']"); if(!grp) return;
  document.getElementById('inDetails').open=true; grp.open=true;
  document.querySelectorAll('#inBox .ingrp.flash').forEach(e => e.classList.remove('flash')); grp.classList.add('flash');
  grp.scrollIntoView({ block:'nearest' });
}

// --- legend: only what is on the map now ---
// one map layer at a time (the tree selection, else pathway, else outcome); input datasets as one short line,
// since their names and colours are already beside their tick boxes in the Input datasets list
function updateLegend(){
  // with no map layer on, analysed plots are still coloured by pathway, so that legend explains them
  const main = nodeLayer ? 'nodeLeg' : shown(riskLayers.pathway) ? 'legPathway' : shown(riskLayers.outcome) ? 'legOutcome' : plotsColoured ? 'legPathway' : null;
  ['legOutcome','legPathway','nodeLeg'].forEach(id => { document.getElementById(id).style.display = id===main ? 'block' : 'none'; });
  const on=inputsOn(), li=document.getElementById('legInputs');
  const sw=n => "<i style='background:#"+esc(inputDefs[n].colour)+"' title='"+esc(n)+"'></i>";
  li.innerHTML = !on.length ? '' : on.length<=3
    ? on.map(n => "<div class='row'>"+sw(n)+esc(n)+"</div>").join('')
    : "<div class='strip'>"+on.map(sw).join('')+"</div>"+on.length+" input datasets on (named in the Input datasets list)";
  li.style.display = on.length ? 'block' : 'none';
  li.style.borderTop = main ? '' : '0';
  document.getElementById('legend').style.display = (main || on.length) ? 'block' : 'none';
}
// outcome and pathway draw in the same place, so they are either/or: switching one on switches the other off
// (deferred so the layer list has finished its own click and redraws its tick boxes)
map.on('overlayadd', e => {
  const other = e.layer===riskLayers.outcome ? riskLayers.pathway : e.layer===riskLayers.pathway ? riskLayers.outcome : null;
  if(other) setTimeout(() => { if(map.hasLayer(other)) map.removeLayer(other); updateLegend(); }, 0);
});
map.on('overlayadd overlayremove', updateLegend);
setTimeout(() => { updateZoomMsg(); updateLegend(); }, 0);
// --- node clicks: show the pixels behind a question (indicator presence) or an outcome (pathway code) ---
let nodeLayer=null, activeNode=null, activeEl=null, hiddenRisk=[];
function nodeIdOf(g){ const m=/^flowchart-(.+?)-\d+$/.exec(g.id||''); return m ? m[1] : null; }
// dim the tree except the selected node; for an outcome also light up its route from the top
function markTree(nid, route){
  const diag=document.getElementById('diagram'); diag.classList.add('has-active');
  diag.querySelectorAll('g.node').forEach(g => { if(route.includes(nodeIdOf(g))) g.classList.add('onroute'); });
  const paths=Array.from(diag.querySelectorAll('.edgePaths path')), labels=Array.from(diag.querySelectorAll('.edgeLabels>.edgeLabel'));
  paths.forEach((p,i) => {
    for(let k=0; k+1<route.length; k++){
      if(p.classList.contains('LS-'+route[k]) && p.classList.contains('LE-'+route[k+1])){
        p.classList.add('onroute'); if(labels.length===paths.length) labels[i].classList.add('onroute');  // Mermaid draws labels in edge order
      }
    }
  });
}
function unmarkTree(){
  const diag=document.getElementById('diagram'); diag.classList.remove('has-active');
  diag.querySelectorAll('.onroute').forEach(e => e.classList.remove('onroute'));
}
function clearNodeLayer(){
  if(nodeLayer){ map.removeLayer(nodeLayer); nodeLayer=null; }
  hiddenRisk.forEach(l => { if(!map.hasLayer(l)) l.addTo(map); }); hiddenRisk=[];
  if(activeEl){ activeEl.classList.remove('active'); activeEl=null; }
  activeNode=null; unmarkTree();
  document.getElementById('selBanner').style.display='none'; document.getElementById('nodeLeg').innerHTML='';
  updateZoomMsg(); updateLegend();
}
function showNodeLayer(nid, el){
  const nt=(CFG.nodeTiles||{})[nid]; if(!nt) return;
  if(activeNode===nid){ clearNodeLayer(); return; }
  clearNodeLayer();
  // hide the combined risk layers while a node layer is shown, so its pixels stand out; restored on clear
  [riskLayers.outcome, riskLayers.pathway].forEach(l => { if(shown(l)){ hiddenRisk.push(l); map.removeLayer(l); } });
  nodeLayer=watchLoading(L.tileLayer(nt.url, { pane:'risk', opacity:layerOpacity, minZoom:LAYER_MIN_ZOOM })).addTo(map);
  if(map.getZoom() < LAYER_MIN_ZOOM) map.setZoom(LAYER_MIN_ZOOM);  // so a click always shows something
  activeNode=nid; activeEl=el; el.classList.add('active');
  markTree(nid, nt.route || [nid]);
  const what = nt.kind==='outcome' ? 'pixels that end at this outcome' : 'pixels where the answer is yes';
  const b=document.getElementById('selBanner');
  b.innerHTML="<i style='background:"+esc(nt.colour)+"'></i><span>Showing: <b>"+esc(nt.label)+"</b><small>"+what+" (risk layers hidden until you clear)</small></span><button id='selClear' type='button' title='or press Esc'>&#10005; Clear</button>";
  b.style.display='flex'; document.getElementById('selClear').onclick=clearNodeLayer;
  document.getElementById('nodeLeg').innerHTML="<h4>Selected in the tree</h4><div class='row'><i style='background:"+esc(nt.colour)+"'></i><span>"+esc(nt.label)+"</span></div>";
  if(nt.kind==='question' && nt.q) revealInputs(nt.q);
  updateZoomMsg(); updateLegend();
}
document.addEventListener('keydown', e => { if(e.key==='Escape' && activeEl) clearNodeLayer(); });
function attachNodeClicks(){
  document.querySelectorAll('#diagram g.node').forEach(g => {
    const nid=nodeIdOf(g); if(!nid || !(CFG.nodeTiles||{})[nid]) return;
    g.classList.add('clickable'); g.addEventListener('click', ev => { ev.stopPropagation(); showNodeLayer(nid, g); });
  });
}
const bm = document.getElementById('bm');
CFG.bookmarks.forEach(([name,lat,lon,z]) => { const a=document.createElement('button'); a.textContent=name; a.onclick=()=>map.setView([lat,lon], z); bm.appendChild(a); });

// --- draw / upload + analyze + live re-threshold ---
const drawn = new L.FeatureGroup(); map.addLayer(drawn);
map.addControl(new L.Control.Draw({ draw:{ polygon:true, rectangle:true, marker:false, polyline:false, circle:false, circlemarker:false }, edit:{ featureGroup:drawn, edit:false, remove:true } }));
function setStatus(t){ document.getElementById('drawStatus').textContent = t; }
const plotLayers = {};   // external_id -> leaflet layer, so each plot is coloured by its own risk
let curToken=null, slidersBuilt=false, _pcount=0;
function classOf(r){ return r==='high'?'high':(r==='low'?'low':'more'); }
function storyOf(props){ try{ return __STORY_FN__(props); }catch(e){ return null; } }

// upload a GeoJSON file (Feature / FeatureCollection / raw geometry; one or many plots)
document.getElementById('geojsonFile').addEventListener('change', e => {
  const f = e.target.files[0]; if(!f) return;
  const rd = new FileReader();
  rd.onload = () => { try{ loadGeojson(JSON.parse(rd.result)); }catch(err){ setStatus('Could not read GeoJSON: '+err.message); } };
  rd.readAsText(f); e.target.value='';
});
function toFC(g){
  if(g && g.type==='FeatureCollection') return g;
  if(g && g.type==='Feature') return { type:'FeatureCollection', features:[g] };
  if(Array.isArray(g)) return { type:'FeatureCollection', features:g };
  return { type:'FeatureCollection', features:[ { type:'Feature', geometry:g, properties:{} } ] };
}
function loadGeojson(g){
  const fc = toFC(g);
  drawn.clearLayers(); Object.keys(plotLayers).forEach(k => delete plotLayers[k]);
  fc.features.forEach(ft => {
    ft.properties = ft.properties || {};
    const eid = ft.properties.external_id || ('plot_'+(++_pcount)); ft.properties.external_id = eid;
    L.geoJSON(ft, { style:{ color:'#666', weight:2, fillOpacity:0.25 } }).eachLayer(l => { plotLayers[eid]=l; drawn.addLayer(l); });
  });
  try{ map.fitBounds(drawn.getBounds(), { padding:[30,30] }); }catch(e){}
  analyzeFC(fc);
}
map.on(L.Draw.Event.CREATED, function(e){
  const eid='draw_'+(++_pcount); const ft=e.layer.toGeoJSON(); ft.properties=ft.properties||{}; ft.properties.external_id=eid;
  plotLayers[eid]=e.layer; drawn.addLayer(e.layer);
  analyzeFC({ type:'FeatureCollection', features:[ft] });
});
map.on(L.Draw.Event.DELETED, function(e){
  e.layers.eachLayer(l => { Object.keys(plotLayers).forEach(k => { if(plotLayers[k]===l) delete plotLayers[k]; }); });
  updateRunBtn();
});
// Run: analyse every plot on the map again with the chosen option (drawing or uploading runs straight away)
function plotsFC(){
  return { type:'FeatureCollection', features:Object.entries(plotLayers).map(([eid, l]) => {
    const ft=l.toGeoJSON(); ft.properties=Object.assign({}, ft.properties, { external_id:eid }); return ft; }) };
}
function updateRunBtn(){
  const mode=document.getElementById('anaMode').value;
  document.getElementById('runBtn').disabled = analysing || !mode || (needsKey(mode) && !apiKey) || !Object.keys(plotLayers).length;
}
document.getElementById('runBtn').onclick=() => analyzeFC(plotsFC());
document.getElementById('anaMode').addEventListener('change', () => { updateKeyUi(); updateRunBtn(); });
function featuresOf(result){
  if(!result) return [];
  if(result.type==='FeatureCollection' && result.features) return result.features;
  if(Array.isArray(result)) return result.map(p => ({ properties:p }));
  if(result.features) return result.features;
  return [ { properties: result.properties || result } ];
}
function applyResults(result, ids){
  const feats = featuresOf(result); if(!feats.length){ setStatus('no result'); return []; }
  feats.forEach((ft,i) => {
    const props = ft.properties || {}; const s = storyOf(props);
    const colour = (s && CODE_COLOUR[s.code]) ? ('#'+CODE_COLOUR[s.code]) : '#888';
    // the API may not echo our ids back, so fall back to the order the plots were sent in
    const lyr = plotLayers[props.external_id] || plotLayers[(ids||[])[i]];
    if(lyr && lyr.setStyle) lyr.setStyle({ color:colour, weight:2, fillColor:colour, fillOpacity:0.45 });
  });
  plotsColoured=true;
  showResult(feats[0].properties || {});
  renderStats(feats);
  updateLegend();
  const n = feats.length;
  setStatus(n+' plot'+(n>1?'s':'')+' analysed'+(n>1?' (panel shows first), ':', ')+RISK_COL+' = '+(feats[0].properties[RISK_COL]||'?'));
  return feats;
}
// light up the analysed plot's route in the tree, unless a box is already selected for the map
function showPlotRoute(code){
  const route=(CFG.routes||{})[code]; if(!route || activeNode) return;
  clearNodeLayer(); markTree(route[route.length-1], route);
  const g=Array.from(document.querySelectorAll('#diagram g.node')).find(g => nodeIdOf(g)===route[route.length-1]);
  if(g){ g.classList.add('active'); activeEl=g; }
}
function showResult(props){
  const s = storyOf(props); const outcome = props[RISK_COL] || (s && s.result);
  if(s) showPlotRoute(s.code);
  document.getElementById('result').style.display='block';
  const badge=document.getElementById('riskBadge'); badge.className='badge '+classOf(outcome); badge.textContent=(RISK_COL+' = '+(outcome||'?'));
  document.getElementById('pathwayTxt').textContent = s && s.pathway ? s.pathway : '';
  document.getElementById('indList').innerHTML = CFG.indicators.map(ind => {
    const yes = (props[ind.label]||'').toString().toLowerCase()==='yes';
    return "<div class='ind'><span class='mk "+(yes?'y':'n')+"'>"+(yes?'✓':'✗')+"</span><code>"+ind.label+"</code></div>";
  }).join('');
  buildSliders();
}
function renderStats(feats){
  const el=document.getElementById('stats'); let total=0; const byCls={low:0,more:0,high:0}, byCode={};
  feats.forEach(ft => { const props=ft.properties||{}; const s=storyOf(props); const cls=classOf(props[RISK_COL]||(s&&s.result)); byCls[cls]++; total++; if(s && s.code!=null) byCode[s.code]=(byCode[s.code]||0)+1; });
  if(!total){ el.innerHTML=''; return; }
  const pct=n=>Math.round(100*n/total);
  const sw=c=>"<span style='display:inline-block;width:11px;height:11px;border-radius:2px;background:"+c+";flex:none'></span>";
  const clsRows=[['low','Low','#238b45'],['more','More info','#d98600'],['high','High','#ce2e27']].map(([k,l,c]) =>
    "<div class='ind'>"+sw(c)+" "+l+" <b style='margin-left:auto'>"+pct(byCls[k])+"%</b> <span style='color:#8a908a'>("+byCls[k]+")</span></div>").join('');
  const compRows=Object.keys(byCode).sort((a,b)=>a-b).map(c => { const col=CODE_COLOUR[c]?('#'+CODE_COLOUR[c]):'#888';
    return "<div class='ind'>"+sw(col)+"<code>"+c+"</code> "+((CODE_NAMES[c]||'').split(': ').pop())+" <b style='margin-left:auto'>"+pct(byCode[c])+"%</b></div>"; }).join('');
  el.innerHTML="<h4>Outcome distribution ("+total+" plot"+(total>1?'s':'')+")</h4>"+clsRows+"<h4>By pathway</h4>"+compRows;
}
function buildSliders(){
  if(slidersBuilt) return; slidersBuilt=true;
  document.getElementById('sliderRows').innerHTML = CFG.indicators.map(ind =>
    "<div class='sld'><code>"+ind.label+"</code><input type='range' min='0' max='100' value='"+ind.default+"' data-param='"+ind.param+"'><span class='v' id='v_"+ind.param+"'>"+ind.default+"</span></div>"
  ).join('');
  document.querySelectorAll('#sliderRows input').forEach(inp => {
    inp.addEventListener('input', e => { document.getElementById('v_'+e.target.dataset.param).textContent=e.target.value; scheduleReclassify(); });
  });
}
function thresholds(){ const t={}; document.querySelectorAll('#sliderRows input').forEach(i=>{ t[i.dataset.param]=Number(i.value); }); return t; }
let _rc=null;
function scheduleReclassify(){ clearTimeout(_rc); _rc=setTimeout(reclassify, 250); }
async function reclassify(){
  if(!curToken) return; setStatus('Re-classifying at new thresholds...');
  try{
    const resp=await fetch(WHISP_API_BASE+'/reclassify',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:curToken,thresholds:thresholds()})});
    if(!resp.ok) throw new Error('reclassify '+resp.status);
    const feats=applyResults((await resp.json()).data); setStatus('Thresholds applied.');
    if(lastApi) showCompare(feats[0] && feats[0].properties, lastApi, true);
  }catch(err){ setStatus('Re-threshold needs the served proxy ('+err.message+').'); }
}

// --- plot analysis: the local Python package (when this page is served by serve_viewer), the Whisp API
// (with a key), or both to compare. Checked when the page opens, so a missing option is greyed out with why.
let apiKey = CFG.apiKey || '';
try{ apiKey = apiKey || localStorage.getItem('whispApiKey') || ''; }catch(e){}
const ana = { local:{ ok:false }, api:{ ok:false } };
async function checkLocal(){
  if(location.protocol==='file:') return;  // opened as a file: no server behind it
  try{ const r=await fetch('/api/info'); if(!r.ok) throw new Error(r.status); ana.local={ ok:true, pkg:await r.json() }; }
  catch(e){ ana.local={ ok:false }; }
}
async function checkApi(){
  // served locally, the server relays to the API, so the browser's CORS rule does not apply. Otherwise the
  // browser only lets the page call the API if the API accepts this site, which is checked with or without
  // a key, so the key box is only offered where a key can work
  const base = ana.local.ok ? '/api/relay' : CFG.apiBase;
  try{
    const r=await fetch(base+'/config', { headers:{ 'x-api-key':apiKey || 'check' } });
    const c=await r.json().catch(() => ({}));
    ana.api={ reachable:true, base:base, version:c.openforisWhispVersion };
  }catch(e){
    ana.api={ reachable:false, why:'Plot analysis is not available in this copy of the page: the Whisp API only accepts calls from whisp.openforis.org (or serve the page locally).' };
  }
}
function needsKey(mode){ return mode==='api' || mode==='both'; }
function fillModes(){
  const sel=document.getElementById('anaMode'), opts=[], need=apiKey ? '' : ' (needs an API key)';
  if(ana.local.ok) opts.push(['local', 'local Python package ('+ana.local.pkg.version+(ana.local.pkg.commit ? ', '+ana.local.pkg.commit : '')+')']);
  if(ana.api.reachable) opts.push(['api', 'Whisp API'+(ana.api.version ? ' ('+ana.api.version+')' : '')+need]);
  if(ana.local.ok && ana.api.reachable) opts.push(['both', 'both, to compare'+need]);
  const prev=sel.value;
  sel.innerHTML = opts.length ? opts.map(([v,t]) => "<option value='"+v+"'>"+esc(t)+"</option>").join('') : "<option value=''>off</option>";
  if(opts.some(o => o[0]===prev)) sel.value=prev;
  sel.disabled = !opts.length; document.getElementById('anaBox').classList.toggle('off', !opts.length);
  document.getElementById('anaWhy').textContent = opts.length ? '' : (ana.api.why || '');
  updateKeyUi(); updateRunBtn();
}
// the key only matters for the API options: hidden for the local package; an empty box to paste into
// while no key is set, then a small button to change it
function updateKeyUi(){
  const show=needsKey(document.getElementById('anaMode').value), kb=document.getElementById('keyBtn'), ki=document.getElementById('keyIn');
  kb.textContent='API key ✓'; kb.style.display = show && apiKey ? '' : 'none';
  if(!show) ki.style.display='none'; else if(!apiKey) ki.style.display='inline-block';
}
(async () => { await checkLocal(); await checkApi(); fillModes(); })();
document.getElementById('keyBtn').onclick=() => { const k=document.getElementById('keyIn'); k.style.display = k.style.display==='inline-block' ? 'none' : 'inline-block'; if(k.style.display!=='none') k.focus(); };
document.getElementById('keyIn').addEventListener('keydown', async e => {
  if(e.key!=='Enter') return;
  apiKey=e.target.value.trim(); e.target.value=''; e.target.style.display='none';
  try{ if(apiKey) localStorage.setItem('whispApiKey', apiKey); else localStorage.removeItem('whispApiKey'); }catch(err){}
  await checkApi(); fillModes();
});
async function localAnalyse(fc){
  const resp=await fetch(WHISP_API_BASE+'/submit/geojson',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(fc)});
  const env=await resp.json().catch(() => ({}));
  if(!resp.ok) throw new Error('local package: '+(env.error || resp.status));
  return env;
}
async function apiAnalyse(fc){
  const hdr={ 'x-api-key':apiKey, 'Content-Type':'application/json' };
  const body=Object.assign({}, fc, { analysisOptions:{ nationalCodes:CFG.nationalCodes||[], unitType:'ha', externalIdColumn:'external_id' } });
  let env=await (await fetch(ana.api.base+'/submit/geojson', { method:'POST', headers:hdr, body:JSON.stringify(body) })).json();
  const until=Date.now()+300000;
  while(env.code!=='analysis_completed'){
    const token=(env.data && env.data.token) || (env.context && env.context.token);
    if(!token || !/queued|processing/.test(env.code||'')) throw new Error('Whisp API: '+(env.message || env.code || 'no result'));
    if(Date.now()>until) throw new Error('Whisp API: no result after 5 minutes');
    await new Promise(r => setTimeout(r, 3000));
    env=await (await fetch(ana.api.base+'/status/'+token, { headers:hdr })).json();
  }
  return env.data;
}
let analysing=false;
async function analyzeFC(fc){
  const mode=document.getElementById('anaMode').value;
  updateRunBtn();
  if(!mode){ setStatus('Plot added. '+(document.getElementById('anaWhy').textContent || 'Plot analysis is off.')); return; }
  if(!fc.features.length){ setStatus('No plots on the map to analyse.'); return; }
  if(needsKey(mode) && !apiKey){ setStatus('Paste a Whisp API key in the box, then press Run.'); updateKeyUi(); return; }
  analysing=true; updateRunBtn();
  const ids=fc.features.map(f => f.properties.external_id);
  setStatus('Analysing '+fc.features.length+' plot(s)'+({ api:' with the Whisp API', both:' with the local package and the Whisp API' }[mode] || '')+' (about 10 to 60 s)...');
  document.getElementById('compare').innerHTML=''; lastApi=null;
  try{
    if(mode==='api'){
      curToken=null;
      const feats=applyResults(await apiAnalyse(fc), ids); showSource('api', feats[0] && feats[0].properties);
    } else {
      const [loc, api]=await Promise.all([localAnalyse(fc), mode==='both' ? apiAnalyse(fc).catch(e => ({ error:e.message })) : null]);
      curToken=loc.token || null;
      const feats=applyResults(loc.data, ids); showSource('local', loc.package);
      if(api) showCompare(feats[0] && feats[0].properties, api, false);
    }
  }catch(err){ setStatus('Analysis failed: '+err.message); }
  analysing=false; updateRunBtn();
  // the sliders re-run the risk step on the local server, so they need the local package
  document.getElementById('sliders').classList.toggle('off', !curToken);
  document.querySelector('#sliders h4').textContent = curToken ? 'Indicator thresholds (%)' : 'Indicator thresholds (%), local Python package only';
}
// which code made the result: the local package (version, branch, commit) or the Whisp API (its version)
function apiVersionOf(props){
  let m=props && props.whisp_processing_metadata;
  if(typeof m==='string'){ try{ m=JSON.parse(m); }catch(e){ m=null; } }
  return (m && m.whisp_version) || ana.api.version || '';
}
function showSource(kind, p){
  const el=document.getElementById('pkgInfo'), nat=(CFG.nationalCodes||[]).map(c => c.toUpperCase()).join(', ') || 'none';
  if(kind==='api'){ el.textContent='Analysed by the Whisp API, openforis-whisp '+(apiVersionOf(p) || '?')+'; national layers: '+nat; return; }
  if(!p){ el.textContent=''; return; }
  let t='Analysed with the local Python package, openforis-whisp '+p.version;
  if(p.commit) t+=', code from '+p.branch+' @ '+p.commit+(p.changed ? ' + uncommitted changes' : '');
  el.textContent=t+'; national layers: '+((p.national_codes||[]).map(c => c.toUpperCase()).join(', ') || 'none');
}
// side by side: the outcome and each indicator from the local package and from the Whisp API
let lastApi=null;
function showCompare(localProps, api, rethresholded){
  const el=document.getElementById('compare'); lastApi=api;
  if(!localProps){ el.innerHTML=''; return; }
  if(api.error){ el.innerHTML="<div class='src'>"+esc(api.error)+"</div>"; return; }
  const apiProps=(featuresOf(api)[0] || {}).properties || {};
  let nDiff=0;
  const rows=[RISK_COL].concat(CFG.indicators.map(i => i.label)).map(k => {
    const a=String(localProps[k]==null ? '' : localProps[k]), b=String(apiProps[k]==null ? '' : apiProps[k]);
    const diff=a.toLowerCase()!==b.toLowerCase(); if(diff) nDiff++;
    return "<tr class='"+(diff ? 'diff' : '')+"'><td><code>"+esc(k)+"</code></td><td>"+esc(a || '-')+"</td><td>"+esc(b || '-')+"</td></tr>";
  }).join('');
  el.innerHTML="<h4>Local package vs Whisp API</h4><table><tr><th></th><th>local</th><th>API "+esc(apiVersionOf(apiProps))+"</th></tr>"+rows+"</table>"
    +"<div class='src'>"+(nDiff ? nDiff+" difference"+(nDiff>1 ? 's' : '')+" highlighted" : "Same outcome and indicators")
    +(rethresholded ? "; local at your slider thresholds, API at its defaults" : "")+"</div>";
}
// what this page was built from, so a map and a plot result from different code can be told apart
(function(){
  const b=CFG.builtWith || {};
  let t='Built '+b.date+' with openforis-whisp '+b.version;
  if(b.commit) t+=' ('+b.branch+' @ '+b.commit+(b.changed ? ' + uncommitted changes' : '')+')';
  document.getElementById('builtNote').textContent = t + (CFG.tiles.outcome ? '; the map layers stop loading about a day after building.' : '; this page has no map layers.');
  if(!CFG.tiles.outcome) document.getElementById('zoomNote').style.display='none';
})();
// draggable splitter
(function(){ const sp=document.getElementById('splitter'), tree=document.getElementById('tree');
  let dragging=false; sp.addEventListener('mousedown',()=>{dragging=true;document.body.style.userSelect='none';});
  document.addEventListener('mousemove',e=>{ if(!dragging) return; const w=Math.max(220,Math.min(e.clientX, window.innerWidth-280)); tree.style.width=w+'px'; });
  document.addEventListener('mouseup',()=>{dragging=false;document.body.style.userSelect='';map.invalidateSize();});
})();
</script></body></html>"""

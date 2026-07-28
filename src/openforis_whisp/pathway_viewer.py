"""Shared, spec-driven pathway VIEWER builder: a self-contained HTML page with the decision-tree diagram
(left) and the combined risk map (right), for ANY WHISP tree spec.

Reusable shell behind the per-commodity viewers. Given a ``DecisionTreeSpec`` (so the Mermaid diagram and
the client-side tree walk come from the single-source ``decision_tree``) plus already-resolved map tile
URLs + palette (from ``timber_map`` or ``crop_map``), it returns one self-contained HTML string.

Features
--------
* decision-tree diagram (left);
* combined risk map (right) with a real Leaflet layer control (each layer an on/off checkbox) + a
  risk-opacity slider + Sentinel-2 background layers + a map/satellite base;
* draw a polygon (Leaflet.draw) -> analyze through a same-origin ``/api`` (the notebook's local proxy /
  ``serve_viewer``); the drawn plot is coloured by its risk and a result panel shows a tick / cross per
  indicator and the outcome + pathway (following the timber viewer);
* per-indicator THRESHOLD SLIDERS (default 10%) that RE-CLASSIFY the drawn plot live: the proxy caches the
  plot's stats once (the slow Earth Engine step) and re-runs the real ``whisp_risk`` with the new thresholds
  on each change (fast pandas, no Earth Engine), so the impact of changing thresholds is visible instantly.

WHISP is a non-authoritative exploration tool, so the map shows outcome SIGNALS (low / more-info / high),
not a legal determination (EUDR is one example framework it can inform, not the focus).
"""

from __future__ import annotations

import json
import re

from . import decision_tree as _dt


def _legend_rows(palette):
    out_names = {1: "Low", 2: "More info", 3: "High"}
    oc = palette["outcome_colour"]
    outcome = "".join(
        "<div class='row'><i style='background:#%s'></i>%s</div>"
        % (oc[k], out_names[k])
        for k in (1, 2, 3)
    )
    cn, cc = palette["code_names"], palette["code_colour"]
    pathway = "".join(
        "<div class='row'><i style='background:#%s'></i><b>%d</b> %s</div>"
        % (cc[c], c, cn[c].split(": ", 1)[-1])
        for c in sorted(cn)
    )
    return outcome, pathway


def _indicators(spec):
    """Ordered unique [{label, param}] for the tree's indicators; param is the whisp_risk threshold arg."""
    seen, out = set(), []
    for cols in spec.q_to_columns.values():
        for c in cols:
            if c in seen:
                continue
            seen.add(c)
            m = re.match(r"Ind_0*(\d+)", c)
            if m:
                out.append(
                    {"label": c, "param": "ind_%d_pcent_threshold" % int(m.group(1))}
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
):
    """Return a self-contained viewer HTML string. See the module docstring for the feature list."""
    outcome_leg, pathway_leg = _legend_rows(palette)
    cfg = json.dumps(
        {
            "tiles": tiles,
            "backgrounds": backgrounds or {},
            "bookmarks": bookmarks or [],
            "center": list(center),
            "zoom": zoom,
            "indicators": _indicators(spec),
        },
        separators=(",", ":"),
    )
    repl = {
        "__TITLE__": title,
        "__SUBTITLE__": subtitle,
        "__OUTCOME_LEG__": outcome_leg,
        "__PATHWAY_LEG__": pathway_leg,
        "__CFG__": cfg,
        "__MERMAID_JSON__": json.dumps(_dt.to_mermaid(spec)),
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
  .mermaid{font-size:13px}
  #result{display:none;margin-top:.8rem;border-top:1px solid var(--line);padding-top:.7rem}
  #result h3{margin:0 0 .4rem;font-size:.9rem}
  .badge{display:inline-block;font-family:var(--mono);font-size:.8rem;font-weight:700;color:#fff;border-radius:6px;padding:.2rem .55rem}
  .badge.low{background:var(--low)} .badge.more{background:var(--more)} .badge.high{background:var(--high)}
  #pathwayTxt{font-size:.8rem;color:var(--ink2);margin:.35rem 0 .5rem}
  .ind{display:flex;align-items:center;gap:.4rem;font-size:.78rem;padding:.12rem 0}
  .ind .mk{font-weight:700;width:1.1rem;text-align:center} .ind .mk.y{color:var(--low)} .ind .mk.n{color:#b7bdb4}
  .ind code{font-family:var(--mono);font-size:.74rem}
  #sliders{margin-top:.7rem} #sliders h4,#stats h4{margin:.2rem 0 .3rem;font-size:.72rem;letter-spacing:.06em;text-transform:uppercase;color:var(--ink2);font-family:var(--mono)}
  #stats{margin-top:.75rem;border-top:1px solid var(--line);padding-top:.55rem}
  .sld{display:grid;grid-template-columns:1fr 92px 3ch;gap:.4rem;align-items:center;font-size:.72rem;padding:.16rem 0}
  .sld code{font-family:var(--mono);font-size:.7rem} .sld input{accent-color:var(--accent);width:100%} .sld .v{font-family:var(--mono);text-align:right}
  #legend{position:absolute;bottom:14px;right:12px;z-index:1000;background:#fff;border:1px solid #999;border-radius:6px;padding:7px 9px;font-size:11px;max-height:66%;overflow:auto}
  #legend b{font-family:var(--mono);color:#555;min-width:1.3ch;display:inline-block}
  #legend .row{display:flex;align-items:center;gap:5px;padding:1px 0}
  #legend .row i{width:13px;height:13px;border-radius:3px;border:1px solid #777;flex:none}
  #legend h4{margin:.3rem 0 .2rem;font-size:10px;letter-spacing:.08em;text-transform:uppercase;color:#777;font-family:var(--mono)}
  .note{font-size:.72rem;color:var(--ink2);padding:.3rem 1rem;background:#fbf4e6;border-top:1px solid var(--line)}
  .note b{color:var(--ink)}
  .leaflet-control-layers{font-size:12px}
</style></head><body>
<header>
  <h1>__TITLE__</h1><span class="sub">__SUBTITLE__</span>
  <span class="grow"></span>
  <label class="upl">upload GeoJSON<input type="file" id="geojsonFile" accept=".geojson,.json,application/geo+json"></label>
  <span id="drawStatus"></span>
  <span class="bm" id="bm">go to:</span>
</header>
<main>
  <div id="tree">
    <pre class="mermaid" id="diagram"></pre>
    <div id="result">
      <h3>Drawn plot</h3>
      <div><span class="badge" id="riskBadge"></span></div>
      <div id="pathwayTxt"></div>
      <div id="indList"></div>
      <div id="sliders"><h4>Indicator thresholds (%)</h4><div id="sliderRows"></div></div>
      <div id="stats"></div>
    </div>
  </div>
  <div id="splitter"></div>
  <div id="mapwrap"><div id="map"></div>
    <div id="legend"><h4>Outcome</h4>__OUTCOME_LEG__<h4>Pathway code</h4>__PATHWAY_LEG__</div>
  </div>
</main>
<div class="note"><b>Zoom in for realistic areas.</b> When zoomed out, a tile-pyramiding artefact inflates disturbance pixels, so some categories (e.g. high risk) appear to cover far larger areas than they actually do; zoom in for a truthful view. Draw a polygon (tools top-left of the map) to test a plot, then move the threshold sliders to see the impact. WHISP is a non-authoritative exploration tool: outcomes are signals, not a legal determination. Tile tokens expire; regenerate to refresh.</div>
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
mermaid.run({ nodes:[_diag] });

// --- map + layers (native control => reliable on/off; z-index keeps risk over Sentinel-2 over base) ---
const map = L.map('map', { center: CFG.center, zoom: CFG.zoom });
// panes guarantee stacking regardless of toggle order: base(200) < Sentinel-2(250) < risk(350) < drawn polygon(overlayPane 400)
map.createPane('s2bg'); map.getPane('s2bg').style.zIndex = 250;
map.createPane('risk'); map.getPane('risk').style.zIndex = 350;
const osm = L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', { maxZoom:19, attribution:'&copy; OSM &copy; CARTO' });
const sat = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', { maxZoom:19, attribution:'Esri' });
const riskLayers = {
  outcome: L.tileLayer(CFG.tiles.outcome, { pane:'risk', opacity:0.85 }),
  pathway: L.tileLayer(CFG.tiles.pathway, { pane:'risk', opacity:0.85 })
};
const s2layers = {};
Object.entries(CFG.backgrounds || {}).forEach(([label,url]) => { s2layers[label] = L.tileLayer(url, { pane:'s2bg', maxZoom:19 }); });
osm.addTo(map); riskLayers.outcome.addTo(map);
const overlays = { 'Risk – outcome': riskLayers.outcome, 'Risk – pathway': riskLayers.pathway };
Object.keys(s2layers).forEach(label => { overlays['Sentinel-2 ' + label] = s2layers[label]; });
L.control.layers({ 'Map': osm, 'Satellite': sat }, overlays, { collapsed:false, position:'topright' }).addTo(map);
// risk opacity slider (see Sentinel-2 / base through the risk overlay)
const OpCtl = L.Control.extend({ options:{position:'topright'}, onAdd:function(){
  const d=L.DomUtil.create('div','leaflet-bar'); d.style.cssText='background:#fff;padding:4px 7px;font:11px sans-serif';
  d.innerHTML='risk opacity <input type="range" min="0" max="100" value="85" style="width:88px;vertical-align:middle">';
  L.DomEvent.disableClickPropagation(d);
  d.querySelector('input').addEventListener('input', e=>{ const o=e.target.value/100; riskLayers.outcome.setOpacity(o); riskLayers.pathway.setOpacity(o); });
  return d; }});
map.addControl(new OpCtl());
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
function featuresOf(result){
  if(!result) return [];
  if(result.type==='FeatureCollection' && result.features) return result.features;
  if(Array.isArray(result)) return result.map(p => ({ properties:p }));
  if(result.features) return result.features;
  return [ { properties: result.properties || result } ];
}
function applyResults(result){
  const feats = featuresOf(result); if(!feats.length){ setStatus('no result'); return; }
  feats.forEach(ft => {
    const props = ft.properties || {}; const s = storyOf(props);
    const colour = (s && CODE_COLOUR[s.code]) ? ('#'+CODE_COLOUR[s.code]) : '#888';
    const lyr = plotLayers[props.external_id]; if(lyr && lyr.setStyle) lyr.setStyle({ color:colour, weight:2, fillColor:colour, fillOpacity:0.45 });
  });
  showResult(feats[0].properties || {});
  renderStats(feats);
  const n = feats.length;
  setStatus(n+' plot'+(n>1?'s':'')+' analyzed'+(n>1?' (panel shows first), ':', ')+RISK_COL+' = '+(feats[0].properties[RISK_COL]||'?'));
}
function showResult(props){
  const s = storyOf(props); const outcome = props[RISK_COL] || (s && s.result);
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
    "<div class='sld'><code>"+ind.label+"</code><input type='range' min='0' max='100' value='10' data-param='"+ind.param+"'><span class='v' id='v_"+ind.param+"'>10</span></div>"
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
    applyResults((await resp.json()).data); setStatus('Thresholds applied.');
  }catch(err){ setStatus('Re-threshold needs the served proxy ('+err.message+').'); }
}
async function analyzeFC(fc){
  setStatus('Analyzing '+fc.features.length+' plot(s) (Earth Engine, ~15-30s)...');
  try{
    const resp=await fetch(WHISP_API_BASE+'/submit/geojson',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(fc)});
    if(!resp.ok) throw new Error('submit '+resp.status);
    const env=await resp.json(); curToken=env.token||null; applyResults(env.data);
  }catch(err){ setStatus('Draw/upload works; serve via the local proxy to analyze ('+err.message+').'); }
}
// draggable splitter
(function(){ const sp=document.getElementById('splitter'), tree=document.getElementById('tree');
  let dragging=false; sp.addEventListener('mousedown',()=>{dragging=true;document.body.style.userSelect='none';});
  document.addEventListener('mousemove',e=>{ if(!dragging) return; const w=Math.max(220,Math.min(e.clientX, window.innerWidth-280)); tree.style.width=w+'px'; });
  document.addEventListener('mouseup',()=>{dragging=false;document.body.style.userSelect='';map.invalidateSize();});
})();
</script></body></html>"""

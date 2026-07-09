"""Single-source export helpers for the timber decision tree.

``risk.TIMBER_ROOT_TREE`` is the ONE source of truth for the timber pathway. The display copies
(the Mermaid diagram, the notebook-viewer JS walk, and the map ``pathway()`` raster) used to
hand-re-implement that tree and drifted apart. This module walks the SAME ``TIMBER_ROOT_TREE`` to
emit each of those, so a tree change flows out by regenerating, never by hand-editing three copies.

What lives here (all derived from the tree, nothing re-stated):
  * ``PATHWAY_TO_CODE``     - the STABLE label -> map-code table (the one thing that is not structural;
                              codes are kept stable across tree edits so palettes/legends do not churn).
  * ``to_mermaid()``        - the flowchart source for the diagram + the notebook tree pane.
  * ``eval_tree_ee(qimg)``  - a GENERIC Earth Engine walker: recursive ``no.where(cond, yes)`` over the
                              tree, so the raster's precedence is the tree's precedence BY CONSTRUCTION
                              (this is what structurally removes the old exoneration-short-circuit bug).
  * ``js_module_source()``  - the JS ``TIMBER_TREE`` const + generic ``timberStory`` walk for the viewer.
  * ``code_names()`` / ``class_codes()`` / ``pathway_label_to_code()`` - the small derived lookups the
                              notebook needs for its palette, legend and drawn-polygon colouring.

Import-light on purpose: it only needs ``TIMBER_ROOT_TREE`` from ``risk``; the EE walker takes the
already-built per-question images as an argument, so this module never imports ``ee`` itself.
"""

from __future__ import annotations

from .risk import TIMBER_ROOT_TREE, _eval_timber_tree

# --------------------------------------------------------------------------------------------------
# 1. The STABLE code table.  Codes are intentionally NOT in DFS order: they preserve the historical
#    map codes so the palette/legend and any committed maps keep their numbering.  Code 14 is retired
#    (its old "<class> 2020 -> other land" meaning was split into 16/17/18).  Code 15 was the empty
#    "regen matured to primary" slot, now reused for the new "primary -> regrowth" leg.  19 is new.
# --------------------------------------------------------------------------------------------------
PATHWAY_TO_CODE = {
    "low: agriculture 2020": 1,
    "low: still primary": 2,
    "low: regen stayed forest": 3,
    "low: stable plantation": 4,
    "more-info: non-forest 2020, unclassified": 5,
    "high: deforestation": 6,
    "high: regen->plantation degradation": 7,
    "more-info: plantation 2020, no 2025 state": 8,
    "more-info: primary 2020, no 2025 state": 9,
    "more-info: regen 2020, no 2025 state": 10,
    "low: other land 2020 (stable)": 11,
    "high: primary->plantation degradation": 12,
    "more-info: other land 2020 changed": 13,
    # 14 revived for the unclassified-forest (treecover only) -> other land leaf; it was the OLD shared
    # "forest/plantation 2020 -> other land" low, whose class-specific cases split off as 16/17/18.
    "low: forest 2020 -> other land": 14,
    # 15 retired (was the dropped Ind_09_treecover_after_2020 "primary -> regrowth" leg)
    "low: primary 2020 -> other land": 16,
    "low: regen 2020 -> other land": 17,
    "low: plantation 2020 -> other land": 18,
    "more-info: forest 2020, unclassified": 19,
}
RETIRED_CODES = {15}

# Per-code display colour (hex, no '#'). Preserves the historical palette for codes 1-18; 15 keeps its
# old grey (now the primary->regrowth leg) and 19 is a new more-info purple-grey.
CODE_COLOUR = {
    1: "c7e9c0", 2: "238b45", 3: "41ab5d", 4: "addd8e", 5: "bcbddc", 6: "e31a1c",
    7: "e7298a", 8: "9e9ac8", 9: "6a51a3", 10: "807dba", 11: "66c2a4", 12: "67000d",
    13: "54278f", 14: "238b8b", 15: "8c8c9e", 16: "80cdc1", 17: "35978f", 18: "01665e",
    19: "b3b3d1",
}

# --------------------------------------------------------------------------------------------------
# 2. Per-question metadata (human label + node kind for the diagram; the drill-down layer wiring).
#    These are the only hand-maintained semantics; they change only when a NEW question is introduced.
# --------------------------------------------------------------------------------------------------
Q_LABEL = {
    "forest_2020": 'Forest in 2020?<br/><small>(treecover / primary / regen / planted)</small>',
    "other_land_2020": 'Other land use in 2020?<br/><small>(built / water / mining / bare)</small>',
    "other_land_2025": 'Became / still other land?<br/><small>(2025)</small>',
    "agriculture_2020": 'Agriculture or commodity in 2020?',
    "agriculture_2025": 'Agriculture after 2020?<br/><small>(deforestation)</small>',
    "plantation_2020": 'Plantation in 2020?',
    "plantation_presence_2025": 'Still a plantation in 2025?<br/><small>(presence)</small>',
    "primary_2020": 'Primary forest in 2020?',
    "plantation_2025": 'New plantation after 2020?<br/><small>(degradation)</small>',
    "primary_2025": 'Still primary in 2025?',
    "regen_planted_2020": 'Regenerating / planted forest in 2020?',
    "disturbance_2025": 'Disturbance after 2020?<br/><small>(logging / loss / degradation)</small>',
}
Q_KIND = {
    "forest_2020": "forest",
    "other_land_2020": "landuse", "other_land_2025": "landuse",
    "agriculture_2020": "landuse", "agriculture_2025": "landuse",
    "plantation_2020": "forest", "plantation_presence_2025": "forest",
    "primary_2020": "forest", "plantation_2025": "forest", "primary_2025": "forest",
    "regen_planted_2020": "forest",
    "disturbance_2025": "forest",
}

# Short, stable id stems per question (for Mermaid node ids + the drill-down mapping).
_ABBREV = {
    "forest_2020": "FOREST2020", "other_land_2020": "OL2020", "other_land_2025": "OL2025",
    "agriculture_2020": "AG2020", "agriculture_2025": "AG2025", "plantation_2020": "PL2020",
    "plantation_presence_2025": "PLpres2025", "primary_2020": "PR2020", "plantation_2025": "PL2025",
    "primary_2025": "PRstill2025", "regen_planted_2020": "RP2020",
    "disturbance_2025": "DIST2025",
}
# Questions that appear more than once get a branch suffix so each occurrence is a distinct node.
_REPEATED = {"other_land_2025", "plantation_2025"}
# Descending into the .yes of one of these sets the branch tag for everything below it.
_BRANCH_ON = {"other_land_2020": "nf", "plantation_2020": "pl", "primary_2020": "pr",
              "regen_planted_2020": "rp"}

# The per-question map layer for the tree's drill-down (click a node -> show its data layer).
_Q_TO_LAYER = {
    "other_land_2020": "agree_other_land",
    "agriculture_2020": ["agree_ag_whole", "agree_cropland", "agree_treecrop", "agree_pasture"],
    "plantation_2020": "agree_plantation",
    "regen_planted_2020": "agree_regen",
    "primary_2020": "agree_primary",
    "agriculture_2025": ["gain_ag_whole_gain", "gain_cropland_gain", "gain_treecrop_gain", "gain_pasture_gain"],
    "plantation_presence_2025": "gain_plantation_presence",
    "plantation_2025": "gain_plantation_gain",
    "primary_2025": "gain_still_primary_2025",
}
# Per-node overrides where the same question means different data in different branches.
_NODE_LAYER_OVERRIDE = {
    "OL2025_nf": "gain_other_land_present_2025",  # non-forest branch: presence, not gain
    "OL2025_pl": "gain_other_land_gain",
    "OL2025_pr": "gain_other_land_gain",
    "OL2025_rp": "gain_other_land_gain",
    "DIST2025": "gain_regen_stayed_2025",          # regen stayed forest (absence of disturbance)
}

# The per-question -> Whisp result column(s) mapping, mirroring risk.py add_risk_timber_col's yes_locals.
# Used only to generate the JS walk (so the viewer popup answers each question the same way risk.py does).
Q_TO_COLUMNS = {
    "forest_2020": ["Ind_01_treecover", "Ind_05_primary_2020", "Ind_06_nat_reg_forest_2020", "Ind_07a_planted_2020"],
    "other_land_2020": ["Ind_12_other_land_2020"],
    "other_land_2025": ["Ind_13_other_land_after_2020"],
    "agriculture_2020": ["Ind_02_commodities", "Ind_15_agriculture_2020"],
    "agriculture_2025": ["Ind_10_agri_after_2020"],
    "plantation_2020": ["Ind_07b_plantation_2020"],
    "plantation_presence_2025": ["Ind_16_plantation_presence_2025"],
    "primary_2020": ["Ind_05_primary_2020"],
    "plantation_2025": ["Ind_08b_plantation_after_2020"],
    "primary_2025": ["primary_2025"],
    "regen_planted_2020": ["Ind_06_nat_reg_forest_2020", "Ind_07a_planted_2020"],
    "disturbance_2025": ["Ind_17_disturbance_after_2020_timber"],
}


def _node_id(q, branch):
    base = _ABBREV[q]
    # Repeated questions get a branch suffix so each occurrence is a distinct node. The forest-half
    # "unclassified" branch carries no forest-class tag, so fall back to "fh" (forest half).
    return "%s_%s" % (base, branch or "fh") if q in _REPEATED else base


def leaf_code(node):
    """Map code for a leaf node (raises KeyError if the tree grew a pathway not in PATHWAY_TO_CODE)."""
    return PATHWAY_TO_CODE[node["pathway"]]


# --------------------------------------------------------------------------------------------------
# 3. Walk helpers -> a JSON-friendly annotated tree (ids/codes baked in), consumed by the emitters.
# --------------------------------------------------------------------------------------------------
def annotate(tree=TIMBER_ROOT_TREE):
    """Return a deep copy of the tree with a stable ``id`` on every node and ``code`` on every leaf."""

    def walk(node, branch):
        if "q" not in node:
            code = leaf_code(node)
            return {"id": "term_%d" % code, "code": code, "risk": node["risk"], "pathway": node["pathway"]}
        q = node["q"]
        nid = _node_id(q, branch)
        yes_branch = _BRANCH_ON.get(q, branch)
        return {
            "id": nid, "q": q, "label": Q_LABEL[q], "kind": Q_KIND[q],
            "is2025": q.endswith("2025"),
            "yes": walk(node["yes"], yes_branch),
            "no": walk(node["no"], branch),
        }

    return walk(tree, "")


def _iter_nodes(anno):
    """Yield every annotated node (decisions and leaves), pre-order."""
    yield anno
    if "q" in anno:
        yield from _iter_nodes(anno["yes"])
        yield from _iter_nodes(anno["no"])


# --------------------------------------------------------------------------------------------------
# 4. Derived lookups the notebook / maps consume.
# --------------------------------------------------------------------------------------------------
def code_names():
    """code -> its pathway label (the map legend). Only codes actually reached by the tree."""
    return {code: label for label, code in PATHWAY_TO_CODE.items()}


def class_codes():
    """Verdict class -> sorted list of its codes: {'low': [...], 'more': [...], 'high': [...]}."""
    out = {"low": [], "more": [], "high": []}
    for anno in _iter_nodes(annotate()):
        if "q" in anno:
            continue
        risk = anno["risk"]
        bucket = "high" if risk == "high" else ("low" if risk == "low" else "more")
        out[bucket].append(anno["code"])
    return {k: sorted(set(v)) for k, v in out.items()}


def pathway_label_to_code():
    """The drawn-polygon colouring map: risk_timber_pathway label -> map code (a copy of PATHWAY_TO_CODE)."""
    return dict(PATHWAY_TO_CODE)


def node_ids():
    """Return (decision_ids, terminal_ids) for the whole tree, pre-order (for the viewer click-binding)."""
    decisions, terminals = [], []
    for anno in _iter_nodes(annotate()):
        (terminals if "q" not in anno else decisions).append(anno["id"])
    return decisions, terminals


def node_to_layer():
    """Mermaid node id -> drill-down map layer key(s) (per-question default + per-branch overrides)."""
    out = {}
    for anno in _iter_nodes(annotate()):
        if "q" not in anno:
            continue
        layer = _NODE_LAYER_OVERRIDE.get(anno["id"]) or _Q_TO_LAYER.get(anno["q"])
        if layer is not None:
            out[anno["id"]] = layer
    return out


# --------------------------------------------------------------------------------------------------
# 5. Mermaid emitter.
# --------------------------------------------------------------------------------------------------
_TERMTEXT = {"low": "Low risk", "more_info_needed": "More info needed", "high": "High risk"}
_CLASSDEFS = """
  classDef forest fill:#bbf7d0,stroke:#16a34a,color:#064e3b;
  classDef landuse fill:#fff7ed,stroke:#f97316,color:#7c2d12;
  classDef low fill:#16a34a,color:#fff,stroke:#166534;
  classDef more fill:#f08c00,color:#fff,stroke:#c56f00;
  classDef high fill:#dc2626,color:#fff,stroke:#b91c1c;
  classDef q2020 stroke-width:3px;
  classDef q2025 stroke-width:3px,stroke-dasharray:4 3;
"""


def _term_class(risk):
    return "high" if risk == "high" else ("low" if risk == "low" else "more")


def to_mermaid(tree=TIMBER_ROOT_TREE):
    """Emit the ``flowchart TB`` Mermaid source for the tree (nodes, edges, styling)."""
    anno = annotate(tree)
    decisions, leaves, edges, classes = [], [], [], []

    def esc(s):
        return s.replace('"', "'")

    def walk(node):
        if "q" not in node:
            risk = node["risk"]
            cls = _term_class(risk)
            label = "%s<br/><small>%s<br/>Code: %d</small>" % (_TERMTEXT[risk], esc(node["pathway"]), node["code"])
            leaves.append('  %s(["%s"])' % (node["id"], label))
            classes.append("  class %s %s" % (node["id"], cls))
            return
        decisions.append('  %s{"%s"}' % (node["id"], node["label"]))
        classes.append("  class %s %s" % (node["id"], node["kind"]))
        classes.append("  class %s %s" % (node["id"], "q2025" if node["is2025"] else "q2020"))
        edges.append("  %s -- Yes --> %s" % (node["id"], node["yes"]["id"]))
        edges.append("  %s -- No --> %s" % (node["id"], node["no"]["id"]))
        walk(node["yes"])
        walk(node["no"])

    walk(anno)
    lines = ["flowchart TB"] + decisions + leaves + edges + [_CLASSDEFS.strip("\n")] + classes
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------------------------------
# 6. Generic Earth Engine walker.  qimg maps each question name -> its ee boolean image; the walk
#    returns an ee code image whose precedence IS the tree's (later .where wins == the yes-branch).
# --------------------------------------------------------------------------------------------------
def eval_tree_ee(qimg, ee_image, tree=TIMBER_ROOT_TREE):
    """Build the per-pixel pathway-code image.

    ``qimg``: dict question-name -> ee boolean image (1 where the question is 'yes').
    ``ee_image``: the ``ee.Image`` constructor (passed in so this module need not import ee).
    """

    def walk(node):
        if "q" not in node:
            return ee_image(leaf_code(node))
        cond = qimg[node["q"]]
        return walk(node["no"]).where(cond, walk(node["yes"]))

    return walk(tree)


# --------------------------------------------------------------------------------------------------
# 7. JS emitter: the annotated tree as a JS literal + a generic walk that mirrors _eval_timber_tree.
# --------------------------------------------------------------------------------------------------
def _js_qyes_body():
    lines = []
    for q, cols in Q_TO_COLUMNS.items():
        expr = " || ".join("y('%s')" % c for c in cols)
        lines.append("      case '%s': return %s;" % (q, expr))
    return "\n".join(lines)


def js_module_source():
    """Return the JS block: TIMBER_TREE (annotated JSON), qYes(p,q), and timberStory(p).

    ``timberStory`` walks TIMBER_TREE exactly like risk.py's _eval_timber_tree, recording each
    question it passes and the leg that fired, so the viewer popup is faithful by construction.
    """
    import json as _json

    tree_json = _json.dumps(annotate(), separators=(",", ":"))
    return (
        "// AUTO-GENERATED from openforis_whisp.timber_tree_export (do not hand-edit; regenerate).\n"
        "const TIMBER_TREE = %s;\n"
        "// Answer one question the SAME way risk.py add_risk_timber_col does (yes_locals).\n"
        "function qYes(p, q) {\n"
        "  const y = (k) => (p && p[k]) === 'yes';\n"
        "  switch (q) {\n"
        "%s\n"
        "    default: return false;\n"
        "  }\n"
        "}\n"
        "// Walk TIMBER_TREE; return {steps, result, pathway, code}. steps = each question passed\n"
        "// (with its yes/no answer) then the fired leaf.\n"
        "function timberStory(p) {\n"
        "  p = p || {};\n"
        "  const steps = [];\n"
        "  let node = TIMBER_TREE;\n"
        "  while (node && node.q) {\n"
        "    const ans = qYes(p, node.q);\n"
        "    steps.push({ q: node.label, ans: ans, fired: false });\n"
        "    node = ans ? node.yes : node.no;\n"
        "  }\n"
        "  if (steps.length) { steps[steps.length - 1].fired = true; steps[steps.length - 1].leg = node.pathway; }\n"
        "  return { steps: steps, result: node.risk, pathway: node.pathway, code: node.code };\n"
        "}\n"
    ) % (tree_json, _js_qyes_body())


# --------------------------------------------------------------------------------------------------
# 8. Self-check: the code table must cover exactly the tree's leaves (catches drift on import/test).
# --------------------------------------------------------------------------------------------------
def selfcheck():
    """Assert PATHWAY_TO_CODE is a bijection over the tree's leaf pathways; return the leaf/code summary."""
    leaf_pathways = [n["pathway"] for n in _iter_nodes(annotate()) if "q" not in n]
    # every leaf has a code
    missing = [p for p in leaf_pathways if p not in PATHWAY_TO_CODE]
    assert not missing, "leaves with no code: %s" % missing
    # no duplicate leaf pathways, no duplicate codes among reached leaves
    assert len(leaf_pathways) == len(set(leaf_pathways)), "duplicate leaf pathway strings"
    reached_codes = [PATHWAY_TO_CODE[p] for p in leaf_pathways]
    assert len(reached_codes) == len(set(reached_codes)), "two leaves share a code"
    # PATHWAY_TO_CODE has no extra label the tree never reaches
    extra = set(PATHWAY_TO_CODE) - set(leaf_pathways)
    assert not extra, "PATHWAY_TO_CODE has labels not in the tree: %s" % sorted(extra)
    # every reached code has a colour
    no_colour = [c for c in reached_codes if c not in CODE_COLOUR]
    assert not no_colour, "codes with no colour: %s" % no_colour
    return {"n_leaves": len(leaf_pathways), "codes": sorted(reached_codes)}

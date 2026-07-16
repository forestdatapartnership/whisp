"""Generic, tree-agnostic core for WHISP decision-tree pathways.

This module is the reusable engine behind the WHISP "diagram A" style land-use decision trees. It holds
NO tree-specific data: no pathway labels, no map codes, no palette colours, and no WHISP indicator column
names. Every one of those lives on a :class:`DecisionTreeSpec`, and each function here takes a spec, so
the SAME code can drive any tree.

Today it drives one instance, the timber tree (see ``openforis_whisp.timber_tree_export``, which builds
``TIMBER_SPEC`` from ``risk.TIMBER_ROOT_TREE`` and binds these functions to it). It is written so a future
perennial-crop tree (``PCROP_SPEC``) and annual-crop tree (``ACROP_SPEC``) can slot in the same way: a
nested-dict tree, a question -> indicator-column mapping, a stable pathway-label -> code map, a palette,
and the per-question display / drill-down metadata, all packed into another ``DecisionTreeSpec``. The
crop specs are intentionally NOT written here; this module only provides the shared machinery for them.

A tree is DATA: a nested dict of DECISION nodes ``{"q": <question-name>, "yes": node, "no": node}`` and
LEAF nodes ``{"risk": <"low"|"more_info_needed"|"high">, "pathway": <stable label>}``. Walking that ONE
structure emits every display copy (the Mermaid diagram, the notebook-viewer JS walk, the Earth Engine
per-pixel pathway raster) plus the small derived lookups, so a tree edit flows out by regenerating rather
than by hand-editing parallel copies that can drift.

WHISP is a non-authoritative exploration tool and a digital public good for compliance workflows (EUDR is
one example of a framework it can inform, not its focus), so the three risk levels are treated as outcome
SIGNALS (low / more-info-needed / high), never as a legal determination.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Callable

# The three WHISP risk-outcome levels are shared by every tree (they are not tree-specific), so their
# diagram display text and their Mermaid class mapping live here rather than on the spec.
TERM_TEXT = {
    "low": "Low risk",
    "more_info_needed": "More info needed",
    "high": "High risk",
}


def _term_class(risk: str) -> str:
    """Map a leaf's risk level to its Mermaid ``classDef`` name (``low`` / ``more`` / ``high``)."""
    return "high" if risk == "high" else ("low" if risk == "low" else "more")


@dataclass
class DecisionTreeSpec:
    """Everything tree-specific for one decision tree, consumed by the generic functions in this module.

    Bundling all of this onto one object is what keeps the generic core tree-agnostic: swap the spec and
    the same ``to_mermaid`` / ``eval_tree_ee`` / ``code_names`` (and the rest) drive a different tree. A
    future ``PCROP_SPEC`` / ``ACROP_SPEC`` is just another instance of this dataclass.

    Fields
    ------
    tree : nested-dict decision tree (the ONE source of truth for this pathway).
    pathway_to_code : stable leaf-``pathway`` label -> integer map code (kept stable across tree edits so
        palettes / legends do not churn; this is the one non-structural table).
    code_colour : map code -> display colour (hex, no leading ``#``).
    q_label : question name -> human label shown on the diagram node.
    q_kind : question name -> node-kind tag used for diagram styling (its Mermaid ``classDef``).
    q_to_columns : question name -> the WHISP result column(s) that answer it (mirrors how the pandas
        walker sets each per-plot boolean); used to emit the JS walk.
    abbrev : question name -> short stable id stem for the Mermaid node ids.
    repeated : the set of questions that appear more than once (each occurrence gets a branch suffix so it
        is a distinct node).
    branch_on : question name -> branch tag set when descending into that question's ``yes`` (so repeated
        questions below it are disambiguated per branch).
    q_to_layer : question name -> default drill-down map layer key(s) for its node.
    node_layer_override : node id -> layer key(s), for nodes whose question means different data per branch.
    mermaid_classdefs : the Mermaid ``classDef`` block (diagram styling for this tree's node kinds plus the
        shared risk classes).
    retired_codes : codes intentionally kept in the palette but no longer emitted by the tree (metadata
        only; carried so a legend keeps stable numbering).
    default_branch_tag : the branch suffix used for a repeated question reached with no branch tag set.
    js_source_module : dotted module name named in the generated JS "AUTO-GENERATED from ..." header.
    js_tree_const : the JS ``const`` name for the embedded annotated tree.
    js_answer_ref : the source referenced in the generated JS "answer the SAME way ... does" comment.
    js_story_fn : the JS walk function name.
    """

    tree: dict
    pathway_to_code: dict
    code_colour: dict
    q_label: dict
    q_kind: dict
    q_to_columns: dict
    abbrev: dict
    repeated: set
    branch_on: dict
    q_to_layer: dict
    node_layer_override: dict
    mermaid_classdefs: str
    retired_codes: set = field(default_factory=set)
    default_branch_tag: str = "x"
    js_source_module: str = "openforis_whisp.decision_tree"
    js_tree_const: str = "TREE"
    js_answer_ref: str = "the WHISP risk logic"
    js_story_fn: str = "story"


# --------------------------------------------------------------------------------------------------
# 1. Node-id + leaf-code helpers (both parameterised by the spec).
# --------------------------------------------------------------------------------------------------
def _node_id(spec: DecisionTreeSpec, q: str, branch: str) -> str:
    """Stable Mermaid node id for question ``q`` on ``branch``.

    Repeated questions get a branch suffix so each occurrence is a distinct node; a repeated question
    reached with no branch tag falls back to ``spec.default_branch_tag``.
    """
    base = spec.abbrev[q]
    if q in spec.repeated:
        return "%s_%s" % (base, branch or spec.default_branch_tag)
    return base


def _leaf_code(spec: DecisionTreeSpec, node: dict) -> int:
    """Map code for a leaf node (raises ``KeyError`` if the tree grew a pathway not in ``pathway_to_code``)."""
    return spec.pathway_to_code[node["pathway"]]


# --------------------------------------------------------------------------------------------------
# 2. Annotate the tree (ids baked onto every node, code onto every leaf) + a pre-order node iterator.
# --------------------------------------------------------------------------------------------------
def annotate(spec: DecisionTreeSpec) -> dict:
    """Return a deep copy of ``spec.tree`` with a stable ``id`` on every node and ``code`` on every leaf."""

    def walk(node, branch):
        if "q" not in node:
            code = _leaf_code(spec, node)
            return {
                "id": "term_%d" % code,
                "code": code,
                "risk": node["risk"],
                "pathway": node["pathway"],
            }
        q = node["q"]
        nid = _node_id(spec, q, branch)
        yes_branch = spec.branch_on.get(q, branch)
        return {
            "id": nid,
            "q": q,
            "label": spec.q_label[q],
            "kind": spec.q_kind[q],
            "is2025": q.endswith("2025"),
            "yes": walk(node["yes"], yes_branch),
            "no": walk(node["no"], branch),
        }

    return walk(spec.tree, "")


def iter_nodes(anno: dict) -> Iterator[dict]:
    """Yield every annotated node (decisions and leaves), pre-order. Takes an ALREADY-annotated tree."""
    yield anno
    if "q" in anno:
        yield from iter_nodes(anno["yes"])
        yield from iter_nodes(anno["no"])


# --------------------------------------------------------------------------------------------------
# 3. Derived lookups the notebook / maps consume.
# --------------------------------------------------------------------------------------------------
def code_names(spec: DecisionTreeSpec) -> dict:
    """code -> its pathway label (the map legend); the inverse of ``spec.pathway_to_code``."""
    return {code: label for label, code in spec.pathway_to_code.items()}


def class_codes(spec: DecisionTreeSpec) -> dict:
    """Risk class -> sorted list of its codes: ``{"low": [...], "more": [...], "high": [...]}``."""
    out = {"low": [], "more": [], "high": []}
    for anno in iter_nodes(annotate(spec)):
        if "q" in anno:
            continue
        out[_term_class(anno["risk"])].append(anno["code"])
    return {k: sorted(set(v)) for k, v in out.items()}


def pathway_label_to_code(spec: DecisionTreeSpec) -> dict:
    """The drawn-polygon colouring map: pathway label -> map code (a copy of ``spec.pathway_to_code``)."""
    return dict(spec.pathway_to_code)


def node_ids(spec: DecisionTreeSpec) -> tuple[list, list]:
    """Return ``(decision_ids, terminal_ids)`` for the whole tree, pre-order (for viewer click-binding)."""
    decisions, terminals = [], []
    for anno in iter_nodes(annotate(spec)):
        (terminals if "q" not in anno else decisions).append(anno["id"])
    return decisions, terminals


def node_to_layer(spec: DecisionTreeSpec) -> dict:
    """Mermaid node id -> drill-down map layer key(s) (per-question default plus per-node overrides)."""
    out = {}
    for anno in iter_nodes(annotate(spec)):
        if "q" not in anno:
            continue
        layer = spec.node_layer_override.get(anno["id"]) or spec.q_to_layer.get(
            anno["q"],
        )
        if layer is not None:
            out[anno["id"]] = layer
    return out


# --------------------------------------------------------------------------------------------------
# 4. Mermaid emitter.
# --------------------------------------------------------------------------------------------------
def to_mermaid(spec: DecisionTreeSpec) -> str:
    """Emit the ``flowchart TB`` Mermaid source for ``spec.tree`` (nodes, edges, styling)."""
    anno = annotate(spec)
    decisions, leaves, edges, classes = [], [], [], []

    def esc(s):
        return s.replace('"', "'")

    def walk(node):
        if "q" not in node:
            risk = node["risk"]
            cls = _term_class(risk)
            label = "%s<br/><small>%s<br/>Code: %d</small>" % (
                TERM_TEXT[risk],
                esc(node["pathway"]),
                node["code"],
            )
            leaves.append('  %s(["%s"])' % (node["id"], label))
            classes.append("  class %s %s" % (node["id"], cls))
            return
        decisions.append('  %s{"%s"}' % (node["id"], node["label"]))
        classes.append("  class %s %s" % (node["id"], node["kind"]))
        classes.append(
            "  class %s %s" % (node["id"], "q2025" if node["is2025"] else "q2020"),
        )
        edges.append("  %s -- Yes --> %s" % (node["id"], node["yes"]["id"]))
        edges.append("  %s -- No --> %s" % (node["id"], node["no"]["id"]))
        walk(node["yes"])
        walk(node["no"])

    walk(anno)
    lines = (
        ["flowchart TB"]
        + decisions
        + leaves
        + edges
        + [spec.mermaid_classdefs.strip("\n")]
        + classes
    )
    return "\n".join(lines) + "\n"


# --------------------------------------------------------------------------------------------------
# 5. Generic Earth Engine walker + a generic pandas walker.  Both walk the SAME tree, so the raster's
#    per-pixel precedence and the tabular outcome agree terminal-for-terminal BY CONSTRUCTION.
# --------------------------------------------------------------------------------------------------
def eval_tree_ee(qimg: dict, ee_image: Callable, spec: DecisionTreeSpec):
    """Build the per-pixel pathway-code image by walking ``spec.tree``.

    ``qimg``: dict question-name -> ee boolean image (1 where the question is 'yes').
    ``ee_image``: the ``ee.Image`` constructor (passed in so this module need not import ee).

    The recursive ``no.where(cond, yes)`` makes the raster's precedence the tree's precedence (a later
    ``.where`` wins == the yes-branch), so it cannot drift from the pandas walk below.
    """

    def walk(node):
        if "q" not in node:
            return ee_image(_leaf_code(spec, node))
        cond = qimg[node["q"]]
        return walk(node["no"]).where(cond, walk(node["yes"]))

    return walk(spec.tree)


def eval_tree(bools: dict, spec: DecisionTreeSpec) -> tuple[str, str]:
    """Walk ``spec.tree`` against a mapping of question-name -> bool; return ``(risk, pathway)``.

    The pure-pandas analogue of ``eval_tree_ee`` (and the mirror of the per-plot walker in ``risk.py``):
    same structure and same precedence, so the tabular outcome matches the raster terminal-for-terminal.
    """
    node = spec.tree
    while "q" in node:
        node = node["yes"] if bools[node["q"]] else node["no"]
    return node["risk"], node["pathway"]


# --------------------------------------------------------------------------------------------------
# 6. JS emitter: the annotated tree as a JS literal + a generic walk that mirrors the pandas walker.
# --------------------------------------------------------------------------------------------------
def _js_qyes_body(spec: DecisionTreeSpec) -> str:
    lines = []
    for q, cols in spec.q_to_columns.items():
        expr = " || ".join("y('%s')" % c for c in cols)
        lines.append("      case '%s': return %s;" % (q, expr))
    return "\n".join(lines)


def js_module_source(spec: DecisionTreeSpec) -> str:
    """Return the JS block: the annotated-tree ``const``, ``qYes(p, q)``, and the story walk.

    The story function walks the embedded tree exactly like the pandas walker, recording each question it
    passes and the leg that fired, so a viewer popup stays faithful to the tree by construction.
    """
    import json as _json

    tree_json = _json.dumps(annotate(spec), separators=(",", ":"))
    template = (
        "// AUTO-GENERATED from %(source_module)s (do not hand-edit; regenerate).\n"
        "const %(tree_const)s = %(tree_json)s;\n"
        "// Answer one question the SAME way %(answer_ref)s does (yes_locals).\n"
        "function qYes(p, q) {\n"
        "  const y = (k) => (p && p[k]) === 'yes';\n"
        "  switch (q) {\n"
        "%(qyes_body)s\n"
        "    default: return false;\n"
        "  }\n"
        "}\n"
        "// Walk %(tree_const)s; return {steps, result, pathway, code}. steps = each question passed\n"
        "// (with its yes/no answer) then the fired leaf.\n"
        "function %(story_fn)s(p) {\n"
        "  p = p || {};\n"
        "  const steps = [];\n"
        "  let node = %(tree_const)s;\n"
        "  while (node && node.q) {\n"
        "    const ans = qYes(p, node.q);\n"
        "    steps.push({ q: node.label, ans: ans, fired: false });\n"
        "    node = ans ? node.yes : node.no;\n"
        "  }\n"
        "  if (steps.length) { steps[steps.length - 1].fired = true; steps[steps.length - 1].leg = node.pathway; }\n"
        "  return { steps: steps, result: node.risk, pathway: node.pathway, code: node.code };\n"
        "}\n"
    )
    return template % {
        "source_module": spec.js_source_module,
        "tree_const": spec.js_tree_const,
        "answer_ref": spec.js_answer_ref,
        "story_fn": spec.js_story_fn,
        "tree_json": tree_json,
        "qyes_body": _js_qyes_body(spec),
    }


# --------------------------------------------------------------------------------------------------
# 7. Self-check: the code table must cover exactly the tree's leaves (catches drift on import / test).
# --------------------------------------------------------------------------------------------------
def selfcheck(spec: DecisionTreeSpec) -> dict:
    """Assert ``spec.pathway_to_code`` is a bijection over the tree's leaf pathways; return a summary."""
    leaf_pathways = [n["pathway"] for n in iter_nodes(annotate(spec)) if "q" not in n]
    # every leaf has a code
    missing = [p for p in leaf_pathways if p not in spec.pathway_to_code]
    assert not missing, "leaves with no code: %s" % missing
    # no duplicate leaf pathways, no duplicate codes among reached leaves
    assert len(leaf_pathways) == len(
        set(leaf_pathways),
    ), "duplicate leaf pathway strings"
    reached_codes = [spec.pathway_to_code[p] for p in leaf_pathways]
    assert len(reached_codes) == len(set(reached_codes)), "two leaves share a code"
    # the code table has no extra label the tree never reaches
    extra = set(spec.pathway_to_code) - set(leaf_pathways)
    assert not extra, "code table has labels not in the tree: %s" % sorted(extra)
    # every reached code has a colour
    no_colour = [c for c in reached_codes if c not in spec.code_colour]
    assert not no_colour, "codes with no colour: %s" % no_colour
    return {"n_leaves": len(leaf_pathways), "codes": sorted(reached_codes)}

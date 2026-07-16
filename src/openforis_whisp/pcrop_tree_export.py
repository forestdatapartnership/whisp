"""Perennial-crop (pcrop) INSTANCE of the generic decision-tree core, plus its export helpers.

``risk.add_risk_pcrop_col`` is the AUTHORITY for the perennial-crop risk outcome. It is a small flat
OR-chain over four WHISP indicators, NOT a walked tree (unlike timber, whose ``risk.TIMBER_ROOT_TREE`` is
itself walked by ``add_risk_timber_col``). This module adds a display-only MIRROR of that flat rule so the
perennial-crop tree gets the same machinery the timber tree already has: a Mermaid diagram, a notebook
viewer JS walk, and a per-pixel Earth Engine pathway raster, all from the generic engine in
``openforis_whisp.decision_tree``.

``PCROP_ROOT_TREE`` reproduces ``add_risk_pcrop_col`` EXACTLY (every one of the 16 indicator combinations
yields the same outcome; see the equivalence check in the module docstring of the crop verification). It
is a display MIRROR, not a second source of truth: ``add_risk_pcrop_col`` stays the authority and is left
untouched, and the flat rule is transcribed here as a tree only so the diagram / map / viewer can be
generated instead of hand-drawn. If the flat rule ever changes, this tree must be re-mirrored (an
equivalence check catches drift).

Like ``timber_tree_export`` this module:

  * defines the pcrop-specific metadata (the stable ``PATHWAY_TO_CODE`` code table, the ``CODE_COLOUR``
    palette, the per-question ``Q_LABEL`` / ``Q_KIND`` / ``Q_TO_COLUMNS`` mapping to WHISP indicator
    columns), then
  * bundles all of it into ``PCROP_SPEC = DecisionTreeSpec(tree=PCROP_ROOT_TREE, ...)``, and
  * binds the generic ``decision_tree`` functions to ``PCROP_SPEC`` (so ``to_mermaid()`` /
    ``eval_tree_ee(qimg, ee_image)`` / ``code_names()`` and the rest work with no spec argument).

The annual-crop tree (acrop) is the sibling instance in ``openforis_whisp.acrop_tree_export``.

WHISP is a non-authoritative exploration tool and a digital public good for compliance workflows (EUDR is
one example of a framework it can inform, not its focus), so the three risk levels are treated as outcome
SIGNALS (low / more-info-needed / high), never as a legal determination.

Import-light on purpose: it needs only the generic ``decision_tree`` core. The Earth Engine walker takes
the already-built per-question images as an argument, so this module never imports ``ee`` itself, and it
does NOT import ``risk`` (the indicator column names are the canonical WHISP names, hard-coded in
``Q_TO_COLUMNS`` to match what ``add_risk_pcrop_col`` reads).
"""

from __future__ import annotations

from . import decision_tree as _dt
from .decision_tree import DecisionTreeSpec


# ---- The perennial-crop tree: a display MIRROR of risk.add_risk_pcrop_col ------------------------
# Each node is a LEAF {"risk", "pathway"} or a DECISION {"q": <question name>, "yes": node, "no": node}.
# The flat rule (risk.add_risk_pcrop_col) is an OR-chain:
#     low  if (Ind_01_treecover == "no" OR Ind_02_commodities == "yes"
#              OR Ind_03_disturbance_before_2020 == "yes")
#     elif Ind_04_disturbance_after_2020 == "no": more_info_needed
#     else: high
# Asking the OR members in order (each firing LOW on its own leg) reproduces that rule for every input:
# no treecover -> low; else commodity -> low; else disturbance-before -> low; else disturbance-after
# decides more-info (no) vs high (yes). Questions are phrased POSITIVELY (yes = the indicator is "yes"),
# so a "no treecover" plot takes the treecover_2020 NO branch to the low leaf.
def _crop_leaf(risk, pathway):
    return {"risk": risk, "pathway": pathway}


PCROP_ROOT_TREE = {
    "q": "treecover_2020",
    # Ind_01_treecover == "no" -> low (no forest to have been cleared for the crop).
    "no": _crop_leaf("low", "low: no treecover 2020"),
    "yes": {
        "q": "commodities_2020",
        # Ind_02_commodities == "yes" -> low (already a commodity in 2020, pre-cutoff land use).
        "yes": _crop_leaf("low", "low: commodity 2020"),
        "no": {
            "q": "disturbance_before_2020",
            # Ind_03_disturbance_before_2020 == "yes" -> low (disturbed before the cutoff).
            "yes": _crop_leaf("low", "low: disturbance before 2020"),
            "no": {
                "q": "disturbance_after_2020",
                # Treecover 2020, no commodity, no pre-2020 disturbance, and no post-2020 disturbance
                # signal -> not enough evidence either way.
                "no": _crop_leaf(
                    "more_info_needed",
                    "more-info: treecover 2020, no disturbance after 2020",
                ),
                # ... but WITH post-2020 disturbance -> high.
                "yes": _crop_leaf(
                    "high", "high: treecover 2020, disturbance after 2020"
                ),
            },
        },
    },
}

# --------------------------------------------------------------------------------------------------
# 1. The STABLE code table (small integers, one per terminal; grouped low -> more-info -> high). Kept
#    stable so a palette / legend does not churn if the tree is edited.
# --------------------------------------------------------------------------------------------------
PATHWAY_TO_CODE = {
    "low: no treecover 2020": 1,
    "low: commodity 2020": 2,
    "low: disturbance before 2020": 3,
    "more-info: treecover 2020, no disturbance after 2020": 4,
    "high: treecover 2020, disturbance after 2020": 5,
}
RETIRED_CODES: set = set()

# Per-code display colour (hex, no '#'): ColorBrewer Greens for the LOW legs, an amber for MORE-INFO,
# a red for HIGH (the same low/more/high families the timber palette uses).
CODE_COLOUR = {
    1: "c7e9c0",  # low  (light green)
    2: "74c476",  # low  (medium green)
    3: "238b45",  # low  (dark green)
    4: "fe9929",  # more-info (amber)
    5: "e31a1c",  # high (red)
}

# --------------------------------------------------------------------------------------------------
# 2. Per-question metadata (human label + node kind for the diagram; the drill-down layer wiring is
#    empty for now, no crop map layers are wired yet).
# --------------------------------------------------------------------------------------------------
Q_LABEL = {
    "treecover_2020": "Tree cover in 2020?<br/><small>(Ind_01_treecover)</small>",
    "commodities_2020": "Commodity / agriculture in 2020?<br/><small>(Ind_02_commodities)</small>",
    "disturbance_before_2020": "Disturbance before 2020?<br/><small>(Ind_03_disturbance_before_2020)</small>",
    "disturbance_after_2020": "Disturbance after 2020?<br/><small>(Ind_04_disturbance_after_2020)</small>",
}
Q_KIND = {
    "treecover_2020": "forest",
    "commodities_2020": "landuse",
    "disturbance_before_2020": "disturbance",
    "disturbance_after_2020": "disturbance",
}

# Short, stable id stems per question (for Mermaid node ids).
_ABBREV = {
    "treecover_2020": "TC2020",
    "commodities_2020": "COMM2020",
    "disturbance_before_2020": "DISTB2020",
    "disturbance_after_2020": "DISTA2020",
}
# No question appears more than once and no branch tagging is needed (a shallow OR-chain).
_REPEATED: set = set()
_BRANCH_ON: dict = {}

# No crop drill-down map layers are wired yet; leave the drill-down mappings empty.
_Q_TO_LAYER: dict = {}
_NODE_LAYER_OVERRIDE: dict = {}

# The per-question -> WHISP indicator column mapping. This is the SINGLE-SOURCE point: the SAME columns
# add_risk_pcrop_col reads. Used to emit the JS walk (so the viewer answers each question the same way).
Q_TO_COLUMNS = {
    "treecover_2020": ["Ind_01_treecover"],
    "commodities_2020": ["Ind_02_commodities"],
    "disturbance_before_2020": ["Ind_03_disturbance_before_2020"],
    "disturbance_after_2020": ["Ind_04_disturbance_after_2020"],
}

# Mermaid classDef block: styling for the crop node kinds (forest / landuse / disturbance) plus the
# shared risk classes (low / more / high) and the 2020 / 2025 question stroke styles. Passed to the spec
# so the generic emitter hard-codes no styling.
_CLASSDEFS = """
  classDef forest fill:#bbf7d0,stroke:#16a34a,color:#064e3b;
  classDef landuse fill:#fff7ed,stroke:#f97316,color:#7c2d12;
  classDef disturbance fill:#fee2e2,stroke:#dc2626,color:#7f1d1d;
  classDef low fill:#16a34a,color:#fff,stroke:#166534;
  classDef more fill:#f08c00,color:#fff,stroke:#c56f00;
  classDef high fill:#dc2626,color:#fff,stroke:#b91c1c;
  classDef q2020 stroke-width:3px;
  classDef q2025 stroke-width:3px,stroke-dasharray:4 3;
"""

# --------------------------------------------------------------------------------------------------
# 3. The pcrop spec: bundle every pcrop-specific piece so the generic decision_tree functions can drive
#    the perennial-crop tree.
# --------------------------------------------------------------------------------------------------
PCROP_SPEC = DecisionTreeSpec(
    tree=PCROP_ROOT_TREE,
    pathway_to_code=PATHWAY_TO_CODE,
    code_colour=CODE_COLOUR,
    q_label=Q_LABEL,
    q_kind=Q_KIND,
    q_to_columns=Q_TO_COLUMNS,
    abbrev=_ABBREV,
    repeated=_REPEATED,
    branch_on=_BRANCH_ON,
    q_to_layer=_Q_TO_LAYER,
    node_layer_override=_NODE_LAYER_OVERRIDE,
    mermaid_classdefs=_CLASSDEFS,
    retired_codes=RETIRED_CODES,
    js_source_module="openforis_whisp.pcrop_tree_export",
    js_tree_const="PCROP_TREE",
    js_answer_ref="risk.py add_risk_pcrop_col",
    js_story_fn="pcropStory",
)


# --------------------------------------------------------------------------------------------------
# 4. Public API: thin bindings of the generic core to PCROP_SPEC (same names / signatures as
#    timber_tree_export exposes for TIMBER_SPEC, so a future pcrop map / viewer can consume them the
#    same way timber_map consumes timber_tree_export).
# --------------------------------------------------------------------------------------------------
def leaf_code(node):
    """Map code for a pcrop leaf node (raises KeyError if the tree grew a pathway not in PATHWAY_TO_CODE)."""
    return PATHWAY_TO_CODE[node["pathway"]]


# Private pre-order node iterator, kept as a module-level name for parity with timber_tree_export.
_iter_nodes = _dt.iter_nodes


def annotate():
    """Return the pcrop tree annotated with a stable ``id`` on every node and ``code`` on every leaf."""
    return _dt.annotate(PCROP_SPEC)


def code_names():
    """code -> its pathway label (the map legend). Only codes actually reached by the pcrop tree."""
    return _dt.code_names(PCROP_SPEC)


def class_codes():
    """Risk class -> sorted list of its codes: {'low': [...], 'more': [...], 'high': [...]}."""
    return _dt.class_codes(PCROP_SPEC)


def pathway_label_to_code():
    """The drawn-polygon colouring map: risk_pcrop pathway label -> map code (a copy of PATHWAY_TO_CODE)."""
    return _dt.pathway_label_to_code(PCROP_SPEC)


def node_ids():
    """Return (decision_ids, terminal_ids) for the pcrop tree, pre-order (for the viewer click-binding)."""
    return _dt.node_ids(PCROP_SPEC)


def node_to_layer():
    """Mermaid node id -> drill-down map layer key(s). Empty until crop map layers are wired."""
    return _dt.node_to_layer(PCROP_SPEC)


def to_mermaid():
    """Emit the ``flowchart TB`` Mermaid source for the pcrop tree (nodes, edges, styling)."""
    return _dt.to_mermaid(PCROP_SPEC)


def eval_tree_ee(qimg, ee_image):
    """Build the per-pixel pcrop pathway-code image.

    ``qimg``: dict question-name -> ee boolean image (1 where the question is 'yes').
    ``ee_image``: the ``ee.Image`` constructor (passed in so this module need not import ee).
    """
    return _dt.eval_tree_ee(qimg, ee_image, PCROP_SPEC)


def js_module_source():
    """Return the JS block: PCROP_TREE (annotated JSON), qYes(p, q), and pcropStory(p).

    ``pcropStory`` walks PCROP_TREE the SAME way risk.py add_risk_pcrop_col decides, recording each
    question it passes and the leg that fired, so a viewer popup is faithful by construction.
    """
    return _dt.js_module_source(PCROP_SPEC)


def selfcheck():
    """Assert PATHWAY_TO_CODE is a bijection over the pcrop tree's leaf pathways; return the summary."""
    return _dt.selfcheck(PCROP_SPEC)

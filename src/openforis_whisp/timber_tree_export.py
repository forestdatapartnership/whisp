"""Timber INSTANCE of the generic decision-tree core, plus the timber export helpers.

``risk.TIMBER_ROOT_TREE`` is the ONE source of truth for the timber pathway. The display copies (the
Mermaid diagram, the notebook-viewer JS walk, and the map ``pathway()`` raster) used to hand-re-implement
that tree and drifted apart. The generic engine in ``openforis_whisp.decision_tree`` walks the SAME tree
to emit each of those; THIS module is the thin timber adapter over that engine:

  * it defines the timber-specific metadata (the stable ``PATHWAY_TO_CODE`` code table, the ``CODE_COLOUR``
    palette, the per-question ``Q_LABEL`` / ``Q_KIND`` / ``Q_TO_COLUMNS`` and the drill-down wiring), then
  * bundles all of it into ``TIMBER_SPEC = DecisionTreeSpec(tree=risk.TIMBER_ROOT_TREE, ...)``, and
  * PRESERVES the historical public API by binding the generic ``decision_tree`` functions to
    ``TIMBER_SPEC`` (so ``to_mermaid()`` / ``eval_tree_ee(qimg, ee_image)`` / ``code_names()`` and the rest
    keep working with the same name and call signature they always had).

The perennial-crop and annual-crop trees (pcrop / acrop) will be separate instances of the same
``DecisionTreeSpec`` (their own tree, question -> indicator-column mapping, code map and palette) built
the same way; they are NOT defined here.

Import-light on purpose: it needs ``TIMBER_ROOT_TREE`` from ``risk`` and the generic ``decision_tree``
core; the Earth Engine walker takes the already-built per-question images as an argument, so this module
never imports ``ee`` itself.
"""

from __future__ import annotations

from . import decision_tree as _dt
from .decision_tree import DecisionTreeSpec
from .risk import TIMBER_ROOT_TREE

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
    1: "c7e9c0",
    2: "238b45",
    3: "41ab5d",
    4: "addd8e",
    5: "bcbddc",
    6: "e31a1c",
    7: "e7298a",
    8: "9e9ac8",
    9: "6a51a3",
    10: "807dba",
    11: "66c2a4",
    12: "67000d",
    13: "54278f",
    14: "238b8b",
    15: "8c8c9e",
    16: "80cdc1",
    17: "35978f",
    18: "01665e",
    19: "b3b3d1",
}

# --------------------------------------------------------------------------------------------------
# 2. Per-question metadata (human label + node kind for the diagram; the drill-down layer wiring).
#    These are the only hand-maintained semantics; they change only when a NEW question is introduced.
# --------------------------------------------------------------------------------------------------
Q_LABEL = {
    "forest_2020": "Forest in 2020?<br/><small>(treecover / primary / regen / planted)</small>",
    "other_land_2020": "Other land use in 2020?<br/><small>(built / water / mining / bare)</small>",
    "other_land_2025": "Became / still other land?<br/><small>(2025)</small>",
    "agriculture_2020": "Agriculture or commodity in 2020?",
    "agriculture_2025": "Agriculture after 2020?<br/><small>(deforestation)</small>",
    "plantation_2020": "Plantation in 2020?",
    "plantation_presence_2025": "Still a plantation in 2025?<br/><small>(presence)</small>",
    "primary_2020": "Primary forest in 2020?",
    "plantation_2025": "New plantation after 2020?<br/><small>(degradation)</small>",
    "primary_2025": "Still primary in 2025?",
    "regen_planted_2020": "Regenerating / planted forest in 2020?",
    "disturbance_2025": "Disturbance after 2020?<br/><small>(logging / loss / degradation)</small>",
}
Q_KIND = {
    "forest_2020": "forest",
    "other_land_2020": "landuse",
    "other_land_2025": "landuse",
    "agriculture_2020": "landuse",
    "agriculture_2025": "landuse",
    "plantation_2020": "forest",
    "plantation_presence_2025": "forest",
    "primary_2020": "forest",
    "plantation_2025": "forest",
    "primary_2025": "forest",
    "regen_planted_2020": "forest",
    "disturbance_2025": "forest",
}

# Short, stable id stems per question (for Mermaid node ids + the drill-down mapping).
_ABBREV = {
    "forest_2020": "FOREST2020",
    "other_land_2020": "OL2020",
    "other_land_2025": "OL2025",
    "agriculture_2020": "AG2020",
    "agriculture_2025": "AG2025",
    "plantation_2020": "PL2020",
    "plantation_presence_2025": "PLpres2025",
    "primary_2020": "PR2020",
    "plantation_2025": "PL2025",
    "primary_2025": "PRstill2025",
    "regen_planted_2020": "RP2020",
    "disturbance_2025": "DIST2025",
}
# Questions that appear more than once get a branch suffix so each occurrence is a distinct node.
_REPEATED = {"other_land_2025", "plantation_2025"}
# Descending into the .yes of one of these sets the branch tag for everything below it.
_BRANCH_ON = {
    "other_land_2020": "nf",
    "plantation_2020": "pl",
    "primary_2020": "pr",
    "regen_planted_2020": "rp",
}
# The forest-half "unclassified" branch carries no forest-class tag, so a repeated question reached there
# falls back to this tag ("fh" = forest half).
_DEFAULT_BRANCH_TAG = "fh"

# The per-question map layer for the tree's drill-down (click a node -> show its data layer).
_Q_TO_LAYER = {
    "forest_2020": "agree_forest",  # the root gate: treecover OR primary OR regen OR planted
    "other_land_2020": "agree_other_land",
    # default for the forest-half branches (gain, not presence); the non-forest branch overrides it below
    "other_land_2025": "gain_other_land_gain",
    "agriculture_2020": [
        "agree_ag_whole",
        "agree_cropland",
        "agree_treecrop",
        "agree_pasture",
    ],
    "plantation_2020": "agree_plantation",
    "regen_planted_2020": "agree_regen",
    "primary_2020": "agree_primary",
    "agriculture_2025": [
        "gain_ag_whole_gain",
        "gain_cropland_gain",
        "gain_treecrop_gain",
        "gain_pasture_gain",
    ],
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
    "DIST2025": "gain_regen_stayed_2025",  # regen stayed forest (absence of disturbance)
}

# The per-question -> Whisp result column(s) mapping, mirroring risk.py add_risk_timber_col's yes_locals.
# Used only to generate the JS walk (so the viewer popup answers each question the same way risk.py does).
Q_TO_COLUMNS = {
    "forest_2020": [
        "Ind_01_treecover",
        "Ind_05_primary_2020",
        "Ind_06_nat_reg_forest_2020",
        "Ind_07a_planted_2020",
    ],
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

# Mermaid classDef block: styling for the timber node kinds (forest / landuse) plus the shared risk
# classes (low / more / high) and the 2020 / 2025 question stroke styles. Passed to the spec so the
# generic emitter hard-codes no styling.
_CLASSDEFS = """
  classDef forest fill:#bbf7d0,stroke:#16a34a,color:#064e3b;
  classDef landuse fill:#fff7ed,stroke:#f97316,color:#7c2d12;
  classDef low fill:#16a34a,color:#fff,stroke:#166534;
  classDef more fill:#f08c00,color:#fff,stroke:#c56f00;
  classDef high fill:#dc2626,color:#fff,stroke:#b91c1c;
  classDef q2020 stroke-width:3px;
  classDef q2025 stroke-width:3px,stroke-dasharray:4 3;
"""

# --------------------------------------------------------------------------------------------------
# 3. The timber spec: bundle every timber-specific piece so the generic decision_tree functions can
#    drive the timber tree.  A future PCROP_SPEC / ACROP_SPEC is another DecisionTreeSpec built the same
#    way (its own tree + question->indicator-column mapping + code map + palette).
# --------------------------------------------------------------------------------------------------
TIMBER_SPEC = DecisionTreeSpec(
    tree=TIMBER_ROOT_TREE,
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
    default_branch_tag=_DEFAULT_BRANCH_TAG,
    js_source_module="openforis_whisp.timber_tree_export",
    js_tree_const="TIMBER_TREE",
    js_answer_ref="risk.py add_risk_timber_col",
    js_story_fn="timberStory",
)


# --------------------------------------------------------------------------------------------------
# 4. Public API: the historical timber functions, now thin bindings of the generic core to TIMBER_SPEC.
#    Every name and call signature below is preserved (used across timber_map, the notebook viewer and
#    the temp_dev_notes/risk_maps scripts).
# --------------------------------------------------------------------------------------------------
def leaf_code(node):
    """Map code for a timber leaf node (raises KeyError if the tree grew a pathway not in PATHWAY_TO_CODE)."""
    return PATHWAY_TO_CODE[node["pathway"]]


# Private pre-order node iterator, kept as a module-level name for back-compat (walks an annotated tree).
_iter_nodes = _dt.iter_nodes


def annotate():
    """Return the timber tree annotated with a stable ``id`` on every node and ``code`` on every leaf."""
    return _dt.annotate(TIMBER_SPEC)


def code_names():
    """code -> its pathway label (the map legend). Only codes actually reached by the timber tree."""
    return _dt.code_names(TIMBER_SPEC)


def class_codes():
    """Risk class -> sorted list of its codes: {'low': [...], 'more': [...], 'high': [...]}."""
    return _dt.class_codes(TIMBER_SPEC)


def pathway_label_to_code():
    """The drawn-polygon colouring map: risk_timber_pathway label -> map code (a copy of PATHWAY_TO_CODE)."""
    return _dt.pathway_label_to_code(TIMBER_SPEC)


def node_ids():
    """Return (decision_ids, terminal_ids) for the timber tree, pre-order (for the viewer click-binding)."""
    return _dt.node_ids(TIMBER_SPEC)


def node_to_layer():
    """Mermaid node id -> drill-down map layer key(s) (per-question default + per-branch overrides)."""
    return _dt.node_to_layer(TIMBER_SPEC)


def to_mermaid():
    """Emit the ``flowchart TB`` Mermaid source for the timber tree (nodes, edges, styling)."""
    return _dt.to_mermaid(TIMBER_SPEC)


def eval_tree_ee(qimg, ee_image):
    """Build the per-pixel timber pathway-code image.

    ``qimg``: dict question-name -> ee boolean image (1 where the question is 'yes').
    ``ee_image``: the ``ee.Image`` constructor (passed in so this module need not import ee).
    """
    return _dt.eval_tree_ee(qimg, ee_image, TIMBER_SPEC)


def js_module_source():
    """Return the JS block: TIMBER_TREE (annotated JSON), qYes(p, q), and timberStory(p).

    ``timberStory`` walks TIMBER_TREE exactly like risk.py's _eval_timber_tree, recording each question
    it passes and the leg that fired, so the viewer popup is faithful by construction.
    """
    return _dt.js_module_source(TIMBER_SPEC)


def selfcheck():
    """Assert PATHWAY_TO_CODE is a bijection over the timber tree's leaf pathways; return the summary."""
    return _dt.selfcheck(TIMBER_SPEC)

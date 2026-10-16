"""
The release workflow copies "Changes to results" and "Breaking changes" lines from PR
descriptions into the release notes and CHANGELOG.md (.github/scripts/release_sections.py).
"""

import importlib.util
from pathlib import Path

SCRIPT = (
    Path(__file__).resolve().parents[2] / ".github" / "scripts" / "release_sections.py"
)
spec = importlib.util.spec_from_file_location("release_sections", SCRIPT)
rs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rs)


def test_sections_are_read_and_the_template_placeholders_ignored():
    body = """Fixes the palm cut-off.

Changes to results:
- Oil_palm_Descals: now only plantations with a planting year of 2020 or earlier

Breaking changes:
-
"""
    found = rs.parse_sections(body)
    assert found["Changes to results"] == [
        "- Oil_palm_Descals: now only plantations with a planting year of 2020 or earlier"
    ]
    assert found["Breaking changes"] == []


def test_headings_in_markdown_style_and_text_after_a_section():
    body = """## Changes to results:
- GLAD-L_before_2020: now includes 2018-2020 alerts where it was 0
Some later paragraph that is not part of it.
- not this line either
**Breaking changes:**
- Runs stop if any plot has no geometry
"""
    found = rs.parse_sections(body)
    assert found["Changes to results"] == [
        "- GLAD-L_before_2020: now includes 2018-2020 alerts where it was 0"
    ]
    assert found["Breaking changes"] == ["- Runs stop if any plot has no geometry"]


def test_no_body_and_no_sections():
    assert rs.parse_sections(None) == {"Changes to results": [], "Breaking changes": []}
    assert rs.build({287: rs.parse_sections("Just a description.")}) == ""


def test_pr_numbers_from_generated_notes_in_order_without_repeats():
    notes = (
        "- Fix A in https://github.com/o/r/pull/300\n"
        "- Fix B in #287\n"
        "- @x made their first contribution in https://github.com/o/r/pull/287\n"
    )
    assert rs.pr_numbers(notes) == [300, 287]


def test_build_merges_prs_in_order():
    per_pr = {
        300: {"Changes to results": ["- A"], "Breaking changes": []},
        287: {"Changes to results": ["- B"], "Breaking changes": ["- C"]},
    }
    assert rs.build(per_pr) == (
        "**Changes to results:**\n- A\n- B\n\n**Breaking changes:**\n- C"
    )

"""
Collect the "Changes to results:" and "Breaking changes:" lines from the PRs in a release.

Each PR description can carry those two headings (see .github/pull_request_template.md),
each followed by "- " lines. This script reads them from every PR listed in the generated
release notes and prints them as two sections, ready to go above "What's Changed".

Usage (in the release workflow, or locally with the gh CLI signed in):
    python release_sections.py <owner/repo> <file with the generated notes>
"""

import json
import re
import subprocess
import sys

SECTIONS = ["Changes to results", "Breaking changes"]


def parse_sections(body):
    """Return {section: [lines]} for the two headings in one PR description."""
    found = {name: [] for name in SECTIONS}
    current = None
    for raw in (body or "").splitlines():
        line = raw.strip()
        heading = re.sub(r"^[#*\s]+|[*\s]+$", "", line).rstrip(":").strip()
        if heading in SECTIONS:
            current = heading
            continue
        if current is None:
            continue
        if line.startswith("- ") and len(line) > 2:
            found[current].append(line)
        elif line and not line.startswith("<!--"):
            current = None  # any other text ends the section
    return found


def pr_numbers(notes):
    """PR numbers in the order they appear in the generated notes."""
    seen = []
    for n in re.findall(r"/pull/(\d+)|#(\d+)", notes):
        num = int(n[0] or n[1])
        if num not in seen:
            seen.append(num)
    return seen


def pr_body(repo, number):
    out = subprocess.run(
        ["gh", "api", f"repos/{repo}/pulls/{number}"],
        capture_output=True,
        text=True,
    )
    if out.returncode != 0:  # an issue number, or a PR from elsewhere
        return ""
    return json.loads(out.stdout).get("body") or ""


def build(per_pr):
    """Merge {pr_number: sections} into the markdown printed above 'What's Changed'."""
    parts = []
    for name in SECTIONS:
        lines = [line for sections in per_pr.values() for line in sections[name]]
        if lines:
            parts.append(f"**{name}:**\n" + "\n".join(lines))
    return "\n\n".join(parts)


def main():
    repo, notes_file = sys.argv[1], sys.argv[2]
    with open(notes_file, encoding="utf-8") as f:
        notes = f.read()
    per_pr = {n: parse_sections(pr_body(repo, n)) for n in pr_numbers(notes)}
    text = build(per_pr)
    if text:
        print(text)


if __name__ == "__main__":
    main()

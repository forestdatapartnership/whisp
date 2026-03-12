# PyPI Download Metrics — Interpretation Notes

> Analysis run: 2026-03-12, covering all data since first release (2025-03-04).
> Source: Google BigQuery `bigquery-public-data.pypi.file_downloads`

## Report headline: "Local use" vs "Wider use"
- **Local use** = Desktop installs + GitHub clones — people choosing to install or clone whisp on their own machines.
- **Wider use** = Colab/Linux + Countries — broader adoption signals from cloud notebooks and geographic reach.

## Key Findings

### 1. "Desktop Installs" (Windows + macOS) is the most reliable/unambigious metric for python package downloads
- These are real `pip install` events on real desktops/laptops.
- Each `pip install`, `pip install --upgrade`, or `pip install --pre` counts as one download.
- Does **not** include editable installs (`pip install -e .`) — those don't download from PyPI.
- Least inflated by automation, bots, or resolution noise.

### 2. Linux downloads are mostly Colab/cloud VMs
- **Distro breakdown**: Debian (1,011) + Ubuntu (977) dominate. Colab runs Debian; Ubuntu includes cloud VMs and some SEPAL users. The US-heavy distribution and Colab-default Python versions suggest most are cloud-based, though some may be genuine Linux desktop users.
- **Country**: 68% from US (1,468 of 2,147) — Colab VMs are hosted on US-based Google infrastructure, so the user's real location is hidden.
- **Python versions**: 3.10.12, 3.11.2, 3.12.12 — these are the default Python versions shipped with Colab and Debian, confirming the Colab/cloud origin.
- **CI (Continuous Integration)**: Only 3% (76) are flagged as CI — automated build/test systems that download packages as part of code testing pipelines, not real users in the classic sense. The rest are interactive or cloud usage.
- Also 32 Raspberry Pi installs and 19 Linux Mint (real Linux desktop users).

### 3. Double-counting (pip resolution downloads)
- pip sometimes downloads a package twice: once to resolve dependencies, once to install.
- For this package: ~48% of download events come in pairs (same minute, country, Python version).
- **Estimated real inflation: ~25-30%** (not all are doubled).
- The ~2,100 Linux pip total ≈ **1,400-1,600 actual install events**.
- This double-counting also affects Windows/macOS numbers, but proportionally the same.

### 4. "Other" column is noise
- OS = `null` in PyPI data — comes from mirrors, dependency resolvers, bots, and tools that don't report OS.
- **Poetry** (229 installs on Linux, no distro info) falls in this category — dependency resolution, not real installs.
- Should not be counted toward real usage.

### 5. Installer breakdown (Linux, all time)
| Installer | Downloads |
|-----------|-----------|
| pip       | 2,147     |
| poetry    | 229       |
| uv        | 11        |

### 6. What each column actually represents

| Column | What it measures | Reliability |
|--------|-----------------|-------------|
| Desktop Installs (Win + Mac) | Real desktop/laptop installs | **High** — best proxy for real users |
| Colab/Linux | Cloud VM + Colab + SEPAL + some real Linux | **Medium** — inflated by re-installs per session, ~25% double-counted |
| Other/Unknown | Mirrors, bots, dependency resolvers | **Low** — mostly noise |
| Countries | Unique countries downloading (excl. mirrors) | **High** — but Linux skews to US (Colab infrastructure) |
| GitHub Clones | Unique users pulling the repo | **High** — requires token, only 14-day retention |
| GitHub Stars | Current total stargazers (snapshot on latest month) | **High** — simple count |

### 7. Things that inflate all PyPI numbers
- **pip double-downloads**: ~25-30% inflation across all OS types.
- **Colab session re-installs**: Each notebook run = new install, same user.
- **Docker rebuilds**: Each container build = new download.
- **Dependency resolution**: Tools checking metadata without installing.
- **`--pre` flag**: Whisp currently uses pre-release versions (alpha/beta), so only users who explicitly `pip install --pre` are counted — this actually *deflates* numbers (casual users won't find it).

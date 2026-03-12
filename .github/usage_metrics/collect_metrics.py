"""Collect whisp usage metrics and update USAGE_STATS.md. Run weekly via GitHub Actions."""

import csv, json, os, sys, urllib.request, urllib.error
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

PACKAGE = "openforis-whisp"
OWNER, REPO = "forestdatapartnership", "whisp"
MONTH_NAMES = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def month_label(ym):
    """'2026-02' -> 'Feb 2026 (2026-02)'"""
    y, m = ym.split("-")
    return f"{MONTH_NAMES[int(m) - 1]} {y} ({ym})"


DIR = Path(__file__).resolve().parent
METRICS_CSV = DIR / "usage_metrics.csv"
TRAFFIC_CSV = DIR / "github_traffic.csv"
COUNTRY_CSV = DIR / "country_downloads.csv"
REPORT_MD = DIR.parent / "USAGE_STATS.md"


def get(url):
    headers = {"User-Agent": "whisp-metrics/1.0"}
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  WARN: {url.split('/')[-1]}: {e}", file=sys.stderr)
        return None


def collect():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = {"date": today}

    # PyPI downloads by OS (30-day)
    d = get(f"https://pypistats.org/api/packages/{PACKAGE}/system?period=month")
    for os_name in ("Windows", "Darwin", "Linux"):
        row[os_name] = sum(
            r["downloads"]
            for r in (d or {}).get("data", [])
            if r["category"] == os_name
        )
    row["other"] = sum(
        r["downloads"] for r in (d or {}).get("data", []) if r["category"] == "null"
    )
    row["total_all"] = sum(
        row.get(k, 0) or 0 for k in ("Windows", "Darwin", "Linux", "other")
    )

    # GitHub clones + views (14-day, needs token)
    for kind in ("clones", "views"):
        d = get(f"https://api.github.com/repos/{OWNER}/{REPO}/traffic/{kind}")
        if d and kind in d:
            row[f"{kind}_unique"] = d["uniques"]
            for entry in d[kind]:
                date = entry["timestamp"][:10]
                _append_traffic(date, kind, entry["count"], entry["uniques"])
        else:
            row[f"{kind}_unique"] = None

    # Append to metrics CSV
    fields = [
        "date",
        "Windows",
        "Darwin",
        "Linux",
        "other",
        "total_all",
        "clones_unique",
        "views_unique",
    ]
    write_hdr = not METRICS_CSV.exists()
    with open(METRICS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_hdr:
            w.writeheader()
        w.writerow(row)

    # BigQuery country downloads (last 30 days, needs GCP_SA_KEY)
    collect_countries()

    return row


def collect_countries():
    """Query BigQuery for country-level downloads (last 30 days). Skips if no credentials or already fresh."""
    sa_key = os.environ.get("GCP_SA_KEY")
    if not sa_key:
        print("  SKIP: country query (no GCP_SA_KEY)", file=sys.stderr)
        return

    # Skip if we already have data for the current month (avoids burning BigQuery quota)
    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    if COUNTRY_CSV.exists():
        with open(COUNTRY_CSV, encoding="utf-8") as f:
            existing_months = {r["month"] for r in csv.DictReader(f)}
        if current_month in existing_months:
            print(
                f"  SKIP: country data already exists for {current_month}",
                file=sys.stderr,
            )
            return

    try:
        from google.cloud import bigquery
        from google.oauth2 import service_account
    except ImportError:
        print(
            "  SKIP: country query (google-cloud-bigquery not installed)",
            file=sys.stderr,
        )
        return

    info = json.loads(sa_key)
    creds = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=info["project_id"], credentials=creds)

    since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    query = f"""
        SELECT
            FORMAT_DATE('%Y-%m', DATE(timestamp)) as month,
            country_code,
            COUNT(*) as downloads
        FROM `bigquery-public-data.pypi.file_downloads`
        WHERE file.project = '{PACKAGE}'
          AND DATE(timestamp) >= '{since}'
          AND country_code IS NOT NULL
          AND details.installer.name NOT IN ('bandersnatch', 'z3c.pypimirror', 'Artifactory', 'devpi')
        GROUP BY month, country_code
        ORDER BY month, downloads DESC
    """
    print("  Querying BigQuery for country downloads...")
    results = list(client.query(query).result())

    # Read existing data to merge
    existing = {}
    if COUNTRY_CSV.exists():
        with open(COUNTRY_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                existing[(r["month"], r["country_code"])] = r

    # Update with new data (overwrites same month+country)
    for r in results:
        existing[(r.month, r.country_code)] = {
            "month": r.month,
            "country_code": r.country_code,
            "downloads": r.downloads,
        }

    # Write back
    fields = ["month", "country_code", "downloads"]
    with open(COUNTRY_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for key in sorted(existing):
            w.writerow(existing[key])

    n_countries = len({r.country_code for r in results})
    print(f"  Countries (last 30d): {n_countries}")


def _append_traffic(date, kind, count, uniques):
    """Append daily traffic row, skipping duplicates."""
    existing = set()
    if TRAFFIC_CSV.exists():
        with open(TRAFFIC_CSV, encoding="utf-8") as f:
            existing = {(r["date"], r["kind"]) for r in csv.DictReader(f)}
    if (date, kind) in existing:
        return
    fields = ["date", "kind", "count", "uniques"]
    write_hdr = not TRAFFIC_CSV.exists()
    with open(TRAFFIC_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_hdr:
            w.writeheader()
        w.writerow({"date": date, "kind": kind, "count": count, "uniques": uniques})


def generate_report():
    """Build USAGE_STATS.md from the accumulated CSVs."""
    # Read metrics
    rows = []
    if METRICS_CSV.exists():
        with open(METRICS_CSV, encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

    # Aggregate clones by month from traffic CSV
    clones_monthly = {}
    if TRAFFIC_CSV.exists():
        with open(TRAFFIC_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["kind"] == "clones":
                    month = r["date"][:7]
                    clones_monthly[month] = clones_monthly.get(month, 0) + int(
                        r["uniques"]
                    )

    # Pick latest metrics row per month
    by_month = {}
    for r in rows:
        by_month[r["date"][:7]] = r

    # Country data
    country_by_month = defaultdict(lambda: {"countries": set(), "total": 0})
    country_totals = defaultdict(int)
    if COUNTRY_CSV.exists():
        with open(COUNTRY_CSV, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                country_by_month[r["month"]]["countries"].add(r["country_code"])
                country_by_month[r["month"]]["total"] += int(r["downloads"])
                country_totals[r["country_code"]] += int(r["downloads"])

    # Version history from PyPI
    version_by_month = {}
    try:
        req = urllib.request.Request(
            f"https://pypi.org/pypi/{PACKAGE}/json",
            headers={"User-Agent": "whisp-metrics/1.0"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            pypi = json.loads(resp.read())
        all_versions = []
        for v, files in pypi["releases"].items():
            if files:
                all_versions.append((v, files[0]["upload_time"][:10]))
        all_versions.sort(key=lambda x: x[1])
        # Map each month to the latest version released that month
        for v, d in all_versions:
            month = d[:7]
            version_by_month[month] = v
    except Exception as e:
        print(f"  WARN: could not fetch version history: {e}", file=sys.stderr)

    all_months = sorted(
        set(by_month)
        | set(clones_monthly)
        | set(country_by_month)
        | set(version_by_month),
        reverse=True,
    )

    # GitHub stars (snapshot)
    stars = None
    d = get(f"https://api.github.com/repos/{OWNER}/{REPO}")
    if d:
        stars = d.get("stargazers_count")

    # --- Headline stats (latest month with actual data) ---
    latest_month = None
    headline_installs = headline_clones = None
    headline_linux = headline_countries = None
    for month in all_months:
        m = by_month.get(month, {})
        win = int(m.get("Windows") or 0)
        mac = int(m.get("Darwin") or 0)
        linux = int(m.get("Linux") or 0)
        if win or mac or month in clones_monthly:
            latest_month = month
            if win or mac:
                headline_installs = win + mac
            if month in clones_monthly:
                headline_clones = clones_monthly[month]
            if linux:
                headline_linux = linux
            cm = country_by_month.get(month)
            if cm:
                headline_countries = len(cm["countries"])
            break

    lines = [
        "# Whisp Usage Stats",
        "",
        "> Auto-updated weekly by [GitHub Actions](workflows/collect_usage_metrics.yml).",
        "",
    ]

    # Headline box
    if latest_month:
        lines.append(f"#### {month_label(latest_month)}")
        lines.append("")
        local_parts = []
        if headline_installs is not None:
            local_parts.append(f"Desktop installs: **{headline_installs}**")
        if headline_clones is not None:
            local_parts.append(f"GitHub clones: **{headline_clones}**")
        else:
            local_parts.append("GitHub clones: **—**")
        lines.append("**Local use** — " + " · ".join(local_parts))
        lines.append("")
        wider_parts = []
        if headline_linux is not None:
            wider_parts.append(f"Colab/Linux: **{headline_linux}**")
        if headline_countries is not None:
            wider_parts.append(f"Countries: **{headline_countries}**")
        if wider_parts:
            lines.append("**Wider use** — " + " · ".join(wider_parts))
            lines.append("")

    # --- Monthly table ---
    lines += [
        "### Monthly Breakdown",
        "",
        "| Month | PyPI: Desktop (Win+Mac) | PyPI: Colab/Linux | PyPI: Other/Unknown | PyPI: Countries | GitHub: Clones | GitHub: Stars | Release |",
        "|-------|------------------------:|------------------:|--------------------:|----------------:|---------------:|--------------:|---------|",
    ]
    for month in all_months:
        m = by_month.get(month, {})
        win = int(m.get("Windows") or 0)
        mac = int(m.get("Darwin") or 0)
        linux = int(m.get("Linux") or 0)
        other = int(m.get("other") or 0)
        active = str(win + mac) if (win or mac) else "\u2014"
        colab = str(linux) if linux else "\u2014"
        other_str = str(other) if other else "\u2014"
        cm = country_by_month.get(month)
        countries = str(len(cm["countries"])) if cm else "\u2014"
        clones = str(clones_monthly[month]) if month in clones_monthly else "\u2014"
        version = version_by_month.get(month, "")
        stars_str = str(stars) if (stars is not None and month == all_months[0]) else ""
        lines.append(
            f"| {month} | {active} | {colab} | {other_str} | {countries} | {clones} | {stars_str} | {version} |"
        )

    # Top countries — latest month with data + all time
    if country_totals:
        # Find latest month with country data
        latest_country_month = None
        for month in all_months:
            if month in country_by_month:
                latest_country_month = month
                break

        if latest_country_month:
            # Read per-country data for that month
            month_country_totals = defaultdict(int)
            if COUNTRY_CSV.exists():
                with open(COUNTRY_CSV, encoding="utf-8") as f:
                    for r in csv.DictReader(f):
                        if r["month"] == latest_country_month:
                            month_country_totals[r["country_code"]] += int(
                                r["downloads"]
                            )
            if month_country_totals:
                top_month = sorted(month_country_totals.items(), key=lambda x: -x[1])[
                    :10
                ]
                lines += [
                    "",
                    f"### Top Countries — {month_label(latest_country_month)}",
                    "",
                    "| Country | Downloads |",
                    "|---------|----------:|",
                ]
                for cc, dl in top_month:
                    lines.append(f"| {cc} | {dl:,} |")

        top = sorted(country_totals.items(), key=lambda x: -x[1])[:15]
        lines += [
            "",
            "### Top Countries — All Time",
            "",
            "| Country | Downloads |",
            "|---------|----------:|",
        ]
        for cc, dl in top:
            lines.append(f"| {cc} | {dl:,} |")

    lines += [
        "",
        "---",
        "",
        "**PyPI columns** = pip install downloads from [pypistats.org](https://pypistats.org). "
        "Desktop = Windows + macOS installs. "
        "Colab/Linux = Linux installs (mostly Colab/SEPAL notebooks). "
        "Other/Unknown = OS not reported (mirrors, bots, dependency resolvers). "
        "Countries = unique countries downloading (via BigQuery, excl. mirrors).",
        "",
        "**GitHub columns** = data from GitHub Traffic API (14-day rolling window). "
        "Clones = unique users who cloned the repo. "
        "Stars = current total (snapshot, shown on latest month only).",
        "",
    ]

    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"Report: {REPORT_MD}")


if __name__ == "__main__":
    row = collect()
    generate_report()
    w, m, t = row["Windows"], row["Darwin"], row["total_all"]
    c = row["clones_unique"]
    print(f"PyPI 30d: Active(Win+Mac)={w+m}  Total(all)={t} | Clones 14d unique: {c}")

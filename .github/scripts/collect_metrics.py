"""Collect whisp usage metrics and update USAGE_STATS.md. Run weekly via GitHub Actions."""

import csv, json, os, sys, urllib.request, urllib.error
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

PACKAGE = "openforis-whisp"
OWNER, REPO = "forestdatapartnership", "whisp"
DIR = Path(__file__).resolve().parent
METRICS_CSV = DIR / "usage_metrics.csv"
TRAFFIC_CSV = DIR / "github_traffic.csv"
COUNTRY_CSV = DIR / "country_downloads.csv"
REPORT_MD = DIR / "USAGE_STATS.md"


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
    """Query BigQuery for country-level downloads (last 30 days). Skips if no credentials."""
    sa_key = os.environ.get("GCP_SA_KEY")
    if not sa_key:
        print("  SKIP: country query (no GCP_SA_KEY)", file=sys.stderr)
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

    all_months = sorted(
        set(by_month) | set(clones_monthly) | set(country_by_month), reverse=True
    )

    lines = [
        "# Whisp Usage Stats",
        "",
        "> Auto-updated weekly by [GitHub Actions](../workflows/collect_usage_metrics.yml).",
        "",
        "| Month | Active Use (Win + Mac) | Colab/Linux | Other | Countries | GitHub Clones (unique) |",
        "|-------|------------------------|-------------|-------|-----------|------------------------|",
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
        lines.append(
            f"| {month} | {active} | {colab} | {other_str} | {countries} | {clones} |"
        )

    # Top countries section
    if country_totals:
        top = sorted(country_totals.items(), key=lambda x: -x[1])[:15]
        lines += [
            "",
            "### Downloads by Country (all time)",
            "",
            "| Country | Downloads |",
            "|---------|----------|",
        ]
        for cc, dl in top:
            lines.append(f"| {cc} | {dl:,} |")

    lines += [
        "",
        "---",
        "**Active Use** = pip installs on Windows + macOS (people using the package locally on their machines). "
        "**Colab/Linux** = Linux pip installs (mostly Colab notebooks re-installing each session). "
        "**Other** = downloads where OS is unknown (mirrors, bots, dependency resolvers). "
        "**Countries** = unique countries downloading (via BigQuery, excl. mirrors). "
        "**Clones** = unique users pulling the repo.",
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

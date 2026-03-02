import ee

ee.Initialize(project="ee-andyarnellgee")
from google.auth.transport.requests import AuthorizedSession
from collections import defaultdict
import json

credentials = ee.data.get_persistent_credentials()
session = AuthorizedSession(credentials)

# List all objects in whisp_cogs/
prefix = "whisp_cogs/"
url = f"https://storage.googleapis.com/storage/v1/b/whisp_bucket/o?prefix={prefix}&maxResults=5000"
blobs = []
while url:
    resp = session.get(url)
    data = resp.json()
    for item in data.get("items", []):
        blobs.append((item["name"], int(item["size"])))
    token = data.get("nextPageToken")
    url = (
        f"https://storage.googleapis.com/storage/v1/b/whisp_bucket/o?prefix={prefix}&maxResults=5000&pageToken={token}"
        if token
        else None
    )

countries = defaultdict(lambda: {"count": 0, "size": 0})
for name, size in blobs:
    short = name.replace("whisp_cogs/", "")
    cp = short[:2] if len(short) > 2 else short
    countries[cp]["count"] += 1
    countries[cp]["size"] += size

print(f"Total files: {len(blobs)}")
total = sum(c["size"] for c in countries.values())
print(f"Total size: {total / 1e9:.2f} GB\n")
for cp in sorted(countries):
    c = countries[cp]
    print(f"  {cp}: {c['count']:3d} files, {c['size'] / 1e9:.2f} GB")

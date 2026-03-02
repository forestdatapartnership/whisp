"""Quick script to list CI per-admin COG files in GCS."""
import ee
import google.auth
import google.auth.transport.requests
import json
import urllib.request
import urllib.parse

ee.Initialize()

creds = ee.data.get_persistent_credentials()
creds.refresh(google.auth.transport.requests.Request())
token = creds.token

url = (
    "https://storage.googleapis.com/storage/v1/b/whisp_bucket/o?"
    + urllib.parse.urlencode({"prefix": "whisp_cogs/CI_gaul1_", "maxResults": 200})
)
req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
resp = urllib.request.urlopen(req)
data = json.loads(resp.read())

items = data.get("items", [])
total = 0
for item in sorted(items, key=lambda x: x["name"]):
    size = int(item["size"])
    total += size
    print(f"  {item['name']:70s}  {size / 1e6:8.1f} MB")

print(f"\n  Total: {len(items)} files, {total / 1e9:.2f} GB")

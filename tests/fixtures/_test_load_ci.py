"""Quick test: list_country_cog_uris discovers sharded files, load_country_cog mosaics them."""
import ee

ee.Initialize(opt_url="https://earthengine-highvolume.googleapis.com")

from openforis_whisp.export_cog import list_country_cog_uris, load_country_cog

# 1) Test URI discovery
uris = list_country_cog_uris(["CI"], date_str="20260223")
print(f"Found {len(uris)} COG files")
for u in uris[:5]:
    print(f"  {u.split('/')[-1]}")
print(f"  ... ({len(uris) - 5} more)")

# 2) Test mosaic loading
print("\nLoading mosaic...")
img = load_country_cog(["CI"], date_str="20260223")
bands = img.bandNames().getInfo()
print(f"Mosaic bands: {len(bands)}")
print(f"First 5: {bands[:5]}")

# 3) Quick reduceRegion test on a point in Yamoussoukro
pt = ee.Geometry.Point([-5.2767, 6.8276]).buffer(100)
result = img.reduceRegion(
    reducer=ee.Reducer.sum(),
    geometry=pt,
    scale=10,
    maxPixels=1e8,
).getInfo()
print(f"\nSample reduceRegion at Yamoussoukro (100m buffer):")
for k, v in list(result.items())[:6]:
    print(f"  {k}: {v}")
print("OK")

"""Quick test of load_country_cog and cog_to_hectares_image helpers."""
import sys, os, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import ee

ee.Initialize(project=None, opt_url="https://earthengine-highvolume.googleapis.com")

from openforis_whisp.export_cog import (
    load_country_cog,
    cog_to_hectares_image,
    list_country_cog_uris,
)

# Test list_country_cog_uris
uris = list_country_cog_uris(["CI"], date_str="20260223")
print(f"URIs for CI: {len(uris)}")
for u in uris[:3]:
    print(f"  {u}")
print(f"  ... ({len(uris) - 3} more)")
print()

# Test cog_to_hectares_image with single Yamoussoukro COG
print("Testing cog_to_hectares_image with Yamoussoukro...")
raw = ee.Image.loadGeoTIFF(
    "gs://whisp_bucket/whisp_cogs/gaul1_1210_i16_noyearly_10m_20260223.tif"
)
ha_img = cog_to_hectares_image(raw)
bands = ha_img.bandNames().getInfo()
print(f"  Bands: {len(bands)}, first 5: {bands[:5]}")
print()

# Quick stats test
from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

print("Running test_2features.geojson...")
t0 = time.time()
df = whisp_formatted_stats_geojson_to_df(
    "tests/fixtures/test_2features.geojson",
    mode="concurrent",
    whisp_image=ha_img,
)
elapsed = time.time() - t0
print(f"  Shape: {df.shape}, Time: {elapsed:.1f}s")
print(f"  Area: {df['Area'].tolist()}")
print(f"  GFC_TC_2020: {df['GFC_TC_2020'].tolist()}")
print(f"  In_waterbody: {df['In_waterbody'].tolist()}")

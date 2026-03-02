"""Quick smoke test: verify hybrid image builds and has expected bands."""
import ee
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
ee.Initialize(project="ee-andyarnellgee")

from src.openforis_whisp.export_cog import load_country_cog, load_country_cog_hybrid
from src.openforis_whisp.datasets import DYNAMIC_BAND_NAMES

ISO2 = ["CI"]
DATE_STR = "20260223"

print("--- Full COG ---")
t0 = time.time()
img_cog = load_country_cog(ISO2, date_str=DATE_STR)
cog_bands = img_cog.bandNames().getInfo()
print(f"  Bands: {len(cog_bands)} ({time.time()-t0:.1f}s)")

print("\n--- Hybrid ---")
t0 = time.time()
img_hybrid = load_country_cog_hybrid(ISO2, date_str=DATE_STR)
hybrid_bands = img_hybrid.bandNames().getInfo()
print(f"  Bands: {len(hybrid_bands)} ({time.time()-t0:.1f}s)")

# Check dynamic bands are present in hybrid
for db in DYNAMIC_BAND_NAMES:
    # Skip national bands not relevant to CI
    if db.startswith("nBR_"):
        continue
    in_cog = db in cog_bands
    in_hybrid = db in hybrid_bands
    print(f"  {db}: COG={'Y' if in_cog else 'N'} Hybrid={'Y' if in_hybrid else 'N'}")

# Band count comparison
print(f"\nCOG bands:    {len(cog_bands)}")
print(f"Hybrid bands: {len(hybrid_bands)}")
print(
    f"Expected:     same count (static from COG + dynamic from GEE replace stale COG ones)"
)

# List any bands in COG but not hybrid, and vice versa
cog_set = set(cog_bands)
hybrid_set = set(hybrid_bands)
only_cog = cog_set - hybrid_set
only_hybrid = hybrid_set - cog_set
if only_cog:
    print(f"\nOnly in COG: {sorted(only_cog)}")
if only_hybrid:
    print(f"\nOnly in Hybrid: {sorted(only_hybrid)}")

"""Benchmark: 25,000 features, 10-100ha, 20-250 vertices, COG-backed GEE."""
import sys, os, time, random, math

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

import ee

try:
    ee.Reset()
except Exception:
    pass
project = os.environ.get("PROJECT", None)
ee.Initialize(project=project, opt_url="https://earthengine-highvolume.googleapis.com")

import geopandas as gpd
import tempfile
from shapely.geometry import Polygon

COG_GCS = "gs://whisp_bucket/whisp_cogs/gaul1_1210_i16_noyearly_10m_20260223.tif"
AREA_SCALE_FACTOR = 10

# Yamoussoukro bbox
YAMA_BBOX = {"min_lon": -5.45, "max_lon": -5.15, "min_lat": 6.65, "max_lat": 7.05}


def generate_random_polygons(
    n, seed, min_ha=10, max_ha=100, min_verts=20, max_verts=250
):
    """Generate irregular polygons with realistic areas and vertex counts."""
    rng = random.Random(seed)
    # At ~6.8°N: 1° lon ≈ 110km, 1° lat ≈ 111km
    # 1 ha = 10000 m²; radius for circle of area A: r = sqrt(A/pi)
    # In degrees: r_deg = r_m / 111000 (approx)
    features = []
    for _ in range(n):
        cx = rng.uniform(YAMA_BBOX["min_lon"] + 0.02, YAMA_BBOX["max_lon"] - 0.02)
        cy = rng.uniform(YAMA_BBOX["min_lat"] + 0.02, YAMA_BBOX["max_lat"] - 0.02)

        # Target area in hectares → radius of equivalent circle in degrees
        target_ha = rng.uniform(min_ha, max_ha)
        radius_m = math.sqrt(target_ha * 10000 / math.pi)
        radius_deg = radius_m / 111000  # approx at equator, close enough for ~7°N

        # Number of vertices
        n_verts = rng.randint(min_verts, max_verts)

        # Generate irregular polygon by varying radius per vertex
        coords = []
        for i in range(n_verts):
            angle = 2 * math.pi * i / n_verts
            # Vary radius 50%-150% for irregular shape
            r = radius_deg * rng.uniform(0.5, 1.5)
            x = cx + r * math.cos(angle)
            y = cy + r * math.sin(angle)
            coords.append((x, y))
        coords.append(coords[0])  # close ring

        poly = Polygon(coords)
        if poly.is_valid:
            features.append(poly)
        else:
            # Make valid by buffering
            poly = poly.buffer(0)
            if not poly.is_empty:
                features.append(poly)

    return gpd.GeoDataFrame(geometry=features, crs="EPSG:4326")


def cog_to_hectares_image(cog_gcs_path):
    raw = ee.Image.loadGeoTIFF(cog_gcs_path)
    band_names = raw.bandNames()
    area_ha = raw.select("Area").divide(AREA_SCALE_FACTOR).toFloat()
    other_bands = band_names.remove("Area")

    def multiply_band(band_name):
        band_name = ee.String(band_name)
        band = raw.select(band_name).toFloat()
        is_admin = band_name.equals("admin_code")
        return ee.Algorithms.If(
            is_admin, band, band.multiply(area_ha).rename(band_name)
        )

    other_converted = other_bands.map(multiply_band)

    def add_band(img_to_add, accumulator):
        return ee.Image(accumulator).addBands(ee.Image(img_to_add))

    return ee.Image(other_converted.iterate(add_band, area_ha))


N = 25000
ts = int(time.time())
tmp_dir = tempfile.gettempdir()
gj_path = os.path.join(tmp_dir, "bench_25k.geojson")
print(f"Generating {N:,} irregular polygons (10-100ha, 20-250 verts, seed={ts})...")
t0 = time.time()
gdf = generate_random_polygons(N, seed=ts)
print(f"  Generated {len(gdf):,} valid polygons in {time.time() - t0:.1f}s")

# Show area distribution
areas = gdf.to_crs("EPSG:32630").geometry.area / 10000  # UTM zone 30N for CI
print(
    f"  Area stats: min={areas.min():.1f} ha, median={areas.median():.1f} ha, "
    f"max={areas.max():.1f} ha, mean={areas.mean():.1f} ha"
)
print(
    f"  Vertex stats: min={gdf.geometry.apply(lambda g: len(g.exterior.coords)).min()}, "
    f"max={gdf.geometry.apply(lambda g: len(g.exterior.coords)).max()}"
)

gdf.to_file(gj_path, driver="GeoJSON")
print(f"  Saved to {gj_path}")
print()

# --- Build COG-ha image ---
print("Building COG-ha image...")
t0 = time.time()
cog_ha_img = cog_to_hectares_image(COG_GCS)
print(f"  Built in {time.time() - t0:.1f}s")
print()

# --- Run stats ---
from openforis_whisp.stats import whisp_formatted_stats_geojson_to_df

print(f"Running COG-ha stats on {N:,} features...")
t0 = time.time()
try:
    df = whisp_formatted_stats_geojson_to_df(
        input_geojson_filepath=gj_path,
        mode="concurrent",
        whisp_image=cog_ha_img,
    )
    elapsed = time.time() - t0
    print(f"  Shape: {df.shape}")
    print(f"  Time: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"  Per feature: {elapsed / len(df):.4f}s")
    print(f"  Features/second: {len(df) / elapsed:.0f}")
    print()

    # Summary stats
    key_cols = [
        "Area",
        "GFC_TC_2020",
        "Forest_FDaP",
        "ESA_TC_2020",
        "Cocoa_FDaP",
        "Cocoa_ETH",
        "GFC_loss_before_2020",
        "GFC_loss_after_2020",
        "MODIS_fire_before_2020",
    ]
    avail = [c for c in key_cols if c in df.columns]
    print("Summary statistics:")
    print(df[avail].describe().round(2).to_string())
    print()
    print(
        f"In_waterbody: {df['In_waterbody'].sum()} True of {len(df)} ({df['In_waterbody'].mean()*100:.1f}%)"
    )
    print()
    print("Sample (first 3):")
    print(df[avail].head(3).to_string())
except Exception as e:
    print(f"  FAILED: {e}")
    import traceback

    traceback.print_exc()

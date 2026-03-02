"""
Benchmark chunk_size with:
  A) Small feature counts (5, 10, 25, 50) — chunk 5 vs 10 vs 25
  B) Large complex features (10-100 ha, 20-200 vertices) — chunk 10 vs 25 vs 50

All features placed within Côte d'Ivoire for the CI COG.
"""
import sys, os, time, math, random, json, tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

# ── Polygon generator ────────────────────────────────────────────────────────
def _random_polygon(center_lon, center_lat, area_ha, n_vertices, seed=None):
    """Generate a random irregular polygon with given area and vertex count.

    Uses a star-polygon approach: random angles sorted, random radii jittered
    around a base radius calculated from the target area.
    """
    rng = random.Random(seed)

    # Base radius in degrees (approx) for desired hectare area
    # 1° lat ≈ 111 km, 1° lon ≈ 111*cos(lat) km
    # area_m2 = area_ha * 10000
    # For a circle: area = pi * r^2  =>  r_m = sqrt(area_m2 / pi)
    r_m = math.sqrt(area_ha * 10000 / math.pi)
    r_deg_lat = r_m / 111_000
    r_deg_lon = r_m / (111_000 * math.cos(math.radians(center_lat)))

    # Generate random angles, sorted
    angles = sorted([rng.uniform(0, 2 * math.pi) for _ in range(n_vertices)])

    # Jitter radii ±30 %
    coords = []
    for a in angles:
        jitter = rng.uniform(0.7, 1.3)
        lon = center_lon + r_deg_lon * jitter * math.cos(a)
        lat = center_lat + r_deg_lat * jitter * math.sin(a)
        coords.append([round(lon, 7), round(lat, 7)])

    # Close the ring
    coords.append(coords[0])
    return coords


def _generate_geojson(n_features, area_range_ha, vertex_range, seed=42):
    """Generate a CI GeoJSON with n_features random polygons."""
    rng = random.Random(seed)
    features = []
    for i in range(n_features):
        # Random center within Côte d'Ivoire (approx bounds)
        lon = rng.uniform(-7.5, -3.5)
        lat = rng.uniform(5.0, 9.5)
        area_ha = rng.uniform(*area_range_ha)
        n_verts = rng.randint(*vertex_range)
        ring = _random_polygon(lon, lat, area_ha, n_verts, seed=seed + i)
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "user_id": i + 1,
                    "target_ha": round(area_ha, 1),
                    "vertices": n_verts,
                },
                "geometry": {"type": "Polygon", "coordinates": [ring]},
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _write_geojson(gj_dict, label):
    """Write to temp dir, return path."""
    path = os.path.join(tempfile.gettempdir(), f"bench_{label}.geojson")
    with open(path, "w") as f:
        json.dump(gj_dict, f)
    return path


# ── Benchmark runner ─────────────────────────────────────────────────────────
def _run_test(path, chunk_size, iso2="CI", verbose=False):
    from openforis_whisp.export_cog import whisp_stats_cog_local

    t0 = time.time()
    df = whisp_stats_cog_local(
        input_geojson_filepath=path,
        iso2_codes=[iso2],
        chunk_size=chunk_size,
        verbose=verbose,
    )
    elapsed = time.time() - t0
    return len(df), elapsed


def _print_table(results, label):
    """results: list of (n_feat, chunk_size, elapsed)"""
    print(f"\n{'=' * 60}")
    print(f"  {label}")
    print(f"{'=' * 60}")
    print("  Feats  ChunkSz  Chunks    Time   s/feat  feat/s")
    print("  -----  -------  ------  ------  ------  ------")
    for n, cs, t in results:
        chunks = (n + cs - 1) // cs
        per = t / n if n else 0
        fps = n / t if t else 0
        print("  %5d  %7d  %6d  %5.1fs  %6.3f  %6.1f" % (n, cs, chunks, t, per, fps))


def main():
    import ee

    ee.Initialize(
        project="ee-whisp", opt_url="https://earthengine-highvolume.googleapis.com"
    )

    # ── Part A: Small feature counts with SMALL polygons (like original test) ──
    print("\n" + "=" * 70)
    print("  PART A: Small feature counts, small plots (~1-4 ha, ~4-10 vertices)")
    print("=" * 70)

    results_a = []
    for n_feat in [5, 10, 25, 50]:
        gj = _generate_geojson(
            n_feat, area_range_ha=(1, 4), vertex_range=(4, 10), seed=100
        )
        path = _write_geojson(gj, f"small_{n_feat}")
        for cs in [5, 10, 25]:
            if cs > n_feat:
                continue
            print(f"\n--- {n_feat} features, chunk_size={cs} ---")
            n, t = _run_test(path, cs)
            results_a.append((n, cs, t))

    _print_table(results_a, "Part A: Small plots, varying feature count & chunk_size")

    # ── Part B: Large complex polygons (10-100 ha, 20-200 vertices) ──────────
    print("\n" + "=" * 70)
    print("  PART B: Large complex features (10-100 ha, 20-200 vertices)")
    print("=" * 70)

    results_b = []
    for n_feat in [10, 25, 50]:
        gj = _generate_geojson(
            n_feat, area_range_ha=(10, 100), vertex_range=(20, 200), seed=200
        )
        path = _write_geojson(gj, f"complex_{n_feat}")
        for cs in [5, 10, 25, 50]:
            if cs > n_feat:
                continue
            print(f"\n--- {n_feat} complex features, chunk_size={cs} ---")
            n, t = _run_test(path, cs)
            results_b.append((n, cs, t))

    _print_table(results_b, "Part B: Large complex plots (10-100 ha, 20-200 verts)")

    # ── Part C: Mix of small and large ───────────────────────────────────────
    print("\n" + "=" * 70)
    print("  PART C: Mixed batch (25 small + 25 large/complex = 50 features)")
    print("=" * 70)

    gj_small = _generate_geojson(
        25, area_range_ha=(1, 4), vertex_range=(4, 10), seed=300
    )
    gj_large = _generate_geojson(
        25, area_range_ha=(10, 100), vertex_range=(20, 200), seed=400
    )
    # Merge
    gj_mixed = {
        "type": "FeatureCollection",
        "features": gj_small["features"] + gj_large["features"],
    }
    for i, f in enumerate(gj_mixed["features"]):
        f["properties"]["user_id"] = i + 1
    path_mixed = _write_geojson(gj_mixed, "mixed_50")

    results_c = []
    for cs in [5, 10, 25, 50]:
        print(f"\n--- 50 mixed features, chunk_size={cs} ---")
        n, t = _run_test(path_mixed, cs)
        results_c.append((n, cs, t))

    _print_table(results_c, "Part C: Mixed batch (25 small + 25 large/complex)")


if __name__ == "__main__":
    main()

"""Measure actual bytes transferred during a cog-local extraction."""
import sys, os, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


def main():
    import ee

    ee.Initialize(
        project="ee-andyarnellgee",
        opt_url="https://earthengine-highvolume.googleapis.com",
    )

    # Enable GDAL/CPL debug to count bytes — or use a simpler approach:
    # measure network traffic before/after via psutil
    try:
        import psutil
    except ImportError:
        print("pip install psutil first")
        return

    from openforis_whisp.export_cog import whisp_stats_cog_local
    import tempfile, json

    # Generate 250 small CI features
    gj_path = os.path.join(tempfile.gettempdir(), "bench_250.geojson")
    if not os.path.exists(gj_path):
        print("ERROR: need bench_250.geojson — run the 250-feature test first")
        return

    # Get network counters before
    net_before = psutil.net_io_counters()

    t0 = time.time()
    df = whisp_stats_cog_local(
        input_geojson_filepath=gj_path,
        iso2_codes=["CI"],
        chunk_size=10,
        verbose=False,
    )
    elapsed = time.time() - t0

    # Get network counters after
    net_after = psutil.net_io_counters()

    bytes_sent = net_after.bytes_sent - net_before.bytes_sent
    bytes_recv = net_after.bytes_recv - net_before.bytes_recv

    print(f"\n250 features extracted in {elapsed:.1f}s")
    print(f"  Data received: {bytes_recv / 1e6:.1f} MB")
    print(f"  Data sent:     {bytes_sent / 1e6:.1f} MB")
    print(f"  Total transfer: {(bytes_sent + bytes_recv) / 1e6:.1f} MB")
    print(f"  Per feature:   {bytes_recv / 250 / 1e3:.1f} KB received")
    print(f"  Egress cost (US→EU): ${bytes_recv / 1e9 * 0.12:.4f}")


if __name__ == "__main__":
    main()

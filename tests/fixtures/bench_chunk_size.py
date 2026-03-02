"""Quick test: vary chunk_size for cog-local on 250 CI features."""
import sys, os, time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))


def main():
    import ee
    import tempfile

    ee.Initialize(
        project="ee-whisp", opt_url="https://earthengine-highvolume.googleapis.com"
    )
    from openforis_whisp.export_cog import whisp_stats_cog_local

    gj = os.path.join(tempfile.gettempdir(), "bench_250.geojson")
    if not os.path.exists(gj):
        print("ERROR: run 250-feature test first to generate bench_250.geojson")
        return

    print("250 features, varying chunk_size\n")

    results = []
    for cs in [5, 10, 25, 50, 125, 250]:
        print("--- chunk_size=%d ---" % cs)
        t0 = time.time()
        df = whisp_stats_cog_local(
            input_geojson_filepath=gj,
            iso2_codes=["CI"],
            chunk_size=cs,
            verbose=False,
        )
        elapsed = time.time() - t0
        n = len(df)
        chunks = (n + cs - 1) // cs
        print(
            "  %d chunks, %.1fs (%.3fs/feat, %d feat/s)"
            % (chunks, elapsed, elapsed / n, n / elapsed)
        )
        results.append((cs, chunks, elapsed, n))

    print("\nChunkSz  Chunks    Time   s/feat  feat/s")
    print("-------  ------  ------  ------  ------")
    for cs, ch, t, n in results:
        print("%7d  %6d  %5.1fs  %6.3f  %6.0f" % (cs, ch, t, t / n, n / t))


if __name__ == "__main__":
    main()

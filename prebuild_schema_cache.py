#!/usr/bin/env python3
"""
Pre-build WHISP schema cache.

This script pre-builds the schema cache to avoid the initial 2-5 second
delay when the library is first used.

⚠️  CRITICAL FOR APIs: Always pre-build a UNIVERSAL cache (no national_codes filter)
   to prevent schema rebuilds when handling requests with different country filters.

Useful for:
- API/production deployments (REQUIRED)
- Docker image builds
- Lambda deployment initialization
- CI/CD pipelines
- Application startup

Usage:
    # For APIs/Production (RECOMMENDED):
    python prebuild_schema_cache.py

    # For specific country filtering (ADVANCED):
    python prebuild_schema_cache.py --national-codes br,co,id

Examples:
    # Pre-build universal cache (works for all country filters)
    python prebuild_schema_cache.py

    # Pre-build for specific countries only
    python prebuild_schema_cache.py --national-codes br,co,id

    # Use in Dockerfile (RECOMMENDED)
    RUN python prebuild_schema_cache.py

    # Use in CI/CD
    - run: python prebuild_schema_cache.py

    # Clear old cache first
    python prebuild_schema_cache.py --clear-first
"""

import argparse
import sys
import time
from pathlib import Path

# Add src to path for development
src_path = Path(__file__).parent / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))

import openforis_whisp as whisp


def prebuild_cache(national_codes=None):
    """Pre-build schema cache."""

    print("=" * 60)
    print("WHISP Schema Cache Pre-builder")
    print("=" * 60)

    # Check existing cache
    cache_info = whisp.get_schema_cache_info()
    print(f"\n📊 Current cache status:")
    print(f"   Directory: {cache_info['cache_dir']}")
    print(f"   Exists: {cache_info['exists']}")
    print(f"   Cached schemas: {cache_info['num_files']}")

    if cache_info["num_files"] > 0:
        print(f"   Total size: {cache_info['total_size_mb']:.3f} MB")

    # Build cache
    print(f"\n🔨 Building schema cache...")
    if national_codes:
        print(f"   National codes filter: {national_codes}")
    else:
        print(f"   No national codes filter (all countries)")

    start_time = time.time()

    try:
        schema = whisp.load_schema_if_any_file_changed(national_codes=national_codes)
        elapsed_ms = (time.time() - start_time) * 1000

        print(f"\n✅ Schema cache built successfully!")
        print(f"   Time taken: {elapsed_ms:.1f} ms")
        print(f"   Schema columns: {len(schema.columns)}")

        # Show updated cache info
        cache_info_after = whisp.get_schema_cache_info()
        print(f"\n📊 Updated cache status:")
        print(f"   Cached schemas: {cache_info_after['num_files']}")
        print(f"   Total size: {cache_info_after['total_size_mb']:.3f} MB")

        return 0

    except Exception as e:
        print(f"\n❌ Failed to build schema cache:")
        print(f"   Error: {e}")
        return 1


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Pre-build WHISP schema cache for faster startup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--national-codes",
        type=str,
        help="Comma-separated list of ISO2 country codes (e.g., br,co,id). "
        "WARNING: For APIs, use without this flag to create a universal cache that works for all requests.",
        default=None,
    )

    parser.add_argument(
        "--clear-first",
        action="store_true",
        help="Clear existing cache before rebuilding",
    )

    parser.add_argument(
        "--api-mode",
        action="store_true",
        help="API mode: Ensures universal cache is built (ignores --national-codes)",
    )

    args = parser.parse_args()

    # Parse national codes
    national_codes = None
    if args.national_codes:
        national_codes = [
            code.strip().lower() for code in args.national_codes.split(",")
        ]

        # Warn if using filtered cache for what might be an API
        if not args.api_mode:
            print(
                "\n⚠️  WARNING: You're building a FILTERED cache (specific countries only)"
            )
            print("   For APIs/production, build a UNIVERSAL cache instead:")
            print("   python prebuild_schema_cache.py  (no --national-codes flag)")
            print("   ")
            print(
                "   Filtered caches cause rebuilds when different country filters are used."
            )
            print("   Press Ctrl+C to cancel, or wait 5 seconds to continue...")
            import time

            time.sleep(5)

    # API mode forces universal cache
    if args.api_mode:
        if national_codes:
            print(
                "\n💡 API mode: Ignoring --national-codes and building universal cache"
            )
        national_codes = None

    # Clear cache if requested
    if args.clear_first:
        print("\n🗑️  Clearing existing cache...")
        removed = whisp.clear_schema_cache()
        print(f"   Removed {removed} cached schema(s)")

    # Build cache
    return prebuild_cache(national_codes=national_codes)


if __name__ == "__main__":
    sys.exit(main())

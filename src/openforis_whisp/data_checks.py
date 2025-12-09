"""
Data validation and constraint checking functions for WHISP.

Provides validation functions to check GeoJSON data against user defined limits
and thresholds, raising informative errors when constraints are violated.
Note: Defaults in each function are not necessarily enforced.
"""

import json
from pathlib import Path
from shapely.geometry import Polygon as ShapelyPolygon, shape as shapely_shape

# Note: area summary stats are estimations for use in deciding pathways for analysis
# (estimation preferred here as allows efficient processing speed and limits overhead of checking file)


def _estimate_area_from_bounds(coords, area_conversion_factor: float) -> float:
    """
    Estimate area from bounding box when actual area calculation fails.
    Extracts bounding box and calculates its area as a fallback estimate.
    Returns area in hectares.
    """
    try:
        # Flatten all coordinates to find bounds
        all_coords = []

        def flatten_coords(c):
            if isinstance(c[0], (list, tuple)) and isinstance(c[0][0], (list, tuple)):
                for sub in c:
                    flatten_coords(sub)
            else:
                all_coords.extend(c)

        flatten_coords(coords)
        if not all_coords:
            return 0

        # Extract lon/lat values
        lons = [c[0] for c in all_coords]
        lats = [c[1] for c in all_coords]

        min_lon, max_lon = min(lons), max(lons)
        min_lat, max_lat = min(lats), max(lats)

        # Bounding box area
        bbox_area = (max_lon - min_lon) * (max_lat - min_lat)

        # Apply conversion factor
        return abs(bbox_area) * area_conversion_factor
    except:
        return 0


def analyze_geojson(
    geojson_data: Path | str | dict,
    metrics=[
        "count",
        "geometry_types",
        "crs",
        "file_size_mb",
        "min_area_ha",
        "mean_area_ha",
        "median_area_ha",
        "max_area_ha",
        "area_percentiles",
        "min_vertices",
        "mean_vertices",
        "median_vertices",
        "max_vertices",
        "vertex_percentiles",
    ],
):
    """
    Analyze GeoJSON polygons with selectable metrics for method selection.

    Fast lightweight analysis - only computes requested metrics.
    Works with or without area_ha property in features.
    All metrics computed in a single sweep through the data for efficiency.

    Warning: area metrics are estimations using EPSG:4326 - accuracy at equator only (extreme differences towards poles)

    Parameters:
    -----------
    geojson_data : Path | str | dict
        GeoJSON FeatureCollection. Can be:
        - dict: GeoJSON FeatureCollection dictionary
        - str: Path to GeoJSON file as string
        - Path: pathlib.Path to GeoJSON file
    metrics : list
        Which metrics to return. Available metrics:
        - 'count': number of polygons
        - 'geometry_types': dict of geometry type counts (e.g., {'Polygon': 95, 'MultiPolygon': 5})
        - 'crs': coordinate reference system (e.g., 'EPSG:4326') - only available when geojson_data is a file path
        - 'file_size_mb': file size in megabytes (only available when geojson_data is a file path)
        - 'min_area_ha', 'mean_area_ha', 'median_area_ha', 'max_area_ha': area statistics (hectares) (accurate only at equator)
        - 'area_percentiles': dict with p25, p50 (median), p75, p90 area values (accurate only at equator)
        - 'min_vertices', 'mean_vertices', 'median_vertices', 'max_vertices': vertex count statistics
        - 'vertex_percentiles': dict with p25, p50 (median), p75, p90 vertex count values

        Default includes all metrics for comprehensive analysis.
        Examples:
          - ['count'] -> just polygon count
          - ['count', 'mean_area_ha', 'max_area_ha'] -> subset of metrics
          - Default: all metrics for full statistical summary

    Returns:
    --------
    dict with requested metrics:
        - 'count': number of polygons
        - 'geometry_types': {'Polygon': int, 'MultiPolygon': int, ...}
        - 'crs': coordinate reference system string (e.g., 'EPSG:4326', only when geojson_data is a file path)
        - 'file_size_mb': file size in megabytes (float, only when geojson_data is a file path)
        - 'min_area_ha': minimum area among all polygons in hectares
        - 'mean_area_ha': mean area per polygon in hectares (calculated from coordinates)
        - 'median_area_ha': median area among all polygons in hectares
        - 'max_area_ha': maximum area among all polygons in hectares
        - 'area_percentiles': {'p25': float, 'p50': float, 'p75': float, 'p90': float}
        - 'min_vertices': minimum number of vertices among all polygons
        - 'mean_vertices': mean number of vertices per polygon
        - 'median_vertices': median number of vertices among all polygons
        - 'max_vertices': maximum number of vertices among all polygons
        - 'vertex_percentiles': {'p25': int, 'p50': int, 'p75': int, 'p90': int}
    """
    # Handle None metrics (use all default metrics)
    if metrics is None:
        metrics = [
            "count",
            "geometry_types",
            "crs",
            "file_size_mb",
            "min_area_ha",
            "mean_area_ha",
            "median_area_ha",
            "max_area_ha",
            "area_percentiles",
            "min_vertices",
            "mean_vertices",
            "median_vertices",
            "max_vertices",
            "vertex_percentiles",
        ]

    results = {}
    crs_warning = None
    detected_crs = None
    file_path = None

    try:
        # Load GeoJSON from file if path provided
        if isinstance(geojson_data, (str, Path)):
            file_path = Path(geojson_data)
            if not file_path.exists():
                raise FileNotFoundError(f"GeoJSON file not found: {file_path}")

            # Quick CRS detection BEFORE loading full file (if requested)
            if "crs" in metrics:
                try:
                    # Use fiona which only reads file metadata (fast, doesn't load features)
                    import fiona

                    with fiona.open(file_path) as src:
                        if src.crs:
                            # Convert fiona CRS dict to EPSG string
                            crs_dict = src.crs
                            if "init" in crs_dict:
                                # Old format: {'init': 'epsg:4326'}
                                detected_crs = (
                                    crs_dict["init"].upper().replace("EPSG:", "EPSG:")
                                )
                            elif isinstance(crs_dict, dict) and crs_dict:
                                # Try to extract EPSG from dict (json already imported at top)
                                detected_crs = json.dumps(crs_dict)
                        else:
                            # No CRS means WGS84 by GeoJSON spec
                            detected_crs = "EPSG:4326"

                    # Check if CRS is WGS84
                    if detected_crs and detected_crs != "EPSG:4326":
                        crs_warning = f"⚠️  CRS is {detected_crs}, not EPSG:4326. Area metrics will be inaccurate. Data will be auto-reprojected during processing."
                except Exception as e:
                    # If fiona fails, assume WGS84 (GeoJSON default)
                    detected_crs = "EPSG:4326"

            # Try UTF-8 first (most common), then fall back to auto-detection
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    geojson_data = json.load(f)
            except UnicodeDecodeError:
                # Auto-detect encoding if UTF-8 fails
                try:
                    import chardet

                    with open(file_path, "rb") as f:
                        raw_data = f.read()
                        detected = chardet.detect(raw_data)
                        encoding = detected.get("encoding", "latin-1")

                    with open(file_path, "r", encoding=encoding, errors="replace") as f:
                        geojson_data = json.load(f)
                except Exception:
                    # Final fallback: use latin-1 which accepts all byte values
                    with open(file_path, "r", encoding="latin-1") as f:
                        geojson_data = json.load(f)

        features = geojson_data.get("features", [])

        # Add file size if requested and available
        if "file_size_mb" in metrics and file_path is not None:
            size_bytes = file_path.stat().st_size
            results["file_size_mb"] = round(size_bytes / (1024 * 1024), 2)

        # Add CRS info if requested and detected
        if "crs" in metrics and detected_crs:
            results["crs"] = detected_crs
            # Add warning if not WGS84
            if crs_warning:
                results["crs_warning"] = crs_warning
                print(crs_warning)

        if "count" in metrics:
            results["count"] = len(features)

        # Initialize tracking variables (used in quality logging later)
        bbox_fallback_count = 0
        geometry_skip_count = 0
        polygon_type_stats = {}

        # Single sweep through features - compute all area/vertex metrics at once
        if any(
            m in metrics
            for m in [
                "geometry_types",
                "min_area_ha",
                "mean_area_ha",
                "median_area_ha",
                "max_area_ha",
                "area_percentiles",
                "min_vertices",
                "mean_vertices",
                "median_vertices",
                "max_vertices",
                "vertex_percentiles",
            ]
        ):
            areas = []
            vertices_list = []
            geometry_type_counts = {}
            valid_polygons = 0

            # Detect CRS to determine area conversion factor
            area_conversion_factor = 1232100  # Default: WGS84 (degrees to ha)
            detected_crs = None

            # Try to detect CRS from file if available
            if file_path:
                try:
                    import geopandas as gpd

                    gdf_temp = gpd.read_file(str(file_path))
                    detected_crs = gdf_temp.crs
                    if detected_crs and detected_crs != "EPSG:4326":
                        # Projected CRS typically uses meters, so convert m² to ha
                        # 1 ha = 10,000 m²
                        area_conversion_factor = 1 / 10000
                except Exception:
                    pass  # Use default if CRS detection fails

            for feature in features:
                try:
                    coords = feature["geometry"]["coordinates"]
                    geom_type = feature["geometry"]["type"]
                    properties = feature.get("properties", {})

                    # Count geometry types
                    geometry_type_counts[geom_type] = (
                        geometry_type_counts.get(geom_type, 0) + 1
                    )

                    if geom_type == "Polygon":
                        # Count vertices in this polygon
                        feature_vertices = 0
                        for ring in coords:
                            feature_vertices += len(ring)
                        vertices_list.append(feature_vertices)

                        # Calculate area from coordinates using shapely
                        try:
                            # Use shapely.geometry.shape to properly handle all geometry components
                            geom = shapely_shape(feature["geometry"])
                            # Convert using detected CRS
                            area_ha = abs(geom.area) * area_conversion_factor
                            areas.append(area_ha)
                        except Exception as e:
                            # Fallback: estimate from bounding box if geometry fails
                            bbox_area = _estimate_area_from_bounds(
                                coords, area_conversion_factor
                            )
                            if bbox_area > 0:
                                areas.append(bbox_area)
                                bbox_fallback_count += 1
                                polygon_type_stats["Polygon_bbox"] = (
                                    polygon_type_stats.get("Polygon_bbox", 0) + 1
                                )
                            else:
                                geometry_skip_count += 1
                                polygon_type_stats["Polygon_skipped"] = (
                                    polygon_type_stats.get("Polygon_skipped", 0) + 1
                                )
                        valid_polygons += 1

                    elif geom_type == "MultiPolygon":
                        # Count vertices in this multipolygon
                        feature_vertices = 0
                        for polygon in coords:
                            for ring in polygon:
                                feature_vertices += len(ring)
                        vertices_list.append(feature_vertices)

                        # Calculate area from coordinates using shapely
                        try:
                            # Use shapely.geometry.shape to properly handle MultiPolygon
                            geom = shapely_shape(feature["geometry"])
                            # Convert using detected CRS - use total area of all parts
                            area_ha = abs(geom.area) * area_conversion_factor
                            areas.append(area_ha)
                        except Exception as e:
                            # Fallback: estimate from bounding box if geometry fails
                            bbox_area = _estimate_area_from_bounds(
                                coords, area_conversion_factor
                            )
                            if bbox_area > 0:
                                areas.append(bbox_area)
                                bbox_fallback_count += 1
                                polygon_type_stats["MultiPolygon_bbox"] = (
                                    polygon_type_stats.get("MultiPolygon_bbox", 0) + 1
                                )
                            else:
                                geometry_skip_count += 1
                                polygon_type_stats["MultiPolygon_skipped"] = (
                                    polygon_type_stats.get("MultiPolygon_skipped", 0)
                                    + 1
                                )
                        valid_polygons += 1

                except:
                    continue

            # Calculate statistics and return requested metrics

            # Geometry type counts
            if "geometry_types" in metrics:
                results["geometry_types"] = geometry_type_counts

            if areas or vertices_list:
                # Area statistics
                if areas:
                    if "min_area_ha" in metrics:
                        results["min_area_ha"] = round(min(areas), 2)
                    if "mean_area_ha" in metrics:
                        results["mean_area_ha"] = round(sum(areas) / len(areas), 2)

                    sorted_areas = sorted(areas)  # Sort once for median and percentiles

                    if "median_area_ha" in metrics:
                        mid = len(sorted_areas) // 2
                        results["median_area_ha"] = round(
                            sorted_areas[mid]
                            if len(sorted_areas) % 2 == 1
                            else (sorted_areas[mid - 1] + sorted_areas[mid]) / 2,
                            2,
                        )
                    if "max_area_ha" in metrics:
                        results["max_area_ha"] = round(max(areas), 2)

                    if "area_percentiles" in metrics:
                        n = len(sorted_areas)
                        p25_idx = n // 4
                        p50_idx = n // 2
                        p75_idx = (n * 3) // 4
                        p90_idx = int(n * 0.9)

                        results["area_percentiles"] = {
                            "p25": round(sorted_areas[p25_idx], 2),
                            "p50": round(
                                sorted_areas[p50_idx]
                                if n % 2 == 1
                                else (sorted_areas[p50_idx - 1] + sorted_areas[p50_idx])
                                / 2,
                                2,
                            ),
                            "p75": round(sorted_areas[p75_idx], 2),
                            "p90": round(sorted_areas[p90_idx], 2),
                        }
                else:
                    # Return zeros for no areas
                    if "min_area_ha" in metrics:
                        results["min_area_ha"] = 0
                    if "mean_area_ha" in metrics:
                        results["mean_area_ha"] = 0
                    if "median_area_ha" in metrics:
                        results["median_area_ha"] = 0
                    if "max_area_ha" in metrics:
                        results["max_area_ha"] = 0
                    if "area_percentiles" in metrics:
                        results["area_percentiles"] = {
                            "p25": 0,
                            "p50": 0,
                            "p75": 0,
                            "p90": 0,
                        }

                # Vertex statistics
                if vertices_list:
                    if "min_vertices" in metrics:
                        results["min_vertices"] = min(vertices_list)
                    if "mean_vertices" in metrics:
                        results["mean_vertices"] = round(
                            sum(vertices_list) / len(vertices_list), 2
                        )

                    sorted_vertices = sorted(
                        vertices_list
                    )  # Sort once for median and percentiles

                    if "median_vertices" in metrics:
                        mid = len(sorted_vertices) // 2
                        results["median_vertices"] = (
                            sorted_vertices[mid]
                            if len(sorted_vertices) % 2 == 1
                            else round(
                                (sorted_vertices[mid - 1] + sorted_vertices[mid]) / 2, 0
                            )
                        )
                    if "max_vertices" in metrics:
                        results["max_vertices"] = max(vertices_list)

                    if "vertex_percentiles" in metrics:
                        n = len(sorted_vertices)
                        p25_idx = n // 4
                        p50_idx = n // 2
                        p75_idx = (n * 3) // 4
                        p90_idx = int(n * 0.9)

                        results["vertex_percentiles"] = {
                            "p25": sorted_vertices[p25_idx],
                            "p50": sorted_vertices[p50_idx]
                            if n % 2 == 1
                            else round(
                                (
                                    sorted_vertices[p50_idx - 1]
                                    + sorted_vertices[p50_idx]
                                )
                                / 2,
                                0,
                            ),
                            "p75": sorted_vertices[p75_idx],
                            "p90": sorted_vertices[p90_idx],
                        }
                else:
                    # Return zeros for no vertices
                    if "min_vertices" in metrics:
                        results["min_vertices"] = 0
                    if "mean_vertices" in metrics:
                        results["mean_vertices"] = 0
                    if "median_vertices" in metrics:
                        results["median_vertices"] = 0
                    if "max_vertices" in metrics:
                        results["max_vertices"] = 0
                    if "vertex_percentiles" in metrics:
                        results["vertex_percentiles"] = {
                            "p25": 0,
                            "p50": 0,
                            "p75": 0,
                            "p90": 0,
                        }
            else:
                # Return zeros for empty datasets
                for metric in [
                    "min_area_ha",
                    "mean_area_ha",
                    "median_area_ha",
                    "max_area_ha",
                    "area_percentiles",
                    "min_vertices",
                    "mean_vertices",
                    "median_vertices",
                    "max_vertices",
                    "vertex_percentiles",
                ]:
                    if metric in metrics:
                        results[metric] = (
                            0
                            if metric not in ["area_percentiles", "vertex_percentiles"]
                            else {"p25": 0, "p50": 0, "p75": 0, "p90": 0}
                        )

        # Add geometry quality logging to results
        if bbox_fallback_count > 0 or geometry_skip_count > 0:
            geometry_quality_log = (
                f"Geometry quality summary:\n"
                f"  - Bounding box fallback used: {bbox_fallback_count} features\n"
                f"  - Geometries skipped: {geometry_skip_count} features"
            )
            if polygon_type_stats:
                geometry_quality_log += "\n  - Breakdown:"
                for stat_type, count in sorted(polygon_type_stats.items()):
                    geometry_quality_log += f"\n    - {stat_type}: {count}"

            results["geometry_quality_note"] = geometry_quality_log
            print(geometry_quality_log)

        return results

    except Exception as e:
        print(f"Error: {str(e)}")
        return {}


def _check_metric_constraints(
    metrics,
    max_polygon_count=250_000,
    max_mean_area_ha=10_000,
    max_max_area_ha=None,
    max_mean_vertices=None,
    max_max_vertices=10_000,
    max_file_size_mb=None,
):
    """
    Check if computed metrics violate any constraints.

    Internal helper function for constraint validation.

    Parameters:
    -----------
    metrics : dict
        Dictionary of computed metrics with keys: count, mean_area_ha, max_area_ha,
        mean_vertices, max_vertices, file_size_mb (optional)
    max_polygon_count : int
        Maximum allowed number of polygons
    max_mean_area_ha : float
        Maximum allowed mean area per polygon in hectares
    max_max_area_ha : float, optional
        Maximum allowed maximum area per polygon in hectares
    max_mean_vertices : float, optional
        Maximum allowed mean vertices per polygon
    max_max_vertices : int, optional
        Maximum allowed vertices per polygon
    max_file_size_mb : float, optional
        Maximum allowed file size in megabytes

    Returns:
    --------
    list
        List of violation strings (empty if all constraints pass)
    """
    violations = []

    polygon_count = metrics["count"]
    mean_area = metrics["mean_area_ha"]
    max_area = metrics["max_area_ha"]
    mean_vertices = metrics["mean_vertices"]
    max_vertices_value = metrics["max_vertices"]
    file_size_mb = metrics.get("file_size_mb")

    if polygon_count > max_polygon_count:
        violations.append(
            f"Polygon count ({polygon_count:,}) exceeds limit ({max_polygon_count:,})"
        )

    if mean_area > max_mean_area_ha:
        violations.append(
            f"Mean area ({mean_area:,.2f} ha) exceeds limit ({max_mean_area_ha:,} ha)"
        )

    if max_max_area_ha is not None and max_area > max_max_area_ha:
        violations.append(
            f"Max area ({max_area:,.2f} ha) exceeds limit ({max_max_area_ha:,} ha)"
        )

    if max_mean_vertices is not None and mean_vertices > max_mean_vertices:
        violations.append(
            f"Mean vertices ({mean_vertices:.2f}) exceeds limit ({max_mean_vertices:,})"
        )

    if max_max_vertices is not None and max_vertices_value > max_max_vertices:
        violations.append(
            f"Max vertices ({max_vertices_value:,}) exceeds limit ({max_max_vertices:,})"
        )

    if (
        max_file_size_mb is not None
        and file_size_mb is not None
        and file_size_mb > max_file_size_mb
    ):
        violations.append(
            f"File size ({file_size_mb:.2f} MB) exceeds limit ({max_file_size_mb:.2f} MB)"
        )

    return violations


def check_geojson_limits(
    geojson_data: Path | str | dict = None,
    analysis_results: dict = None,
    max_polygon_count=250_000,
    max_mean_area_ha=50_000,
    max_max_area_ha=50_000,
    max_mean_vertices=50_000,
    max_max_vertices=50_000,
    max_file_size_mb=None,
    allowed_crs=["EPSG:4326"],
    verbose=True,
):
    """
    Check GeoJSON data against defined limits for processing readiness.

    Raises ValueError if any metrics exceed the specified limits.
    Uses analyze_geojson to compute metrics efficiently in a single sweep.

    Parameters:
    -----------
    geojson_data : Path | str | dict, optional
        GeoJSON FeatureCollection to validate. Can be:
        - dict: GeoJSON FeatureCollection dictionary
        - str: Path to GeoJSON file as string
        - Path: pathlib.Path to GeoJSON file
        Note: Cannot be used together with analysis_results
    analysis_results : dict, optional
        Pre-computed results from analyze_geojson(). Must contain keys:
        'count', 'mean_area_ha', 'max_area_ha', 'mean_vertices', 'max_vertices'
        Note: Cannot be used together with geojson_data
    max_polygon_count : int, optional
        Maximum allowed number of polygons (default: 250,000)
    max_mean_area_ha : float, optional
        Maximum allowed mean area per polygon in hectares (default: 50,000)
    max_max_area_ha : float, optional
        Maximum allowed maximum area per polygon in hectares (default: 50,000)
    max_mean_vertices : float, optional
        Maximum allowed mean vertices per polygon (default: 50,000)
    max_max_vertices : int, optional
        Maximum allowed vertices per polygon (default: 50,000)
    max_file_size_mb : float, optional
        Maximum allowed file size in megabytes (default: None, no limit)
    allowed_crs : list, optional
        List of allowed coordinate reference systems (default: ["EPSG:4326"])
        Set to None to skip CRS validation
    verbose : bool
        Print validation results (default: True)

    Returns:
    --------
    dict
        Dictionary containing computed metrics that passed validation:
        {
            'count': int,
            'mean_area_ha': float,
            'max_area_ha': float,
            'mean_vertices': float,
            'max_vertices': int,
            'valid': bool
        }

    Raises:
    -------
    ValueError
        If any constraint is violated, or if both geojson_data and analysis_results are provided,
        or if neither is provided
    """
    # Validate input parameters
    if geojson_data is not None and analysis_results is not None:
        raise ValueError(
            "Cannot provide both 'geojson_data' and 'analysis_results'. "
            "Please provide only one input source."
        )

    if geojson_data is None and analysis_results is None:
        raise ValueError(
            "Must provide either 'geojson_data' or 'analysis_results'. "
            "Both cannot be None."
        )

    if verbose:
        print("\n" + "=" * 80)
        print("GEOJSON LIMITS CHECK")
        print("=" * 80)
        print("\nConstraint Limits:")
        print(f"  - Max polygon count:     {max_polygon_count:,}")
        print(f"  - Max mean area (ha):    {max_mean_area_ha:,}")
        if max_max_area_ha is not None:
            print(f"  - Max area per polygon (ha): {max_max_area_ha:,}")
        if max_mean_vertices is not None:
            print(f"  - Max mean vertices:     {max_mean_vertices:,}")
        if max_max_vertices is not None:
            print(f"  - Max vertices per polygon: {max_max_vertices:,}")
        if max_file_size_mb is not None:
            print(f"  - Max file size (MB):    {max_file_size_mb:.2f}")

    # Get metrics either from analysis_results or by analyzing geojson_data
    if analysis_results is not None:
        # Use pre-computed analysis results
        metrics = analysis_results
    else:
        # Use analyze_geojson to compute all required metrics in a single sweep
        metrics_to_compute = [
            "count",
            "file_size_mb",
            "mean_area_ha",
            "max_area_ha",
            "mean_vertices",
            "max_vertices",
        ]
        # Add CRS if validation is requested
        if allowed_crs is not None:
            metrics_to_compute.append("crs")
        metrics = analyze_geojson(geojson_data, metrics=metrics_to_compute)

    # Build results dict with required keys
    results = {
        "count": metrics.get("count", 0),
        "file_size_mb": metrics.get("file_size_mb"),
        "mean_area_ha": metrics.get("mean_area_ha", 0),
        "max_area_ha": metrics.get("max_area_ha", 0),
        "mean_vertices": metrics.get("mean_vertices", 0),
        "max_vertices": metrics.get("max_vertices", 0),
        "crs": metrics.get("crs"),
        "valid": True,
    }

    if verbose:
        print("\nComputed Metrics:")
        print(f"  - Polygon count:         {results['count']:,}")
        if results.get("file_size_mb") is not None:
            print(f"  - File size (MB):        {results['file_size_mb']:,.2f}")
        if results.get("crs") is not None:
            print(f"  - CRS:                   {results['crs']}")
        print(f"  - Mean area (ha):        {results['mean_area_ha']:,}")
        print(f"  - Max area (ha):         {results['max_area_ha']:,}")
        print(f"  - Mean vertices:         {results['mean_vertices']:,}")
        print(f"  - Max vertices:          {results['max_vertices']:,}")

    # Check constraints using dedicated method
    violations = _check_metric_constraints(
        results,
        max_polygon_count=max_polygon_count,
        max_mean_area_ha=max_mean_area_ha,
        max_max_area_ha=max_max_area_ha,
        max_mean_vertices=max_mean_vertices,
        max_max_vertices=max_max_vertices,
        max_file_size_mb=max_file_size_mb,
    )

    # Check CRS if validation is requested
    if allowed_crs is not None and results.get("crs"):
        if results["crs"] not in allowed_crs:
            violations.append(
                f"CRS '{results['crs']}' is not in allowed list: {allowed_crs}"
            )

    # Report results
    if verbose:
        print("\n" + "=" * 80)
        if violations:
            print("LIMITS CHECK FAILED")
            print("=" * 80)
            for violation in violations:
                print(f"\n{violation}")
            results["valid"] = False
        else:
            print("LIMITS CHECK PASSED")
            print("=" * 80)
            print("\nAll metrics within acceptable limits")

    # Raise error with detailed message if any constraint violated
    if violations:
        error_message = "GeoJSON limits check failed:\n" + "\n".join(violations)
        raise ValueError(error_message)

    return results


# Backward compatibility aliases
screen_geojson = check_geojson_limits
validate_geojson_constraints = check_geojson_limits


def suggest_processing_mode(
    feature_count,
    mean_area_ha=None,
    mean_vertices=None,
    file_size_mb=None,
    feature_type="polygon",
    verbose=True,
):
    """
    Suggest processing mode based on feature characteristics.

    Decision thresholds from comprehensive benchmark data (Nov 2025):

    FILE SIZE:
    - Files >= 10 MB: recommend sequential mode (avoids payload size limits)

    POINTS:
    - Break-even: 750-1000 features
    - Sequential faster: < 750 features
    - Concurrent faster: >= 750 features

    POLYGONS (area-based thresholds):
    - Tiny (< 1 ha): break-even ~500 features
    - Small (1-5 ha, simple): break-even ~500 features
    - Small (1-5 ha, complex 20-50v): break-even ~500 features
    - Medium (5-20 ha): break-even ~250 features
    - Large (20-100 ha): break-even ~250 features
    - Very large (50-200 ha): break-even ~250 features

    Vertex complexity adjustment: High vertex counts (>50) favor concurrent at lower thresholds

    Parameters:
    -----------
    feature_count : int
        Number of features (polygons or points)
    mean_area_ha : float, optional
        Mean area per polygon in hectares (required for polygons, ignored for points)
    mean_vertices : float, optional
        Mean number of vertices per polygon (influences decision for complex geometries)
    file_size_mb : float, optional
        File size in megabytes (if >= 10 MB, recommends sequential mode)
    feature_type : str
        'polygon', 'multipolygon', or 'point' (default: 'polygon')
    verbose : bool
        Print recommendation explanation

    Returns:
    --------
    str: 'concurrent' or 'sequential'
    """

    # File size check: large files should use sequential mode
    if file_size_mb is not None and file_size_mb >= 10:
        if verbose:
            print(f"\nMETHOD RECOMMENDATION (File Size Constraint)")
            print(f"   File size: {file_size_mb:.2f} MB (>= 10 MB threshold)")
            print(f"   Method: SEQUENTIAL (avoids payload size limits)")
        return "sequential"

    # Points: simple threshold-based decision
    if feature_type == "point":
        breakeven = 750
        method = "concurrent" if feature_count >= breakeven else "sequential"

        if verbose:
            print(f"\nMETHOD RECOMMENDATION (Points)")
            print(f"   Features: {feature_count} points")
            print(f"   Break-even: {breakeven} features | Method: {method.upper()}")

        return method

    # Polygons and MultiPolygons: area and complexity-based decision
    # MultiPolygons use same breakpoints as Polygons
    if mean_area_ha is None:
        # Default to conservative threshold if area unknown
        breakeven = 500
        method = "concurrent" if feature_count >= breakeven else "sequential"

        if verbose:
            print(f"\nMETHOD RECOMMENDATION (Polygons - area unknown)")
            print(f"   Features: {feature_count} polygons")
            print(
                f"   Break-even: {breakeven} (conservative) | Method: {method.upper()}"
            )

        return method

    # Area-based thresholds from benchmark data
    if mean_area_ha >= 20:  # Large to very large polygons
        breakeven = 250
    elif mean_area_ha >= 5:  # Medium polygons
        breakeven = 250
    elif mean_area_ha >= 1:  # Small polygons
        # Vertex complexity matters more for small polygons
        if mean_vertices is not None and mean_vertices >= 30:
            breakeven = 500  # Complex small polygons
        else:
            breakeven = 500  # Simple small polygons
    else:  # Tiny polygons (< 1 ha)
        breakeven = 500

    # Vertex complexity adjustment for high-complexity geometries
    if mean_vertices is not None and mean_vertices >= 50:
        # High complexity: reduce breakeven by 20% (concurrent beneficial sooner)
        breakeven = int(breakeven * 0.8)

    method = "concurrent" if feature_count >= breakeven else "sequential"

    if verbose:
        print(f"\nMETHOD RECOMMENDATION (Polygons)")
        print(
            f"   Features: {feature_count} | Mean Area: {mean_area_ha:.1f} ha", end=""
        )
        if mean_vertices is not None:
            print(f" | Mean Vertices: {mean_vertices:.1f}", end="")
        print()
        print(f"   Break-even: {breakeven} features | Method: {method.upper()}")

    return method

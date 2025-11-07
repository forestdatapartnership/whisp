"""
Data validation and constraint checking functions for WHISP.

Provides validation functions to check GeoJSON data against defined limits
and thresholds, raising informative errors when constraints are violated.
"""

import json
from pathlib import Path
from shapely.geometry import Polygon as ShapelyPolygon

# Note: area summary stats are estimations for use in deciding pathways for analysis
# (estimation preferred here as allows efficient processing speed and limits overhead of checking file)
def analyze_geojson(
    geojson_data: Path | str | dict,
    metrics=[
        "count",
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
    results = {}

    try:
        # Load GeoJSON from file if path provided
        if isinstance(geojson_data, (str, Path)):
            file_path = Path(geojson_data)
            if not file_path.exists():
                raise FileNotFoundError(f"GeoJSON file not found: {file_path}")
            with open(file_path, "r") as f:
                geojson_data = json.load(f)

        features = geojson_data.get("features", [])

        if "count" in metrics:
            results["count"] = len(features)

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
                            poly = ShapelyPolygon(coords[0])
                            # Convert square degrees to hectares (near equator)
                            # 1 degree latitude ≈ 111 km, so 1 degree² ≈ 111² km² = 12,321 km² = 1,232,100 ha
                            area_ha = abs(poly.area) * 1232100
                            areas.append(area_ha)
                        except:
                            pass  # Skip if calculation fails
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
                            for polygon in coords:
                                poly = ShapelyPolygon(polygon[0])
                                area_ha = abs(poly.area) * 1232100
                                areas.append(area_ha)
                        except:
                            pass  # Skip if calculation fails
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
):
    """
    Check if computed metrics violate any constraints.

    Internal helper function for constraint validation.

    Parameters:
    -----------
    metrics : dict
        Dictionary of computed metrics with keys: count, mean_area_ha, max_area_ha,
        mean_vertices, max_vertices
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

    return violations


def validate_geojson_constraints(
    geojson_data: Path | str | dict,
    max_polygon_count=250_000,
    max_mean_area_ha=10_000,
    max_max_area_ha=None,
    max_mean_vertices=None,
    max_max_vertices=10_000,
    verbose=True,
):
    """
    Validate GeoJSON data against defined constraints.

    Raises ValueError if any metrics exceed the specified limits.
    Uses analyze_geojson to compute metrics efficiently in a single sweep.

    Parameters:
    -----------
    geojson_data : Path | str | dict
        GeoJSON FeatureCollection to validate. Can be:
        - dict: GeoJSON FeatureCollection dictionary
        - str: Path to GeoJSON file as string
        - Path: pathlib.Path to GeoJSON file
    max_polygon_count : int, optional
        Maximum allowed number of polygons (default: 250,000)
    max_mean_area_ha : float, optional
        Maximum allowed mean area per polygon in hectares (default: 10,000)
    max_max_area_ha : float, optional
        Maximum allowed maximum area per polygon in hectares (default: None, no limit)
    max_mean_vertices : float, optional
        Maximum allowed mean vertices per polygon (default: None, no limit)
    max_max_vertices : int, optional
        Maximum allowed vertices per polygon (default: 10,000)
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
        If any constraint is violated
    """
    from openforis_whisp.data_conversion import convert_geojson_to_ee
    from shapely.geometry import Polygon as ShapelyPolygon

    # Load GeoJSON from file if path provided
    if isinstance(geojson_data, (str, Path)):
        file_path = Path(geojson_data)
        if not file_path.exists():
            raise FileNotFoundError(f"GeoJSON file not found: {file_path}")
        with open(file_path, "r") as f:
            geojson_data = json.load(f)

    if verbose:
        print("\n" + "=" * 80)
        print("GEOJSON CONSTRAINT VALIDATION")
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

    # Collect all metrics we need to compute
    metrics_to_compute = [
        "count",
        "mean_area_ha",
        "max_area_ha",
        "mean_vertices",
        "max_vertices",
    ]

    # Import analyze_geojson (will be available after function is defined elsewhere)
    # For now, we'll compute it here efficiently in a single sweep
    features = geojson_data.get("features", [])

    # Single sweep computation
    total_area = 0
    total_vertices = 0
    max_area = 0
    max_vertices_value = 0
    valid_polygons = 0

    for feature in features:
        try:
            coords = feature["geometry"]["coordinates"]
            geom_type = feature["geometry"]["type"]

            if geom_type == "Polygon":
                # Count vertices
                feature_vertices = 0
                for ring in coords:
                    feature_vertices += len(ring)
                total_vertices += feature_vertices
                max_vertices_value = max(max_vertices_value, feature_vertices)

                # Calculate area
                try:
                    poly = ShapelyPolygon(coords[0])
                    area_ha = abs(poly.area) * 1232100
                    total_area += area_ha
                    max_area = max(max_area, area_ha)
                except:
                    pass
                valid_polygons += 1

            elif geom_type == "MultiPolygon":
                # Count vertices
                feature_vertices = 0
                for polygon in coords:
                    for ring in polygon:
                        feature_vertices += len(ring)
                total_vertices += feature_vertices
                max_vertices_value = max(max_vertices_value, feature_vertices)

                # Calculate area
                try:
                    for polygon in coords:
                        poly = ShapelyPolygon(polygon[0])
                        area_ha = abs(poly.area) * 1232100
                        total_area += area_ha
                        max_area = max(max_area, area_ha)
                except:
                    pass
                valid_polygons += 1

        except:
            continue

    # Compute means
    polygon_count = len(features)
    mean_area = total_area / valid_polygons if valid_polygons > 0 else 0
    mean_vertices = total_vertices / valid_polygons if valid_polygons > 0 else 0

    results = {
        "count": polygon_count,
        "mean_area_ha": round(mean_area, 2),
        "max_area_ha": round(max_area, 2),
        "mean_vertices": round(mean_vertices, 2),
        "max_vertices": max_vertices_value,
        "valid": True,
    }

    if verbose:
        print("\nComputed Metrics:")
        print(f"  - Polygon count:         {results['count']:,}")
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
    )

    # Report results
    if verbose:
        print("\n" + "=" * 80)
        if violations:
            print("VALIDATION FAILED")
            print("=" * 80)
            for violation in violations:
                print(f"\n{violation}")
            results["valid"] = False
        else:
            print("VALIDATION PASSED")
            print("=" * 80)
            print("\nAll metrics within acceptable limits")

    # Raise error with detailed message if any constraint violated
    if violations:
        error_message = "Constraint validation failed:\n" + "\n".join(violations)
        raise ValueError(error_message)

    return results


def suggest_method(polygon_count, mean_area_ha, mean_vertices=None, verbose=True):
    """
    Suggest processing method based on polygon characteristics.

    Decision thresholds from benchmark data (area per polygon × polygon count):
    - Small polygons (10 ha): need 250+ polygons for concurrent
    - Medium polygons (100 ha): breakeven at ~100 polygons
    - Large polygons (500 ha): concurrent wins at 50+ polygons

    Parameters:
    -----------
    polygon_count : int
        Number of polygons
    mean_area_ha : float
        Mean area per polygon in hectares
    mean_vertices : float, optional
        Mean number of vertices per polygon (can influence decision for complex geometries)
    verbose : bool
        Print recommendation explanation

    Returns:
    --------
    str: 'concurrent' or 'sequential'
    """

    # Primary decision based on area
    if mean_area_ha >= 300:  # Large polygons
        breakeven = 50
        method = "concurrent" if polygon_count >= breakeven else "sequential"
    elif mean_area_ha >= 50:  # Medium polygons
        breakeven = 100
        method = "concurrent" if polygon_count >= breakeven else "sequential"
    else:  # Small polygons
        breakeven = 250
        method = "concurrent" if polygon_count >= breakeven else "sequential"

    # Optional adjustment based on vertex complexity (very high complexity favors concurrent)
    if mean_vertices is not None and mean_vertices > 500:
        # Reduce breakeven by 25% for very complex geometries
        adjusted_breakeven = int(breakeven * 0.75)
        method = "concurrent" if polygon_count >= adjusted_breakeven else "sequential"

    if verbose:
        print(f"\nMETHOD RECOMMENDATION")
        print(
            f"   Polygons: {polygon_count} | Mean Area: {mean_area_ha:.1f} ha", end=""
        )
        if mean_vertices is not None:
            print(f" | Mean Vertices: {mean_vertices:.1f}", end="")
        print()
        print(f"   Breakeven: {breakeven} polygons | Method: {method.upper()}")

    return method

"""Fetch GeoJSON features from a DynaStore OGC API - Features endpoint.

Wraps the FAO DynaStore (asset registry) API to retrieve vector features (polygons, points)
by item ID or entire collection, returning standard GeoJSON that can be
fed directly into the Whisp pipeline as any other geojson
i.e., via whisp_formatted_stats_geojson_to_df().

Typical usage::

    import openforis_whisp as whisp

    filepath = whisp.fetch_and_save(
        catalog_id="my_catalog",
        collection_id="my_plots",
        item_ids=["plot-001", "plot-002"],
    )
    df = whisp.whisp_formatted_stats_geojson_to_df(filepath)
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Union

import requests

logger = logging.getLogger("whisp")

# ---------------------------------------------------------------------------
# Default base URL for the FAO DynaStore OGC Features endpoint.
# Override per-call for other deployments.
# ---------------------------------------------------------------------------
DEFAULT_BASE_URL = "https://data.apps.fao.org/geospatial/v2/api/catalog/features"

# Maximum number of features to fetch per page (DynaStore default is 10).
_DEFAULT_PAGE_SIZE = 100

# Safety cap when auto-paginating an entire collection.
_MAX_FEATURES_DEFAULT = 10_000


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_headers(api_key: str | None = None) -> dict:
    """Return HTTP headers for DynaStore requests."""
    headers = {"Accept": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def _resolve_api_key(api_key: str | None) -> str | None:
    """Use the provided key or fall back to the DYNASTORE_API_KEY env var."""
    if api_key is not None:
        return api_key
    return os.environ.get("DYNASTORE_API_KEY")


def _get_json(url: str, headers: dict, timeout: int) -> dict:
    """GET *url* and return the parsed JSON, raising on HTTP errors."""
    logger.debug("GET %s", url)
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _items_url(
    base_url: str,
    catalog_id: str,
    collection_id: str,
) -> str:
    """Build the items endpoint URL for a collection."""
    base = base_url.rstrip("/")
    return f"{base}/catalogs/{catalog_id}/collections/{collection_id}/items"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_feature(
    catalog_id: str,
    collection_id: str,
    item_id: str,
    base_url: str = DEFAULT_BASE_URL,
    api_key: str | None = None,
    timeout: int = 60,
) -> dict:
    """Fetch a single GeoJSON Feature from DynaStore by item ID.

    Parameters
    ----------
    catalog_id : str
        The DynaStore catalog (schema) identifier.
    collection_id : str
        The collection (layer) identifier within the catalog.
    item_id : str
        Unique feature identifier.
    base_url : str, optional
        DynaStore Features API root URL.
    api_key : str, optional
        Bearer token / API key.  Falls back to ``DYNASTORE_API_KEY`` env var.
    timeout : int, optional
        HTTP request timeout in seconds (default 60).

    Returns
    -------
    dict
        A GeoJSON Feature dict with ``type``, ``id``, ``geometry``, ``properties``.

    Raises
    ------
    requests.HTTPError
        On non-2xx HTTP responses.
    """
    api_key = _resolve_api_key(api_key)
    headers = _build_headers(api_key)
    url = f"{_items_url(base_url, catalog_id, collection_id)}/{item_id}"
    return _get_json(url, headers, timeout)


def fetch_collection_features(
    catalog_id: str,
    collection_id: str,
    base_url: str = DEFAULT_BASE_URL,
    api_key: str | None = None,
    limit: int = _DEFAULT_PAGE_SIZE,
    offset: int = 0,
    max_features: int | None = _MAX_FEATURES_DEFAULT,
    timeout: int = 60,
) -> dict:
    """Fetch features from a DynaStore collection with auto-pagination.

    Iterates through paginated responses until all features are retrieved or
    *max_features* is reached.

    Parameters
    ----------
    catalog_id : str
        The DynaStore catalog identifier.
    collection_id : str
        The collection identifier.
    base_url : str, optional
        DynaStore Features API root URL.
    api_key : str, optional
        Bearer token / API key.
    limit : int, optional
        Page size per request (default 100).
    offset : int, optional
        Starting offset (default 0).
    max_features : int or None, optional
        Safety cap on total features fetched.  Set to ``None`` to disable.
        Default is 10 000.
    timeout : int, optional
        HTTP request timeout in seconds per request.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection with all retrieved features.
    """
    api_key = _resolve_api_key(api_key)
    headers = _build_headers(api_key)
    items_base = _items_url(base_url, catalog_id, collection_id)

    all_features = []
    current_offset = offset

    while True:
        page_limit = limit
        if max_features is not None:
            remaining = max_features - len(all_features)
            if remaining <= 0:
                logger.info(
                    "Reached max_features cap (%s). Stopping pagination.",
                    max_features,
                )
                break
            page_limit = min(limit, remaining)

        url = f"{items_base}?limit={page_limit}&offset={current_offset}"
        data = _get_json(url, headers, timeout)

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)

        # Truncate if we overshot max_features.
        if max_features is not None and len(all_features) > max_features:
            all_features = all_features[:max_features]

        logger.info(
            "Fetched %d features (total so far: %d).",
            len(features),
            len(all_features),
        )

        # If we received fewer features than requested, we've reached the end.
        if len(features) < page_limit:
            break

        current_offset += len(features)

    return {"type": "FeatureCollection", "features": all_features}


def fetch_features_by_ids(
    catalog_id: str,
    collection_id: str,
    item_ids: list[str],
    base_url: str = DEFAULT_BASE_URL,
    api_key: str | None = None,
    timeout: int = 60,
) -> dict:
    """Fetch multiple features by ID and assemble into a FeatureCollection.

    Parameters
    ----------
    catalog_id : str
        The DynaStore catalog identifier.
    collection_id : str
        The collection identifier.
    item_ids : list[str]
        List of feature IDs to retrieve.
    base_url : str, optional
        DynaStore Features API root URL.
    api_key : str, optional
        Bearer token / API key.
    timeout : int, optional
        HTTP request timeout in seconds per request.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection containing the requested features.

    Raises
    ------
    requests.HTTPError
        If any individual feature request fails.
    """
    api_key = _resolve_api_key(api_key)
    headers = _build_headers(api_key)
    items_base = _items_url(base_url, catalog_id, collection_id)

    features = []
    for item_id in item_ids:
        url = f"{items_base}/{item_id}"
        feature = _get_json(url, headers, timeout)
        features.append(feature)

    logger.info("Fetched %d features by ID.", len(features))
    return {"type": "FeatureCollection", "features": features}


def save_geojson(geojson_dict: dict, output_path: Union[str, Path]) -> Path:
    """Write a GeoJSON dict to a file.

    Parameters
    ----------
    geojson_dict : dict
        A GeoJSON Feature or FeatureCollection.
    output_path : str or Path
        Destination file path (will be created / overwritten).

    Returns
    -------
    Path
        The resolved output path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(geojson_dict, f, indent=2)
    logger.info("Saved GeoJSON to %s", output_path)
    return output_path


def fetch_to_dict(
    catalog_id: str,
    collection_id: str,
    item_ids: list[str] | None = None,
    base_url: str = DEFAULT_BASE_URL,
    api_key: str | None = None,
    timeout: int = 60,
    max_features: int | None = _MAX_FEATURES_DEFAULT,
) -> dict:
    """Fetch features from DynaStore and return as a GeoJSON dict.

    Use this when you need the raw GeoJSON in memory rather than a saved file.
    For saving to disk, use :func:`fetch_and_save` instead.

    Parameters
    ----------
    catalog_id : str
        The DynaStore catalog identifier.
    collection_id : str
        The collection identifier.
    item_ids : list[str] or None, optional
        Specific feature IDs to fetch.  If ``None``, fetches the entire
        collection (subject to *max_features*).
    base_url : str, optional
        DynaStore Features API root URL.
    api_key : str, optional
        Bearer token / API key.
    timeout : int, optional
        HTTP request timeout in seconds.
    max_features : int or None, optional
        Safety cap when fetching an entire collection.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection.
    """
    if item_ids:
        return fetch_features_by_ids(
            catalog_id,
            collection_id,
            item_ids,
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
        )
    return fetch_collection_features(
        catalog_id,
        collection_id,
        base_url=base_url,
        api_key=api_key,
        timeout=timeout,
        max_features=max_features,
    )


def fetch_and_save(
    catalog_id: str,
    collection_id: str,
    item_ids: list[str] | None = None,
    output_path: Union[str, Path, None] = None,
    base_url: str = DEFAULT_BASE_URL,
    api_key: str | None = None,
    timeout: int = 60,
    max_features: int | None = _MAX_FEATURES_DEFAULT,
) -> Path:
    """Fetch features from DynaStore and save as a GeoJSON file.

    Convenience wrapper that combines fetching and saving.  The returned
    file path can be passed directly to ``whisp_formatted_stats_geojson_to_df()``.
    For the raw GeoJSON dict, use :func:`fetch_to_dict` instead.

    Parameters
    ----------
    catalog_id : str
        The DynaStore catalog identifier.
    collection_id : str
        The collection identifier.
    item_ids : list[str] or None, optional
        Specific feature IDs to fetch.  If ``None``, fetches the entire
        collection (subject to *max_features*).
    output_path : str, Path, or None, optional
        Where to save the GeoJSON.  If ``None``, a temporary file is created.
    base_url : str, optional
        DynaStore Features API root URL.
    api_key : str, optional
        Bearer token / API key.
    timeout : int, optional
        HTTP request timeout in seconds.
    max_features : int or None, optional
        Safety cap when fetching an entire collection.

    Returns
    -------
    Path
        The path to the saved ``.geojson`` file.
    """
    geojson = fetch_to_dict(
        catalog_id,
        collection_id,
        item_ids=item_ids,
        base_url=base_url,
        api_key=api_key,
        timeout=timeout,
        max_features=max_features,
    )

    if output_path is None:
        fd, tmp = tempfile.mkstemp(suffix=".geojson", prefix="dynastore_")
        os.close(fd)
        output_path = Path(tmp)

    return save_geojson(geojson, output_path)

"""
Export Whisp multiband images to Cloud Optimized GeoTIFFs (COGs) in Google Cloud Storage.

This module provides functionality to export the combined Whisp dataset image
to GCS as COGs for efficient local processing with exactextract.

Key features:
- Uses GAUL raster for country masking (avoids memory issues with vector clipToCollection)
- Configurable band selection (all bands, risk-relevant only, or custom list)
- Configurable resolution (1000m for testing, 10m for production)
- Exports to GCS bucket as Cloud Optimized GeoTIFF

Example usage:
    from openforis_whisp.export_cog import export_whisp_image_to_cog

    # Export Côte d'Ivoire at 1000m for testing
    task = export_whisp_image_to_cog(
        iso2_codes=["CI"],
        bucket="whisp_bucket",
        scale=1000,
    )
    task.start()
    print(task.status())
"""

import ee
import os
import pandas as pd
from datetime import datetime

from openforis_whisp.datasets import (
    combine_datasets,
    g_gaul_admin_code,
    get_gaul_codes_for_iso2,
    list_functions,
    DYNAMIC_BAND_NAMES,
)


def get_lookup_gee_datasets_df():
    """
    Load the lookup_gee_datasets.csv as a DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame containing dataset metadata including band names, themes, and risk flags
    """
    # Get the path relative to this module
    module_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(module_dir, "parameters", "lookup_gee_datasets.csv")
    return pd.read_csv(csv_path)


def get_risk_bands(include_timber=False):
    """
    Get band names used for risk assessment.

    Parameters
    ----------
    include_timber : bool, optional
        If True, include bands used for timber risk assessment.
        If False, only include bands where use_for_risk=1. Default is False.

    Returns
    -------
    list
        List of band names (from 'name' column) where use_for_risk=1
        (and optionally use_for_risk_timber=1)
    """
    df = get_lookup_gee_datasets_df()

    if include_timber:
        # Include bands used for either standard risk or timber risk
        mask = (df["use_for_risk"] == 1) | (df["use_for_risk_timber"] == 1)
    else:
        mask = df["use_for_risk"] == 1

    return df.loc[mask, "name"].tolist()


def get_bands_by_theme(themes):
    """
    Get band names filtered by theme(s).

    Parameters
    ----------
    themes : list
        List of theme names to include. Valid themes:
        - 'treecover'
        - 'commodities'
        - 'disturbance_before'
        - 'disturbance_after'

    Returns
    -------
    list
        List of band names matching the specified themes
    """
    df = get_lookup_gee_datasets_df()
    mask = df["theme"].isin(themes)
    return df.loc[mask, "name"].tolist()


def get_all_band_names(include_context=True):
    """
    Get all band names from the lookup CSV.

    Parameters
    ----------
    include_context : bool, optional
        If True, include context bands (admin_code, In_waterbody).
        Default is True.

    Returns
    -------
    list
        List of all band names
    """
    df = get_lookup_gee_datasets_df()
    bands = df["name"].tolist()

    if include_context:
        # Context bands are added by combine_datasets but not in CSV
        bands.extend(["admin_code", "In_waterbody"])

    return bands


def export_whisp_image_to_cog(
    iso2_codes=None,
    gaul1_codes=None,
    bucket="whisp_bucket",
    bands=None,
    scale=1000,
    national_codes=None,
    cog_folder="whisp_cogs",
    description=None,
    max_pixels=1e13,
    output_dtype="float32",
    exclude_yearly=False,
    file_dimensions=65536,
    region_label=None,
):
    """
    Export Whisp multiband image to Google Cloud Storage as a Cloud Optimized GeoTIFF.

    Uses GAUL admin codes to mask the image to specified countries or admin units,
    avoiding memory issues with vector clipToCollection operations.

    Parameters
    ----------
    iso2_codes : list, optional
        List of ISO2 country codes to include (e.g., ["CI", "BR"]).
        The image will be masked to only include these countries.
        Mutually exclusive with gaul1_codes (if both given, they are combined).
    gaul1_codes : list, optional
        List of GAUL Level 1 admin codes (integers) to include (e.g., [1210] for
        Yamoussoukro). Allows sub-national exports. See lookup_gaul1_admin.py for codes.
    bucket : str, optional
        GCS bucket name. Default is "whisp_bucket".
    bands : list, optional
        List of band names to export. If None, exports all bands from
        combine_datasets(). Use get_risk_bands() or get_bands_by_theme()
        to get predefined band lists.
    scale : int, optional
        Output resolution in meters. Default is 1000 (for testing).
        Use 10 for production quality matching local_stats.py.
    national_codes : list, optional
        ISO2 codes for national datasets to include via combine_datasets().
        If None, defaults to iso2_codes so that country-specific datasets
        (e.g., nCI_bnetd_cocoa_prep for Côte d'Ivoire) are included
        automatically. Pass an empty list [] to export global-only.
    cog_folder : str, optional
        Folder prefix within the GCS bucket. Default is "whisp_cogs".
    description : str, optional
        Task description for Earth Engine. If None, auto-generated from
        iso2_codes/gaul1_codes and scale.
    max_pixels : float, optional
        Maximum number of pixels to export. Default is 1e13.
    output_dtype : str, optional
        Output data type: 'float32' (default) or 'int16'.
        - 'float32': All bands exported as Float32.
        - 'int16': Band 1 = pixelArea × scale factor (Int16), remaining data
          bands = binary 0/1 (Int16), context bands = Int16. ~50% smaller files.
    exclude_yearly : bool, optional
        If True, exclude per-year timeseries datasets (~148 bands), reducing
        the image from ~196 to ~48 bands. Risk assessment is unaffected.
        Default: False.
    file_dimensions : int, optional
        Maximum pixel dimensions per output shard file. Larger values produce
        fewer files (better read performance), smaller values produce more files
        (more reliable for very large exports). Default is 65536 but will be
        automatically reduced if the resulting shard size would exceed the GEE
        17 GB per-file limit. Set to None to auto-calculate.

    Returns
    -------
    ee.batch.Task
        Earth Engine export task. Call task.start() to begin export.

    Examples
    --------
    # Export all bands for Côte d'Ivoire at 1000m (testing)
    >>> task = export_whisp_image_to_cog(iso2_codes=["CI"], scale=1000)
    >>> task.start()

    # Export Yamoussoukro at 10m, Int16, no yearly (48 bands)
    >>> task = export_whisp_image_to_cog(
    ...     gaul1_codes=[1210],
    ...     scale=10,
    ...     output_dtype="int16",
    ...     exclude_yearly=True,
    ... )
    >>> task.start()

    # Export only risk-relevant bands for Brazil at 10m
    >>> from openforis_whisp.export_cog import get_risk_bands
    >>> task = export_whisp_image_to_cog(
    ...     iso2_codes=["BR"],
    ...     bands=get_risk_bands(),
    ...     scale=10,
    ... )
    >>> task.start()
    """
    # Validate inputs
    if not iso2_codes and not gaul1_codes:
        raise ValueError("Must provide iso2_codes and/or gaul1_codes")

    # Collect all GAUL codes from both sources
    all_gaul_codes = []

    if iso2_codes:
        iso2_codes = [code.upper() for code in iso2_codes]
        all_gaul_codes.extend(get_gaul_codes_for_iso2(iso2_codes))

    if gaul1_codes:
        all_gaul_codes.extend(gaul1_codes)

    # Deduplicate
    all_gaul_codes = sorted(set(all_gaul_codes))

    if not all_gaul_codes:
        raise ValueError(
            f"No GAUL codes found for iso2_codes={iso2_codes}, gaul1_codes={gaul1_codes}"
        )

    # Default national_codes to iso2_codes so national datasets are included
    if national_codes is None and iso2_codes:
        national_codes = iso2_codes

    # Get the combined multiband image
    whisp_image = combine_datasets(
        national_codes=national_codes,
        include_context_bands=True,
        output_dtype=output_dtype,
        exclude_yearly=exclude_yearly,
    )

    # Select specific bands if requested
    if bands is not None:
        whisp_image = whisp_image.select(bands)

    # Create mask from GAUL codes (works for both country-level and L1 admin)
    admin_image = g_gaul_admin_code()
    ones_list = ee.List.repeat(1, len(all_gaul_codes))
    region_mask = admin_image.remap(all_gaul_codes, ones_list, 0).selfMask()

    # Apply mask to image
    masked_image = whisp_image.updateMask(region_mask)

    # Get export region geometry from GAUL FeatureCollection
    gaul_fc = ee.FeatureCollection(
        "projects/sat-io/open-datasets/FAO/GAUL/GAUL_2024_L1"
    )
    filtered_fc = gaul_fc.filter(ee.Filter.inList("gaul1_code", all_gaul_codes))
    export_region = filtered_fc.geometry()

    # Cast based on output dtype
    if output_dtype == "int16":
        masked_image = masked_image.toInt16()
        bytes_per_band = 2
    else:
        masked_image = masked_image.toFloat()
        bytes_per_band = 4

    # Auto-calculate safe file_dimensions to stay under GEE 17GB per-shard limit.
    # GEE validates: file_dimensions^2 * n_bands * bytes_per_band < 17,179,869,184
    n_bands = (
        len(bands) if bands is not None else masked_image.bandNames().size().getInfo()
    )
    bytes_per_pixel = n_bands * bytes_per_band
    max_shard_bytes = 16_000_000_000  # ~16 GB with safety margin under 17 GB limit
    max_pixels_per_shard = max_shard_bytes // bytes_per_pixel
    safe_file_dim = int(max_pixels_per_shard**0.5)
    # Round down to nearest 256 for clean tiling
    safe_file_dim = (safe_file_dim // 256) * 256
    if file_dimensions is not None and file_dimensions > safe_file_dim:
        print(
            f"  Reducing file_dimensions from {file_dimensions} to {safe_file_dim} "
            f"({n_bands} bands × {bytes_per_band}B = {bytes_per_pixel}B/pixel)"
        )
        file_dimensions = safe_file_dim
    elif file_dimensions is None:
        file_dimensions = safe_file_dim

    # Generate descriptive file name prefix
    if region_label is None:
        if gaul1_codes and not iso2_codes:
            region_label = "gaul1_" + "_".join(str(c) for c in sorted(gaul1_codes))
        elif iso2_codes and not gaul1_codes:
            region_label = "_".join(sorted(iso2_codes))
        else:
            region_label = (
                "_".join(sorted(iso2_codes))
                + "_gaul1_"
                + "_".join(str(c) for c in sorted(gaul1_codes))
            )

    dtype_label = output_dtype.replace("float", "f").replace("int", "i")  # f32 or i16
    yearly_label = "noyearly" if exclude_yearly else "allyearly"
    date_str = datetime.now().strftime("%Y%m%d")
    file_prefix = (
        f"{cog_folder}/{region_label}_{dtype_label}_{yearly_label}_{scale}m_{date_str}"
    )

    # Generate description if not provided
    if description is None:
        description = f"whisp_cog_{region_label}_{dtype_label}_{yearly_label}_{scale}m"
        # GEE descriptions max 100 chars
        if len(description) > 100:
            description = description[:100]

    # Create export task
    export_kwargs = dict(
        image=masked_image,
        description=description,
        bucket=bucket,
        fileNamePrefix=file_prefix,
        region=export_region,
        scale=scale,
        crs="EPSG:4326",
        maxPixels=max_pixels,
        fileFormat="GeoTIFF",
        formatOptions={"cloudOptimized": True, "noData": 0},
    )
    if file_dimensions is not None:
        export_kwargs["fileDimensions"] = file_dimensions

    task = ee.batch.Export.image.toCloudStorage(**export_kwargs)

    return task


def export_country_by_admin(
    iso2_codes,
    bucket="whisp_bucket",
    bands=None,
    scale=10,
    national_codes=None,
    cog_folder="whisp_cogs",
    max_pixels=1e13,
    output_dtype="int16",
    exclude_yearly=True,
    file_dimensions=65536,
    auto_start=True,
):
    """
    Export a country as separate COGs per GAUL Level 1 admin region.

    Works around the GEE per-file size limit (~17 GB uncompressed) by splitting
    large countries into admin-level tiles.

    Parameters
    ----------
    iso2_codes : list
        ISO2 country codes (e.g., ["CI"]).
    bucket : str
        GCS bucket name.
    bands : list, optional
        Specific bands to export. None = all.
    scale : int
        Resolution in meters.
    national_codes : list, optional
        ISO2 codes for national datasets. Defaults to iso2_codes.
    cog_folder : str
        GCS folder prefix.
    max_pixels : float
        Max pixels per export.
    output_dtype : str
        "int16" or "float32".
    exclude_yearly : bool
        Exclude per-year timeseries bands.
    file_dimensions : int
        Max pixel dimensions per shard.
    auto_start : bool
        If True, starts all tasks immediately.

    Returns
    -------
    list of tuple
        List of (gaul1_code, gaul1_name, task) for each admin region.
    """
    from openforis_whisp.parameters.lookup_gaul1_admin import lookup_dict as GAUL1_ADMIN

    iso2_codes = [code.upper() for code in iso2_codes]

    # Find all L1 regions for these countries
    regions = []
    for gaul_code, info in sorted(GAUL1_ADMIN.items()):
        if info["iso2_code"] in iso2_codes:
            regions.append((gaul_code, info["gaul1_name"]))

    if not regions:
        raise ValueError(f"No GAUL L1 regions found for {iso2_codes}")

    print(f"Exporting {len(regions)} admin regions for {iso2_codes}")

    iso2_label = "_".join(sorted(iso2_codes))

    tasks = []
    for gaul_code, name in regions:
        print(f"  Creating task: {name} (GAUL {gaul_code})...")
        task = export_whisp_image_to_cog(
            gaul1_codes=[gaul_code],
            # Don't pass iso2_codes here — it would expand the mask to whole country.
            # national_codes controls which datasets are included (CI-specific etc.)
            iso2_codes=None,
            bucket=bucket,
            bands=bands,
            scale=scale,
            national_codes=national_codes if national_codes else iso2_codes,
            cog_folder=cog_folder,
            max_pixels=max_pixels,
            output_dtype=output_dtype,
            exclude_yearly=exclude_yearly,
            file_dimensions=file_dimensions,
            # Keep ISO2 prefix in filename for discoverability
            region_label=iso2_label + "_gaul1_" + str(gaul_code),
        )
        if auto_start:
            task.start()
            print(f"    Started: {task.id}")
        tasks.append((gaul_code, name, task))

    print(f"\n{len(tasks)} tasks {'started' if auto_start else 'created'}")
    return tasks


# ============================================================================
# COG LOADING HELPERS (for using exported COGs with GEE)
# ============================================================================

# Scale factor must match what was used during export in combine_datasets()
AREA_SCALE_FACTOR_INT16 = 10

# Bands that should NOT be multiplied by the area band
_SKIP_AREA_MULTIPLY = {"Area", "admin_code"}


def list_country_cog_uris(
    iso2_codes,
    bucket="whisp_bucket",
    cog_folder="whisp_cogs",
    output_dtype="int16",
    exclude_yearly=True,
    scale=10,
    date_str=None,
):
    """
    Discover GCS URIs for per-admin COGs exported by export_country_by_admin().

    Handles both single-file and sharded exports (where GEE appends
    row-column suffixes like 0000000000-0000000000 to the filename).

    Parameters
    ----------
    iso2_codes : list
        ISO2 country codes (e.g., ["CI"]).
    bucket : str
        GCS bucket name.
    cog_folder : str
        Folder prefix within bucket.
    output_dtype : str
        "int16" or "float32" — must match what was exported.
    exclude_yearly : bool
        Must match what was exported.
    scale : int
        Resolution in meters — must match export.
    date_str : str, optional
        Export date string (YYYYMMDD). If None, must be provided.

    Returns
    -------
    list of str
        GCS URIs (gs://bucket/folder/file.tif) for each COG file found,
        including all shards. Sorted by name.
    """
    from openforis_whisp.parameters.lookup_gaul1_admin import lookup_dict as GAUL1_ADMIN

    if date_str is None:
        raise ValueError(
            "date_str is required (YYYYMMDD format, e.g., '20260223'). "
            "Check your GCS bucket for available exports."
        )

    iso2_codes = [code.upper() for code in iso2_codes]
    iso2_label = "_".join(sorted(iso2_codes))
    dtype_label = output_dtype.replace("float", "f").replace("int", "i")
    yearly_label = "noyearly" if exclude_yearly else "allyearly"

    # Build prefix for each admin region and list matching files from GCS.
    # This discovers both single-file and sharded exports.
    uris = []
    for gaul_code, info in sorted(GAUL1_ADMIN.items()):
        if info["iso2_code"] in iso2_codes:
            region_label = f"{iso2_label}_gaul1_{gaul_code}"
            prefix = f"{cog_folder}/{region_label}_{dtype_label}_{yearly_label}_{scale}m_{date_str}"
            found = _list_gcs_blobs(bucket, prefix, suffix=".tif")
            uris.extend(found)

    return sorted(uris)


def _list_gcs_blobs(bucket, prefix, suffix=".tif"):
    """
    List GCS blobs matching a prefix using EE credentials.

    Returns list of gs:// URIs.
    """
    import google.auth.transport.requests
    import json
    import urllib.parse
    import urllib.request

    creds = ee.data.get_persistent_credentials()
    creds.refresh(google.auth.transport.requests.Request())
    token = creds.token

    uris = []
    page_token = None
    while True:
        params = {"prefix": prefix, "maxResults": 1000}
        if page_token:
            params["pageToken"] = page_token
        url = (
            f"https://storage.googleapis.com/storage/v1/b/{bucket}/o?"
            + urllib.parse.urlencode(params)
        )
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        resp = urllib.request.urlopen(req)
        data = json.loads(resp.read())
        for item in data.get("items", []):
            name = item["name"]
            if name.endswith(suffix):
                uris.append(f"gs://{bucket}/{name}")
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return uris


def load_country_cog(
    iso2_codes,
    bucket="whisp_bucket",
    cog_folder="whisp_cogs",
    output_dtype="int16",
    exclude_yearly=True,
    scale=10,
    date_str=None,
    convert_to_hectares=True,
):
    """
    Load per-admin COGs for a country and mosaic into a single ee.Image.

    Optionally converts int16 binary bands to hectare values by multiplying
    each data band by the embedded Area band (divided by scale factor).

    Parameters
    ----------
    iso2_codes : list
        ISO2 country codes (e.g., ["CI"]).
    bucket : str
        GCS bucket name.
    cog_folder : str
        Folder prefix within bucket.
    output_dtype : str
        "int16" or "float32" — must match what was exported.
    exclude_yearly : bool
        Must match what was exported.
    scale : int
        Resolution in meters — must match export.
    date_str : str
        Export date string (YYYYMMDD). Required.
    convert_to_hectares : bool
        If True (default), multiply binary data bands by Area/scale_factor
        so that reduceRegions(sum) returns hectare values. Only relevant
        for int16 COGs. For float32, set to False.

    Returns
    -------
    ee.Image
        Mosaicked image ready for use with whisp_formatted_stats_geojson_to_df()
        via the whisp_image parameter.

    Examples
    --------
    >>> img = load_country_cog(["CI"], date_str="20260223")
    >>> df = whisp_formatted_stats_geojson_to_df(
    ...     "plots.geojson", mode="concurrent", whisp_image=img
    ... )
    """
    uris = list_country_cog_uris(
        iso2_codes=iso2_codes,
        bucket=bucket,
        cog_folder=cog_folder,
        output_dtype=output_dtype,
        exclude_yearly=exclude_yearly,
        scale=scale,
        date_str=date_str,
    )

    if not uris:
        raise ValueError(f"No COG URIs found for {iso2_codes}")

    # Load and mosaic all admin region COGs
    images = [ee.Image.loadGeoTIFF(uri) for uri in uris]
    mosaic = ee.ImageCollection(images).mosaic()

    if convert_to_hectares and output_dtype == "int16":
        mosaic = cog_to_hectares_image(mosaic)

    return mosaic


def cog_to_hectares_image(cog_image):
    """
    Convert an int16 COG image to hectare-equivalent values.

    Multiplies each binary data band (0/1) by the Area band (divided by
    AREA_SCALE_FACTOR_INT16) so that reduceRegions(sum) returns hectare values
    matching the live pipeline output.

    Skips: Area (already represents area) and admin_code (categorical integer).

    Parameters
    ----------
    cog_image : ee.Image
        Int16 COG image loaded via ee.Image.loadGeoTIFF() or mosaic thereof.
        Must have an "Area" band containing pixelArea × scale factor.

    Returns
    -------
    ee.Image
        Float image where each data band = binary × pixel_area_ha.
        Band names are preserved.
    """
    band_names = cog_image.bandNames()

    # Area band in true hectares
    area_ha = cog_image.select("Area").divide(AREA_SCALE_FACTOR_INT16).toFloat()

    # Other bands: multiply by area_ha unless in skip list
    other_bands = band_names.remove("Area")

    def _multiply_band(band_name):
        band_name = ee.String(band_name)
        band = cog_image.select(band_name).toFloat()
        is_admin = band_name.equals("admin_code")
        return ee.Algorithms.If(
            is_admin, band, band.multiply(area_ha).rename(band_name)
        )

    other_converted = other_bands.map(_multiply_band)

    def _add_band(img_to_add, accumulator):
        return ee.Image(accumulator).addBands(ee.Image(img_to_add))

    result = other_converted.iterate(_add_band, area_ha)
    return ee.Image(result)


def load_country_cog_hybrid(
    iso2_codes,
    national_codes=None,
    bucket="whisp_bucket",
    cog_folder="whisp_cogs",
    exclude_yearly=True,
    scale=10,
    date_str=None,
):
    """
    Load static bands from COG and dynamic bands from live GEE.

    Combines the speed of pre-computed COGs (for ~40 static bands) with
    fresh data from GEE (for ~6 frequently updated bands like RADD, GLAD,
    DIST, MODIS fire, DETER).

    The COG provides bands that rarely change (treecover, commodities,
    historical disturbance). Dynamic bands (weekly/monthly alert systems)
    are fetched live from GEE to ensure up-to-date results.

    Parameters
    ----------
    iso2_codes : list
        ISO2 country codes for COG loading (e.g., ["CI"]).
    national_codes : list, optional
        ISO2 codes for national datasets (e.g., ["BR"]).
        If None, defaults to iso2_codes.
    bucket : str
        GCS bucket name.
    cog_folder : str
        Folder prefix within bucket.
    exclude_yearly : bool
        Must match what was exported for the COGs.
    scale : int
        Resolution in meters — must match COG export.
    date_str : str
        Export date string (YYYYMMDD) for COG lookup.

    Returns
    -------
    ee.Image
        Combined image with static bands from COG and dynamic bands from
        live GEE. Ready for use with whisp_formatted_stats_geojson_to_df()
        via the whisp_image parameter.

    Examples
    --------
    >>> img = load_country_cog_hybrid(["CI"], date_str="20260223")
    >>> df = whisp_formatted_stats_geojson_to_df(
    ...     "plots.geojson", mode="concurrent", whisp_image=img
    ... )
    """
    if national_codes is None:
        national_codes = iso2_codes

    # 1. Load COG mosaic (raw int16, before hectare conversion)
    uris = list_country_cog_uris(
        iso2_codes=iso2_codes,
        bucket=bucket,
        cog_folder=cog_folder,
        output_dtype="int16",
        exclude_yearly=exclude_yearly,
        scale=scale,
        date_str=date_str,
    )

    if not uris:
        raise ValueError(f"No COG URIs found for {iso2_codes}")

    images = [ee.Image.loadGeoTIFF(uri) for uri in uris]
    mosaic = ee.ImageCollection(images).mosaic()

    # 2. Remove stale dynamic bands from COG
    dynamic_names = ee.List(DYNAMIC_BAND_NAMES)
    all_bands = mosaic.bandNames()
    static_bands = all_bands.removeAll(dynamic_names)
    mosaic_static = mosaic.select(static_bands)

    # 3. Convert static int16 COG to area-weighted values
    mosaic_ha = cog_to_hectares_image(mosaic_static)

    # 4. Build fresh dynamic bands from live GEE
    # Uses list_functions(only_dynamic=True) to get the dynamic _prep functions,
    # then multiplies by pixelArea to match the COG hectares conversion units.
    dynamic_funcs = list_functions(
        national_codes=national_codes,
        exclude_yearly=exclude_yearly,
        only_dynamic=True,
    )

    if dynamic_funcs:
        dynamic_images = []
        for func in dynamic_funcs:
            try:
                dynamic_images.append(func().multiply(ee.Image.pixelArea()))
            except ee.EEException as e:
                print(f"Warning: Could not load dynamic band: {e}")

        if dynamic_images:
            live_dynamic = ee.Image.cat(dynamic_images)
            mosaic_ha = mosaic_ha.addBands(live_dynamic)

    n_static = static_bands.size()
    n_dynamic = len(dynamic_funcs)
    print(
        f"Hybrid image: {n_static.getInfo()} static bands from COG + {n_dynamic} dynamic bands from live GEE"
    )

    return mosaic_ha


# NOTE: export_pixel_area_cog is commented out for now. With int16 mode,
# band 1 of the main COG already contains pixelArea × scale_factor, making
# a separate area COG redundant. May revisit if float32 exports need it.
#
# def export_pixel_area_cog(
#     iso2_codes,
#     bucket="whisp_bucket",
#     scale=1000,
#     cog_folder="whisp_cogs",
#     description=None,
#     max_pixels=1e13,
# ):
#     """
#     Export pixel area band to GCS as a Cloud Optimized GeoTIFF.
#
#     Uses ee.Image.pixelArea() to get accurate geodetic pixel areas in m².
#     Exported as Float32 to preserve precision.
#     """
#     if not iso2_codes:
#         raise ValueError("iso2_codes must be a non-empty list")
#
#     iso2_codes = [code.upper() for code in iso2_codes]
#
#     pixel_area = ee.Image.pixelArea().rename("pixel_area")
#     country_mask = get_country_mask_from_gaul(iso2_codes)
#     masked_area = pixel_area.updateMask(country_mask).toFloat()
#     export_region = get_country_geometry_from_gaul(iso2_codes)
#
#     iso2_joined = "_".join(sorted(iso2_codes))
#     file_prefix = f"{cog_folder}/{iso2_joined}_pixel_area_{scale}m"
#
#     if description is None:
#         description = f"pixel_area_{iso2_joined}_{scale}m"
#
#     task = ee.batch.Export.image.toCloudStorage(
#         image=masked_area,
#         description=description,
#         bucket=bucket,
#         fileNamePrefix=file_prefix,
#         region=export_region,
#         scale=scale,
#         crs="EPSG:4326",
#         maxPixels=max_pixels,
#         fileFormat="GeoTIFF",
#         formatOptions={"cloudOptimized": True, "noData": 0, "skipEmptyTiles": True},
#     )
#
#     return task


def check_export_status(task):
    """
    Check and print the status of an export task.

    Parameters
    ----------
    task : ee.batch.Task
        The export task to check

    Returns
    -------
    dict
        Task status dictionary
    """
    status = task.status()
    state = status.get("state", "UNKNOWN")
    description = status.get("description", "")

    print(f"Task: {description}")
    print(f"State: {state}")

    if state == "FAILED":
        print(f"Error: {status.get('error_message', 'Unknown error')}")
    elif state == "COMPLETED":
        print("Export completed successfully!")
    elif state == "RUNNING":
        print("Export in progress...")

    return status


def list_active_exports():
    """
    List all active Earth Engine export tasks.

    Returns
    -------
    list
        List of active task status dictionaries
    """
    tasks = ee.batch.Task.list()
    active = [t for t in tasks if t.status()["state"] in ["READY", "RUNNING"]]

    if not active:
        print("No active export tasks")
    else:
        print(f"Active export tasks: {len(active)}")
        for task in active:
            status = task.status()
            print(f"  - {status.get('description')}: {status.get('state')}")

    return active


def list_cog_files_in_gcs(
    bucket, prefix, project=None, credentials_path=None, use_ee_credentials=True
):
    """
    List COG files in a GCS bucket matching a prefix.

    Parameters
    ----------
    bucket : str
        GCS bucket name
    prefix : str
        Path prefix to filter files (e.g., 'whisp_cogs/CI_int16_30m')
    project : str, optional
        GCP project ID. If None, uses default from environment.
    credentials_path : str, optional
        Path to service account JSON file. If None, uses default credentials.
    use_ee_credentials : bool, optional
        If True (default), reuses Earth Engine OAuth credentials for GCS access.
        This avoids needing separate Application Default Credentials.

    Returns
    -------
    list
        List of full GCS paths (gs://bucket/path/file.tif)
    """
    from google.cloud import storage
    from google.oauth2 import service_account

    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path
        )
        client = storage.Client(project=project, credentials=credentials)
    elif use_ee_credentials:
        # Reuse Earth Engine's OAuth credentials for GCS
        credentials = ee.data.get_persistent_credentials()
        client = storage.Client(project=project, credentials=credentials)
    else:
        client = storage.Client(project=project)

    bucket_obj = client.bucket(bucket)
    blobs = bucket_obj.list_blobs(prefix=prefix)

    tif_files = []
    for blob in blobs:
        if blob.name.endswith(".tif"):
            tif_files.append(f"gs://{bucket}/{blob.name}")

    return sorted(tif_files)


def _get_vrt_cache_dir():
    """
    Get the default VRT cache directory (~/.whisp/vrt_cache).

    Creates it if it doesn't exist.

    Returns
    -------
    str
        Path to the cache directory
    """
    cache_dir = os.path.join(os.path.expanduser("~"), ".whisp", "vrt_cache")
    os.makedirs(cache_dir, exist_ok=True)
    return cache_dir


def _gcs_blob_exists(bucket, blob_name):
    """
    Check if a blob exists in GCS using EE credentials.

    Returns True if the object exists, False otherwise.
    """
    import google.auth.transport.requests
    import urllib.request
    import urllib.parse
    import urllib.error

    creds = ee.data.get_persistent_credentials()
    creds.refresh(google.auth.transport.requests.Request())
    token = creds.token

    encoded_name = urllib.parse.quote(blob_name, safe="")
    url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded_name}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    try:
        urllib.request.urlopen(req)
        return True
    except urllib.error.HTTPError:
        return False


def _download_from_gcs(bucket, blob_name, local_path):
    """
    Download a blob from GCS to a local file using EE credentials.
    """
    import google.auth.transport.requests
    import urllib.request
    import urllib.parse

    creds = ee.data.get_persistent_credentials()
    creds.refresh(google.auth.transport.requests.Request())
    token = creds.token

    encoded_name = urllib.parse.quote(blob_name, safe="")
    url = f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{encoded_name}?alt=media"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    resp = urllib.request.urlopen(req)

    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, "wb") as f:
        f.write(resp.read())


def _upload_to_gcs(local_path, bucket, blob_name):
    """
    Upload a local file to GCS using EE credentials.
    """
    import google.auth.transport.requests
    import urllib.request
    import urllib.parse

    creds = ee.data.get_persistent_credentials()
    creds.refresh(google.auth.transport.requests.Request())
    token = creds.token

    encoded_name = urllib.parse.quote(blob_name, safe="")
    url = (
        f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o"
        f"?uploadType=media&name={encoded_name}"
    )

    with open(local_path, "rb") as f:
        data = f.read()

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/xml",
        },
        method="POST",
    )
    urllib.request.urlopen(req)


def create_vrt_from_gcs(
    bucket,
    prefix,
    output_vrt_path=None,
    project=None,
    credentials_path=None,
    use_ee_credentials=True,
    force_rebuild=False,
):
    """
    Create a VRT file from sharded COG files in GCS, with local caching.

    Uses /vsicurl/ paths with OAuth headers for authenticated GCS access.
    By default, caches the VRT in ~/.whisp/vrt_cache/ and reuses it on
    subsequent calls unless force_rebuild=True.

    Parameters
    ----------
    bucket : str
        GCS bucket name
    prefix : str
        Path prefix for the sharded files (e.g., 'whisp_cogs/CI_gaul1_')
    output_vrt_path : str, optional
        Local path for output VRT file. If None, uses ~/.whisp/vrt_cache/<prefix>.vrt
    project : str, optional
        GCP project ID for GCS access. If None, uses default from environment.
    credentials_path : str, optional
        Path to service account JSON file. If None, uses default credentials.
    use_ee_credentials : bool, optional
        If True (default), reuses Earth Engine OAuth credentials for GCS.
    force_rebuild : bool, optional
        If True, rebuild VRT even if cached version exists. Default False.

    Returns
    -------
    str
        Path to the created VRT file

    Example
    -------
    >>> vrt_path = create_vrt_from_gcs('whisp_bucket', 'whisp_cogs/CI_gaul1_')
    >>> # Second call reuses cached VRT (from local cache or bucket):
    >>> vrt_path = create_vrt_from_gcs('whisp_bucket', 'whisp_cogs/CI_gaul1_')
    """
    import google.auth.transport.requests

    # Derive a stable VRT filename from the prefix
    safe_name = prefix.rstrip("/").split("/")[-1]
    if not safe_name:
        safe_name = prefix.replace("/", "_").strip("_")
    vrt_filename = f"{safe_name}.vrt"

    # Determine local cache path
    if output_vrt_path is None:
        cache_dir = _get_vrt_cache_dir()
        output_vrt_path = os.path.join(cache_dir, vrt_filename)

    # GCS blob name for the VRT (stored alongside the COGs)
    vrt_blob_name = f"{prefix.rstrip('/')}.vrt"
    # e.g.  whisp_cogs/CI_gaul1_.vrt  →  whisp_cogs/CI_gaul1.vrt
    vrt_blob_name = vrt_blob_name.replace("_.vrt", ".vrt")

    # --- Priority 1: local cache ------------------------------------------------
    if not force_rebuild and os.path.exists(output_vrt_path):
        print(f"Using cached VRT: {output_vrt_path}")
        if use_ee_credentials:
            _setup_gdal_gcs_auth()
        return output_vrt_path

    # --- Priority 2: VRT already in bucket --------------------------------------
    if not force_rebuild and _gcs_blob_exists(bucket, vrt_blob_name):
        print(f"Downloading VRT from gs://{bucket}/{vrt_blob_name}")
        os.makedirs(os.path.dirname(output_vrt_path), exist_ok=True)
        _download_from_gcs(bucket, vrt_blob_name, output_vrt_path)
        if use_ee_credentials:
            _setup_gdal_gcs_auth()
        print(f"Cached locally: {output_vrt_path}")
        return output_vrt_path

    # --- Priority 3: build from COG headers and store ---------------------------
    # List files in GCS
    gcs_files = list_cog_files_in_gcs(
        bucket,
        prefix,
        project=project,
        credentials_path=credentials_path,
        use_ee_credentials=use_ee_credentials,
    )

    if not gcs_files:
        raise ValueError(f"No TIF files found in gs://{bucket}/{prefix}*")

    # Set up OAuth authentication for GDAL via HTTP headers
    if use_ee_credentials:
        _setup_gdal_gcs_auth()

    # Convert gs:// to /vsicurl/ URLs for GDAL
    vsicurl_files = [
        f"/vsicurl/https://storage.googleapis.com/{f.replace('gs://', '')}"
        for f in gcs_files
    ]

    print(f"Found {len(vsicurl_files)} COG files")

    # Handle single file case - no VRT needed
    if len(vsicurl_files) == 1:
        print("Single file, returning path directly")
        return vsicurl_files[0]

    # Build VRT using direct XML (parallel metadata reads)
    os.makedirs(os.path.dirname(output_vrt_path), exist_ok=True)
    _build_vrt_xml(output_vrt_path, vsicurl_files)
    print(f"VRT created: {output_vrt_path}")

    # Upload VRT to bucket so other users don't need to rebuild
    try:
        _upload_to_gcs(output_vrt_path, bucket, vrt_blob_name)
        print(f"VRT uploaded to gs://{bucket}/{vrt_blob_name}")
    except Exception as e:
        print(f"Warning: could not upload VRT to bucket: {e}")

    return output_vrt_path


def _setup_gdal_gcs_auth():
    """
    Set up GDAL environment variables for authenticated /vsicurl/ access to GCS.

    Uses Earth Engine OAuth credentials to create a header file that GDAL
    reads for HTTP requests.
    """
    import tempfile
    import google.auth.transport.requests

    creds = ee.data.get_persistent_credentials()
    creds.refresh(google.auth.transport.requests.Request())
    access_token = creds.token

    header_file = os.path.join(tempfile.gettempdir(), "gdal_gcs_headers.txt")
    with open(header_file, "w") as f:
        f.write(f"Authorization: Bearer {access_token}\n")

    os.environ["GDAL_HTTP_HEADER_FILE"] = header_file
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
    os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = ".tif"


def _build_vrt_xml(output_path, file_paths):
    """
    Build a VRT file from a list of source files using XML.

    This avoids rio_vrt's path handling issues on Windows.
    Reads file metadata in parallel to minimize HTTP roundtrip latency.
    """
    import rasterio
    from xml.etree import ElementTree as ET
    from concurrent.futures import ThreadPoolExecutor

    def _read_metadata(fp):
        with rasterio.open(fp) as src:
            return {
                "bounds": src.bounds,
                "shape": (src.height, src.width),
                "count": src.count,
                "dtype": src.dtypes[0],
                "crs": src.crs,
                "res": src.res,
                "descriptions": src.descriptions,
            }

    # Read all file metadata in parallel (60 files × ~0.5s each → ~3s with 20 threads)
    with ThreadPoolExecutor(max_workers=20) as pool:
        all_meta = list(pool.map(_read_metadata, file_paths))

    # Use first file for reference metadata
    ref = all_meta[0]
    band_count = ref["count"]
    dtype = ref["dtype"]
    crs = ref["crs"]
    band_descriptions = ref["descriptions"]
    res_x, res_y = ref["res"]

    all_bounds = [m["bounds"] for m in all_meta]
    all_shapes = [m["shape"] for m in all_meta]

    # Calculate overall bounds and resolution
    min_left = min(b.left for b in all_bounds)
    min_bottom = min(b.bottom for b in all_bounds)
    max_right = max(b.right for b in all_bounds)
    max_top = max(b.top for b in all_bounds)

    # Calculate total size
    total_width = int((max_right - min_left) / res_x)
    total_height = int((max_top - min_bottom) / res_y)

    # Build VRT XML
    vrt = ET.Element(
        "VRTDataset",
        {"rasterXSize": str(total_width), "rasterYSize": str(total_height)},
    )

    # Add SRS
    srs = ET.SubElement(vrt, "SRS")
    srs.text = crs.to_wkt()

    # Add GeoTransform
    geo = ET.SubElement(vrt, "GeoTransform")
    geo.text = f"{min_left}, {res_x}, 0, {max_top}, 0, {-res_y}"

    # Add bands
    for band_idx in range(1, band_count + 1):
        band_elem = ET.SubElement(
            vrt,
            "VRTRasterBand",
            {
                "dataType": dtype.capitalize() if dtype != "int16" else "Int16",
                "band": str(band_idx),
            },
        )

        # Add description if available
        if (
            band_descriptions
            and band_idx <= len(band_descriptions)
            and band_descriptions[band_idx - 1]
        ):
            desc = ET.SubElement(band_elem, "Description")
            desc.text = band_descriptions[band_idx - 1]

        # Add sources for this band from each file
        for i, fp in enumerate(file_paths):
            source = ET.SubElement(band_elem, "SimpleSource")

            source_filename = ET.SubElement(
                source, "SourceFilename", {"relativeToVRT": "0"}
            )
            source_filename.text = fp

            source_band = ET.SubElement(source, "SourceBand")
            source_band.text = str(band_idx)

            # Get this file's bounds and dimensions
            bounds = all_bounds[i]
            height, width = all_shapes[i]

            # Calculate position in VRT
            x_off = int((bounds.left - min_left) / res_x)
            y_off = int((max_top - bounds.top) / res_y)

            src_rect = ET.SubElement(
                source,
                "SrcRect",
                {"xOff": "0", "yOff": "0", "xSize": str(width), "ySize": str(height)},
            )
            dst_rect = ET.SubElement(
                source,
                "DstRect",
                {
                    "xOff": str(x_off),
                    "yOff": str(y_off),
                    "xSize": str(width),
                    "ySize": str(height),
                },
            )

    # Write VRT file
    tree = ET.ElementTree(vrt)
    tree.write(output_path, encoding="utf-8", xml_declaration=True)


def run_exactextract_on_cog(
    bucket,
    prefix,
    geojson_path,
    ops=None,
    output_csv_path=None,
    project=None,
    credentials_path=None,
    use_ee_credentials=True,
):
    """
    Run exactextract on COG files in GCS.

    Parameters
    ----------
    bucket : str
        GCS bucket name
    prefix : str
        Path prefix for COG files (e.g., 'whisp_cogs/CI_int16_30m')
    geojson_path : str
        Path to input GeoJSON file with features to extract stats for
    ops : list, optional
        Operations for exactextract. Default is ['sum'].
    output_csv_path : str, optional
        Path to save results as CSV. If None, returns DataFrame only.
    project : str, optional
        GCP project ID for GCS access. If None, uses default from environment.
    credentials_path : str, optional
        Path to service account JSON file. If None, uses default credentials.
    use_ee_credentials : bool, optional
        If True (default), reuses Earth Engine OAuth credentials for GCS.

    Returns
    -------
    pd.DataFrame
        DataFrame with extracted statistics
    """
    import exactextract
    import geopandas as gpd

    if ops is None:
        ops = ["sum"]

    # Create VRT from GCS files
    vrt_path = create_vrt_from_gcs(
        bucket,
        prefix,
        project=project,
        credentials_path=credentials_path,
        use_ee_credentials=use_ee_credentials,
    )

    # Load features
    gdf = gpd.read_file(geojson_path)

    print(f"Running exactextract on {len(gdf)} features...")

    # Run exactextract
    result = exactextract.exact_extract(
        rast=vrt_path,
        vec=gdf,
        ops=ops,
        output="pandas",
    )

    if output_csv_path:
        result.to_csv(output_csv_path, index=False)
        print(f"Results saved to {output_csv_path}")

    return result


def pixel_area_m2(lat_deg, res_deg=0.00026949):
    """
    Calculate geodetic pixel area in square meters at a given latitude.

    For EPSG:4326 rasters, pixel area varies with latitude because longitude
    degrees represent smaller distances near the poles.

    Parameters
    ----------
    lat_deg : float or array-like
        Latitude in degrees (positive for N, negative for S)
    res_deg : float, optional
        Pixel resolution in degrees. Default is 0.00026949 (~30m at equator).

    Returns
    -------
    float or array
        Pixel area in square meters

    Notes
    -----
    Uses WGS84 approximations:
    - 1 degree latitude ≈ 110,574 m
    - 1 degree longitude ≈ 111,320 × cos(latitude) m
    """
    import numpy as np

    m_per_deg_lon = 111320 * np.cos(np.radians(lat_deg))
    m_per_deg_lat = 110574
    return (res_deg * m_per_deg_lon) * (res_deg * m_per_deg_lat)


def pixel_area_ha(lat_deg, res_deg=0.00026949):
    """
    Calculate geodetic pixel area in hectares at a given latitude.

    Parameters
    ----------
    lat_deg : float or array-like
        Latitude in degrees
    res_deg : float, optional
        Pixel resolution in degrees. Default is 0.00026949 (~30m at equator).

    Returns
    -------
    float or array
        Pixel area in hectares
    """
    return pixel_area_m2(lat_deg, res_deg) / 10000


def convert_sum_to_hectares(df, geometry_col="geometry", res_deg=0.00026949):
    """
    Convert pixel sum columns to hectares based on feature centroid latitude.

    For binary rasters (0/1 values), the sum represents pixel count.
    This function multiplies each sum by the geodetic pixel area at the
    feature's centroid latitude to get area in hectares.

    Parameters
    ----------
    df : pd.DataFrame or gpd.GeoDataFrame
        DataFrame with sum columns (e.g., 'band_1_sum') and geometry
    geometry_col : str, optional
        Column name containing geometry. Default is 'geometry'.
    res_deg : float, optional
        Pixel resolution in degrees. Default is 0.00026949 (~30m at equator).

    Returns
    -------
    pd.DataFrame
        DataFrame with sum columns converted to hectares, renamed with '_ha' suffix

    Example
    -------
    >>> result = exactextract.exact_extract(rast, gdf, ['sum'], output='pandas')
    >>> result['geometry'] = gdf.geometry  # Add geometry back
    >>> result_ha = convert_sum_to_hectares(result)
    >>> print(result_ha['band_1_ha'].values)  # Area in hectares
    """
    import geopandas as gpd

    df = df.copy()

    # Get centroid latitudes
    if isinstance(df, gpd.GeoDataFrame):
        centroids = df[geometry_col].centroid
    else:
        # If plain DataFrame, geometry_col should contain shapely geometries
        import shapely

        centroids = df[geometry_col].apply(
            lambda g: g.centroid if hasattr(g, "centroid") else g
        )

    lats = centroids.apply(lambda p: p.y if hasattr(p, "y") else p)

    # Calculate pixel area for each feature
    pixel_areas = pixel_area_ha(lats.values, res_deg)

    # Find sum columns and convert
    sum_cols = [c for c in df.columns if c.endswith("_sum") or c == "sum"]
    for col in sum_cols:
        new_col = col.replace("_sum", "_ha") if "_sum" in col else f"{col}_ha"
        df[new_col] = df[col] * pixel_areas

    return df


def _create_area_band_vrt(main_vrt_path, output_path=None):
    """
    Create a single-band VRT referencing only band 1 (Area) of the main VRT.

    In Int16 COGs, band 1 contains the scaled pixel area (area_ha × AREA_SCALE_FACTOR).
    This VRT is used as the weights input for exactextract weighted_sum.

    Parameters
    ----------
    main_vrt_path : str
        Path to the main multi-band VRT file
    output_path : str, optional
        Output path for the area VRT. If None, appends '_area' to the main VRT name.

    Returns
    -------
    str
        Path to the created area-band VRT
    """
    import rasterio
    from xml.etree import ElementTree as ET

    with rasterio.open(main_vrt_path) as src:
        width, height = src.width, src.height
        crs_wkt = src.crs.to_wkt()
        gt = src.transform

    if output_path is None:
        output_path = main_vrt_path.replace(".vrt", "_area.vrt")

    vrt_elem = ET.Element(
        "VRTDataset", {"rasterXSize": str(width), "rasterYSize": str(height)}
    )
    srs = ET.SubElement(vrt_elem, "SRS")
    srs.text = crs_wkt
    geo = ET.SubElement(vrt_elem, "GeoTransform")
    geo.text = f"{gt.c}, {gt.a}, {gt.b}, {gt.f}, {gt.d}, {gt.e}"

    band_elem = ET.SubElement(
        vrt_elem, "VRTRasterBand", {"dataType": "Int16", "band": "1"}
    )
    source = ET.SubElement(band_elem, "SimpleSource")
    src_file = ET.SubElement(source, "SourceFilename", {"relativeToVRT": "0"})
    src_file.text = main_vrt_path
    src_band = ET.SubElement(source, "SourceBand")
    src_band.text = "1"
    ET.SubElement(
        source,
        "SrcRect",
        {
            "xOff": "0",
            "yOff": "0",
            "xSize": str(width),
            "ySize": str(height),
        },
    )
    ET.SubElement(
        source,
        "DstRect",
        {
            "xOff": "0",
            "yOff": "0",
            "xSize": str(width),
            "ySize": str(height),
        },
    )

    ET.ElementTree(vrt_elem).write(output_path, encoding="utf-8", xml_declaration=True)
    return output_path


def whisp_stats_cog_local(
    input_geojson_filepath,
    iso2_codes,
    date_str=None,
    bucket="whisp_bucket",
    cog_folder="whisp_cogs",
    national_codes=None,
    max_extract_workers=None,
    chunk_size=10,
    unit_type="ha",
    decimal_places=3,
    external_id_column=None,
    custom_bands=None,
    remove_median_columns=True,
    convert_water_flag=True,
    water_flag_threshold=0.5,
    sort_column="plotId",
    geometry_audit_trail=False,
    verbose=True,
    force_rebuild_vrt=False,
):
    """
    Run Whisp statistics using exactextract on pre-exported COGs in GCS.

    This reuses the local_stats exactextract pipeline (``exact_extract_in_chunks_parallel``)
    on a VRT built from COG files already on GCS, skipping the GeoTIFF download step.
    The VRT is cached locally in ``~/.whisp/vrt_cache/`` and reused between calls.

    Parameters
    ----------
    input_geojson_filepath : str
        Path to input GeoJSON file with features to process
    iso2_codes : list of str
        ISO2 country codes to load COGs for (e.g., ["CI"])
    date_str : str, optional
        Date string in COG filenames (e.g., "20260223"). If None, discovers automatically.
    bucket : str, optional
        GCS bucket name (default: "whisp_bucket")
    cog_folder : str, optional
        Folder in bucket containing COGs (default: "whisp_cogs")
    national_codes : list of str, optional
        ISO2 codes for national dataset filtering in post-processing
    max_extract_workers : int, optional
        Number of parallel extract processes (default: CPU count - 1)
    chunk_size : int, optional
        Features per chunk for parallel extraction (default: 25)
    unit_type : str, optional
        Output unit type — 'ha' (hectares) or 'percent' (default: 'ha')
    decimal_places : int, optional
        Decimal places for rounding (default: 3)
    external_id_column : str, optional
        Column name in input GeoJSON to preserve as external_id
    custom_bands : list, optional
        Custom band handling for validation (None=strict, list=preserve specified)
    remove_median_columns : bool, optional
        Whether to remove '_median' columns after processing (default: True)
    convert_water_flag : bool, optional
        Whether to convert water flag to boolean (default: True)
    water_flag_threshold : float, optional
        Threshold for water flag ratio (default: 0.5)
    sort_column : str, optional
        Column to sort output by (default: 'plotId', None to skip)
    geometry_audit_trail : bool, optional
        If True, includes geo_original column with input geometry (default: False)
    verbose : bool, optional
        If True, print progress messages (default: True)
    force_rebuild_vrt : bool, optional
        If True, rebuild VRT even if cached version exists (default: False)

    Returns
    -------
    pandas.DataFrame
        Formatted zonal statistics matching concurrent/local mode output
    """
    import time
    import json
    import logging
    import geopandas as gpd
    from datetime import datetime, timezone
    from importlib.metadata import version as get_version
    from shapely.geometry import mapping

    from openforis_whisp.local_stats import (
        exact_extract_in_chunks_parallel,
        get_band_names_from_raster,
        _normalize_int16_stats,
        _suppress_gdal_warnings,
    )
    from openforis_whisp.datasets import AREA_SCALE_FACTOR_INT16
    from openforis_whisp.reformat import (
        format_stats_dataframe,
        validate_dataframe_using_lookups_flexible,
    )
    from openforis_whisp.advanced_stats import (
        extract_centroid_and_geomtype_client,
        join_admin_codes,
    )
    from openforis_whisp.stats import (
        reformat_geometry_type,
        set_point_geometry_area_to_zero,
    )
    from openforis_whisp.parameters.lookup_gaul1_admin import (
        lookup_dict as gaul_lookup_dict,
    )
    from openforis_whisp.parameters.config_runtime import (
        plot_id_column,
        geometry_column,
        geometry_area_column,
    )
    from openforis_whisp.logger import get_whisp_logger

    logger = get_whisp_logger()
    if not verbose:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.INFO)

    _suppress_gdal_warnings()

    if national_codes is None:
        national_codes = iso2_codes

    if max_extract_workers is None:
        max_extract_workers = max(1, os.cpu_count() - 1)

    start_time = time.time()

    # ----------------------------------------------------------------
    # Step 1: Build or load cached VRT from GCS COGs
    # ----------------------------------------------------------------
    # Build prefix for COG discovery (e.g., 'whisp_cogs/CI_gaul1_')
    iso2_label = "_".join(sorted(iso2_codes))
    prefix = f"{cog_folder}/{iso2_label}_gaul1_"

    logger.info("Mode: cog-local")
    logger.info(f"Building/loading VRT for {iso2_label}...")

    t0 = time.time()
    vrt_path = create_vrt_from_gcs(
        bucket,
        prefix,
        force_rebuild=force_rebuild_vrt,
    )
    t_vrt = time.time() - t0
    logger.info(f"VRT ready in {t_vrt:.1f}s: {vrt_path}")

    # ----------------------------------------------------------------
    # Step 2: Create area-band VRT for Int16 weighted_sum
    # ----------------------------------------------------------------
    area_vrt_path = _create_area_band_vrt(vrt_path)
    logger.debug(f"Area band VRT: {area_vrt_path}")

    # Get band names from VRT
    band_names = get_band_names_from_raster(vrt_path)
    if band_names:
        logger.debug(f"VRT has {len(band_names)} bands")
    else:
        raise RuntimeError(f"Could not read band names from VRT: {vrt_path}")

    # ----------------------------------------------------------------
    # Step 3: Run exactextract with area weights (Int16 mode)
    # ----------------------------------------------------------------
    int16_ops = ["weighted_sum", "sum", "median"]

    logger.info(f"Running exactextract on {input_geojson_filepath}...")
    t0 = time.time()
    raw_stats_df = exact_extract_in_chunks_parallel(
        rasters=vrt_path,
        vector_file=input_geojson_filepath,
        chunk_size=chunk_size,
        ops=int16_ops,
        max_workers=max_extract_workers,
        band_names=band_names,
        verbose=verbose,
        weights=area_vrt_path,
        use_threads=True,  # Threads work well for I/O-bound COG range requests
    )
    t_extract = time.time() - t0
    logger.info(f"Extraction done in {t_extract:.1f}s ({len(raw_stats_df)} features)")

    # Normalize Int16 results to match Float32 _sum convention
    stats_df = _normalize_int16_stats(raw_stats_df, band_names, AREA_SCALE_FACTOR_INT16)

    # ----------------------------------------------------------------
    # Step 4: Post-processing (same as whisp_stats_local Steps 5a-5h)
    # ----------------------------------------------------------------
    logger.debug("Formatting output...")

    # 4a: Add plotId
    stats_df[plot_id_column] = [str(i) for i in range(1, len(stats_df) + 1)]

    # 4b: Extract centroid and geometry type from original GeoJSON
    gdf = gpd.read_file(input_geojson_filepath)

    if external_id_column and external_id_column in gdf.columns:
        if external_id_column != "external_id":
            gdf = gdf.rename(columns={external_id_column: "external_id"})
        gdf["external_id"] = gdf["external_id"].astype(str)

    df_metadata = extract_centroid_and_geomtype_client(
        gdf,
        external_id_column=external_id_column,
        return_attributes_only=True,
    )
    df_metadata[plot_id_column] = [str(i) for i in range(1, len(df_metadata) + 1)]
    stats_df = stats_df.merge(df_metadata, on=plot_id_column, how="left")

    if "external_id" not in stats_df.columns:
        stats_df["external_id"] = None

    # 4c: Add geometry column
    stats_df[geometry_column] = gdf.geometry.apply(
        lambda g: json.dumps(mapping(g)) if g else None
    ).values

    # 4d: Join admin codes
    admin_col = (
        "admin_code_median"
        if "admin_code_median" in stats_df.columns
        else "admin_code_sum"
    )
    if admin_col in stats_df.columns:
        if admin_col != "admin_code_median":
            stats_df["admin_code_median"] = stats_df[admin_col]
        stats_df = join_admin_codes(
            stats_df, gaul_lookup_dict, id_col="admin_code_median"
        )

    # 4e: Format stats (unit conversion, strip _sum suffix, convert water flag)
    area_col = f"{geometry_area_column}_sum"
    stats_df = format_stats_dataframe(
        df=stats_df,
        area_col=area_col,
        decimal_places=decimal_places,
        unit_type=unit_type,
        remove_columns=remove_median_columns,
        convert_water_flag=convert_water_flag,
        water_flag_threshold=water_flag_threshold,
        sort_column=sort_column,
    )

    # 4f: Reformat geometry type and handle point areas
    try:
        stats_df = reformat_geometry_type(stats_df)
    except Exception as e:
        logger.warning(f"Error reformatting geometry type: {e}")

    try:
        stats_df = set_point_geometry_area_to_zero(stats_df)
    except Exception as e:
        logger.warning(f"Error setting point geometry area to zero: {e}")

    # 4g: Schema validation
    stats_df = validate_dataframe_using_lookups_flexible(
        df_stats=stats_df,
        national_codes=national_codes,
        custom_bands=custom_bands,
    )

    # 4g-2: Geometry audit trail
    if geometry_audit_trail:
        geo_original_series = pd.Series(
            gdf.geometry.apply(lambda g: json.dumps(mapping(g)) if g else None).values,
            name="geo_original",
        )
        stats_df = pd.concat([stats_df, geo_original_series], axis=1)
        logger.info("Audit trail added: geo_original column")

    # 4h: Processing metadata
    metadata_dict = {
        "whisp_version": get_version("openforis-whisp"),
        "processing_timestamp_utc": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S%z"
        ),
        "mode": "cog-local",
    }
    metadata_series = pd.Series(
        [metadata_dict] * len(stats_df),
        name="whisp_processing_metadata",
    )
    stats_df = pd.concat([stats_df, metadata_series], axis=1)

    total_time = time.time() - start_time
    logger.info(f"Processing complete: {len(stats_df):,} features in {total_time:.1f}s")

    return stats_df

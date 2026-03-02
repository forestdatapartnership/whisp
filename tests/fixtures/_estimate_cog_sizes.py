"""Estimate COG storage for EUDR and tropical country sets using CI as baseline."""

# CI baseline: 322,463 km² → 22.22 GB compressed int16 49-band 10m
CI_AREA_KM2 = 322_463
CI_SIZE_GB = 22.22
KB_PER_KM2 = (CI_SIZE_GB * 1e6) / CI_AREA_KM2  # ~68.9 KB/km²

# Country areas in km² (approximate, from standard references)
COUNTRY_AREAS = {
    # Major EUDR commodity producers
    "BR": ("Brazil", 8_515_767),
    "ID": ("Indonesia", 1_904_569),
    "CD": ("DR Congo", 2_344_858),
    "CO": ("Colombia", 1_141_748),
    "PY": ("Paraguay", 406_752),
    "BO": ("Bolivia", 1_098_581),
    "CI": ("Côte d'Ivoire", 322_463),
    "GH": ("Ghana", 238_533),
    "CM": ("Cameroon", 475_442),
    "MY": ("Malaysia", 330_803),
    "PG": ("Papua New Guinea", 462_840),
    "MM": ("Myanmar", 676_578),
    "VN": ("Vietnam", 331_212),
    "PE": ("Peru", 1_285_216),
    "EC": ("Ecuador", 283_561),
    "HN": ("Honduras", 112_492),
    "GT": ("Guatemala", 108_889),
    "NG": ("Nigeria", 923_768),
    "TH": ("Thailand", 513_120),
    "ET": ("Ethiopia", 1_104_300),
    "UG": ("Uganda", 241_038),
    "LA": ("Laos", 236_800),
    "KH": ("Cambodia", 181_035),
    "AR": ("Argentina", 2_780_400),
    "MX": ("Mexico", 1_964_375),
    "CR": ("Costa Rica", 51_100),
    "SL": ("Sierra Leone", 71_740),
    "LR": ("Liberia", 111_369),
    "TZ": ("Tanzania", 947_303),
    # Additional tropical countries (23.5°N - 23.5°S) not already above
    "CG": ("Congo Republic", 342_000),
    "GA": ("Gabon", 267_668),
    "GQ": ("Equatorial Guinea", 28_051),
    "CF": ("Central African Republic", 622_984),
    "AO": ("Angola", 1_246_700),
    "MZ": ("Mozambique", 801_590),
    "MG": ("Madagascar", 587_041),
    "ZM": ("Zambia", 752_618),
    "ZW": ("Zimbabwe", 390_757),
    "MW": ("Malawi", 118_484),
    "GN": ("Guinea", 245_857),
    "GW": ("Guinea-Bissau", 36_125),
    "SN": ("Senegal", 196_722),
    "GM": ("Gambia", 11_295),
    "ML": ("Mali", 1_240_192),  # partly tropical
    "BF": ("Burkina Faso", 274_200),
    "BJ": ("Benin", 112_622),
    "TG": ("Togo", 56_785),
    "NE": ("Niger", 1_267_000),  # mostly non-tropical
    "TD": ("Chad", 1_284_000),  # partly tropical
    "SD": ("Sudan", 1_861_484),  # partly tropical
    "SS": ("South Sudan", 644_329),
    "SO": ("Somalia", 637_657),
    "KE": ("Kenya", 580_367),
    "RW": ("Rwanda", 26_338),
    "BI": ("Burundi", 27_834),
    "DJ": ("Djibouti", 23_200),
    "ER": ("Eritrea", 117_600),
    "IN": ("India", 3_287_263),  # partly tropical
    "LK": ("Sri Lanka", 65_610),
    "BD": ("Bangladesh", 147_570),
    "NP": ("Nepal", 147_181),  # partly tropical
    "PH": ("Philippines", 300_000),
    "TL": ("Timor-Leste", 14_874),
    "BN": ("Brunei", 5_765),
    "SG": ("Singapore", 733),
    "FJ": ("Fiji", 18_274),
    "SB": ("Solomon Islands", 28_896),
    "VU": ("Vanuatu", 12_189),
    "WS": ("Samoa", 2_842),
    "TO": ("Tonga", 747),
    "VE": ("Venezuela", 916_445),
    "GY": ("Guyana", 214_969),
    "SR": ("Suriname", 163_820),
    "GF": ("French Guiana", 83_534),
    "PA": ("Panama", 75_417),
    "BZ": ("Belize", 22_966),
    "NI": ("Nicaragua", 130_373),
    "SV": ("El Salvador", 21_041),
    "CU": ("Cuba", 109_884),
    "HT": ("Haiti", 27_750),
    "DO": ("Dominican Republic", 48_671),
    "JM": ("Jamaica", 10_991),
    "TT": ("Trinidad & Tobago", 5_130),
    "PR": ("Puerto Rico", 9_104),
}

# Define groups
EUDR_COUNTRIES = {
    "BR",
    "ID",
    "CD",
    "CO",
    "PY",
    "BO",
    "CI",
    "GH",
    "CM",
    "MY",
    "PG",
    "MM",
    "VN",
    "PE",
    "EC",
    "HN",
    "GT",
    "NG",
    "TH",
    "ET",
    "UG",
    "LA",
    "KH",
    "AR",
    "MX",
    "CR",
    "SL",
    "LR",
    "TZ",
}

# Tropical belt (all countries significantly between 23.5°N and 23.5°S)
TROPICAL_COUNTRIES = set(COUNTRY_AREAS.keys())


def estimate_gb(area_km2):
    return (area_km2 * KB_PER_KM2) / 1e6


def print_group(title, iso_set):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")
    print(f"  {'ISO2':<6} {'Country':<30} {'Area (km²)':>12} {'Est. COG (GB)':>14}")
    print(f"  {'-'*6} {'-'*30} {'-'*12} {'-'*14}")

    total_area = 0
    total_gb = 0
    rows = []
    for iso, (name, area) in sorted(COUNTRY_AREAS.items(), key=lambda x: -x[1][1]):
        if iso in iso_set:
            gb = estimate_gb(area)
            rows.append((iso, name, area, gb))
            total_area += area
            total_gb += gb

    for iso, name, area, gb in rows:
        print(f"  {iso:<6} {name:<30} {area:>12,} {gb:>13.1f}")

    print(f"  {'-'*6} {'-'*30} {'-'*12} {'-'*14}")
    print(
        f"  {'TOTAL':<6} {f'{len(rows)} countries':<30} {total_area:>12,} {total_gb:>13.1f}"
    )
    return total_area, total_gb


print(f"Baseline: CI = {CI_AREA_KM2:,} km² → {CI_SIZE_GB} GB → {KB_PER_KM2:.1f} KB/km²")
print(f"(int16, 49 bands, 10m resolution, cloud-optimized GeoTIFF)")

a1, g1 = print_group(
    "1) EUDR HIGH/STANDARD RISK COMMODITY COUNTRIES (29)", EUDR_COUNTRIES
)
a2, g2 = print_group("2) ALL TROPICAL COUNTRIES (23.5°N–23.5°S)", TROPICAL_COUNTRIES)

overlap = EUDR_COUNTRIES & TROPICAL_COUNTRIES
a3, g3 = print_group("3) OVERLAP: EUDR ∩ TROPICAL", overlap)

tropical_only = TROPICAL_COUNTRIES - EUDR_COUNTRIES
a4, g4 = print_group("EXTRA: Tropical countries NOT in EUDR list", tropical_only)

print(f"\n{'='*80}")
print(f"  SUMMARY")
print(f"{'='*80}")
print(
    f"  1) EUDR countries:          {g1:>8.1f} GB  ({a1:>12,} km²)  {len(EUDR_COUNTRIES)} countries"
)
print(
    f"  2) All tropical:            {g2:>8.1f} GB  ({a2:>12,} km²)  {len(TROPICAL_COUNTRIES)} countries"
)
print(
    f"  3) EUDR ∩ Tropical:         {g3:>8.1f} GB  ({a3:>12,} km²)  {len(overlap)} countries"
)
print(
    f"  4) Tropical-only additions: {g4:>8.1f} GB  ({a4:>12,} km²)  {len(tropical_only)} countries"
)

print(
    f"\n  Note: These are estimates based on CI's compression ratio ({KB_PER_KM2:.1f} KB/km²)."
)
print(f"  Actual sizes depend on data density (forest cover, etc). Countries with")
print(f"  large desert/ocean areas (AR, MX, NE, TD, SD) will compress much better.")
print(f"  Real totals could be 30-50% lower for arid/sparse countries.")

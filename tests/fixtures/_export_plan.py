"""Tiered COG export plan for EUDR countries."""

KB_PER_KM2 = 68.9  # from CI baseline

data = {
    "CI": (322_463, 22.2, "1 nat. ds (cocoa) - DONE"),
    "CO": (1_141_748, 0, "2 nat. ds (IDEAM forest)"),
    "CM": (475_442, 0, "1 nat. ds (treecover)"),
    "BR": (8_515_767, 0, "25 nat. ds (PRODES, MapBiomas, INPE...)"),
    "GH": (238_533, 0, "cocoa #2 after CI"),
    "ID": (1_904_569, 0, "palm oil, rubber, timber"),
    "MY": (330_803, 0, "palm oil, rubber"),
    "EC": (283_561, 0, "cocoa, coffee, palm oil"),
    "ET": (1_104_300, 0, "coffee"),
    "PG": (462_840, 0, "palm oil, timber"),
    "VN": (331_212, 0, "rubber, coffee"),
    "PE": (1_285_216, 0, "coffee, timber"),
    "GT": (108_889, 0, "coffee, palm oil"),
    "HN": (112_492, 0, "coffee, palm oil"),
    "NG": (923_768, 0, "cocoa, palm oil"),
    "TH": (513_120, 0, "rubber, palm oil"),
    "MM": (676_578, 0, "rubber, timber"),
    "KH": (181_035, 0, "rubber, timber"),
    "LA": (236_800, 0, "rubber, timber"),
    "PY": (406_752, 0, "soy, cattle"),
    "BO": (1_098_581, 0, "soy, cattle"),
    "AR": (2_780_400, 0, "soy, cattle"),
    "CD": (2_344_858, 0, "timber, cocoa"),
    "MX": (1_964_375, 0, "coffee, cattle, timber"),
    "TZ": (947_303, 0, "coffee, timber"),
    "UG": (241_038, 0, "coffee, cocoa"),
    "LR": (111_369, 0, "palm oil, rubber, cocoa"),
    "SL": (71_740, 0, "cocoa"),
    "CR": (51_100, 0, "coffee, palm oil"),
}

tiers = [
    (
        "TIER 1 - Quick wins: national datasets + top demand (<80 GB each)",
        ["GH", "CO", "CM", "EC"],
    ),
    (
        "TIER 2 - Small countries (<25 GB each)",
        ["MY", "VN", "GT", "HN", "CR", "LR", "SL", "UG", "KH", "LA"],
    ),
    (
        "TIER 3 - Medium countries (25-65 GB each)",
        ["ID", "ET", "PG", "TH", "NG", "MM", "PY"],
    ),
    ("TIER 4 - Large countries (65-135 GB each)", ["PE", "BO", "TZ", "MX", "CD"]),
    ("TIER 5 - Massive (>190 GB each)", ["BR", "AR"]),
]

grand_total = 0
for tier_name, tier_codes in tiers:
    total = 0
    print(f"\n{tier_name}")
    print(f"  {'ISO2':<5} {'Area':>13}  {'Est. GB':>8}  Notes")
    print(f"  {'-'*5} {'-'*13}  {'-'*8}  {'-'*40}")
    for iso in tier_codes:
        area, done_gb, notes = data[iso]
        gb = done_gb if done_gb else round(area * KB_PER_KM2 / 1e6, 1)
        total += gb
        marker = " << DONE" if done_gb else ""
        print(f"  {iso:<5} {area:>10,} km2  {gb:>7.1f}   {notes}{marker}")
    print(f"  {'':>19}  {'-'*8}")
    print(f"  {'Subtotal':>19}  {total:>7.1f} GB")
    grand_total += total

print(f"\n{'='*70}")
print(
    f"  GRAND TOTAL (29 EUDR countries): {grand_total:,.0f} GB (~{grand_total/1000:.1f} TB)"
)
print(f"  CI already done:                 {data['CI'][1]:.1f} GB")
print(
    f"  Remaining:                       {grand_total - data['CI'][1]:,.0f} GB (~{(grand_total - data['CI'][1])/1000:.1f} TB)"
)

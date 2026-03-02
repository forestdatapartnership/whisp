import csv

with open("src/openforis_whisp/parameters/lookup_gee_datasets.csv") as f:
    rows = list(csv.DictReader(f))

dynamic_funcs = {
    "g_radd_after_2020_prep",
    "g_glad_dist_after_2020_prep",
    "g_glad_l_after_2020_prep",
    "g_glad_s2_after_2020_prep",
    "g_modis_fire_after_2020_prep",
    "nbr_deter_amazon_after_2020_prep",
}

print("Dynamic bands (from CSV):")
for r in rows:
    func = r["corresponding_variable"]
    if func in dynamic_funcs:
        name = r["name"]
        print(f"  {name:50s} {func}")

print()

static_count = 0
for r in rows:
    func = r["corresponding_variable"]
    if func not in dynamic_funcs:
        static_count += 1

print(f"Static bands: {static_count}")
print(f"Dynamic bands: {len(rows) - static_count}")

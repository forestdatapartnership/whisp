"""Quick debug: test weighted_sum + median on 10m TIF."""

from exactextract import exact_extract
import traceback

TIF = "tests/fixtures/local_10m_test/feature_1.tif"
GEO = "tests/fixtures/test_10ha.geojson"
AREA = "tests/fixtures/local_10m_test/area_int32.tif"

try:
    df_ws = exact_extract(
        rast=TIF, vec=GEO, ops=["weighted_sum"], weights=AREA, output="pandas"
    )
    print(f"ws OK: {len(df_ws.columns)} cols")
except Exception as e:
    traceback.print_exc()

try:
    cat_ops = [
        "admin_code_median=median(band_196)",
        "waterbody_median=median(band_197)",
    ]
    df_med = exact_extract(rast=TIF, vec=GEO, ops=cat_ops, output="pandas")
    print(f"med OK: {list(df_med.columns)}")
except Exception as e:
    traceback.print_exc()

# Show key results
area_col = "band_1_weight_weighted_sum"
gfc_col = "band_45_weight_weighted_sum"
cocoa_col = "band_195_weight_weighted_sum"

print(f"Area = {df_ws[area_col].values[0] / 10000:.4f} ha")
print(f"GFC_TC_2020 = {df_ws[gfc_col].values[0] / 10000:.4f} ha")
print(f"Cocoa = {df_ws[cocoa_col].values[0] / 10000:.4f} ha")
print(f"admin = {df_med['admin_code_median'].values[0]:.0f}")
print(f"waterbody = {df_med['waterbody_median'].values[0]:.0f}")

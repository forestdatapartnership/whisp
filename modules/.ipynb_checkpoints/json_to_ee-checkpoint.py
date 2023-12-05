
def json_to_feature_with_id(poly_json,geo_id,geo_id_column):
    """converts json into a feature with a specified id column"""
    return ee.Feature(ee.Geometry.Polygon(poly_json),ee.Dictionary([geo_id_column,geo_id]))

import json

import pandas as pd

try:
    import pandera.pandas as pa
except (ImportError, ModuleNotFoundError):
    import pandera as pa

from openforis_whisp.reformat import validate_dataframe


def test_shapes_from_earth_engine_are_written_as_json():
    # Earth Engine returns each shape as a dict. As text it must be JSON (double quotes), as local
    # mode writes it, not the way Python prints a dict with single quotes (#323)
    point = {"type": "Point", "coordinates": [32.4185, 0.0035]}
    schema = pa.DataFrameSchema({"geo": pa.Column(str, coerce=True)})
    out = validate_dataframe(pd.DataFrame({"geo": [point]}), schema)
    assert json.loads(out["geo"].iloc[0]) == point

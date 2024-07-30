import pandera as pa
from pandera.typing import DataFrame, Index, Series


class DfGEEDatasetsSchema(pa.DataFrameModel):

    dataset_id: Index[int]
    dataset_order: Series[int]
    dataset_name: Series[str]
    presence_only_flag: Series[int | bool]
    exclude: Series[int | bool]
    theme: Series[str]
    use_for_risktheme: Series[str]


DfGEEDatasetsType = DataFrame[DfGEEDatasetsSchema]

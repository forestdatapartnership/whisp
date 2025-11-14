# Support both old and new pandera import paths
try:
    import pandera.pandas as pa
    from pandera.typing.pandas import DataFrame, Series
except (ImportError, ModuleNotFoundError):
    import pandera as pa
    from pandera.typing import DataFrame, Series

# Define a schema for validating a DataFrame related to GEE (Google Earth Engine) datasets.
class DataLookupSchema(pa.DataFrameModel):

    # Ensure the name is unique
    name: Series[str] = pa.Field(unique=True, nullable=False)
    order: Series[int] = pa.Field(nullable=False)
    theme: Series[str] = pa.Field(nullable=True)

    # Define fields without checks
    use_for_risk: Series[pa.Int | bool] = pa.Field(nullable=True)
    exclude_from_output: Series[pa.Int | bool] = pa.Field(nullable=False)

    # Define col_type without checks
    col_type: Series[str] = pa.Field(nullable=False)

    is_nullable: Series[pa.Int | bool] = pa.Field(nullable=False)
    is_required: Series[pa.Int | bool] = pa.Field(nullable=False)

    corresponding_variable: Series[str] = pa.Field(nullable=True)


# For type annotation - not used for validation yet
data_lookup_type = DataFrame[DataLookupSchema]


# checks (below) not working currently so using without


# import pandera as pa
# from pandera.typing import DataFrame, Series

# # Define a schema for validating a DataFrame related to GEE (Google Earth Engine) datasets.
# class DataLookupSchema(pa.DataFrameModel):

#     # Ensure the name is unique
#     name: Series[str] = pa.Field(unique=True, nullable=False)
#     order: Series[int] = pa.Field(nullable=False)
#     theme: Series[str] = pa.Field(nullable=True)

#     # Restrict use_for_risk to 0 or 1, either as int or bool
#     use_for_risk: Series[pa.Int | bool] = pa.Field(
#         checks=pa.Check.isin([0, 1]),  # Using 'checks' keyword argument
#         nullable=True
#     )

#     # Restrict exclude_from_input and exclude_from_output to 0 or 1
#     exclude_from_input: Series[pa.Int | bool] = pa.Field(
#         checks=pa.Check.isin([0, 1]),
#         nullable=False
#     )
#     exclude_from_output: Series[pa.Int | bool] = pa.Field(
#         checks=pa.Check.isin([0, 1]),
#         nullable=False
#     )

#     # Restrict col_type to specific values
#     col_type: Series[str] = pa.Field(
#         checks=pa.Check.isin(['int', 'int64', 'string', 'float32', 'float64', 'bool']),
#         nullable=False
#     )

#     is_nullable: Series[pa.Int | bool] = pa.Field(
#         checks=pa.Check.isin([0, 1]),
#         nullable=False
#     )
#     is_required: Series[pa.Int | bool] = pa.Field(
#         checks=pa.Check.isin([0, 1]),
#         nullable=False
#     )

#     corresponding_variable: Series[str] = pa.Field(nullable=True)

# # For type annotation
# data_lookup_type = DataFrame[DataLookupSchema]

"""ods works with datamatrices object. This utilities converts
them to an pandas dataframe for ease of use."""

import pandas as pd

import asam_odsbox.proto.ods_pb2 as ods

# pylint: disable=E1101, C0116


def __get_datamatrix_column_values(column: ods.DataMatrix.Column):
    if column.WhichOneof("ValuesOneOf") is None:
        return None

    if column.HasField("string_array"):
        return column.string_array.values
    if column.HasField("long_array"):
        return column.long_array.values
    if column.HasField("float_array"):
        return column.float_array.values
    if column.HasField("boolean_array"):
        return column.boolean_array.values
    if column.HasField("byte_array"):
        return column.byte_array.values
    if column.HasField("double_array"):
        return column.double_array.values
    if column.HasField("longlong_array"):
        return column.longlong_array.values
    # vector attributes. Look for the additional 's'
    if column.HasField("long_arrays"):
        return column.long_arrays.values

    raise ValueError(f"DataType {column.WhichOneof('ValuesOneOf')} not handled!")


def to_pandas(data_matrices: ods.DataMatrices) -> pd.DataFrame:
    """Converts data in an ASAM ODS DataMatrices into a pandas DataFrame."""
    if 0 == len(data_matrices.matrices):
        return pd.DataFrame()

    if 0 == len(data_matrices.matrices[0].columns):
        return pd.DataFrame()

    column_dict = {}

    for matrix in data_matrices.matrices:
        for column in matrix.columns:
            values = __get_datamatrix_column_values(column)
            # The flags are ignored here. There might be NULL in here. Check column IsNull for this.
            column_dict[matrix.name + "." + column.name] = [] if values is None else list(values)

    return pd.DataFrame(column_dict)
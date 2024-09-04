"""ods works with datamatrices object. This utilities converts
them to an pandas dataframe for ease of use."""

import pandas as pd
import numpy as np

import odsbox.proto.ods_pb2 as ods


def unknown_array_values(
    unknown_array: ods.DataMatrix.Column.UnknownArray,
) -> list:
    """
    Get the values of an UnknownArray as list

    :param ods.DataMatrix.Column.UnknownArray unknown_array: ASAM ODS unknown array to transport array of any
    :raises ValueError: If standard is extended by new types
    :return list | np.array: list containing the values of the Unknown array.
    """
    if unknown_array.WhichOneof("UnknownOneOf") is None:
        return []

    if unknown_array.HasField("string_array"):
        return list(unknown_array.string_array.values)
    if unknown_array.HasField("long_array"):
        return list(unknown_array.long_array.values)
    if unknown_array.HasField("float_array"):
        if ods.DT_COMPLEX == unknown_array.data_type:
            return np.array(unknown_array.float_array.values, dtype=np.float32).view(np.complex64)
        return list(unknown_array.float_array.values)
    if unknown_array.HasField("boolean_array"):
        return list(unknown_array.boolean_array.values)
    if unknown_array.HasField("byte_array"):
        return list(unknown_array.byte_array.values)
    if unknown_array.HasField("double_array"):
        if ods.DT_DCOMPLEX == unknown_array.data_type:
            return np.array(unknown_array.double_array.values, dtype=np.float64).view(np.complex128)
        return list(unknown_array.double_array.values)
    if unknown_array.HasField("longlong_array"):
        return list(unknown_array.longlong_array.values)
    if unknown_array.HasField("bytestr_array"):
        return list(unknown_array.bytestr_array.values)

    raise ValueError(f"DataType {unknown_array.WhichOneof('UnknownOneOf')} not handled in python code!")


def __get_datamatrix_column_values(column: ods.DataMatrix.Column) -> list:
    if column.WhichOneof("ValuesOneOf") is None:
        return None

    if column.HasField("string_array"):
        rv = list(column.string_array.values)
        if ods.DT_EXTERNALREFERENCE == column.data_type:
            return list(zip(rv[::3], rv[1::3], rv[2::3]))
        return rv
    if column.HasField("long_array"):
        return list(column.long_array.values)
    if column.HasField("float_array"):
        if ods.DT_COMPLEX == column.data_type:
            return np.array(column.float_array.values, dtype=np.float32).view(np.complex64)
        return list(column.float_array.values)
    if column.HasField("boolean_array"):
        return list(column.boolean_array.values)
    if column.HasField("byte_array"):
        return list(column.byte_array.values)
    if column.HasField("double_array"):
        if ods.DT_DCOMPLEX == column.data_type:
            return np.array(column.double_array.values, dtype=np.float64).view(np.complex128)
        return list(column.double_array.values)
    if column.HasField("longlong_array"):
        return list(column.longlong_array.values)
    if column.HasField("bytestr_array"):
        return list(column.bytestr_array.values)
    # vector attributes. Look for the additional 's'
    if column.HasField("string_arrays"):
        if ods.DS_EXTERNALREFERENCE == column.data_type:
            return [
                list(zip(item.values[::3], item.values[1::3], item.values[2::3]))
                for item in column.string_arrays.values
            ]
        return [list(item.values) for item in column.string_arrays.values]
    if column.HasField("long_arrays"):
        return [list(item.values) for item in column.long_arrays.values]
    if column.HasField("float_arrays"):
        if ods.DS_COMPLEX == column.data_type:
            return [(np.array(item.values, dtype=np.float32).view(np.complex64)) for item in column.float_arrays.values]
        return [list(item.values) for item in column.float_arrays.values]
    if column.HasField("boolean_arrays"):
        return [list(item.values) for item in column.boolean_arrays.values]
    if column.HasField("byte_arrays"):
        return [list(item.values) for item in column.byte_arrays.values]
    if column.HasField("double_arrays"):
        if ods.DS_DCOMPLEX == column.data_type:
            return [
                (np.array(item.values, dtype=np.float64).view(np.complex128)) for item in column.double_arrays.values
            ]
        return [list(item.values) for item in column.double_arrays.values]
    if column.HasField("longlong_arrays"):
        return [list(item.values) for item in column.longlong_arrays.values]
    if column.HasField("bytestr_arrays"):
        return [list(item.values) for item in column.bytestr_arrays.values]
    if column.HasField("unknown_arrays"):
        return [unknown_array_values(item) for item in column.unknown_arrays.values]

    raise ValueError(f"DataType '{column.WhichOneof('ValuesOneOf')}' not handled!")


def to_pandas(data_matrices: ods.DataMatrices) -> pd.DataFrame:
    """
    Converts data in an ASAM ODS DataMatrices into a pandas DataFrame.

    :param ods.DataMatrices data_matrices: matrices to be converted.
    :return pd.DataFrame: A pandas dataframe containing all the single matrices in a single frame. The
                          columns are named by the schema `ENTITY_NAME.ATTRIBUTE_NAME`.
    """
    if 0 == len(data_matrices.matrices):
        return pd.DataFrame()

    if 0 == len(data_matrices.matrices[0].columns):
        return pd.DataFrame()

    column_dict = {}

    for matrix in data_matrices.matrices:
        for column in matrix.columns:
            # The flags are ignored here. There might be NULL in here. Check `column.is_null` for this.
            values = __get_datamatrix_column_values(column)
            column_dict[matrix.name + "." + column.name] = [] if values is None else values

    return pd.DataFrame(column_dict)

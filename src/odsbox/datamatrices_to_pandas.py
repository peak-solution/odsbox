"""ods works with datamatrices object. This utilities converts
them to an pandas DataFrame for ease of use."""

from __future__ import annotations

import pandas as pd
import numpy as np

import odsbox.proto.ods_pb2 as ods
from odsbox.model_cache import ModelCache
from odsbox.asam_time import to_pd_timestamp


def unknown_array_values(
    unknown_array: ods.DataMatrix.Column.UnknownArray,
    date_as_timestamp: bool = False,
) -> list:
    """
    Get the values of an UnknownArray as list

    :param ods.DataMatrix.Column.UnknownArray unknown_array: ASAM ODS unknown array to transport array of any
    :param bool date_as_timestamp: columns of type DT_DATE or DS_DATE are returned as string.
                                   If this is set to True the strings are converted to pandas Timestamp.

    :raises ValueError: If standard is extended by new types
    :return list | np.array: list containing the values of the Unknown array.
    """
    if unknown_array.WhichOneof("UnknownOneOf") is None:
        return []

    if unknown_array.HasField("string_array"):
        if date_as_timestamp and ods.DT_DATE == unknown_array.data_type:
            return list(map(lambda x: to_pd_timestamp(x), unknown_array.string_array.values))
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


def __adjust_enums(
    model_cache: ModelCache | None, enumeration: ods.Model.Enumeration | None, values: list[int] | None
) -> list[int] | list[str] | None:
    if values is None or enumeration is None or model_cache is None:
        return values

    return list(map(lambda x: model_cache.enumeration_value_to_key(enumeration, x), values))


def __get_datamatrix_column_values(
    column: ods.DataMatrix.Column,
    model_cache: ModelCache | None,
    enumeration: ods.Model.Enumeration | None,
    date_as_timestamp: bool,
) -> list | None:
    if column.WhichOneof("ValuesOneOf") is None:
        return None

    if column.HasField("string_array"):
        rv = list(column.string_array.values)
        if ods.DT_EXTERNALREFERENCE == column.data_type:
            return list(zip(rv[::3], rv[1::3], rv[2::3]))
        if date_as_timestamp and ods.DT_DATE == column.data_type:
            return list(map(lambda x: to_pd_timestamp(x), rv))
        return rv
    if column.HasField("long_array"):
        if ods.DT_ENUM == column.data_type:
            return __adjust_enums(model_cache, enumeration, list(column.long_array.values))
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
        if date_as_timestamp and ods.DS_DATE == column.data_type:
            return [list(map(lambda x: to_pd_timestamp(x), item.values)) for item in column.string_arrays.values]
        return [list(item.values) for item in column.string_arrays.values]
    if column.HasField("long_arrays"):
        if ods.DS_ENUM == column.data_type:
            return [__adjust_enums(model_cache, enumeration, list(item.values)) for item in column.long_arrays.values]
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
        return [unknown_array_values(item, date_as_timestamp) for item in column.unknown_arrays.values]

    raise ValueError(f"DataType '{column.WhichOneof('ValuesOneOf')}' not handled!")


def __get_datamatrix_column_values_ex(
    column: ods.DataMatrix.Column,
    model_cache: ModelCache | None,
    enum_as_string: bool,
    entity: ods.Model.Entity | None,
    date_as_timestamp: bool,
) -> list:
    enumeration = None
    if (
        enum_as_string
        and entity is not None
        and model_cache is not None
        and column.data_type in [ods.DT_ENUM, ods.DS_ENUM]
    ):
        attribute = model_cache.attribute_no_throw(entity, column.name)
        if attribute is not None:
            if attribute.enumeration is not None:
                enumeration = model_cache.model().enumerations[attribute.enumeration]

    values = __get_datamatrix_column_values(column, model_cache, enumeration, date_as_timestamp)
    return_values = [] if values is None else values

    return return_values


def to_pandas(
    data_matrices: ods.DataMatrices,
    model_cache: ModelCache | None = None,
    enum_as_string: bool = False,
    date_as_timestamp: bool = False,
    name_separator: str = ".",
) -> pd.DataFrame:
    """
    Converts data in an ASAM ODS DataMatrices into a pandas DataFrame.

    :param ods.DataMatrices data_matrices: matrices to be converted.
    :param ModelCache | None model_cache: ModelCache is used to do enum conversion
    :param bool enum_as_string: columns of type DT_ENUM or DS_ENUM are returned as int values.
                                If this is set to True the model_cache is used to map the int values
                                to the corresponding string values.
    :param bool date_as_timestamp: columns of type DT_DATE or DS_DATE are returned as string.
                                   If this is set to True the strings are converted to pandas Timestamp.
    :param str name_separator: separator used to concatenate entity and attribute names to define column name.

    :return pd.DataFrame: A pandas DataFrame containing all the single matrices in a single frame. The
                          columns are named by the schema `ENTITY_NAME.ATTRIBUTE_NAME[.AGGREGATE]`.
    """
    if 0 == len(data_matrices.matrices):
        return pd.DataFrame()

    if 0 == len(data_matrices.matrices[0].columns):
        return pd.DataFrame()

    column_dict = {}

    for matrix in data_matrices.matrices:
        entity = model_cache.entity(matrix.name) if model_cache is not None else None
        for column in matrix.columns:
            aggregate_postfix = (
                ""
                if ods.AggregateEnum.AG_NONE == column.aggregate
                else name_separator + ods.AggregateEnum.Name(column.aggregate)
            )
            column_name = f"{matrix.name}{name_separator}{column.name}{aggregate_postfix}"
            # The flags are ignored here. There might be NULL in here. Check `column.is_null` for this.
            column_dict[column_name] = __get_datamatrix_column_values_ex(
                column, model_cache, enum_as_string, entity, date_as_timestamp
            )

    return pd.DataFrame(column_dict)

"""converts a submatrix delivered as a datamatrix protobuf object into a pandas dataframe."""

from typing import TYPE_CHECKING

import pandas as pd

import odsbox.proto.ods_pb2 as ods
from odsbox.datamatrices_to_pandas import unknown_array_values

if TYPE_CHECKING:
    from .con_i import ConI


def __get_column_from_dms(dms: ods.DataMatrices, entity: ods.Model.Entity, name: str) -> ods.DataMatrix.Column:
    for matrix in dms.matrices:
        if entity.aid == matrix.aid:
            for column in matrix.columns:
                if name == column.name:
                    return column

    raise ValueError(f"Matrices do not contain column for {entity.name}.{name}")


def __convert_bulk_to_pandas_data_frame(
    con_i: "ConI", local_column_id_lookup: dict, localcolumn_bulk: ods.DataMatrices
) -> pd.DataFrame:
    if 1 != len(localcolumn_bulk.matrices):
        raise ValueError("Only allowed to have one matrix")

    values_matrix = localcolumn_bulk.matrices[0]
    start_index = values_matrix.row_start

    local_column_entity = con_i.mc.entity_by_base_name("AoLocalColumn")
    local_column_id_name = con_i.mc.attribute_by_base_name(local_column_entity.name, "id").name
    local_column_values_name = con_i.mc.attribute_by_base_name(local_column_entity.name, "values").name

    id_column = None
    values_column = None
    for column in values_matrix.columns:
        if column.name == local_column_id_name:
            id_column = column
        elif column.name == local_column_values_name:
            values_column = column

    if None is id_column or None is values_column:
        raise ValueError("id and values column must be present")

    id_array = id_column.longlong_array.values
    values_array = values_column.unknown_arrays.values

    if len(id_array) != len(values_array):
        raise ValueError("size of values differ")

    column_dict = {}
    number_of_rows = 0

    independent_local_column_name = None
    for local_column_id, local_column_values in zip(id_array, values_array):
        local_column_meta = local_column_id_lookup[local_column_id]
        local_column_name = local_column_meta["name"]
        local_column_values = unknown_array_values(local_column_values)
        local_column_len = len(local_column_values)
        number_of_rows = local_column_len if local_column_len > number_of_rows else number_of_rows
        column_dict[local_column_name] = local_column_values
        if local_column_meta["independent"]:
            independent_local_column_name = local_column_name

    for local_column_id in id_array:
        local_column_meta = local_column_id_lookup[local_column_id]
        local_column_name = local_column_meta["name"]
        if 2 == local_column_meta["sequence_representation"]:
            generation_parameters = column_dict[local_column_name]
            offset = generation_parameters[0]
            factor = generation_parameters[1]
            column_dict[local_column_name] = [
                offset + x * factor for x in range(0 + start_index, number_of_rows + start_index)
            ]

    rv = pd.DataFrame(column_dict)
    if None is not independent_local_column_name:
        rv.set_index(independent_local_column_name)

    return rv


def submatrix_to_pandas(con_i: "ConI", submatrix_iid: int) -> pd.DataFrame:
    """
    Loads an ASAM ODS SubMatrix and returns it as a pandas DataFrame.

    :param ConI con_i: ASAM ODS server session.
    :param int submatrix_iid: id of an submatrix to be retrieved.
    :return pd.DataFrame: A pandas dataframe containing the values of the localcolumn as pandas columns.
                          The name of the localcolumn is used as pandas column name. The flags are ignored.
    """
    local_column_entity = con_i.mc.entity_by_base_name("AoLocalColumn")

    lc_meta_select_statement = ods.SelectStatement()
    column = lc_meta_select_statement.columns.add()
    column.aid = local_column_entity.aid
    column.attribute = con_i.mc.attribute_by_base_name(local_column_entity, "id").name
    column = lc_meta_select_statement.columns.add()
    column.aid = local_column_entity.aid
    column.attribute = con_i.mc.attribute_by_base_name(local_column_entity, "name").name
    column = lc_meta_select_statement.columns.add()
    column.aid = local_column_entity.aid
    column.attribute = con_i.mc.attribute_by_base_name(local_column_entity, "independent").name
    column = lc_meta_select_statement.columns.add()
    column.aid = local_column_entity.aid
    column.attribute = con_i.mc.attribute_by_base_name(local_column_entity, "sequence_representation").name

    condition_item = lc_meta_select_statement.where.add()
    condition_item.condition.aid = local_column_entity.aid
    condition_item.condition.attribute = con_i.mc.relation_by_base_name(local_column_entity, "submatrix").name
    condition_item.condition.operator = ods.SelectStatement.ConditionItem.Condition.OperatorEnum.OP_EQ
    condition_item.condition.longlong_array.values.append(submatrix_iid)

    lc_meta_dms = con_i.data_read(lc_meta_select_statement)

    id_column = __get_column_from_dms(
        lc_meta_dms,
        local_column_entity,
        con_i.mc.attribute_by_base_name(local_column_entity, "id").name,
    )
    name_column = __get_column_from_dms(
        lc_meta_dms,
        local_column_entity,
        con_i.mc.attribute_by_base_name(local_column_entity, "name").name,
    )
    independent_column = __get_column_from_dms(
        lc_meta_dms,
        local_column_entity,
        con_i.mc.attribute_by_base_name(local_column_entity, "independent").name,
    )
    sequence_representation_column = __get_column_from_dms(
        lc_meta_dms,
        local_column_entity,
        con_i.mc.attribute_by_base_name(local_column_entity, "sequence_representation").name,
    )

    localcolumn_id_lookup = {}

    for id_value, name_value, independent_value, sequence_representation_value in zip(
        id_column.longlong_array.values,
        name_column.string_array.values,
        independent_column.long_array.values,
        sequence_representation_column.long_array.values,
    ):
        localcolumn_id_lookup[id_value] = {
            "name": name_value,
            "id": id_value,
            "independent": 0 != independent_value,
            "sequence_representation": sequence_representation_value,
        }

    localcolumn_bulk_select_statement = ods.SelectStatement()
    column = localcolumn_bulk_select_statement.columns.add()
    column.aid = local_column_entity.aid
    column.attribute = con_i.mc.attribute_by_base_name(local_column_entity, "id").name
    column = localcolumn_bulk_select_statement.columns.add()
    column.aid = local_column_entity.aid
    column.attribute = con_i.mc.attribute_by_base_name(local_column_entity, "values").name
    condition_item = localcolumn_bulk_select_statement.where.add()
    condition_item.condition.aid = local_column_entity.aid
    condition_item.condition.attribute = con_i.mc.relation_by_base_name(local_column_entity, "submatrix").name
    condition_item.condition.operator = ods.SelectStatement.ConditionItem.Condition.OperatorEnum.OP_EQ
    condition_item.condition.longlong_array.values.append(submatrix_iid)

    localcolumn_bulk = con_i.data_read(localcolumn_bulk_select_statement)

    return __convert_bulk_to_pandas_data_frame(con_i, localcolumn_id_lookup, localcolumn_bulk)

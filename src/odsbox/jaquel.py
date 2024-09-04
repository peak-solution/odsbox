"""Used to convert JAQueL queries to ASAM ODS SelectStatements"""

import datetime
import json
import re
from typing import Tuple, List, Any

import odsbox.proto.ods_pb2 as ods

OperatorEnum = ods.SelectStatement.ConditionItem.Condition.OperatorEnum

_jo_aggregates = {
    "$none": ods.AggregateEnum.AG_NONE,
    "$count": ods.AggregateEnum.AG_COUNT,
    "$dcount": ods.AggregateEnum.AG_DCOUNT,
    "$min": ods.AggregateEnum.AG_MIN,
    "$max": ods.AggregateEnum.AG_MAX,
    "$avg": ods.AggregateEnum.AG_AVG,
    "$sum": ods.AggregateEnum.AG_SUM,
    "$distinct": ods.AggregateEnum.AG_DISTINCT,
    "$point": ods.AggregateEnum.AG_VALUES_POINT,
    "$ia": ods.AggregateEnum.AG_INSTANCE_ATTRIBUTE,
}
_jo_operators = {
    "$eq": OperatorEnum.OP_EQ,
    "$neq": OperatorEnum.OP_NEQ,
    "$lt": OperatorEnum.OP_LT,
    "$gt": OperatorEnum.OP_GT,
    "$lte": OperatorEnum.OP_LTE,
    "$gte": OperatorEnum.OP_GTE,
    "$in": OperatorEnum.OP_INSET,
    "$notinset": OperatorEnum.OP_NOTINSET,
    "$like": OperatorEnum.OP_LIKE,
    "$null": OperatorEnum.OP_IS_NULL,
    "$notnull": OperatorEnum.OP_IS_NOT_NULL,
    "$notlike": OperatorEnum.OP_NOTLIKE,
    "$between": OperatorEnum.OP_BETWEEN,
}
_jo_operators_ci_map = {
    OperatorEnum.OP_EQ: OperatorEnum.OP_CI_EQ,
    OperatorEnum.OP_NEQ: OperatorEnum.OP_CI_NEQ,
    OperatorEnum.OP_LT: OperatorEnum.OP_CI_LT,
    OperatorEnum.OP_GT: OperatorEnum.OP_CI_GT,
    OperatorEnum.OP_LTE: OperatorEnum.OP_CI_LTE,
    OperatorEnum.OP_GTE: OperatorEnum.OP_CI_GTE,
    OperatorEnum.OP_INSET: OperatorEnum.OP_CI_INSET,
    OperatorEnum.OP_NOTINSET: OperatorEnum.OP_CI_NOTINSET,
    OperatorEnum.OP_LIKE: OperatorEnum.OP_CI_LIKE,
    OperatorEnum.OP_NOTLIKE: OperatorEnum.OP_CI_NOTLIKE,
}


def __model_get_relation_by_base_name(
    model: ods.Model, entity: ods.Model.Entity, relation_base_name: str
) -> ods.Model.Relation | None:
    for rel in entity.relations:
        if entity.relations[rel].base_name.lower() == relation_base_name.lower():
            return entity.relations[rel]
    return None


def __model_get_relation_by_application_name(
    model: ods.Model, entity: ods.Model.Entity, relation_application_name: str
) -> ods.Model.Relation | None:
    for rel in entity.relations:
        if entity.relations[rel].name.lower() == relation_application_name.lower():
            return entity.relations[rel]
    return None


def __model_get_relation(model: ods.Model, entity: ods.Model.Entity, relation_name: str) -> ods.Model.Relation | None:
    rv = __model_get_relation_by_application_name(model, entity, relation_name)
    if rv is None:
        rv = __model_get_relation_by_base_name(model, entity, relation_name)
    if rv is not None:
        return rv

    return None


def __model_get_attribute_by_base_name(
    model: ods.Model, entity: ods.Model.Entity, attribute_base_name: str
) -> ods.Model.Attribute | None:
    for attr in entity.attributes:
        if entity.attributes[attr].base_name.lower() == attribute_base_name.lower():
            return entity.attributes[attr]
    return None


def __model_get_attribute_by_application_name(
    model: ods.Model, entity: ods.Model.Entity, attribute_name: str
) -> ods.Model.Attribute | None:
    for attr in entity.attributes:
        if entity.attributes[attr].name.lower() == attribute_name.lower():
            return entity.attributes[attr]
    return None


def __model_get_attribute(
    model: ods.Model, entity: ods.Model.Entity, attribute_name: str
) -> ods.Model.Attribute | None:
    rv = __model_get_attribute_by_application_name(model, entity, attribute_name)
    if rv is None:
        rv = __model_get_attribute_by_base_name(model, entity, attribute_name)
    if rv is not None:
        return rv

    return None


def __model_get_entity_ex(model: ods.Model, entity_name_or_aid: str | int) -> ods.Model.Entity | None:
    if isinstance(entity_name_or_aid, int) or entity_name_or_aid.isdigit():
        entity_aid = int(entity_name_or_aid)
        for key in model.entities:
            entity = model.entities[key]
            if entity.aid == entity_aid:
                return entity
        raise SyntaxError(f"{entity_aid} is no valid entity aid.")

    for key in model.entities:
        entity = model.entities[key]
        if entity.name.lower() == entity_name_or_aid.lower() or entity.base_name.lower() == entity_name_or_aid.lower():
            return entity

    raise SyntaxError(f"Entity '{entity_name_or_aid}' is unknown in model.")


def __model_get_enum_index(model: ods.Model, entity: ods.Model.Entity, attribute_name: str, str_val: str) -> int | None:
    attr = entity.attributes[attribute_name]
    enum = model.enumerations[attr.enumeration]
    for key in enum.items:
        if key.lower() == str_val.lower():
            return enum.items[key]

    raise SyntaxError('Enum entry for "' + str_val + '" does not exist')


def _jo_enum_get_numeric_value(
    model: ods.Model,
    attribute_entity: ods.Model.Entity,
    attribute_name: str,
    name_or_number: str | int,
) -> int:
    if isinstance(name_or_number, str):
        return int(__model_get_enum_index(model, attribute_entity, attribute_name, name_or_number))

    return int(name_or_number)


def __jo_date(date_string: str) -> str:
    if date_string.endswith("Z"):
        tv = datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
        return re.sub(r"(?<=[^\s]{14})0+$", "", tv.strftime("%Y%m%d%H%M%S%f"))
    return date_string


def __parse_path_and_add_joins(
    model: ods.Model,
    entity: ods.Model.Entity,
    attribute_path: str,
    joins: List[ods.SelectStatement.JoinItem],
) -> Tuple["ods.DataTypeEnum", str, ods.Model.Entity]:
    attribute_type = ods.DataTypeEnum.DT_UNKNOWN
    attribute_name = ""
    attribute_entity = entity
    path_parts = attribute_path.split(".")
    path_part_length = len(path_parts)
    for i in range(path_part_length):
        path_part = path_parts[i]
        join_type = ods.SelectStatement.JoinItem.JoinTypeEnum.JT_DEFAULT
        if path_part.endswith(":OUTER"):
            path_part = path_part[:-6]
            join_type = ods.SelectStatement.JoinItem.JoinTypeEnum.JT_OUTER

        if i != path_part_length - 1:
            # Must be a relation
            relation = __model_get_relation(model, attribute_entity, path_part)
            if relation is None:
                raise SyntaxError(f"'{path_part}' is no relation of entity '{attribute_entity.name}'")
            attribute_name = relation.name

            # add join
            if (-1 == relation.range_max) and (1 == relation.inverse_range_max):
                inverse_entity = model.entities[relation.entity_name]
                inverse_relation = inverse_entity.relations[relation.inverse_name]
                __add_join_to_seq(model, inverse_entity, inverse_relation, joins, join_type)
            else:
                __add_join_to_seq(model, attribute_entity, relation, joins, join_type)

            attribute_entity = model.entities[relation.entity_name]
        else:
            if "*" == path_part:
                attribute_name = "*"
                attribute_type = ods.DataTypeEnum.DT_UNKNOWN
            else:
                # maybe relation or attribute
                attribute = __model_get_attribute(model, attribute_entity, path_part)
                if attribute is not None:
                    attribute_name = attribute.name
                    attribute_type = attribute.data_type
                else:
                    relation = __model_get_relation(model, attribute_entity, path_part)
                    if relation is None:
                        raise SyntaxError(
                            f"'{path_part}' is neither attribute nor relation of entity '{attribute_entity.name}'"
                        )
                    attribute_name = relation.name
                    attribute_type = ods.DataTypeEnum.DT_LONGLONG  # its an id
    return attribute_type, attribute_name, attribute_entity


def __add_join_to_seq(
    model: ods.Model,
    entity_from: ods.Model.Entity,
    relation: ods.Model.Relation,
    join_sequence: List[ods.SelectStatement.JoinItem],
    join_type: ods.SelectStatement.JoinItem.JoinTypeEnum,
) -> None:
    entity_to = model.entities[relation.entity_name]
    for join in join_sequence:
        if join.aid_from == entity_from.aid and join.aid_to == entity_to.aid and join.relation == relation.name:
            # already in sequence
            return

    join_sequence.add(
        aid_from=entity_from.aid,
        aid_to=entity_to.aid,
        relation=relation.name,
        join_type=join_type,
    )


def __parse_global_options(elem_dict: dict, target: ods.SelectStatement) -> None:
    for elem in elem_dict:
        if elem.startswith("$"):
            if "$rowlimit" == elem:
                target.row_limit = int(elem_dict[elem])
            elif "$rowskip" == elem:
                target.row_start = int(elem_dict[elem])
            elif "$seqlimit" == elem:
                target.values_limit = int(elem_dict[elem])
            elif "$seqskip" == elem:
                target.values_start = int(elem_dict[elem])
            else:
                raise SyntaxError('Undefined options "' + elem + '"')
        else:
            raise SyntaxError('No undefined options allowed "' + elem + '"')


def __parse_attributes(
    model: ods.Model,
    entity: ods.Model.Entity,
    target: ods.SelectStatement,
    element_dict: dict,
    attribute_dict: dict,
):
    for element in element_dict:
        element_attribute = attribute_dict.copy()

        if element.startswith("$"):
            if element in _jo_aggregates:
                element_attribute["aggr"] = _jo_aggregates[element]
            elif "$unit" == element:
                element_attribute["unit"] = element_dict[element]
                continue
            elif "$calculated" == element:
                raise SyntaxError('currently not supported "' + element + '"')
            elif "$options" == element:
                raise SyntaxError("Actually no $options defined for attributes")
            else:
                raise SyntaxError('Unknown aggregate "' + element + '"')
        else:
            if element_attribute["path"]:
                element_attribute["path"] += "."
            element_attribute["path"] += element

        if isinstance(element_dict[element], dict):
            __parse_attributes(model, entity, target, element_dict[element], element_attribute)
        elif isinstance(element_dict[element], list):
            raise SyntaxError("attributes is not allowed to contain arrays")
        else:
            _attribute_type, attribute_name, attribute_entity = __parse_path_and_add_joins(
                model, entity, element_attribute["path"], target.joins
            )
            if "*" == attribute_name:
                target.columns.add(aid=attribute_entity.aid, attribute=attribute_name)
            else:
                target.columns.add(
                    aid=attribute_entity.aid,
                    attribute=attribute_name,
                    unit_id=int(element_attribute["unit"]),
                    aggregate=element_attribute["aggr"],
                )


def __parse_orderby(
    model: ods.Model,
    entity: ods.Model.Entity,
    target: ods.SelectStatement,
    element_dict: dict,
    attribute_dict: dict,
) -> None:
    for elem in element_dict:
        if elem.startswith("$"):
            raise SyntaxError(f"no predefined element '{elem}' defined in orderby")
        elem_attribute = attribute_dict.copy()
        if elem_attribute["path"]:
            elem_attribute["path"] += "."
        elem_attribute["path"] += elem

        if isinstance(element_dict[elem], dict):
            __parse_orderby(model, entity, target, element_dict[elem], elem_attribute)
        elif isinstance(element_dict[elem], list):
            raise SyntaxError("attributes is not allowed to contain arrays")
        else:
            _attribute_type, attribute_name, attribute_entity = __parse_path_and_add_joins(
                model, entity, elem_attribute["path"], target.joins
            )
            order = ods.SelectStatement.OrderByItem.OD_ASCENDING
            if 0 == element_dict[elem]:
                order = ods.SelectStatement.OrderByItem.OD_DESCENDING
            elif 1 == element_dict[elem]:
                order = ods.SelectStatement.OrderByItem.OD_ASCENDING
            else:
                raise SyntaxError(str(element_dict[elem]) + " not supported for orderby")
            target.order_by.add(aid=attribute_entity.aid, attribute=attribute_name, order=order)


def __parse_groupby(
    model: ods.Model,
    entity: ods.Model.Entity,
    target: ods.SelectStatement,
    element_dict: dict,
    attribute_dict: dict,
) -> None:
    for elem in element_dict:
        if elem.startswith("$"):
            raise SyntaxError(f"no predefined element '{elem}' defined in orderby")
        elem_attribute = attribute_dict.copy()
        if elem_attribute["path"]:
            elem_attribute["path"] += "."
        elem_attribute["path"] += elem
        if isinstance(element_dict[elem], dict):
            __parse_groupby(model, entity, target, element_dict[elem], elem_attribute)
        elif isinstance(element_dict[elem], list):
            raise SyntaxError("attributes is not allowed to contain arrays")
        else:
            if 1 != element_dict[elem]:
                raise SyntaxError(str(element_dict[elem]) + " only 1 supported in groupby")
            _attribute_type, attribute_name, attribute_entity = __parse_path_and_add_joins(
                model, entity, elem_attribute["path"], target.joins
            )
            target.group_by.add(aid=attribute_entity.aid, attribute=attribute_name)


def __parse_conditions_conjunction(
    model: ods.Model,
    entity: ods.Model.Entity,
    conjunction: ods.SelectStatement.ConditionItem.ConjuctionEnum,
    target: ods.SelectStatement,
    element_dict: dict,
    attribute_dict: dict,
) -> None:
    if not isinstance(element_dict, list):
        raise SyntaxError("$and and $or must always contain array")

    if attribute_dict["conjuction_count"] > 0:
        target.where.add().conjunction = attribute_dict["conjuction"]

    if len(element_dict) > 1:
        target.where.add().conjunction = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_OPEN

    first_time = True
    for elem in element_dict:
        if not isinstance(element_dict, object):
            raise SyntaxError("$and and $or array always contains objects")

        if not first_time:
            target.where.add().conjunction = conjunction

        target.where.add().conjunction = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_OPEN
        elem_attribute = attribute_dict.copy()
        elem_attribute["conjuction_count"] = 0
        elem_attribute["conjuction"] = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_AND
        elem_attribute["options"] = ""
        __parse_conditions(model, entity, target, elem, elem_attribute)
        target.where.add().conjunction = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_CLOSE
        first_time = False

    if len(element_dict) > 1:
        target.where.add().conjunction = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_CLOSE


def __parse_conditions_not(
    model: ods.Model,
    entity: ods.Model.Entity,
    target: ods.SelectStatement,
    element_dict: dict,
    attribute_dict: dict,
) -> None:
    if not isinstance(element_dict, object):
        raise SyntaxError("$not must always contain object")

    if attribute_dict["conjuction_count"] > 0:
        target.where.add().conjunction = attribute_dict["conjuction"]

    elem_attribute = attribute_dict.copy()
    elem_attribute["conjuction_count"] = 0
    elem_attribute["conjuction"] = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_AND
    elem_attribute["options"] = ""

    target.where.add().conjunction = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_NOT
    target.where.add().conjunction = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_OPEN
    __parse_conditions(model, entity, target, element_dict, elem_attribute)
    target.where.add().conjunction = ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_CLOSE


def __set_condition_value(
    model: ods.Model,
    attribute_entity: ods.Model.Entity,
    attribute_name: str,
    attribute_type: ods.DataTypeEnum,
    src_values: List[Any] | Any,
    condition_item: ods.SelectStatement.ConditionItem,
) -> None:
    if isinstance(src_values, list):
        if attribute_type in (ods.DataTypeEnum.DT_BYTE, ods.DataTypeEnum.DS_BYTE):
            for src_value in src_values:
                condition_item.byte_array.values.append(int(src_value))
        elif attribute_type in (
            ods.DataTypeEnum.DT_BOOLEAN,
            ods.DataTypeEnum.DS_BOOLEAN,
        ):
            for src_value in src_values:
                condition_item.boolean_array.values.append(int(src_value))
        elif attribute_type in (ods.DataTypeEnum.DT_SHORT, ods.DataTypeEnum.DS_SHORT):
            for src_value in src_values:
                condition_item.long_array.values.append(int(src_value))
        elif attribute_type in (ods.DataTypeEnum.DT_LONG, ods.DataTypeEnum.DS_LONG):
            for src_value in src_values:
                condition_item.long_array.values.append(int(src_value))
        elif attribute_type in (
            ods.DataTypeEnum.DT_LONGLONG,
            ods.DataTypeEnum.DS_LONGLONG,
        ):
            for src_value in src_values:
                condition_item.longlong_array.values.append(int(src_value))
        elif attribute_type in (ods.DataTypeEnum.DT_FLOAT, ods.DataTypeEnum.DS_FLOAT):
            for src_value in src_values:
                condition_item.float_array.values.append(float(src_value))
        elif attribute_type in (ods.DataTypeEnum.DT_DOUBLE, ods.DataTypeEnum.DS_DOUBLE):
            for src_value in src_values:
                condition_item.double_array.values.append(float(src_value))
        elif attribute_type in (ods.DataTypeEnum.DT_DATE, ods.DataTypeEnum.DS_DATE):
            for src_value in src_values:
                condition_item.string_array.values.append(__jo_date(src_value))
        elif attribute_type in (ods.DataTypeEnum.DT_STRING, ods.DataTypeEnum.DS_STRING):
            for src_value in src_values:
                condition_item.string_array.values.append(str(src_value))
        elif attribute_type in (ods.DataTypeEnum.DT_ENUM, ods.DataTypeEnum.DS_ENUM):
            for src_value in src_values:
                condition_item.long_array.values.append(
                    _jo_enum_get_numeric_value(model, attribute_entity, attribute_name, src_value)
                )
        elif attribute_type in (
            ods.DataTypeEnum.DT_COMPLEX,
            ods.DataTypeEnum.DS_COMPLEX,
        ):
            for src_value in src_values:
                condition_item.float_array.values.append(float(src_value))
        elif attribute_type in (
            ods.DataTypeEnum.DT_DCOMPLEX,
            ods.DataTypeEnum.DS_DCOMPLEX,
        ):
            for src_value in src_values:
                condition_item.double_array.values.append(float(src_value))
        elif attribute_type in (
            ods.DataTypeEnum.DT_EXTERNALREFERENCE,
            ods.DataTypeEnum.DS_EXTERNALREFERENCE,
        ):
            for src_value in src_values:
                condition_item.string_array.values.append(str(src_value))
        else:
            raise ValueError(f"Unknown how to attach array, does not exist as {attribute_type} union.")
    else:
        if attribute_type == ods.DataTypeEnum.DT_BYTE:
            condition_item.byte_array.values.append(int(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_BOOLEAN:
            condition_item.boolean_array.values.append(int(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_SHORT:
            condition_item.long_array.values.append(int(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_LONG:
            condition_item.long_array.values.append(int(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_LONGLONG:
            condition_item.longlong_array.values.append(int(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_FLOAT:
            condition_item.float_array.values.append(float(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_DOUBLE:
            condition_item.double_array.values.append(float(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_DATE:
            condition_item.string_array.values.append(__jo_date(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_STRING:
            condition_item.string_array.values.append(str(src_values))
        elif attribute_type == ods.DataTypeEnum.DT_ENUM:
            condition_item.long_array.values.append(
                _jo_enum_get_numeric_value(model, attribute_entity, attribute_name, src_values)
            )
        else:
            raise ValueError(f"Unknown how to attach '{src_values}' does not exist as {attribute_type} union.")


def __get_ods_operator(
    attribute_type: ods.DataTypeEnum,
    condition_operator: OperatorEnum,
    condition_options: str,
) -> OperatorEnum:
    if attribute_type in (ods.DataTypeEnum.DT_STRING, ods.DataTypeEnum.DS_STRING):
        if -1 != condition_options.find("i"):
            # check if there is an CI operator
            if condition_operator in _jo_operators_ci_map:
                return _jo_operators_ci_map[condition_operator]

    return condition_operator


def __add_condition(
    model: ods.Model,
    entity: ods.Model.Entity,
    target: ods.SelectStatement,
    condition_path: str,
    condition_operator: OperatorEnum,
    condition_operand_value: List[Any] | Any,
    condition_unit_id: int,
    condition_options: str,
) -> None:
    attribute_type, attribute_name, attribute_entity = __parse_path_and_add_joins(
        model, entity, condition_path, target.joins
    )
    condition_item = target.where.add()
    condition_item.condition.aid = attribute_entity.aid
    condition_item.condition.attribute = attribute_name
    condition_item.condition.operator = __get_ods_operator(attribute_type, condition_operator, condition_options)
    condition_item.condition.unit_id = int(condition_unit_id)
    __set_condition_value(
        model,
        attribute_entity,
        attribute_name,
        attribute_type,
        condition_operand_value,
        condition_item.condition,
    )


def __parse_conditions(
    model: ods.Model,
    entity: ods.Model.Entity,
    target: ods.SelectStatement,
    element_dict: dict,
    attribute_dict: dict,
) -> None:
    for elem in element_dict:
        elem_attribute = attribute_dict.copy()
        if "$options" in element_dict:
            elem_attribute["options"] = element_dict["$options"]

        if elem.startswith("$"):
            if elem in _jo_operators:
                elem_attribute["operator"] = _jo_operators[elem]
            elif "$unit" == elem:
                elem_attribute["unit"] = element_dict[elem]
            elif "$and" == elem:
                __parse_conditions_conjunction(
                    model,
                    entity,
                    ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_AND,
                    target,
                    element_dict[elem],
                    attribute_dict,
                )
                attribute_dict["conjuction_count"] = attribute_dict["conjuction_count"] + 1
                continue
            elif "$or" == elem:
                __parse_conditions_conjunction(
                    model,
                    entity,
                    ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_OR,
                    target,
                    element_dict[elem],
                    attribute_dict,
                )
                attribute_dict["conjuction_count"] = attribute_dict["conjuction_count"] + 1
                continue
            elif "$not" == elem:
                __parse_conditions_not(model, entity, target, element_dict[elem], attribute_dict)
                attribute_dict["conjuction_count"] = attribute_dict["conjuction_count"] + 1
                continue
            elif "$options" == elem:
                continue
            else:
                raise SyntaxError('Unknown operator "' + elem + '"')
        else:
            if elem_attribute["path"]:
                elem_attribute["path"] += "."
            elem_attribute["path"] += elem

        if isinstance(element_dict[elem], dict):
            old_conjuction_count = elem_attribute["conjuction_count"]
            __parse_conditions(model, entity, target, element_dict[elem], elem_attribute)
            if old_conjuction_count != elem_attribute["conjuction_count"]:
                attribute_dict["conjuction_count"] = attribute_dict["conjuction_count"] + 1
        else:
            if 0 != attribute_dict["conjuction_count"]:
                target.where.add().conjunction = elem_attribute["conjuction"]

            condition_path = elem_attribute["path"]
            condition_operator = elem_attribute["operator"]
            condition_operand_value = element_dict[elem]
            condition_options = elem_attribute["options"]
            condition_unit_id = elem_attribute["unit"]

            __add_condition(
                model,
                entity,
                target,
                condition_path,
                condition_operator,
                condition_operand_value,
                condition_unit_id,
                condition_options,
            )
            attribute_dict["conjuction_count"] = attribute_dict["conjuction_count"] + 1


def jaquel_to_ods(model: ods.Model, jaquel_query: str | dict) -> Tuple[ods.Model.Entity, ods.SelectStatement]:
    """
    Convert a given JAQueL query into an ASAM ODS SelectStatement.

    :param ods.Model model: application model to be used for conversion.
    :param str | dict jaquel_query: JAQueL query as dict or json string.
    :raises SyntaxError: If contains syntactical errors.
    :raises ValueError: If conversion fail.
    :raises json.decoder.JSONDecodeError: If JSON string contains syntax errors.
    :return Tuple[ods.Model.Entity, ods.SelectStatement]: A tuple defining the target entity
        and the ASAM ODS SelectStatement
    """
    if isinstance(jaquel_query, dict):
        query = jaquel_query
    else:
        query = json.loads(jaquel_query)

    entity = None
    aid = None

    qse = ods.SelectStatement()

    # first parse conditions to get entity
    for elem in query:
        if not (isinstance(elem, str) and elem.startswith("$")):
            if entity is not None:
                raise SyntaxError('Only one start point allowed "' + elem + '"')

            entity = __model_get_entity_ex(model, elem)
            aid = entity.aid
            if isinstance(query[elem], dict):
                __parse_conditions(
                    model,
                    entity,
                    qse,
                    query[elem],
                    {
                        "conjuction": ods.SelectStatement.ConditionItem.ConjuctionEnum.CO_AND,
                        "conjuction_count": 0,
                        "path": "",
                        "operator": OperatorEnum.OP_EQ,
                        "options": "",
                        "unit": 0,
                    },
                )
            else:
                _id_value = query[elem]
                if isinstance(_id_value, str) and not _id_value.isdigit():
                    raise SyntaxError(f"Only id value can be assigned directly. But '{_id_value}' was assigned.")
                # id given
                __add_condition(
                    model,
                    entity,
                    qse,
                    "id",
                    OperatorEnum.OP_EQ,
                    int(_id_value),
                    0,
                    "",
                )

    if entity is None:
        raise SyntaxError("Does not define a target entity.")

    # parse the others
    for elem in query:
        if elem.startswith("$"):
            if "$attributes" == elem:
                __parse_attributes(
                    model,
                    entity,
                    qse,
                    query[elem],
                    {"path": "", "aggr": ods.AggregateEnum.AG_NONE, "unit": 0},
                )
            elif "$orderby" == elem:
                __parse_orderby(model, entity, qse, query[elem], {"path": ""})
            elif "$groupby" == elem:
                __parse_groupby(model, entity, qse, query[elem], {"path": ""})
            elif "$options" == elem:
                __parse_global_options(query[elem], qse)
            else:
                raise SyntaxError('unknown first level define "' + elem + '"')

    if 0 == len(qse.columns):
        qse.columns.add(aid=aid, attribute="*")

    return entity, qse

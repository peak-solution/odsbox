"""
Helps to access the unit catalog and find physical dimensions.
"""

from pandas import DataFrame

import odsbox.proto.ods_pb2 as ods
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .con_i import ConI
from odsbox.datamatrices_to_pandas import to_pandas


def query_physical_dimensions(
    con_i: "ConI",
    length: int = 0,
    mass: int = 0,
    time: int = 0,
    current: int = 0,
    temperature: int = 0,
    molar_amount: int = 0,
    luminous_intensity: int = 0,
) -> DataFrame:
    """
    Search for a physical dimension by its SI exponents.
    """
    physical_dimension_entity = con_i.mc.entity_by_base_name("AoPhysicalDimension")

    select = ods.SelectStatement()
    select.columns.add(aid=physical_dimension_entity.aid, attribute="*")
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "length_exp").name
    ci.condition.long_array.values.append(length)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "mass_exp").name
    ci.condition.long_array.values.append(mass)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "time_exp").name
    ci.condition.long_array.values.append(time)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "current_exp").name
    ci.condition.long_array.values.append(current)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "temperature_exp").name
    ci.condition.long_array.values.append(temperature)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "molar_amount_exp").name
    ci.condition.long_array.values.append(molar_amount)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "luminous_intensity_exp").name
    ci.condition.long_array.values.append(luminous_intensity)
    return to_pandas(con_i.data_read(select))


def query_units(
    con_i: "ConI",
    length: int = 0,
    mass: int = 0,
    time: int = 0,
    current: int = 0,
    temperature: int = 0,
    molar_amount: int = 0,
    luminous_intensity: int = 0,
) -> DataFrame:
    """
    Search for a unit by its SI exponents.
    """
    unit_entity = con_i.mc.entity_by_base_name("AoUnit")
    physical_dimension_entity = con_i.mc.entity_by_base_name("AoPhysicalDimension")

    select = ods.SelectStatement()  # pylint: disable=E1101
    select.columns.add(aid=unit_entity.aid, attribute="*")
    select.columns.add(aid=physical_dimension_entity.aid, attribute="Name")
    select.joins.add(
        aid_from=unit_entity.aid,
        aid_to=physical_dimension_entity.aid,
        relation=con_i.mc.relation_by_base_name(unit_entity, "phys_dimension").name,
    )
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "length_exp").name
    ci.condition.long_array.values.append(length)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "mass_exp").name
    ci.condition.long_array.values.append(mass)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "time_exp").name
    ci.condition.long_array.values.append(time)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "current_exp").name
    ci.condition.long_array.values.append(current)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "temperature_exp").name
    ci.condition.long_array.values.append(temperature)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "molar_amount_exp").name
    ci.condition.long_array.values.append(molar_amount)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "luminous_intensity_exp").name
    ci.condition.long_array.values.append(luminous_intensity)
    return to_pandas(con_i.data_read(select))


def query_quantity(
    con_i: "ConI",
    length: int = 0,
    mass: int = 0,
    time: int = 0,
    current: int = 0,
    temperature: int = 0,
    molar_amount: int = 0,
    luminous_intensity: int = 0,
) -> DataFrame:
    """
    Search for a quantity by its SI exponents.
    """
    unit_entity = con_i.mc.entity_by_base_name("AoUnit")
    physical_dimension_entity = con_i.mc.entity_by_base_name("AoPhysicalDimension")
    quantity_entity = con_i.mc.entity_by_base_name("AoQuantity")

    select = ods.SelectStatement()
    select.columns.add(aid=quantity_entity.aid, attribute="*")
    select.columns.add(aid=unit_entity.aid, attribute="Name")
    select.columns.add(aid=physical_dimension_entity.aid, attribute="Name")
    select.joins.add(
        aid_from=quantity_entity.aid,
        aid_to=unit_entity.aid,
        relation=con_i.mc.relation_by_base_name(quantity_entity, "default_unit").name,
    )
    select.joins.add(
        aid_from=unit_entity.aid,
        aid_to=physical_dimension_entity.aid,
        relation=con_i.mc.relation_by_base_name(unit_entity, "phys_dimension").name,
    )
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "length_exp").name
    ci.condition.long_array.values.append(length)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "mass_exp").name
    ci.condition.long_array.values.append(mass)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "time_exp").name
    ci.condition.long_array.values.append(time)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "current_exp").name
    ci.condition.long_array.values.append(current)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "temperature_exp").name
    ci.condition.long_array.values.append(temperature)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "molar_amount_exp").name
    ci.condition.long_array.values.append(molar_amount)
    ci = select.where.add()
    ci.condition.aid = physical_dimension_entity.aid
    ci.condition.attribute = con_i.mc.attribute_by_base_name(physical_dimension_entity, "luminous_intensity_exp").name
    ci.condition.long_array.values.append(luminous_intensity)
    return to_pandas(con_i.data_read(select))

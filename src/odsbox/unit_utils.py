"""
Helps to access the unit catalog and find physical dimensions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pandas import DataFrame

import odsbox.proto.ods_pb2 as ods

if TYPE_CHECKING:
    from .con_i import ConI

from odsbox.datamatrices_to_pandas import to_pandas


def query_physical_dimensions(
    con_i: ConI,
    length: int = 0,
    mass: int = 0,
    time: int = 0,
    current: int = 0,
    temperature: int = 0,
    molar_amount: int = 0,
    luminous_intensity: int = 0,
    **kwargs: Any,
) -> DataFrame:
    """Search for physical dimensions matching the given SI base-unit exponents.

    Each argument is the integer exponent for that SI base quantity. For example,
    velocity (m/s) is ``length=1, time=-1``; acceleration (m/s²) is
    ``length=1, time=-2``.

    Args:
        con_i: Active ODS server session.
        length: Exponent of length (metre, m). Defaults to 0.
        mass: Exponent of mass (kilogram, kg). Defaults to 0.
        time: Exponent of time (second, s). Defaults to 0.
        current: Exponent of electric current (ampere, A). Defaults to 0.
        temperature: Exponent of thermodynamic temperature (kelvin, K). Defaults to 0.
        molar_amount: Exponent of amount of substance (mole, mol). Defaults to 0.
        luminous_intensity: Exponent of luminous intensity (candela, cd). Defaults to 0.
        **kwargs: Additional keyword arguments forwarded to :func:`to_pandas`.

    Returns:
        DataFrame with all matching ``AoPhysicalDimension`` instances.
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
    return to_pandas(con_i.data_read(select), **kwargs)


def query_units(
    con_i: ConI,
    length: int = 0,
    mass: int = 0,
    time: int = 0,
    current: int = 0,
    temperature: int = 0,
    molar_amount: int = 0,
    luminous_intensity: int = 0,
    **kwargs: Any,
) -> DataFrame:
    """Search for units matching the given SI base-unit exponents.

    Joins ``AoUnit`` with ``AoPhysicalDimension`` and filters by the SI
    exponents of the associated physical dimension. The returned DataFrame
    includes unit columns plus the physical dimension name.

    Args:
        con_i: Active ODS server session.
        length: Exponent of length (metre, m). Defaults to 0.
        mass: Exponent of mass (kilogram, kg). Defaults to 0.
        time: Exponent of time (second, s). Defaults to 0.
        current: Exponent of electric current (ampere, A). Defaults to 0.
        temperature: Exponent of thermodynamic temperature (kelvin, K). Defaults to 0.
        molar_amount: Exponent of amount of substance (mole, mol). Defaults to 0.
        luminous_intensity: Exponent of luminous intensity (candela, cd). Defaults to 0.
        **kwargs: Additional keyword arguments forwarded to :func:`to_pandas`.

    Returns:
        DataFrame with all matching ``AoUnit`` instances including the
        physical dimension name column.
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
    return to_pandas(
        con_i.data_read(select),
        **kwargs,
    )


def query_quantity(
    con_i: ConI,
    length: int = 0,
    mass: int = 0,
    time: int = 0,
    current: int = 0,
    temperature: int = 0,
    molar_amount: int = 0,
    luminous_intensity: int = 0,
    **kwargs: Any,
) -> DataFrame:
    """Search for quantities matching the given SI base-unit exponents.

    Joins ``AoQuantity`` → ``AoUnit`` → ``AoPhysicalDimension`` and filters by
    the SI exponents of the associated physical dimension. The returned
    DataFrame includes quantity columns plus the default unit and physical
    dimension name columns.

    Args:
        con_i: Active ODS server session.
        length: Exponent of length (metre, m). Defaults to 0.
        mass: Exponent of mass (kilogram, kg). Defaults to 0.
        time: Exponent of time (second, s). Defaults to 0.
        current: Exponent of electric current (ampere, A). Defaults to 0.
        temperature: Exponent of thermodynamic temperature (kelvin, K). Defaults to 0.
        molar_amount: Exponent of amount of substance (mole, mol). Defaults to 0.
        luminous_intensity: Exponent of luminous intensity (candela, cd). Defaults to 0.
        **kwargs: Additional keyword arguments forwarded to :func:`to_pandas`.

    Returns:
        DataFrame with all matching ``AoQuantity`` instances including the
        default unit name and physical dimension name columns.
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
    return to_pandas(con_i.data_read(select), **kwargs)

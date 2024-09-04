"""Helper class for Units in ASAM ODS"""

import logging

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .con_i import ConI
import odsbox.proto.ods_pb2 as ods


class UnitCatalog:
    """
    This class caches the units stored in the ASAM ODS server.
    If a Unit does not exist it is created with a physical dimension `unknown`.
    """

    __log: logging.Logger = logging.getLogger(__name__)

    def __init__(self, con_i: "ConI"):
        self.__con_i = con_i
        units_df = con_i.query_data({"AoUnit": {}, "$attributes": {"name": 1, "id": 1}})
        self.__unit_map = {}
        for _, row in units_df.iterrows():
            unit_name = row.iloc[0]
            unit_id = row.iloc[1]
            self.__unit_map[unit_name] = unit_id

        self.unknown_physical_dimension = None

    def get(self, unit_name: str) -> int | None:
        """
        Get a unit by its case sensitive name.

        :param str unit_name: Case sensitive name of an unit.
        :return int | None: The unit id if the unit exists. Else `None` is returned.
        """
        if unit_name is None or "" == unit_name:
            return self.get("-")
        return self.__unit_map.get(unit_name) if unit_name in self.__unit_map else None

    def get_or_create(self, unit_name: str) -> int:
        """
        Get a unit by its case sensitive name or create one using an unknown physical dimension.

        :param str unit_name: Case sensitive name of an unit.
        :return int: The unit id if the unit exists.
        """
        if unit_name is None or "" == unit_name:
            # Unit is obligatory
            return self.get_or_create("-")
        rv = self.__unit_map.get(unit_name)
        if rv is not None:
            return rv
        new_unit_id = self.create(unit_name)
        self.__unit_map[unit_name] = new_unit_id
        return new_unit_id

    def create(self, unit_name: str) -> int:
        """
        Create a unit by its case sensitive name using an unknown physical dimension.

        :param str unit_name: Case sensitive name of an unit.
        :return int: The unit id if the unit exists.
        """
        physical_dimension_id = self.__get_or_create_unknown_physical_dimension()
        unit_id = self.__create_auto_unit(unit_name, physical_dimension_id)
        return unit_id

    def __get_or_create_unknown_physical_dimension(self):
        if self.unknown_physical_dimension is None:
            self.unknown_physical_dimension = self.__get_or_create_unknown_phys_dim("unknown")

        return self.unknown_physical_dimension

    def __get_or_create_unknown_phys_dim(self, name):
        physical_dimension_entity = self.__con_i.mc.entity_by_base_name("AoPhysicalDimension")
        existing_physical_dimension = self.__con_i.query_data(
            {"AoPhysicalDimension": {"name": name}, "$attributes": {"id": 1}}
        )
        if existing_physical_dimension.shape[0] > 0:
            physical_dimension_id = existing_physical_dimension.iloc[0, 0]
            self.__log.debug(
                "Physical dimension '%s' already exists. Using existing ID: %s",
                name,
                physical_dimension_id,
            )
        else:
            ts = ods.DataMatrices()
            dm = ts.matrices.add(aid=physical_dimension_entity.aid)
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "name").name
            ).string_array.values[:] = [name]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "mime_type").name
            ).string_array.values[:] = ["application/x-asam.aophysicaldimension"]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "length_exp").name
            ).long_array.values[:] = [0]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "mass_exp").name
            ).long_array.values[:] = [0]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "time_exp").name
            ).long_array.values[:] = [0]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "current_exp").name
            ).long_array.values[:] = [0]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "temperature_exp").name
            ).long_array.values[:] = [0]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "molar_amount_exp").name
            ).long_array.values[:] = [0]
            dm.columns.add(
                name=self.__con_i.mc.attribute_by_base_name(physical_dimension_entity, "luminous_intensity_exp").name
            ).long_array.values[:] = [0]
            if "angle" in physical_dimension_entity.attributes:
                dm.columns.add(name=physical_dimension_entity.attributes["angle"].name).long_array.values[:] = [0]

            ids = self.__con_i.data_create(ts)
            physical_dimension_id = ids[0]
            self.__log.info(
                "Created new physical dimension '%s' with ID: %s",
                name,
                physical_dimension_id,
            )

        return physical_dimension_id

    def __create_auto_unit(self, name: str, physical_dimension_id: int):
        unit = self.__con_i.mc.entity_by_base_name("AoUnit")
        ts = ods.DataMatrices()
        dm = ts.matrices.add(aid=unit.aid)
        dm.columns.add(name=self.__con_i.mc.attribute_by_base_name(unit, "name").name).string_array.values[:] = [name]
        dm.columns.add(name=self.__con_i.mc.attribute_by_base_name(unit, "mime_type").name).string_array.values[:] = [
            "application/x-asam.aounit"
        ]
        dm.columns.add(name=self.__con_i.mc.attribute_by_base_name(unit, "factor").name).double_array.values[:] = [1.0]
        dm.columns.add(name=self.__con_i.mc.attribute_by_base_name(unit, "offset").name).double_array.values[:] = [0.0]
        dm.columns.add(name=self.__con_i.mc.relation_by_base_name(unit, "phys_dimension").name).longlong_array.values[
            :
        ] = [physical_dimension_id]

        ids = self.__con_i.data_create(ts)
        return ids[0]

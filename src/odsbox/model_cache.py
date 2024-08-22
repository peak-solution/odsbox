"""helps working with the ASAM ODS model"""

import logging

import odsbox.proto.ods_pb2 as _ods


class ModelCache:
    """The model object returned from ods server needs some utilities to work with it."""

    __model: _ods.Model = None
    __log: logging.Logger = logging.getLogger(__name__)

    def __init__(self, model: _ods.Model):
        self.__model = model

    def model(self) -> _ods.Model:
        """Get the attached ASAM ODS model"""
        return self.__model

    def aid_by_entity_name(self, entity_name: str) -> int:
        """
        Determine the application element id of an entity by its name.

        :entity_name: Case sensitive name to look for
        :raises ValueError: If the entity does not exist.
        """
        if entity_name in self.__model.entities:
            return self.__model.entities[entity_name].aid
        raise ValueError(f"Entity {entity_name} does not exist in model")

    def entity_by_base_name(self, entity_base_name: str) -> _ods.Model.Entity:
        """
        Get the entity by its base name.

        :entity_base_name: case insensitive name of the base model element.
        :raises ValueError: If the entity does not exist.
        """
        entities = self.__model.entities
        for key in entities:
            entity = entities[key]
            if entity_base_name.casefold() == entity.base_name.casefold():
                return entity
        raise ValueError(f"No entity derived from {entity_base_name}")

    def relation_by_base_name(
        self, entity_or_name: str | _ods.Model.Entity, relation_base_name: str
    ) -> _ods.Model.Relation:
        """
        Get the relation by base name.

        :entity_or_name: entity object or case sensitive application name to lookup
        :relation_base_name: case insensitive name of the base model element.
        :raises ValueError: If the relation does not exist.
        """
        entity = self.__entity(entity_or_name)
        relations = entity.relations
        for key in relations:
            relation = relations[key]
            if relation_base_name.casefold() == relation.base_name.casefold():
                return relation
        raise ValueError(f"Entity {entity.name} does not have relation derived from {relation_base_name}")

    def relation_name_by_base_name(self, entity_or_name: str | _ods.Model.Entity, relation_base_name: str) -> str:
        """
        Get the relation application name by base name.

        :entity_or_name: entity object or case sensitive application name to lookup
        :relation_base_name: case insensitive name of the base model element.
        :raises ValueError: If the relation does not exist.
        """
        return self.relation_by_base_name(entity_or_name, relation_base_name).name

    def attribute_by_base_name(
        self, entity_or_name: str | _ods.Model.Entity, attribute_base_name: str
    ) -> _ods.Model.Attribute:
        """
        Get the attribute by base name.

        :entity_or_name: entity object or case sensitive application name to lookup
        :attribute_base_name: case insensitive name of the base model element.
        :raises ValueError: If the attribute does not exist.
        """
        entity = self.__entity(entity_or_name)
        attributes = entity.attributes
        for key in attributes:
            attribute = attributes[key]
            if attribute_base_name.casefold() == attribute.base_name.casefold():
                return attribute
        raise ValueError(f"Entity {entity.name} does not have attribute derived from {attribute_base_name}")

    def attribute_name_by_base_name(self, entity_or_name: str | _ods.Model.Entity, attribute_base_name: str) -> str:
        """
        Get the attribute by base name.

        :entity_or_name: entity object or case sensitive application name to lookup
        :attribute_base_name: case insensitive name of the base model element.
        :raises ValueError: If the attribute does not exist.
        """
        return self.attribute_by_base_name(entity_or_name, attribute_base_name).name

    def enumeration(self, enumeration_name: str) -> _ods.Model.Enumeration:
        """
        Get enumeration by its name.

        :enumeration_name: case insensitive name of the application model enumeration.
        :raises ValueError: If the enumeration does not exist.
        """
        if enumeration_name in self.__model.enumerations:
            return self.__model.enumerations[enumeration_name]

        for key in self.__model.enumerations:
            if key.casefold() == enumeration_name.casefold():
                return self.__model.enumerations[key]
        raise ValueError(f"Enumeration {enumeration_name} does not exist in datamodel")

    def enumeration_value_to_key(self, enumeration_or_name: str | _ods.Model.Enumeration, lookup_value: int):
        """
        Convert an enumeration value into its string representation.

        :enumeration_or_name: ods enumeration or its case insensitive name
        :lookup_value: integer value to check
        :raises ValueError: If the enumeration does not exist or does not contain value.
        """
        enumeration = (
            enumeration_or_name
            if isinstance(enumeration_or_name, _ods.Model.Enumeration)
            else self.enumeration(enumeration_or_name)
        )
        for key, value in enumeration.items.items():
            if value == lookup_value:
                return key
        raise ValueError(f"Enumeration {enumeration.name} does not contain the int value {lookup_value}")

    def enumeration_key_to_value(self, enumeration_or_name: str | _ods.Model.Enumeration, lookup_key: str):
        """
        Convert an enumeration integer value into its string representation.

        :enumeration_or_name: ods enumeration or its case insensitive name
        :lookup_key: case insensitive string key value to check
        :raises ValueError: If the enumeration does not exist or does not contain the key.
        """
        enumeration = (
            enumeration_or_name
            if isinstance(enumeration_or_name, _ods.Model.Enumeration)
            else self.enumeration(enumeration_or_name)
        )

        if lookup_key in enumeration.items:
            return enumeration.items[lookup_key]

        for key, value in enumeration.items.items():
            if key.casefold() == lookup_key.casefold():
                return value
        raise ValueError(f"Enumeration {enumeration.name} does not contain the key {lookup_key}")

    def __entity(self, entity_or_name: str | _ods.Model.Entity) -> _ods.Model.Entity:
        rv = entity_or_name if isinstance(entity_or_name, _ods.Model.Entity) else self.__model.entities[entity_or_name]
        if rv is None:
            raise ValueError(f"No entity named {entity_or_name} found")
        return rv

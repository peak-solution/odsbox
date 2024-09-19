"""helps working with the ASAM ODS model"""

from __future__ import annotations

import logging

import odsbox.proto.ods_pb2 as ods


class ModelCache:
    """
    The ods.Model object returned from ods server needs some utilities to work with it.
    This cache functionality useful for daily work.
    """

    __model: ods.Model
    __log: logging.Logger = logging.getLogger(__name__)

    def __init__(self, model: ods.Model):
        self.__model = model

    def model(self) -> ods.Model:
        """
        Get the attached ASAM ODS model

        :return ods.Model: The used model.
        """
        return self.__model

    def aid(self, entity_or_name: str | ods.Model.Entity) -> int:
        """
        Determine the application element id of an entity by its name.

        :param str entity_or_name: entity object or case sensitive application name to lookup
        :raises ValueError: If the entity does not exist.
        :return int: The ApplicationElementId of the entity.
        """
        return self.__entity(entity_or_name).aid

    def entity(self, entity_name: str) -> ods.Model.Entity:
        """
        Get the entity name.

        :param str entity_name: case insensitive name of an entity.
        :raises ValueError: If the entity does not exist.
        """
        entity = self.__model.entities.get(entity_name)
        if entity is not None:
            return entity
        name_casefold = entity_name.casefold()
        for key, entity in self.__model.entities.items():
            if key.casefold() == name_casefold:
                return entity
        raise ValueError(f"No entity named '{entity_name}' found.")

    def entity_by_base_name(self, entity_base_name: str) -> ods.Model.Entity:
        """
        Get the entity by its base name.

        :param str entity_base_name: case insensitive name of the base model element.
        :raises ValueError: If the entity does not exist.
        """
        name_casefold = entity_base_name.casefold()
        for _, entity in self.__model.entities.items():
            if name_casefold == entity.base_name.casefold():
                return entity
        raise ValueError(f"No entity derived from base type '{entity_base_name}' found.")

    def entity_by_aid(self, aid: int) -> ods.Model.Entity:
        """
        Get the entity by its ApplicationElementId(aid).

        :param int aid: ApplicationElementId of an entity to lookup.
        :raises ValueError: If the entity does not exist.
        :return ods.Model.Entity: Entity corresponding to given aid.
        """
        for _, entity in self.__model.entities.items():
            if aid == entity.aid:
                return entity
        raise ValueError(f"No entity found with aid '{aid}'.")

    def attribute_no_throw(
        self, entity_or_name: str | ods.Model.Entity, application_or_base_name: str
    ) -> ods.Model.Attribute | None:
        """
        This is a convenience method to find an attribute. It will first check for
        an attribute with the given application name and afterwards check for an
        attribute with the given base name.

        :param str | ods.Model.Entity entity_or_name: entity or case insensitive name of an entity
        :param str application_or_base_name: case insensitive name to lookup
        :raises ValueError: If entity does not exist.
        :return ods.Model.Attribute | None: The found attribute or None.
        """
        entity = self.__entity(entity_or_name)
        attribute = entity.attributes.get(application_or_base_name)
        if attribute is not None:
            return attribute
        name_casefold = application_or_base_name.casefold()
        for _, attribute in entity.attributes.items():
            if attribute.name.casefold() == name_casefold or attribute.base_name.casefold() == name_casefold:
                return attribute
        return None

    def attribute(self, entity_or_name: str | ods.Model.Entity, application_or_base_name: str) -> ods.Model.Attribute:
        """
        This is a convenience method to find an attribute. It will first check for
        an attribute with the given application name and afterwards check for an
        attribute with the given base name.

        :param str | ods.Model.Entity entity_or_name: entity or case insensitive name of an entity
        :param str application_or_base_name: case insensitive name to lookup
        :raises ValueError: If attribute does not.
        :return ods.Model.Attribute: The found attribute.
        """
        entity = self.__entity(entity_or_name)
        attribute = self.attribute_no_throw(entity, application_or_base_name)
        if attribute is not None:
            return attribute
        raise ValueError(
            f"'{entity.name}' has no attribute named '{application_or_base_name}' as base or application name."
        )

    def attribute_by_base_name(
        self, entity_or_name: str | ods.Model.Entity, attribute_base_name: str
    ) -> ods.Model.Attribute:
        """
        Get the attribute by base name.

        :param str | ods.Model.Entity entity_or_name: entity object or case sensitive application name to lookup
        :param str attribute_base_name: case insensitive name of the base model element.
        :raises ValueError: If the attribute does not exist.
        :return ods.Model.Attribute: Corresponding attribute.
        """
        entity = self.__entity(entity_or_name)
        attributes = entity.attributes
        for _, attribute in attributes.items():
            if attribute_base_name.casefold() == attribute.base_name.casefold():
                return attribute
        raise ValueError(f"Entity '{entity.name}' does not have attribute derived from '{attribute_base_name}'.")

    def relation_no_throw(
        self, entity_or_name: str | ods.Model.Entity, application_or_base_name: str
    ) -> ods.Model.Relation | None:
        """
        This is a convenience method to find a relation. It will first check for
        a relation with the given application name and afterwards check for a
        relation with the given base name.

        :param str | ods.Model.Entity entity_or_name: entity or case insensitive name of an entity
        :param str application_or_base_name: case insensitive name to lookup
        :raises ValueError: If entity does not exist.
        :return ods.Model.Relation | None: The relation or None if it does not exist.
        """
        entity = self.__entity(entity_or_name)
        relation = entity.relations.get(application_or_base_name)
        if relation is not None:
            return relation
        # retry case insensitive
        name_casefold = application_or_base_name.casefold()
        for _, relation in entity.relations.items():
            if relation.name.casefold() == name_casefold or relation.base_name.casefold() == name_casefold:
                return relation
        return None

    def relation(self, entity_or_name: str | ods.Model.Entity, application_or_base_name: str) -> ods.Model.Relation:
        """
        This is a convenience method to find a relation. It will first check for
        a relation with the given application name and afterwards check for a
        relation with the given base name.

        :param str | ods.Model.Entity entity_or_name: entity or case insensitive name of an entity
        :param str application_or_base_name: case insensitive name to lookup
        :raises ValueError: If relation does not exist.
        :return ods.Model.Relation: The found relation.
        """
        entity = self.__entity(entity_or_name)
        relation = self.relation_no_throw(entity, application_or_base_name)
        if relation is not None:
            return relation
        raise ValueError(
            f"'{entity.name}' has no relation named '{application_or_base_name}' as base or application name."
        )

    def relation_by_base_name(
        self, entity_or_name: str | ods.Model.Entity, relation_base_name: str
    ) -> ods.Model.Relation:
        """
        Get the relation by base name.

        :param str | ods.Model.Entity entity_or_name: entity object or case sensitive application name to lookup
        :param str relation_base_name: case insensitive name of the base model element.
        :raises ValueError: If the relation does not exist.
        :return ods.Model.Relation: Corresponding relation.
        """
        entity = self.__entity(entity_or_name)
        relations = entity.relations
        for _, relation in relations.items():
            if relation_base_name.casefold() == relation.base_name.casefold():
                return relation
        raise ValueError(f"Entity '{entity.name}' does not have relation derived from '{relation_base_name}'.")

    def enumeration(self, enumeration_name: str) -> ods.Model.Enumeration:
        """
        Get enumeration by its name.

        :param str enumeration_name: case insensitive name of the application model enumeration.
        :raises ValueError: If the enumeration does not exist.
        :return ods.Model.Enumeration: Corresponding enumeration.
        """
        enumeration = self.__model.enumerations.get(enumeration_name)
        if enumeration is not None:
            return enumeration
        name_casefold = enumeration_name.casefold()
        for key, enumeration in self.__model.enumerations.items():
            if key.casefold() == name_casefold:
                return enumeration
        raise ValueError(f"Enumeration '{enumeration_name}' does not exist in data model.")

    def enumeration_value_to_key(self, enumeration_or_name: str | ods.Model.Enumeration, lookup_value: int) -> str:
        """
        Convert an enumeration value into its string representation.

        :param str | ods.Model.Enumeration enumeration_or_name: ods enumeration or its case insensitive name
        :param int lookup_value: integer value to check
        :raises ValueError: If the enumeration does not exist or does not contain value.
        :return str: String representation of int value.
        """
        enumeration = self.__enumeration(enumeration_or_name)
        for key, value in enumeration.items.items():
            if value == lookup_value:
                return key
        raise ValueError(f"Enumeration '{enumeration.name}' does not contain the int value '{lookup_value}'.")

    def enumeration_key_to_value(self, enumeration_or_name: str | ods.Model.Enumeration, lookup_key: str) -> int:
        """
        Convert an enumeration integer value into its string representation.

        :param str | ods.Model.Enumeration enumeration_or_name: ods enumeration or its case insensitive name
        :param str lookup_key: case insensitive string key value to check
        :raises ValueError: If the enumeration does not exist or does not contain the key.
        :return str: Int representation of string value.
        """
        enumeration = self.__enumeration(enumeration_or_name)
        if lookup_key in enumeration.items:
            return enumeration.items[lookup_key]
        name_casefold = lookup_key.casefold()
        for key, value in enumeration.items.items():
            if key.casefold() == name_casefold:
                return value
        raise ValueError(f"Enumeration '{enumeration.name}' does not contain the key '{lookup_key}'.")

    def __entity(self, entity_or_name: str | ods.Model.Entity) -> ods.Model.Entity:
        if isinstance(entity_or_name, ods.Model.Entity):
            return entity_or_name
        return self.entity(entity_or_name)

    def __enumeration(self, enumeration_or_name: str | ods.Model.Enumeration) -> ods.Model.Enumeration:
        if isinstance(enumeration_or_name, ods.Model.Enumeration):
            return enumeration_or_name
        return self.enumeration(enumeration_or_name)

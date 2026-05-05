"""helps working with the ASAM ODS model"""

from __future__ import annotations

import logging

import odsbox.proto.ods_pb2 as ods
from odsbox.model_suggestions import ModelSuggestions


class ModelCache:
    """
    The ods.Model object returned from ods server needs some utilities to work with it.
    This cache functionality useful for daily work.
    """

    __model: ods.Model
    __log: logging.Logger = logging.getLogger(__name__)

    def __init__(self, model: ods.Model) -> None:
        self.__model = model

    def model(self) -> ods.Model:
        """
        Get the attached ASAM ODS model.

        Returns:
            The used model.
        """
        return self.__model

    def aid(self, entity_or_name: str | ods.Model.Entity) -> int:
        """
        Determine the application element id of an entity by its name.

        Args:
            entity_or_name: Entity object or case sensitive application name to lookup.

        Returns:
            The ApplicationElementId of the entity.

        Raises:
            ValueError: If the entity does not exist.
        """
        return self.__entity(entity_or_name).aid

    def entity(self, entity_name: str) -> ods.Model.Entity:
        """
        Get the entity by name. If no application name matches,
        it will try to match the base name.

        Args:
            entity_name: Case insensitive name of an entity.

        Returns:
            The found entity.

        Raises:
            ValueError: If the entity does not exist.
        """
        entity = self.entity_no_throw(entity_name)
        if entity is not None:
            return entity
        raise ValueError(
            f"No entity named '{entity_name}' found.{ModelSuggestions.get_entity(self.__model, entity_name)}"
        )

    def entity_no_throw(self, entity_name: str) -> ods.Model.Entity | None:
        """
        Get the entity by name. Returns None if not found. if no application name matches,
        it will try to match the base name.

        Args:
            entity_name: Case insensitive name of an entity or base name.

        Returns:
            The found entity or None.
        """
        entity = self.__model.entities.get(entity_name)
        if entity is not None:
            return entity
        name_casefold = entity_name.casefold()
        for key, entity in self.__model.entities.items():
            if key.casefold() == name_casefold or entity.base_name.casefold() == name_casefold:
                return entity
        return None

    def entity_by_base_name(self, entity_base_name: str) -> ods.Model.Entity:
        """
        Get the entity by its base name.

        Args:
            entity_base_name: Case insensitive name of the base model element.

        Raises:
            ValueError: If the entity does not exist.
        """
        name_casefold = entity_base_name.casefold()
        for _, entity in self.__model.entities.items():
            if name_casefold == entity.base_name.casefold():
                return entity
        raise ValueError(
            f"No entity derived from base type '{entity_base_name}' found."
            f"{ModelSuggestions.get_entity_by_base_name(self.__model, entity_base_name)}"
        )

    def entity_by_aid(self, aid: int) -> ods.Model.Entity:
        """
        Get the entity by its ApplicationElementId(aid).

        Args:
            aid: ApplicationElementId of an entity to lookup.

        Returns:
            Entity corresponding to given aid.

        Raises:
            ValueError: If the entity does not exist.
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

        Args:
            entity_or_name: Entity or case insensitive name of an entity.
            application_or_base_name: Case insensitive name to lookup.

        Returns:
            The found attribute or None.

        Raises:
            ValueError: If entity does not exist.
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

        Args:
            entity_or_name: Entity or case insensitive name of an entity.
            application_or_base_name: Case insensitive name to lookup.

        Returns:
            The found attribute.

        Raises:
            ValueError: If attribute does not exist.
        """
        entity = self.__entity(entity_or_name)
        attribute = self.attribute_no_throw(entity, application_or_base_name)
        if attribute is not None:
            return attribute
        raise ValueError(
            f"'{entity.name}' has no attribute named '{application_or_base_name}' as base or application name."
            f"{ModelSuggestions.get_attribute(entity, application_or_base_name)}"
        )

    def attribute_by_base_name(
        self, entity_or_name: str | ods.Model.Entity, attribute_base_name: str
    ) -> ods.Model.Attribute:
        """
        Get the attribute by base name.

        Args:
            entity_or_name: Entity object or case sensitive application name to lookup.
            attribute_base_name: Case insensitive name of the base model element.

        Returns:
            Corresponding attribute.

        Raises:
            ValueError: If the attribute does not exist.
        """
        entity = self.__entity(entity_or_name)
        attributes = entity.attributes
        for _, attribute in attributes.items():
            if attribute_base_name.casefold() == attribute.base_name.casefold():
                return attribute
        raise ValueError(
            f"Entity '{entity.name}' does not have attribute derived from '{attribute_base_name}'."
            f"{ModelSuggestions.get_attribute_by_base_name(entity, attribute_base_name)}"
        )

    def relation_no_throw(
        self, entity_or_name: str | ods.Model.Entity, application_or_base_name: str
    ) -> ods.Model.Relation | None:
        """
        This is a convenience method to find a relation. It will first check for
        a relation with the given application name and afterwards check for a
        relation with the given base name.

        Args:
            entity_or_name: Entity or case insensitive name of an entity.
            application_or_base_name: Case insensitive name to lookup.

        Returns:
            The relation or None if it does not exist.

        Raises:
            ValueError: If entity does not exist.
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

        Args:
            entity_or_name: Entity or case insensitive name of an entity.
            application_or_base_name: Case insensitive name to lookup.

        Returns:
            The found relation.

        Raises:
            ValueError: If relation does not exist.
        """
        entity = self.__entity(entity_or_name)
        relation = self.relation_no_throw(entity, application_or_base_name)
        if relation is not None:
            return relation
        raise ValueError(
            f"'{entity.name}' has no relation named '{application_or_base_name}' as base or application name."
            f"{ModelSuggestions.get_relation(entity, application_or_base_name)}"
        )

    def relation_by_base_name(
        self, entity_or_name: str | ods.Model.Entity, relation_base_name: str
    ) -> ods.Model.Relation:
        """
        Get the relation by base name.

        Args:
            entity_or_name: Entity object or case sensitive application name to lookup.
            relation_base_name: Case insensitive name of the base model element.

        Returns:
            Corresponding relation.

        Raises:
            ValueError: If the relation does not exist.
        """
        entity = self.__entity(entity_or_name)
        relations = entity.relations
        for _, relation in relations.items():
            if relation_base_name.casefold() == relation.base_name.casefold():
                return relation
        raise ValueError(
            f"Entity '{entity.name}' does not have relation derived from '{relation_base_name}'."
            f"{ModelSuggestions.get_relation_by_base_name(entity, relation_base_name)}"
        )

    def enumeration(self, enumeration_name: str) -> ods.Model.Enumeration:
        """
        Get enumeration by its name.

        Args:
            enumeration_name: Case insensitive name of the application model enumeration.

        Returns:
            Corresponding enumeration.

        Raises:
            ValueError: If the enumeration does not exist.
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

        Args:
            enumeration_or_name: ODS enumeration or its case insensitive name.
            lookup_value: Integer value to check.

        Returns:
            String representation of int value.

        Raises:
            ValueError: If the enumeration does not exist or does not contain value.
        """
        enumeration = self.__enumeration(enumeration_or_name)
        for key, value in enumeration.items.items():
            if value == lookup_value:
                return key
        raise ValueError(f"Enumeration '{enumeration.name}' does not contain the int value '{lookup_value}'.")

    def enumeration_key_to_value(self, enumeration_or_name: str | ods.Model.Enumeration, lookup_key: str) -> int:
        """
        Convert an enumeration integer value into its string representation.

        Args:
            enumeration_or_name: ODS enumeration or its case insensitive name.
            lookup_key: Case insensitive string key value to check.

        Returns:
            Int representation of string value.

        Raises:
            ValueError: If the enumeration does not exist or does not contain the key.
        """
        enumeration = self.__enumeration(enumeration_or_name)
        if lookup_key in enumeration.items:
            return enumeration.items[lookup_key]
        name_casefold = lookup_key.casefold()
        for key, value in enumeration.items.items():
            if key.casefold() == name_casefold:
                return value
        raise ValueError(
            f"Enumeration '{enumeration.name}' does not contain the key '{lookup_key}'."
            f"{ModelSuggestions.get_enum(enumeration, lookup_key)}"
        )

    def __entity(self, entity_or_name: str | ods.Model.Entity) -> ods.Model.Entity:
        if isinstance(entity_or_name, ods.Model.Entity):
            return entity_or_name
        return self.entity(entity_or_name)

    def __enumeration(self, enumeration_or_name: str | ods.Model.Enumeration) -> ods.Model.Enumeration:
        if isinstance(enumeration_or_name, ods.Model.Enumeration):
            return enumeration_or_name
        return self.enumeration(enumeration_or_name)

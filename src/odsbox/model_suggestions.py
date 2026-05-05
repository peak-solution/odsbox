from __future__ import annotations

from difflib import get_close_matches
from typing import Any

import odsbox.proto.ods_pb2 as ods


class ModelSuggestions:
    """Provides fuzzy-match suggestions for ODS model names.

    Used to generate helpful error messages when an entity, attribute, relation,
    or enum value name is not found in the ODS application model. All methods
    return a suggestion string (e.g. " Did you mean 'AoMeasurement'?") or an
    empty string if no close match is found.
    """

    @staticmethod
    def get(lower_case_dict: dict[str, Any], str_val: str) -> str:
        """Return a suggestion string from a lowercase-keyed lookup dict.

        Args:
            lower_case_dict: Mapping of lowercase name → canonical name.
            str_val: The name that was not found, to find a suggestion for.

        Returns:
            A suggestion string like " Did you mean 'Name'?", or "" if no
            close match is found.
        """
        suggestions = get_close_matches(
            str_val.lower(),
            lower_case_dict,
            n=1,
            cutoff=0.3,
        )
        if len(suggestions) > 0:
            return_value = lower_case_dict[suggestions[0]]
            return f" Did you mean '{return_value}'?"
        return ""

    @staticmethod
    def get_enum(enumeration: ods.Model.Enumeration, str_val: str) -> str:
        """Return a suggestion for an enum item name.

        Args:
            enumeration: The ODS model enumeration to search.
            str_val: The enum item name that was not found.

        Returns:
            A suggestion string, or "" if no close match is found.
        """
        available = {key.lower(): key for key in enumeration.items}
        return ModelSuggestions.get(available, str_val)

    @staticmethod
    def get_attribute(entity: ods.Model.Entity, attribute_or_relation_name: str) -> str:
        """Return a suggestion for an attribute or relation name (application or base name).

        Args:
            entity: The ODS model entity to search within.
            attribute_or_relation_name: The name that was not found.

        Returns:
            A suggestion string, or "" if no close match is found.
        """
        available = {}
        available.update({relation.base_name.lower(): relation.base_name for key, relation in entity.relations.items()})
        available.update(
            {attribute.base_name.lower(): attribute.base_name for key, attribute in entity.attributes.items()}
        )
        available.update({relation.name.lower(): relation.name for key, relation in entity.relations.items()})
        available.update({attribute.name.lower(): attribute.name for key, attribute in entity.attributes.items()})
        return ModelSuggestions.get(available, attribute_or_relation_name)

    @staticmethod
    def get_attribute_by_base_name(entity: ods.Model.Entity, attribute_or_relation_name: str) -> str:
        """Return a suggestion for an attribute or relation base name only.

        Args:
            entity: The ODS model entity to search within.
            attribute_or_relation_name: The base name that was not found.

        Returns:
            A suggestion string, or "" if no close match is found.
        """
        available = {}
        available.update({relation.base_name.lower(): relation.base_name for key, relation in entity.relations.items()})
        available.update(
            {attribute.base_name.lower(): attribute.base_name for key, attribute in entity.attributes.items()}
        )
        return ModelSuggestions.get(available, attribute_or_relation_name)

    @staticmethod
    def get_relation(entity: ods.Model.Entity, relation_name: str) -> str:
        """Return a suggestion for a relation name (application or base name).

        Args:
            entity: The ODS model entity to search within.
            relation_name: The relation name that was not found.

        Returns:
            A suggestion string, or "" if no close match is found.
        """
        available = {}
        available.update({relation.base_name.lower(): relation.base_name for key, relation in entity.relations.items()})
        available.update({relation.name.lower(): relation.name for key, relation in entity.relations.items()})
        return ModelSuggestions.get(available, relation_name)

    @staticmethod
    def get_relation_by_base_name(entity: ods.Model.Entity, relation_name: str) -> str:
        """Return a suggestion for a relation base name only.

        Args:
            entity: The ODS model entity to search within.
            relation_name: The relation base name that was not found.

        Returns:
            A suggestion string, or "" if no close match is found.
        """
        available = {}
        available.update({relation.base_name.lower(): relation.base_name for key, relation in entity.relations.items()})
        return ModelSuggestions.get(available, relation_name)

    @staticmethod
    def get_entity(model: ods.Model, entity_name: str) -> str:
        """Return a suggestion for an entity name (application or base name).

        Args:
            model: The ODS application model to search within.
            entity_name: The entity name that was not found.

        Returns:
            A suggestion string, or "" if no close match is found.
        """
        available = {}
        available.update({entity.base_name.lower(): entity.base_name for key, entity in model.entities.items()})
        available.update({entity.name.lower(): entity.name for key, entity in model.entities.items()})
        return ModelSuggestions.get(available, entity_name)

    @staticmethod
    def get_entity_by_base_name(model: ods.Model, entity_name: str) -> str:
        """Return a suggestion for an entity base name only.

        Args:
            model: The ODS application model to search within.
            entity_name: The entity base name that was not found.

        Returns:
            A suggestion string, or "" if no close match is found.
        """
        available = {}
        available.update({entity.base_name.lower(): entity.base_name for key, entity in model.entities.items()})
        return ModelSuggestions.get(available, entity_name)

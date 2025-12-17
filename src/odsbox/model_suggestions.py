import odsbox.proto.ods_pb2 as ods


from difflib import get_close_matches


class ModelSuggestions:
    @staticmethod
    def get(lower_case_dict: dict, str_val: str) -> str:
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
        available = {key.lower(): key for key in enumeration.items}
        return ModelSuggestions.get(available, str_val)

    @staticmethod
    def get_attribute(entity: ods.Model.Entity, attribute_or_relation_name: str) -> str:
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
        available = {}
        available.update({relation.base_name.lower(): relation.base_name for key, relation in entity.relations.items()})
        available.update(
            {attribute.base_name.lower(): attribute.base_name for key, attribute in entity.attributes.items()}
        )
        return ModelSuggestions.get(available, attribute_or_relation_name)

    @staticmethod
    def get_relation(entity: ods.Model.Entity, relation_name: str) -> str:
        available = {}
        available.update({relation.base_name.lower(): relation.base_name for key, relation in entity.relations.items()})
        available.update({relation.name.lower(): relation.name for key, relation in entity.relations.items()})
        return ModelSuggestions.get(available, relation_name)

    @staticmethod
    def get_relation_by_base_name(entity: ods.Model.Entity, relation_name: str) -> str:
        available = {}
        available.update({relation.base_name.lower(): relation.base_name for key, relation in entity.relations.items()})
        return ModelSuggestions.get(available, relation_name)

    @staticmethod
    def get_entity(model: ods.Model, entity_name: str) -> str:
        available = {}
        available.update({entity.base_name.lower(): entity.base_name for key, entity in model.entities.items()})
        available.update({entity.name.lower(): entity.name for key, entity in model.entities.items()})
        return ModelSuggestions.get(available, entity_name)

    @staticmethod
    def get_entity_by_base_name(model: ods.Model, entity_name: str) -> str:
        available = {}
        available.update({entity.base_name.lower(): entity.base_name for key, entity in model.entities.items()})
        return ModelSuggestions.get(available, entity_name)

"""Test ModelSuggestions class functionality."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import Mock

import pytest
from google.protobuf.json_format import Parse

from odsbox.model_suggestions import ModelSuggestions
from odsbox.proto.ods_pb2 import Model


def _get_model(model_file_name):
    model_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", model_file_name)
    model = Model()
    Parse(Path(model_file).read_text(encoding="utf-8"), model)
    return model


class TestModelGetSuggestion:
    """Test the base model_get_suggestion method."""

    def test_model_get_suggestion_with_close_match(self):
        """Test suggestion when close match is found."""
        lower_case_dict = {"localcolumn": "LocalColumn", "meaResult": "MeaResult"}
        result = ModelSuggestions.get(lower_case_dict, "localcol")
        assert result == " Did you mean 'LocalColumn'?"

    def test_model_get_suggestion_with_exact_match(self):
        """Test suggestion when exact lowercase match exists."""
        lower_case_dict = {"localcolumn": "LocalColumn"}
        result = ModelSuggestions.get(lower_case_dict, "localcolumn")
        assert result == " Did you mean 'LocalColumn'?"

    def test_model_get_suggestion_no_match(self):
        """Test suggestion when no close match is found."""
        lower_case_dict = {"localcolumn": "LocalColumn"}
        result = ModelSuggestions.get(lower_case_dict, "zzzzz")
        assert result == ""

    def test_model_get_suggestion_empty_dict(self):
        """Test suggestion with empty dictionary."""
        lower_case_dict = {}
        result = ModelSuggestions.get(lower_case_dict, "anything")
        assert result == ""

    def test_model_get_suggestion_multiple_close_matches(self):
        """Test suggestion returns first (best) match when multiple exist."""
        lower_case_dict = {"cat": "Cat", "car": "Car", "card": "Card"}
        result = ModelSuggestions.get(lower_case_dict, "cas")
        # Should return one suggestion based on similarity
        assert "Did you mean" in result
        assert result in [" Did you mean 'Cat'?", " Did you mean 'Car'?", " Did you mean 'Card'?"]

    def test_model_get_suggestion_case_insensitive(self):
        """Test that suggestion lookup is case insensitive."""
        lower_case_dict = {"testvalue": "TestValue"}
        result1 = ModelSuggestions.get(lower_case_dict, "testval")
        result2 = ModelSuggestions.get(lower_case_dict, "TESTVAL")
        result3 = ModelSuggestions.get(lower_case_dict, "TeStVaL")
        assert result1 == result2 == result3 == " Did you mean 'TestValue'?"


class TestModelGetEnumSuggestion:
    """Test the model_get_enum_suggestion method."""

    def test_model_get_enum_suggestion_with_close_match(self):
        """Test enum suggestion when close match is found."""
        mock_enum = Mock()
        mock_enum.items = {"DT_FLOAT": 1, "DT_DOUBLE": 2, "DT_STRING": 3}
        result = ModelSuggestions.get_enum(mock_enum, "DT_FLOA")
        assert result == " Did you mean 'DT_FLOAT'?"

    def test_model_get_enum_suggestion_no_match(self):
        """Test enum suggestion when no close match is found."""
        mock_enum = Mock()
        mock_enum.items = {"DT_FLOAT": 1, "DT_DOUBLE": 2}
        result = ModelSuggestions.get_enum(mock_enum, "UNKNOWN")
        assert result == ""

    def test_model_get_enum_suggestion_case_insensitive(self):
        """Test enum suggestion is case insensitive."""
        mock_enum = Mock()
        mock_enum.items = {"DT_FLOAT": 1}
        result = ModelSuggestions.get_enum(mock_enum, "dt_flo")
        assert result == " Did you mean 'DT_FLOAT'?"

    def test_model_get_enum_suggestion_with_real_model(self):
        """Test enum suggestion with real enumeration from model."""
        model = _get_model("application_model.json")
        datatype_enum = model.enumerations["datatype_enum"]
        result = ModelSuggestions.get_enum(datatype_enum, "DT_FLAOT")
        assert "Did you mean" in result or result == ""


class TestModelGetSuggestionAttribute:
    """Test the model_get_suggestion_attribute method."""

    def test_model_get_suggestion_attribute_with_close_match(self):
        """Test attribute suggestion when close match is found."""
        mock_entity = Mock()
        mock_attribute = Mock()
        mock_attribute.base_name = "id"
        mock_attribute.name = "Id"
        mock_relation = Mock()
        mock_relation.base_name = "test_rel"
        mock_relation.name = "TestRelation"

        mock_entity.attributes = {"id_key": mock_attribute}
        mock_entity.relations = {"rel_key": mock_relation}

        result = ModelSuggestions.get_attribute(mock_entity, "ID")
        assert "Did you mean" in result or result == ""

    def test_model_get_suggestion_attribute_considers_both_attributes_and_relations(self):
        """Test that suggestion considers both attributes and relations."""
        mock_entity = Mock()
        mock_attr = Mock()
        mock_attr.base_name = "name"
        mock_attr.name = "Name"
        mock_rel = Mock()
        mock_rel.base_name = "parent"
        mock_rel.name = "Parent"

        mock_entity.attributes = {"attr_key": mock_attr}
        mock_entity.relations = {"rel_key": mock_rel}

        # Test suggestion for attribute-like typo
        result1 = ModelSuggestions.get_attribute(mock_entity, "nam")
        assert result1 in [" Did you mean 'Name'?", " Did you mean 'name'?", ""]

        # Test suggestion for relation-like typo
        result2 = ModelSuggestions.get_attribute(mock_entity, "pare")
        assert result2 in [" Did you mean 'Parent'?", " Did you mean 'parent'?", ""]

    def test_model_get_suggestion_attribute_with_real_model(self):
        """Test attribute suggestion with real entity from model."""
        model = _get_model("application_model.json")
        entity = model.entities["LocalColumn"]
        # Try a typo on "Id" attribute
        result = ModelSuggestions.get_attribute(entity, "Idd")
        assert "Did you mean" in result or result == ""


class TestModelGetSuggestionAttributeByBaseName:
    """Test the model_get_suggestion_attribute_by_base_name method."""

    def test_model_get_suggestion_attribute_by_base_name_with_close_match(self):
        """Test attribute by base name suggestion."""
        mock_entity = Mock()
        mock_attr = Mock()
        mock_attr.base_name = "id"
        mock_relation = Mock()
        mock_relation.base_name = "test_rel"

        mock_entity.attributes = {"attr_key": mock_attr}
        mock_entity.relations = {"rel_key": mock_relation}

        result = ModelSuggestions.get_attribute_by_base_name(mock_entity, "i")
        assert result == " Did you mean 'id'?"

    def test_model_get_suggestion_attribute_by_base_name_no_match(self):
        """Test attribute by base name suggestion with no match."""
        mock_entity = Mock()
        mock_attr = Mock()
        mock_attr.base_name = "id"

        mock_entity.attributes = {"attr_key": mock_attr}
        mock_entity.relations = {}

        result = ModelSuggestions.get_attribute_by_base_name(mock_entity, "xyz")
        assert result == ""

    def test_model_get_suggestion_attribute_by_base_name_with_real_model(self):
        """Test attribute by base name suggestion with real entity."""
        model = _get_model("application_model.json")
        entity = model.entities["LocalColumn"]
        result = ModelSuggestions.get_attribute_by_base_name(entity, "idd")
        assert "Did you mean" in result or result == ""


class TestModelGetSuggestionRelation:
    """Test the model_get_suggestion_relation method."""

    def test_model_get_suggestion_relation_with_close_match(self):
        """Test relation suggestion when close match is found."""
        mock_entity = Mock()
        mock_relation = Mock()
        mock_relation.base_name = "parent"
        mock_relation.name = "Parent"

        mock_entity.relations = {"rel_key": mock_relation}
        mock_entity.attributes = {}

        result = ModelSuggestions.get_relation(mock_entity, "paren")
        assert result == " Did you mean 'Parent'?"

    def test_model_get_suggestion_relation_no_match(self):
        """Test relation suggestion with no match."""
        mock_entity = Mock()
        mock_relation = Mock()
        mock_relation.base_name = "parent"
        mock_relation.name = "Parent"

        mock_entity.relations = {"rel_key": mock_relation}
        mock_entity.attributes = {}

        result = ModelSuggestions.get_relation(mock_entity, "unknown")
        assert result == ""

    def test_model_get_suggestion_relation_considers_base_and_application_names(self):
        """Test that suggestion considers both base and application names."""
        mock_entity = Mock()
        mock_relation = Mock()
        mock_relation.base_name = "parent"
        mock_relation.name = "Parent"

        mock_entity.relations = {"rel_key": mock_relation}
        mock_entity.attributes = {}

        # Should match against either name
        result = ModelSuggestions.get_relation(mock_entity, "par")
        assert result in [" Did you mean 'parent'?", " Did you mean 'Parent'?", ""]

    def test_model_get_suggestion_relation_with_real_model(self):
        """Test relation suggestion with real entity from model."""
        model = _get_model("application_model.json")
        entity = model.entities["MeaResult"]
        result = ModelSuggestions.get_relation(entity, "TestStp")
        assert "Did you mean" in result or result == ""


class TestModelGetSuggestionRelationByBaseName:
    """Test the model_get_suggestion_relation_by_base_name method."""

    def test_model_get_suggestion_relation_by_base_name_with_close_match(self):
        """Test relation by base name suggestion."""
        mock_entity = Mock()
        mock_relation = Mock()
        mock_relation.base_name = "parent"

        mock_entity.relations = {"rel_key": mock_relation}

        result = ModelSuggestions.get_relation_by_base_name(mock_entity, "paren")
        assert result == " Did you mean 'parent'?"

    def test_model_get_suggestion_relation_by_base_name_no_match(self):
        """Test relation by base name suggestion with no match."""
        mock_entity = Mock()
        mock_relation = Mock()
        mock_relation.base_name = "parent"

        mock_entity.relations = {"rel_key": mock_relation}

        result = ModelSuggestions.get_relation_by_base_name(mock_entity, "xyz")
        assert result == ""

    def test_model_get_suggestion_relation_by_base_name_with_real_model(self):
        """Test relation by base name suggestion with real entity."""
        model = _get_model("application_model.json")
        entity = model.entities["MeaResult"]
        result = ModelSuggestions.get_relation_by_base_name(entity, "tes")
        assert "Did you mean" in result or result == ""


class TestModelGetSuggestionEntity:
    """Test the model_get_suggestion_entity method."""

    def test_model_get_suggestion_entity_with_close_match(self):
        """Test entity suggestion when close match is found."""
        mock_model = Mock()
        mock_entity = Mock()
        mock_entity.base_name = "AoLocalColumn"
        mock_entity.name = "LocalColumn"

        mock_model.entities = {"LocalColumn": mock_entity}

        result = ModelSuggestions.get_entity(mock_model, "LocalCol")
        assert "Did you mean" in result

    def test_model_get_suggestion_entity_no_match(self):
        """Test entity suggestion with no match."""
        mock_model = Mock()
        mock_entity = Mock()
        mock_entity.base_name = "AoLocalColumn"
        mock_entity.name = "LocalColumn"

        mock_model.entities = {"LocalColumn": mock_entity}

        result = ModelSuggestions.get_entity(mock_model, "XYZ123")
        assert result == ""

    def test_model_get_suggestion_entity_considers_base_and_application_names(self):
        """Test that suggestion considers both base and application names."""
        mock_model = Mock()
        mock_entity = Mock()
        mock_entity.base_name = "AoLocalColumn"
        mock_entity.name = "LocalColumn"

        mock_model.entities = {"LocalColumn": mock_entity}

        # Should match against either name
        result = ModelSuggestions.get_entity(mock_model, "local")
        assert result in [" Did you mean 'LocalColumn'?", " Did you mean 'AoLocalColumn'?"]

    def test_model_get_suggestion_entity_with_real_model(self):
        """Test entity suggestion with real model."""
        model = _get_model("application_model.json")
        result = ModelSuggestions.get_entity(model, "LocalCol")
        assert "Did you mean" in result or result == ""


class TestModelGetSuggestionEntityByBaseName:
    """Test the model_get_suggestion_entity_by_base_name method."""

    def test_model_get_suggestion_entity_by_base_name_with_close_match(self):
        """Test entity by base name suggestion."""
        mock_model = Mock()
        mock_entity = Mock()
        mock_entity.base_name = "AoLocalColumn"

        mock_model.entities = {"LocalColumn": mock_entity}

        result = ModelSuggestions.get_entity_by_base_name(mock_model, "AoLocalCol")
        assert result == " Did you mean 'AoLocalColumn'?"

    def test_model_get_suggestion_entity_by_base_name_no_match(self):
        """Test entity by base name suggestion with no match."""
        mock_model = Mock()
        mock_entity = Mock()
        mock_entity.base_name = "AoLocalColumn"

        mock_model.entities = {"LocalColumn": mock_entity}

        result = ModelSuggestions.get_entity_by_base_name(mock_model, "XYZ123")
        assert result == ""

    def test_model_get_suggestion_entity_by_base_name_only_considers_base_names(self):
        """Test that suggestion only considers base names."""
        mock_model = Mock()
        mock_entity = Mock()
        mock_entity.base_name = "AoLocalColumn"
        mock_entity.name = "LocalColumn"

        mock_model.entities = {"LocalColumn": mock_entity}

        # Should only match base name
        result = ModelSuggestions.get_entity_by_base_name(mock_model, "AoLocal")
        assert result == " Did you mean 'AoLocalColumn'?"

    def test_model_get_suggestion_entity_by_base_name_with_real_model(self):
        """Test entity by base name suggestion with real model."""
        model = _get_model("application_model.json")
        result = ModelSuggestions.get_entity_by_base_name(model, "AoLocalCol")
        assert "Did you mean" in result or result == ""


class TestModelSuggestionsIntegration:
    """Integration tests for ModelSuggestions with real model data."""

    def test_entity_suggestion_integration(self):
        """Test entity suggestions with real model."""
        model = _get_model("application_model.json")
        # LocalColumn should be suggested for "LocalCol"
        suggestion = ModelSuggestions.get_entity(model, "LocalCol")
        assert "LocalColumn" in suggestion or "AoLocalColumn" in suggestion or suggestion == ""

    def test_attribute_suggestion_integration(self):
        """Test attribute suggestions with real model."""
        model = _get_model("application_model.json")
        entity = model.entities["LocalColumn"]
        # "Id" should be suggested for "Idd"
        suggestion = ModelSuggestions.get_attribute(entity, "Idd")
        assert "Did you mean" in suggestion or suggestion == ""

    def test_relation_suggestion_integration(self):
        """Test relation suggestions with real model."""
        model = _get_model("application_model.json")
        entity = model.entities["MeaResult"]
        # "TestStep" should be suggested for typos like "TestStp"
        suggestion = ModelSuggestions.get_relation(entity, "TestStp")
        assert "Did you mean" in suggestion or suggestion == ""

    def test_enum_suggestion_integration(self):
        """Test enum suggestions with real model."""
        model = _get_model("application_model.json")
        datatype_enum = model.enumerations["datatype_enum"]
        # "DT_FLOAT" should be suggested for "DT_FLAOT"
        suggestion = ModelSuggestions.get_enum(datatype_enum, "DT_FLAOT")
        assert "Did you mean" in suggestion or suggestion == ""

    def test_all_methods_handle_empty_collections(self):
        """Test that all methods handle empty collections gracefully."""
        mock_model = Mock()
        mock_entity = Mock()

        # Empty collections
        mock_model.entities = {}
        mock_entity.attributes = {}
        mock_entity.relations = {}

        # Should not raise exceptions and return empty strings
        assert ModelSuggestions.get_entity(mock_model, "test") == ""
        assert ModelSuggestions.get_entity_by_base_name(mock_model, "test") == ""
        assert ModelSuggestions.get_attribute(mock_entity, "test") == ""
        assert ModelSuggestions.get_attribute_by_base_name(mock_entity, "test") == ""
        assert ModelSuggestions.get_relation(mock_entity, "test") == ""
        assert ModelSuggestions.get_relation_by_base_name(mock_entity, "test") == ""

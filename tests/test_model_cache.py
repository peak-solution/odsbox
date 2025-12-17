"""Test ModelCache functionality."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from google.protobuf.json_format import Parse

from odsbox.model_cache import ModelCache
from odsbox.proto.ods_pb2 import Model


def __get_model(model_file_name):
    model_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", model_file_name)
    model = Model()
    Parse(Path(model_file).read_text(encoding="utf-8"), model)
    return model


def test_base_name_access():
    model = __get_model("application_model.json")

    mc = ModelCache(model)
    local_column_entity = mc.entity_by_base_name("AoLocalColumn")
    assert 82 == local_column_entity.aid
    assert "LocalColumn" == local_column_entity.name
    assert "AoLocalColumn" == local_column_entity.base_name
    assert "LocalColumn" == mc.entity_by_base_name("aolocalcolumn").name
    assert 82 == mc.aid("LocalColumn")

    id_attribute = mc.attribute_by_base_name(local_column_entity, "id")
    assert "id" == id_attribute.base_name
    assert "Id" == id_attribute.name
    assert "id" == mc.attribute_by_base_name(local_column_entity, "ID").base_name
    assert "id" == mc.attribute_by_base_name("LocalColumn", "id").base_name
    assert "id" == mc.attribute_by_base_name("LocalColumn", "ID").base_name
    assert "id" == mc.attribute_by_base_name("localcolumn", "id").base_name
    assert "id" == mc.attribute_by_base_name("localcolumn", "ID").base_name

    submatrix_relation = mc.relation_by_base_name(local_column_entity, "submatrix")
    assert "submatrix" == submatrix_relation.base_name
    assert "SubMatrix" == submatrix_relation.name
    assert "submatrix" == mc.relation_by_base_name(local_column_entity, "SUBMATRIX").base_name
    assert "submatrix" == mc.relation_by_base_name("LocalColumn", "submatrix").base_name
    assert "submatrix" == mc.relation_by_base_name("LocalColumn", "SUBMATRIX").base_name
    assert "submatrix" == mc.relation_by_base_name("localcolumn", "submatrix").base_name
    assert "submatrix" == mc.relation_by_base_name("localcolumn", "SUBMATRIX").base_name

    with pytest.raises(ValueError, match="No entity derived from base type 'DoesNotExist' found."):
        mc.entity_by_base_name("DoesNotExist")
    with pytest.raises(ValueError, match="No entity named 'DoesNotExist' found."):
        mc.attribute_by_base_name("DoesNotExist", "id")
    with pytest.raises(ValueError, match="No entity named 'DoesNotExist' found."):
        mc.relation_by_base_name("DoesNotExist", "id")
    with pytest.raises(ValueError, match="Entity 'LocalColumn' does not have attribute derived from 'DoesNotExist'."):
        mc.attribute_by_base_name("LocalColumn", "DoesNotExist")
    with pytest.raises(ValueError, match="Entity 'LocalColumn' does not have relation derived from 'DoesNotExist'."):
        mc.relation_by_base_name("LocalColumn", "DoesNotExist")


def test_entity():
    mc = ModelCache(__get_model("application_model.json"))
    assert "LocalColumn" == mc.entity("LocalColumn").name
    assert "LocalColumn" == mc.entity("localcolumn").name
    with pytest.raises(ValueError, match="No entity named 'DoesNotExist' found."):
        mc.entity("DoesNotExist")


def test_aid():
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("LocalColumn")
    assert entity.aid == mc.aid("LocalColumn")
    assert entity.aid == mc.aid(entity)
    with pytest.raises(ValueError, match="No entity named 'DoesNotExist' found."):
        mc.aid("DoesNotExist")


def test_entity_by_aid():
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("LocalColumn")
    assert entity.name == mc.entity_by_aid(entity.aid).name
    with pytest.raises(ValueError, match="No entity found with aid '0'."):
        mc.entity_by_aid(0)


def test_attribute_no_throw():
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("MeaResult")
    assert "StorageType" == mc.attribute_no_throw(entity, "StorageType").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw(entity, "STORAGETYPE").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw(entity, "ao_storagetype").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw(entity, "ao_storagetype").name  # type: ignore
    assert mc.attribute_no_throw(entity, "DoesNotExist") is None

    assert "StorageType" == mc.attribute_no_throw("MeaResult", "StorageType").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw("MeaResult", "STORAGETYPE").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw("MeaResult", "ao_storagetype").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw("MeaResult", "ao_storagetype").name  # type: ignore
    assert mc.attribute_no_throw("MeaResult", "DoesNotExist") is None

    assert "StorageType" == mc.attribute_no_throw("MEARESULT", "StorageType").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw("MEARESULT", "STORAGETYPE").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw("MEARESULT", "ao_storagetype").name  # type: ignore
    assert "StorageType" == mc.attribute_no_throw("MEARESULT", "ao_storagetype").name  # type: ignore
    assert mc.attribute_no_throw("MEARESULT", "DoesNotExist") is None


def test_attribute():
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("MeaResult")
    assert "StorageType" == mc.attribute(entity, "StorageType").name
    assert "StorageType" == mc.attribute(entity, "STORAGETYPE").name
    assert "StorageType" == mc.attribute(entity, "ao_storagetype").name
    assert "StorageType" == mc.attribute(entity, "ao_storagetype").name

    assert "StorageType" == mc.attribute("MeaResult", "StorageType").name
    assert "StorageType" == mc.attribute("MeaResult", "STORAGETYPE").name
    assert "StorageType" == mc.attribute("MeaResult", "ao_storagetype").name
    assert "StorageType" == mc.attribute("MeaResult", "ao_storagetype").name

    assert "StorageType" == mc.attribute("MEARESULT", "StorageType").name
    assert "StorageType" == mc.attribute("MEARESULT", "STORAGETYPE").name
    assert "StorageType" == mc.attribute("MEARESULT", "ao_storagetype").name
    assert "StorageType" == mc.attribute("MEARESULT", "ao_storagetype").name
    with pytest.raises(
        ValueError, match="'MeaResult' has no attribute named 'DoesNotExist' as base or application name."
    ):
        mc.attribute(entity, "DoesNotExist")
    with pytest.raises(
        ValueError, match="'MeaResult' has no attribute named 'DoesNotExist' as base or application name."
    ):
        mc.attribute("MeaResult", "DoesNotExist")
    with pytest.raises(
        ValueError, match="'MeaResult' has no attribute named 'DoesNotExist' as base or application name."
    ):
        mc.attribute("MEARESULT", "DoesNotExist")


def test_relation_no_throw():
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("MeaResult")
    assert "TestStep" == mc.relation_no_throw(entity, "test").name  # type: ignore
    assert "TestStep" == mc.relation_no_throw(entity, "TEST").name  # type: ignore
    assert "TestStep" == mc.relation_no_throw(entity, "TestStep").name  # type: ignore
    assert "TestStep" == mc.relation_no_throw(entity, "TESTSTEP").name  # type: ignore
    assert mc.relation_no_throw(entity, "DoesNotExist") is None

    assert "TestStep" == mc.relation_no_throw("MeaResult", "test").name  # type: ignore
    assert "TestStep" == mc.relation_no_throw("MeaResult", "TEST").name  # type: ignore
    assert "TestStep" == mc.relation_no_throw("MEARESULT", "TestStep").name  # type: ignore
    assert "TestStep" == mc.relation_no_throw("MEARESULT", "TESTSTEP").name  # type: ignore
    assert mc.relation_no_throw("MEARESULT", "DoesNotExist") is None


def test_relation():
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("MeaResult")
    assert "TestStep" == mc.relation(entity, "test").name
    assert "TestStep" == mc.relation(entity, "TEST").name
    assert "TestStep" == mc.relation(entity, "TestStep").name
    assert "TestStep" == mc.relation(entity, "TESTSTEP").name

    assert "TestStep" == mc.relation("MeaResult", "test").name
    assert "TestStep" == mc.relation("MeaResult", "TEST").name
    assert "TestStep" == mc.relation("MEARESULT", "TestStep").name
    assert "TestStep" == mc.relation("MEARESULT", "TESTSTEP").name

    with pytest.raises(
        ValueError, match="'MeaResult' has no relation named 'DoesNotExist' as base or application name."
    ):
        mc.relation(entity, "DoesNotExist")
    with pytest.raises(
        ValueError, match="'MeaResult' has no relation named 'DoesNotExist' as base or application name."
    ):
        mc.relation("MEARESULT", "DoesNotExist")


def test_enumeration_access():
    mc = ModelCache(__get_model("application_model.json"))
    assert "datatype_enum" == mc.enumeration("datatype_enum").name
    assert "datatype_enum" == mc.enumeration("DataType_Enum").name
    with pytest.raises(ValueError):
        mc.enumeration("DoesNotExist")


def test_enumeration_values():
    mc = ModelCache(__get_model("application_model.json"))
    datatype_enum = mc.enumeration("datatype_enum")
    assert datatype_enum is not None

    assert 3 == mc.enumeration_key_to_value(datatype_enum, "DT_FLOAT")
    assert 3 == mc.enumeration_key_to_value(datatype_enum, "dt_float")
    assert "DT_FLOAT" == mc.enumeration_value_to_key(datatype_enum, 3)

    assert 3 == mc.enumeration_key_to_value("datatype_enum", "DT_FLOAT")
    assert 3 == mc.enumeration_key_to_value("datatype_enum", "dt_float")
    assert "DT_FLOAT" == mc.enumeration_value_to_key("datatype_enum", 3)

    assert 3 == mc.enumeration_key_to_value("DATATYPE_ENUM", "DT_FLOAT")
    assert 3 == mc.enumeration_key_to_value("DATATYPE_ENUM", "dt_float")
    assert "DT_FLOAT" == mc.enumeration_value_to_key("DATATYPE_ENUM", 3)

    with pytest.raises(ValueError):
        assert 3 == mc.enumeration_key_to_value("DOESNOTEXIST", "DT_FLOAT")
    with pytest.raises(ValueError):
        assert 3 == mc.enumeration_key_to_value("DATATYPE_ENUM", "DOESNOTEXIST")
    with pytest.raises(ValueError):
        assert "DT_FLOAT" == mc.enumeration_value_to_key("DOESNOTEXIST", 3)
    with pytest.raises(ValueError):
        assert "DT_FLOAT" == mc.enumeration_value_to_key("DATATYPE_ENUM", 501)


def test_entity_suggestion_in_error():
    """Test that entity suggestion is included in error message for invalid entity."""
    mc = ModelCache(__get_model("application_model.json"))
    # Test suggestion for typo close to "LocalColumn"
    with pytest.raises(ValueError, match="No entity named 'LocalCol' found."):
        mc.entity("LocalCol")
    # Test that error message includes suggestion
    with pytest.raises(ValueError, match="Did you mean"):
        mc.entity("LocalCol")


def test_entity_by_base_name_suggestion_in_error():
    """Test that entity suggestion is included in error message for invalid base name."""
    mc = ModelCache(__get_model("application_model.json"))
    # Test suggestion for typo close to "AoLocalColumn"
    with pytest.raises(ValueError, match="No entity derived from base type"):
        mc.entity_by_base_name("AoLocalCol")
    # Test that error message includes suggestion
    with pytest.raises(ValueError, match="Did you mean"):
        mc.entity_by_base_name("AoLocalCol")


def test_attribute_suggestion_in_error():
    """Test that attribute suggestion is included in error message for invalid attribute."""
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("LocalColumn")
    # Test suggestion for typo close to "Id"
    with pytest.raises(ValueError, match="'LocalColumn' has no attribute named 'Ident'"):
        mc.attribute(entity, "Ident")
    # Test that error message includes suggestion
    with pytest.raises(ValueError, match="Did you mean"):
        mc.attribute(entity, "Ident")


def test_attribute_by_base_name_suggestion_in_error():
    """Test that attribute suggestion is included in error message for invalid base name."""
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("LocalColumn")
    # Test suggestion for typo close to "id"
    with pytest.raises(ValueError, match="Entity 'LocalColumn' does not have attribute derived from"):
        mc.attribute_by_base_name(entity, "idd")
    # Test that error message includes suggestion
    with pytest.raises(ValueError, match="Did you mean"):
        mc.attribute_by_base_name(entity, "idd")


def test_relation_suggestion_in_error():
    """Test that relation suggestion is included in error message for invalid relation."""
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("MeaResult")
    # Test suggestion for typo close to "TestStep"
    with pytest.raises(ValueError, match="'MeaResult' has no relation named 'TestStp'"):
        mc.relation(entity, "TestStp")
    # Test that error message includes suggestion
    with pytest.raises(ValueError, match="Did you mean"):
        mc.relation(entity, "TestStp")


def test_relation_by_base_name_suggestion_in_error():
    """Test that relation suggestion is included in error message for invalid base name."""
    mc = ModelCache(__get_model("application_model.json"))
    entity = mc.entity("MeaResult")
    # Test suggestion for typo close to "test" (base name)
    with pytest.raises(ValueError, match="Entity 'MeaResult' does not have relation derived from"):
        mc.relation_by_base_name(entity, "tes")
    # Test that error message includes suggestion
    with pytest.raises(ValueError, match="Did you mean"):
        mc.relation_by_base_name(entity, "tes")


def test_enum_value_suggestion_in_error():
    """Test that enum value suggestion is included in error message for invalid enum key."""
    mc = ModelCache(__get_model("application_model.json"))
    datatype_enum = mc.enumeration("datatype_enum")
    # Test suggestion for typo close to "DT_FLOAT"
    with pytest.raises(ValueError, match="Enumeration 'datatype_enum' does not contain the key 'DT_FLAOT'"):
        mc.enumeration_key_to_value(datatype_enum, "DT_FLAOT")
    # Test that error message includes suggestion
    with pytest.raises(ValueError, match="Did you mean"):
        mc.enumeration_key_to_value(datatype_enum, "DT_FLAOT")

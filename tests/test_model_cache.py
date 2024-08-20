"""Test ModelCache functionality."""

from __future__ import annotations

import os
from pathlib import Path

from google.protobuf.json_format import Parse

from odsbox.model_cache import ModelCache
from odsbox.proto.ods_pb2 import Model


def __get_model(model_file_name):
    model_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", model_file_name)
    model = Model()
    Parse(Path(model_file).read_text(encoding="utf-8"), model)
    return model


def test_model_cache():
    model = __get_model("mdm_nvh_model.json")

    mc = ModelCache(model)
    local_column_entity = mc.entity_by_base_name("AoLocalColumn")
    assert 82 == local_column_entity.aid
    assert "LocalColumn" == local_column_entity.name
    assert "AoLocalColumn" == local_column_entity.base_name
    assert "LocalColumn" == mc.entity_by_base_name("aolocalcolumn").name
    assert 82 == mc.aid_by_entity_name("LocalColumn")

    id_attribute = mc.attribute_by_base_name(local_column_entity, "id")
    assert "id" == id_attribute.base_name
    assert "Id" == id_attribute.name
    assert "id" == mc.attribute_by_base_name(local_column_entity, "ID").base_name
    assert "id" == mc.attribute_by_base_name("LocalColumn", "id").base_name
    assert "id" == mc.attribute_by_base_name("LocalColumn", "ID").base_name

    submatrix_relation = mc.relation_by_base_name(local_column_entity, "submatrix")
    assert "submatrix" == submatrix_relation.base_name
    assert "SubMatrix" == submatrix_relation.name
    assert "submatrix" == mc.relation_by_base_name(local_column_entity, "SUBMATRIX").base_name
    assert "submatrix" == mc.relation_by_base_name("LocalColumn", "submatrix").base_name
    assert "submatrix" == mc.relation_by_base_name("LocalColumn", "SUBMATRIX").base_name

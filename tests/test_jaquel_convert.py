# pylint: disable=C0114, C0115, C0116, E1101
import os
import json
from pathlib import Path
import logging
import pytest

from google.protobuf.json_format import MessageToJson, Parse

from odsbox.jaquel import jaquel_to_ods
import odsbox.proto.ods_pb2 as ods


def __get_model(model_file_name):
    model_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", model_file_name)
    model = ods.Model()
    Parse(Path(model_file).read_text(encoding="utf-8"), model)
    return model


def test_conversion1():
    model = __get_model("mdm_nvh_model.json")

    entity, ss = jaquel_to_ods(
        model,
        {
            "AoMeasurement": {
                "$or": [
                    {"measurement_quantities.maximum": {"$gte": 1, "$lt": 2}},
                    {"measurement_quantities.maximum": {"$gte": 3, "$lt": 4}},
                    {"measurement_quantities.maximum": {"$gte": 6, "$lt": 7}},
                ]
            },
            "$options": {"$rowlimit": 1000, "$rowskip": 500, "$seqlimit": 1000, "$seqskip": 500},
            "$attributes": {"name": 1, "id": 1, "test": {"name": 1, "id": 1}},
            "$orderby": {"name": 1},
        },
    )
    logging.getLogger().info(MessageToJson(ss))
    assert entity is not None
    assert entity.name == "MeaResult"
    assert ss is not None


def test_conversion2():
    model = __get_model("mdm_nvh_model.json")

    entity, ss = jaquel_to_ods(
        model,
        """{
    "AoMeasurement": {
        "measurement_begin": {
            "$between": [
                "2012-04-22T00:00:00.010000Z",
                "2012-04-23T00:00:00.000000Z"
            ]
        }
    }
}""",
    )
    logging.getLogger().info(MessageToJson(ss))
    assert entity is not None
    assert entity.name == "MeaResult"
    assert ss is not None


def __read_json_file(file_path):
    with open(file_path, encoding="utf-8") as fh:
        return json.load(fh)


def test_predefined():
    predefined_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", "jaquel")

    model = __get_model("mdm_nvh_model.json")

    for path in Path(predefined_path).rglob("*.json"):
        logging.getLogger().info(path.stem)

        jaquel_dict = __read_json_file(path)

        _entity, select_statement = jaquel_to_ods(model, jaquel_dict)

        select_statement_ref = ods.SelectStatement()
        Parse(
            Path(str(path) + ".proto").read_text(encoding="utf-8"),
            select_statement_ref,
        )
        assert select_statement_ref == select_statement


def test_syntax_errors():
    model = __get_model("mdm_nvh_model.json")

    with pytest.raises(SyntaxError, match="Does not define a target entity."):
        jaquel_to_ods(model, {"$attributes": {"factor": {"$min": 1}}})

    with pytest.raises(SyntaxError, match='Unknown aggregate "\\$mi"'):
        jaquel_to_ods(model, {"AoUnit": {}, "$attributes": {"factor": {"$mi": 1}}})

    with pytest.raises(SyntaxError, match='Unknown operator "\\$lik"'):
        jaquel_to_ods(model, {"AoLocalColumn": {"name": {"$lik": "abc"}}})

    with pytest.raises(SyntaxError, match="'name' is no relation of entity 'LocalColumn'"):
        jaquel_to_ods(model, {"AoLocalColumn": {"name": {"like": "abc"}}})

    with pytest.raises(json.decoder.JSONDecodeError):
        jaquel_to_ods(model, "{")

    with pytest.raises(SyntaxError, match="'nr_of_rows' is neither attribute nor relation of entity 'SubMatrix'"):
        jaquel_to_ods(
            model, {"AoLocalColumn": {}, "$attributes": {"Id": 1, "name": 1, "submatrix": {"nr_of_rows": 1, "name": 1}}}
        )

    with pytest.raises(SyntaxError, match="'nr_of_rows' is neither attribute nor relation of entity 'SubMatrix'"):
        jaquel_to_ods(model, {"AoLocalColumn": {}, "$attributes": {"Id": 1, "name": 1, "submatrix.nr_of_rows": 1}})

    with pytest.raises(SyntaxError, match="'nr_of_rows' is neither attribute nor relation of entity 'SubMatrix'"):
        jaquel_to_ods(model, {"AoSubmatrix": {}, "$attributes": {"Id": 1, "nr_of_rows": 1}})

    with pytest.raises(SyntaxError, match="'DoesNotExist' is neither attribute nor relation of entity 'Unit'"):
        jaquel_to_ods(model, {"AoUnit": {}, "$attributes": {"DoesNotExist": 1}})

    with pytest.raises(SyntaxError, match="'doesnotexist' is neither attribute nor relation of entity 'PhysDimension'"):
        jaquel_to_ods(model, {"AoUnit": {"phys_dimension.doesnotexist": "abc"}})

    with pytest.raises(SyntaxError, match="'physical_dimension' is no relation of entity 'Unit'"):
        jaquel_to_ods(model, {"AoUnit": {"physical_dimension.doesnotexist": "abc"}})

    with pytest.raises(SyntaxError, match="'doesnotexist' is neither attribute nor relation of entity 'Unit'"):
        jaquel_to_ods(model, {"AoUnit": {"doesnotexist": "abc"}})

    with pytest.raises(SyntaxError, match="Entity 'DoesNotExist' is unknown in model."):
        jaquel_to_ods(model, {"DoesNotExist": 1})

    with pytest.raises(SyntaxError, match="47567 is no valid entity aid."):
        jaquel_to_ods(model, {"47567": 1})

    with pytest.raises(SyntaxError, match="Only id value can be assigned directly. But 'abc' was assigned."):
        jaquel_to_ods(model, {26: "abc"})

    with pytest.raises(SyntaxError, match="47567 is no valid entity aid."):
        jaquel_to_ods(model, {47567: 1})

    with pytest.raises(SyntaxError, match=r"Does not define a target entity."):
        jaquel_to_ods(model, "{}")

    with pytest.raises(SyntaxError, match=r"Does not define a target entity."):
        jaquel_to_ods(model, {})

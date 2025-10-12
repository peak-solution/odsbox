# pylint: disable=C0114, C0115, C0116, E1101
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

import pytest
from google.protobuf.json_format import MessageToJson, Parse

import odsbox.proto.ods_pb2 as ods
from odsbox.jaquel import jaquel_to_ods


def __get_model(model_file_name):
    model_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", model_file_name)
    model = ods.Model()
    Parse(Path(model_file).read_text(encoding="utf-8"), model)
    return model


def test_conversion1():
    model = __get_model("application_model.json")

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
    model = __get_model("application_model.json")

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

    model = __get_model("application_model.json")

    for path in Path(predefined_path).rglob("*.json"):
        logging.getLogger().info(path.stem)

        jaquel_dict = __read_json_file(path)

        _entity, select_statement = jaquel_to_ods(model, jaquel_dict)

        select_statement_ref = ods.SelectStatement()
        Parse(
            Path(str(path) + ".proto").read_text(encoding="utf-8"),
            select_statement_ref,
        )

        assert select_statement_ref == select_statement, f"Does not match {MessageToJson(select_statement)}"


def test_syntax_errors():
    model = __get_model("application_model.json")

    with pytest.raises(SyntaxError, match="Does not define a target entity."):
        jaquel_to_ods(model, {"$attributes": {"factor": {"$min": 1}}})

    with pytest.raises(SyntaxError, match="Unknown aggregate '\\$mi'."):
        jaquel_to_ods(model, {"AoUnit": {}, "$attributes": {"factor": {"$mi": 1}}})

    with pytest.raises(SyntaxError, match="Unknown operator '\\$lik'. Did you mean '\\$like'?'"):
        jaquel_to_ods(model, {"AoLocalColumn": {"name": {"$lik": "abc"}}})

    with pytest.raises(SyntaxError, match="'name' is no relation of entity 'LocalColumn'."):
        jaquel_to_ods(model, {"AoLocalColumn": {"name": {"like": "abc"}}})

    with pytest.raises(json.decoder.JSONDecodeError):
        jaquel_to_ods(model, "{")

    with pytest.raises(
        SyntaxError,
        match="'nr_of_rows' is neither attribute nor relation of entity 'SubMatrix'. Did you mean 'number_of_rows'?",
    ):
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

    with pytest.raises(SyntaxError, match="'47567' is not a valid entity aid."):
        jaquel_to_ods(model, {"47567": 1})

    with pytest.raises(SyntaxError, match="Only id value can be assigned directly. But 'abc' was assigned."):
        jaquel_to_ods(model, {26: "abc"})

    with pytest.raises(SyntaxError, match="'47567' is not a valid entity aid."):
        jaquel_to_ods(model, {47567: 1})

    with pytest.raises(SyntaxError, match=r"Does not define a target entity."):
        jaquel_to_ods(model, "{}")

    with pytest.raises(SyntaxError, match=r"Does not define a target entity."):
        jaquel_to_ods(model, {})


def test_example_queries():
    model = __get_model("application_model.json")

    with open(
        os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", "examples_from_jaquel_doc.json"),
        encoding="utf-8",
    ) as fh:
        example_queries = json.load(fh)

    for example_query in example_queries:
        logging.getLogger().info(example_query)

        entity, select_statement = jaquel_to_ods(model, example_query)
        assert entity is not None
        assert select_statement is not None


def test_datetime_handling():
    model = __get_model("application_model.json")
    _, select_statement = jaquel_to_ods(
        model, {"AoMeasurement": {"measurement_begin": datetime(2024, 1, 15, 16, 33, 55, 123456)}}
    )
    assert '"20240115163355123456"' in MessageToJson(select_statement)

    _, select_statement = jaquel_to_ods(
        model, {"AoMeasurement": {"measurement_begin": datetime(2024, 1, 15, 16, 33, 55)}}
    )
    assert '"20240115163355"' in MessageToJson(select_statement)

    _, select_statement = jaquel_to_ods(model, {"AoMeasurement": {"measurement_begin": datetime(2024, 1, 15, 16, 33)}})
    assert '"20240115163300"' in MessageToJson(select_statement)

    _, select_statement = jaquel_to_ods(model, {"AoMeasurement": {"measurement_begin": "20240115163355123456"}})
    assert '"20240115163355123456"' in MessageToJson(select_statement)

    _, select_statement = jaquel_to_ods(model, {"AoMeasurement": {"measurement_begin": "2024-01-15T16:33:55.123456Z"}})
    assert '"20240115163355123456"' in MessageToJson(select_statement)

    _, select_statement = jaquel_to_ods(model, {"AoMeasurement": {"measurement_begin": "2024-01-15T16:33:55Z"}})
    assert '"20240115163355"' in MessageToJson(select_statement)

    _, select_statement = jaquel_to_ods(model, {"AoMeasurement": {"measurement_begin": "2024-01-15T16:33:55"}})
    assert '"20240115163355"' in MessageToJson(select_statement)


def test_is_in():
    model = __get_model("application_model.json")
    _, select_statement = jaquel_to_ods(
        model, {"AoMeasurementQuantity": {"datatype": {"$in": ["DT_STRING", "DT_DOUBLE"]}}}
    )
    assert select_statement is not None

    _, select_statement = jaquel_to_ods(model, {"AoMeasurementQuantity": {"name": {"$in": ["first", "second"]}}})
    assert select_statement is not None

    with pytest.raises(SyntaxError, match="Enum entry for 'does_not_exist' does not exist."):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"datatype": {"$in": ["does_not_exist"]}}})

    with pytest.raises(SyntaxError, match="Enum entry for 'DTLONG' does not exist."):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"datatype": {"$in": ["DTLONG"]}}})


def test_suggestions_enum():
    model = __get_model("application_model.json")

    with pytest.raises(SyntaxError, match="Enum entry for 'DTLONG' does not exist. Did you mean 'DT_LONG'?"):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"datatype": "DTLONG"}})

    with pytest.raises(SyntaxError, match="Enum entry for 'dtlong' does not exist. Did you mean 'DT_LONG'?"):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"datatype": "dtlong"}})

    with pytest.raises(SyntaxError, match="Enum entry for 'LONG' does not exist. Did you mean 'DT_LONG'?"):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"datatype": "LONG"}})

    with pytest.raises(SyntaxError, match="Enum entry for 'INT32' does not exist."):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"datatype": "INT32"}})


def test_suggestions_attribute():
    model = __get_model("application_model.json")

    with pytest.raises(
        SyntaxError,
        match="'data_type' is neither attribute nor relation of entity 'MeaQuantity'. Did you mean 'DataType'?",
    ):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"data_type": "DT_LONG"}})

    with pytest.raises(
        SyntaxError,
        match="'units' is neither attribute nor relation of entity 'MeaQuantity'. Did you mean 'Unit'?",
    ):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"units": 4711}})


def test_suggestions_relation():
    model = __get_model("application_model.json")

    with pytest.raises(
        SyntaxError,
        match="'units' is no relation of entity 'MeaQuantity'. Did you mean 'Unit'?",
    ):
        jaquel_to_ods(model, {"AoMeasurementQuantity": {"units.name": "m"}})


def test_suggestions_entity():
    model = __get_model("application_model.json")

    with pytest.raises(
        SyntaxError,
        match="Entity 'AoMeasurmentQuantity' is unknown in model. Did you mean 'AoMeasurementQuantity'?",
    ):
        jaquel_to_ods(model, {"AoMeasurmentQuantity": {"datatype": "DT_LONG"}})

    with pytest.raises(
        SyntaxError,
        match="Entity 'MeasurmentQuantity' is unknown in model. Did you mean 'AoMeasurementQuantity'?",
    ):
        jaquel_to_ods(model, {"MeasurmentQuantity": {"datatype": "DT_LONG"}})

    with pytest.raises(
        SyntaxError,
        match="Entity 'AoMeasurmentQuantity' is unknown in model. Did you mean 'AoMeasurementQuantity'?",
    ):
        jaquel_to_ods(model, {"AoMeasurmentQuantity": {"datatype": "DT_LONG"}})

    with pytest.raises(
        SyntaxError,
        match="Entity 'AoTests' is unknown in model. Did you mean 'AoTest'?",
    ):
        jaquel_to_ods(model, {"AoTests": {"name": "Start"}})


def test_suggestions_aggregate():
    model = __get_model("application_model.json")

    with pytest.raises(SyntaxError, match="Unknown aggregate '\\$stev'. Did you mean '\\$stddev'?"):
        jaquel_to_ods(model, {"AoUnit": {}, "$attributes": {"factor": {"$stev": 1}}})

    with pytest.raises(SyntaxError, match="Unknown aggregate '\\$regexp'. Did you mean '\\$nested'?"):
        jaquel_to_ods(model, {"AoUnit": {}, "$attributes": {"factor": {"$regexp": "a.*"}}})

    with pytest.raises(SyntaxError, match="Unknown operator '\\$GTEQ'. Did you mean '\\$gte'?"):
        jaquel_to_ods(model, {"AoUnit": {"factor": {"$GTEQ": 2.0}}})

    with pytest.raises(SyntaxError, match="Unknown operator '\\$gtE'. Did you mean '\\$gte'?."):
        jaquel_to_ods(model, {"AoUnit": {"factor": {"$gtE": 2.0}}})

    with pytest.raises(SyntaxError, match="Unknown operator '\\$abc'. Did you mean '\\$between'?"):
        jaquel_to_ods(model, {"AoUnit": {"factor": {"$abc": 2.0}}})


def test_issue_127_not_null():
    model = __get_model("application_model.json")

    entity, ss = jaquel_to_ods(
        model,
        {"AoMeasurement": {"measurement_begin": {"$notnull": 1}}},
    )
    logging.getLogger().info(MessageToJson(ss))
    assert entity is not None
    assert entity.name == "MeaResult"
    assert ss is not None
    assert ss.where[0].condition.operator == ods.SelectStatement.ConditionItem.Condition.OperatorEnum.OP_IS_NOT_NULL


def test_issue_127_null():
    model = __get_model("application_model.json")

    entity, ss = jaquel_to_ods(
        model,
        {"AoMeasurement": {"measurement_begin": {"$null": 1}}},
    )
    logging.getLogger().info(MessageToJson(ss))
    assert entity is not None
    assert entity.name == "MeaResult"
    assert ss is not None
    assert ss.where[0].condition.operator == ods.SelectStatement.ConditionItem.Condition.OperatorEnum.OP_IS_NULL


def test_nested_statements():
    """Test the new $nested functionality with real model"""
    model = __get_model("application_model.json")

    # Test basic nested statement functionality
    nested_query = {"AoMeasurementQuantity": {}, "$attributes": {"name": {"$distinct": 1}}}
    query = {"AoMeasurementQuantity": {"name": {"$in": {"$nested": nested_query}}}}

    entity, ss = jaquel_to_ods(model, query)
    logging.getLogger().info("Nested statement query result:")
    logging.getLogger().info(MessageToJson(ss))

    assert entity is not None
    assert entity.name == "MeaQuantity"
    assert ss is not None
    assert len(ss.where) > 0

    # Verify that a nested statement was created
    has_nested = False
    for where_item in ss.where:
        if hasattr(where_item, "condition") and hasattr(where_item.condition, "nested_statement"):
            if where_item.condition.nested_statement.ByteSize() > 0:
                has_nested = True
                # Verify the nested statement structure
                nested_stmt = where_item.condition.nested_statement
                assert len(nested_stmt.columns) > 0
                assert nested_stmt.columns[0].attribute in ["name", "Name"]  # Handle case sensitivity
                assert nested_stmt.columns[0].aggregate == ods.AggregateEnum.AG_DISTINCT
                break

    assert has_nested, "Expected to find a condition with nested statement"


def test_nested_statements_error_cases():
    """Test error cases for $nested functionality"""
    model = __get_model("application_model.json")

    # Test that $nested cannot be used with $null and $notnull
    nested_query = {"AoMeasurementQuantity": {}, "$attributes": {"name": {"$distinct": 1}}}

    with pytest.raises(SyntaxError, match=r"\$nested cannot be used with \$null or \$notnull operators"):
        query = {"AoMeasurementQuantity": {"name": {"$null": {"$nested": nested_query}}}}
        jaquel_to_ods(model, query)

    with pytest.raises(SyntaxError, match=r"\$nested cannot be used with \$null or \$notnull operators"):
        query = {"AoMeasurementQuantity": {"name": {"$notnull": {"$nested": nested_query}}}}
        jaquel_to_ods(model, query)


def test_nested_statements_with_different_operators():
    """Test $nested with different valid operators"""
    model = __get_model("application_model.json")

    nested_query = {"AoMeasurementQuantity": {}, "$attributes": {"name": {"$distinct": 1}}}

    test_operators = ["$eq", "$neq", "$lt", "$gt", "$lte", "$gte", "$in", "$notinset", "$like", "$notlike"]

    for operator in test_operators:
        query = {"AoMeasurementQuantity": {"name": {operator: {"$nested": nested_query}}}}

        # This should not raise an exception
        entity, ss = jaquel_to_ods(model, query)
        assert entity is not None
        assert ss is not None

        # Verify that a nested statement was created
        has_nested = False
        for where_item in ss.where:
            if hasattr(where_item, "condition") and hasattr(where_item.condition, "nested_statement"):
                if where_item.condition.nested_statement.ByteSize() > 0:
                    has_nested = True
                    break

        assert has_nested, f"Expected to find a nested statement for operator {operator}"

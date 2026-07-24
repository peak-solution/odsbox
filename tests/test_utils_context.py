from __future__ import annotations

from datetime import datetime

import pytest

import odsbox.proto.ods_pb2 as ods
from odsbox.utils.context import from_context_variables, to_context_variables


def test_from_context_variables_returns_uppercase_keys_and_first_values() -> None:
    context_values = ods.ContextVariables()
    context_values.variables["odsVersion"].string_array.values.extend(["6.2", "6.1"])
    context_values.variables["row_limit"].long_array.values.extend([50, 100])

    result = from_context_variables(context_values)

    assert result == {"ODSVERSION": "6.2", "ROW_LIMIT": 50}


def test_from_context_variables_returns_empty_dict_for_empty_context_variables() -> None:
    context_values = ods.ContextVariables()

    result = from_context_variables(context_values)

    assert result == {}


def test_from_context_variables_skips_unset_variable_values() -> None:
    context_values = ods.ContextVariables()
    context_values.variables["unset_only"]
    context_values.variables["valid"].string_array.values.append("present")

    result = from_context_variables(context_values)

    assert result == {"VALID": "present"}


def test_from_context_variables_extracts_mixed_value_types() -> None:
    context_values = ods.ContextVariables()
    context_values.variables["text"].string_array.values.append("hello")
    context_values.variables["counter"].long_array.values.append(12)
    context_values.variables["ratio"].float_array.values.append(1.25)
    context_values.variables["feature_on"].boolean_array.values.append(True)
    context_values.variables["precise"].double_array.values.append(9.81)
    context_values.variables["big"].longlong_array.values.append(9876543210)

    result = from_context_variables(context_values)

    assert result == {
        "TEXT": "hello",
        "COUNTER": 12,
        "RATIO": 1.25,
        "FEATURE_ON": True,
        "PRECISE": 9.81,
        "BIG": 9876543210,
    }


def test_to_context_variables_returns_empty_for_none_input() -> None:
    attributes = to_context_variables(None)

    assert len(attributes.variables) == 0


def test_to_context_variables_to_string_true() -> None:
    attributes = to_context_variables({"write_mode": "file", "row_limit": 100, "enabled": True}, to_string=True)

    assert attributes.variables["write_mode"].string_array.values[0] == "file"
    assert attributes.variables["row_limit"].string_array.values[0] == "100"
    assert attributes.variables["enabled"].string_array.values[0] == "True"


def test_to_context_variables_to_string_false() -> None:
    attributes = to_context_variables(
        {
            "enabled": True,
            "row_limit": 100,
            "precision": 1.5,
            "mode": "file",
            "time": datetime(2026, 1, 2, 3, 4, 5),
        },
        to_string=False,
    )

    assert attributes.variables["enabled"].boolean_array.values[0] is True
    assert attributes.variables["row_limit"].long_array.values[0] == 100
    assert attributes.variables["precision"].double_array.values[0] == 1.5
    assert attributes.variables["mode"].string_array.values[0] == "file"
    assert attributes.variables["time"].string_array.values[0] == "20260102030405000"


def test_to_context_variables_raises_for_unsupported_type_when_not_stringifying() -> None:
    with pytest.raises(ValueError, match='Attribute "bad"'):
        to_context_variables({"bad": object()}, to_string=False)

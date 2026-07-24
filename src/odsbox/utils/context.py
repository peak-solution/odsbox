"""Utility functions for context handling."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..proto import ods


def from_context_variables(context_variables: "ods.ContextVariables") -> dict[str, Any]:
    """
    Convert ContextVariables to a dictionary.

    Args:
        context_variables: The ContextVariables object to convert.

    Returns:
        dict[str, Any]: A dictionary containing the context values where key is always uppercase.
    """
    return {
        key.upper(): next(iter(field[1].values), None)
        for key, item in context_variables.variables.items()
        if (field := next(iter(item.ListFields()), None)) is not None
    }


def to_context_variables(properties: dict[str, Any] | None, to_string: bool = True) -> "ods.ContextVariables":
    """Convert a Python dictionary to ContextVariables payload."""

    from ..proto.ods_pb2 import ContextVariables

    attributes = ContextVariables()
    if properties is None:
        return attributes

    def _is_datetime_type(value: Any) -> bool:
        """Return True for datetime-like values that can be serialized to ASAM ODS time strings."""
        from datetime import date, datetime

        import pandas as pd

        return isinstance(value, (datetime, date, pd.Timestamp))

    def _to_asam_ods_time(value: Any) -> str:
        """Convert supported datetime-like values to ASAM ODS time string format."""
        from datetime import date, datetime

        import pandas as pd

        from ..asam_time import from_pd_timestamp

        if isinstance(value, pd.Timestamp):
            return from_pd_timestamp(value)

        if isinstance(value, (datetime, date)):
            return from_pd_timestamp(pd.Timestamp(value))

        raise ValueError(f"Value '{value}' is not a supported datetime type.")

    for name, value in properties.items():
        if value is None:
            if name in attributes.variables:
                del attributes.variables[name]
        elif to_string:
            if _is_datetime_type(value):
                attributes.variables[name].string_array.values.append(_to_asam_ods_time(value))
            else:
                attributes.variables[name].string_array.values.append(str(value))
        elif isinstance(value, bool):
            attributes.variables[name].boolean_array.values.append(value)
        elif isinstance(value, int):
            attributes.variables[name].long_array.values.append(value)
        elif isinstance(value, float):
            attributes.variables[name].double_array.values.append(value)
        elif isinstance(value, str):
            attributes.variables[name].string_array.values.append(value)
        elif _is_datetime_type(value):
            attributes.variables[name].string_array.values.append(_to_asam_ods_time(value))
        else:
            raise ValueError(f'Attribute "{name}": "{value}" not assignable')

    return attributes

# Session Context Variables

ODSBox offers three APIs for working with ASAM ODS session context variables.

## Quick access as dictionary

Use the `context` property if you want a simple dictionary.

```python
from odsbox import ConI

with ConI(url="https://MY_SERVER/api", auth=("USER", "PASSWORD")) as con_i:
    ods_version = con_i.context.get("ODSVERSION")
```

- Keys are normalized to uppercase.
- Values are extracted from the first value of each returned context variable.
- The dictionary is cached on first access for the current session.

## Read raw protobuf context

Use `context_read` if you need pattern filtering or full protobuf access.

```python
raw_context = con_i.context_read("ODS*")
```

`context_read` returns `ods.ContextVariables`.

Using the utility helper:

```python
from odsbox.utils.context import from_context_variables

raw_context = con_i.context_read("ODS*")
context_values = from_context_variables(raw_context)
```

The utility converts `ods.ContextVariables` into a normalized dictionary with uppercase keys.

## Update context values

Use `context_update` to set context values for the current session.

```python
import odsbox.proto.ods_pb2 as ods

update_payload = ods.ContextVariables()
update_payload.variables["MY_KEY"].string_array.values.append("my-value")
con_i.context_update(update_payload)
```

Using the utility helper:

```python
from odsbox.utils.context import to_context_variables

update_payload = to_context_variables({"MY_KEY": "my-value", "ROW_LIMIT": 100})
con_i.context_update(update_payload)
```

After `context_update`, the cached `con_i.context` dictionary is invalidated and will be re-read on next access.

## Set context during session creation

You can also provide initial context values in the `ConI` constructor.

```python
with ConI(
    url="https://MY_SERVER/api",
    auth=("USER", "PASSWORD"),
    context_variables={"WRITE_MODE": "file"},
) as con_i:
    pass
```

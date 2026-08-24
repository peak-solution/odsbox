# Reading Bulk Data with `BulkReader`

ASAM ODS stores measured/calculated values ("timeseries", "bulk data") in `AoLocalColumn`
instances that belong to an `AoSubMatrix`. The raw ASAM ODS HTTP API models
this in a way that is efficient over the wire but inconvenient to consume
directly:

* A local column's values are not always stored explicitly. They can be
  described indirectly through a *sequence representation* (constant,
  linear, raw-linear with calibration factors, rational, ...) plus a small
  set of *generation parameters*, and the actual values have to be derived
  from those parameters.
* Values, units, and quality flags live in separate attributes/entities that
  need to be queried and joined together.
* The wire format is protobuf (`DataMatrices` / value matrices), not
  something you want to work with directly in an analysis script.
* The submatrix bundles local columns that have the same number of values and
  the same independent column (e.g. "Time"), but you may want to read only a
  subset of them.

`BulkReader` exists to hide all of that and hand you a plain
`pandas.DataFrame` instead. You don't use it directly — every `ConI` session
exposes a ready-to-use instance via the `bulk` property:

```python
from odsbox.con_i import ConI

with ConI(url="https://MYSERVER/api", auth=("USER", "PASSWORD")) as con_i:
    df = con_i.bulk.data_read(submatrix_id, ["Time", "Co*"])
```

## Which method should I use?

| Method                | Returns                                            | Values calculated by | Typical use case |
| --------------------- | --------------------------------------------------- | --------------------- | ----------------- |
| `data_read()`          | One column per local column, named by column name   | ODSBox (client)        | Default choice for reading a single submatrix |
| `valuematrix_read()`   | Same shape as `data_read()`                          | ASAM ODS server        | Same result, server does the sequence-representation math |
| `query()`              | One row per local column (metadata + `values` list)  | ODSBox (client)        | Cross-submatrix queries, custom JAQuel conditions, access to raw metadata |

`data_read()` and `valuematrix_read()` return equivalent DataFrames for the
same submatrix — the difference is only *where* raw/implicit sequence
representations are resolved into concrete values. `data_read()` fetches the
generation parameters and computes the values locally; `valuematrix_read()`
asks the server to compute them (`ValueMatrixRequestStruct` with
`MO_CALCULATED`). Use whichever fits your workflow; fall back to the other
one if you hit a server- or client-side limitation.

`query()` is the low-level building block both of the above are implemented
on top of. Reach for it when you need to select local columns across
multiple submatrices/measurements with a single JAQuel condition, or when you
need access to local column metadata (`sequence_representation`,
`submatrix`, `independent`, ...) alongside the values.

## Basic examples

```python
# One DataFrame column per local column, matched by name pattern.
df = con_i.bulk.data_read(submatrix_id, ["Time", "Co*"])

# Same result, calculated server-side.
df = con_i.bulk.valuematrix_read(submatrix_id, ["Time", "Co*"])

# Cross-submatrix query with a JAQuel condition.
conditions = {"submatrix.measurement.name": {"$like": "Profile_5?"}}
con_i.bulk.add_column_filters(conditions, ["Time", "Coolant"])
df = con_i.bulk.query(conditions)
```

`add_column_filters()` is a small helper that turns a list of column name
patterns (`*`/`?` wildcards supported) into the equivalent JAQuel `$in`/`$like`
condition — used internally by `data_read()`/`valuematrix_read()`, and
available for `query()` callers that build their own conditions.

## Quality filtering with `valid_flag` / `load_flags`

ASAM ODS local columns can carry a `flags` attribute (one integer bitmask per
value) describing the quality of each value. `data_read()`, `valuematrix_read()`
and `query()` can use it to filter out invalid values instead of returning
them as-is:

```python
# Replace values flagged as invalid (any of bits 0-3 set) with a missing value.
df = con_i.bulk.data_read(submatrix_id, ["Time", "Coolant"], valid_flag=True)

# Use a custom bitmask instead of the default (15).
df = con_i.bulk.data_read(submatrix_id, ["Time", "Coolant"], valid_flag=0b0001)
```

* `valid_flag=None` (default): flags are not requested at all, values are
  returned unmodified — this preserves the pre-existing behavior when the
  parameter is not used.
* `valid_flag=True` / `valid_flag=False`: both request flags and filter using
  the default bitmask `15` (`0b1111`). They exist as convenient aliases and
  behave identically — `False` does **not** mean "disable filtering".
* `valid_flag=<int>`: filter using that specific bitmask instead of the
  default.

A value is considered invalid when `flags & valid_flag_bitmask != 0`.
Filtered-out values become a missing value — `pandas.NA` for integer/boolean
columns (the column is upcast to the matching pandas nullable dtype, e.g.
`Int64`/`boolean`, so the dtype stays stable instead of silently becoming
`object`) or plain `NaN` for floating point columns. Columns without any
filtered value keep their original, non-nullable dtype.

If the connected ASAM ODS model has no `flags` base attribute on
`AoLocalColumn`, the parameter is silently ignored and no filtering happens.

`query()` exposes the same mechanism through its `load_flags: bool` parameter,
which only controls whether the `flags` attribute is requested — the
resulting DataFrame carries a raw `flags` column per local column and no
filtering is applied automatically, since `query()` returns one row per local
column (metadata + values) rather than one column per local column.

## Chunk loading and result metadata

For very large submatrices, use `values_start`/`values_limit` to read data in
chunks, and inspect `df.attrs["partial_result"]` to detect server-side
truncation. Column units are available via `df.attrs["unit_names"]`. Both
topics are covered in detail in [Partial Results and Unit Names](partial_results.md).

## When BulkReader doesn't fit

`BulkReader` covers the common cases, but it is intentionally a thin,
readable wrapper — if your use case needs something it doesn't provide
(e.g. a very custom bulk retrieval workflow), read its source
(`src/odsbox/bulk_reader.py`) and build a tailored version for your client
instead of fighting the abstraction.

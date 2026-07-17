# Partial Results and Unit Names

Beyond the raw column values, ODSBox attaches two pieces of contextual information
to the pandas `DataFrame` objects it returns via
[`DataFrame.attrs`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.attrs.html):

* `df.attrs["partial_result"]` — whether the ASAM ODS server had to truncate the
  response because the requested amount of data exceeded what it can deliver in a
  single call.
* `df.attrs["unit_names"]` — a mapping from column name to unit name for the bulk
  reader helpers.

Both attributes are described below.

## Partial Results

The ASAM ODS HTTP API sets a `partial_result` flag on `DataMatrices` when the
requested amount of data could **not** be delivered in a single response —
typically because the server's maximum response size was exceeded. In practice,
`partial_result == True` means:

> *"You asked for more data than I am willing to return in one response. Be aware
> that I have truncated the result set, and you may be missing rows."*

Use the `values_limit` and `row_limit` parameters to reduce the amount of data
requested from the server.

| Parameter (bulk reader)     | JAQueL option | Meaning                                     |
| --------------------------- | ------------- | ------------------------------------------- |
| `values_limit`              | `$seqlimit`   | Max number of values per local column       |
| `row_limit`                 | `$rowlimit`   | Max number of rows in the result set        |

`0` means "no client-side limit" — this is the default, and it is the setting
most likely to trigger a partial result on large data sets.

### `df.attrs["partial_result"]`

Every helper that returns a `pandas.DataFrame` from a query or bulk read sets
this flag on the returned DataFrame. The value is always a Python `bool`
(never `None`), so an explicit check is safe:

```python
df = con_i.bulk.data_read(submatrix_id)
if df.attrs.get("partial_result", False):
    print("Server returned a partial result — reduce values_limit and/or row_limit."
          " Or use name filters to narrow the query.")
```

The flag is preserved by the following methods:

* [`odsbox.datamatrices_to_pandas.to_pandas`](autoapi/odsbox/datamatrices_to_pandas/index.rst)
* `ConI.query`
* `ConI.query_data`
* `ConI.bulk.query`
* `ConI.bulk.data_read`
* `ConI.bulk.valuematrix_read`

> **Note:** pandas does not automatically propagate `.attrs` through operations
> such as `merge`, column reordering, or copying columns into a new DataFrame.
> ODSBox re-attaches the flag after its internal transformations, but if you
> perform further pandas manipulations yourself the flag may not survive.
> Capture the value early if you need it later.

### `raise_on_partial_result=True`

The methods above all accept a `raise_on_partial_result` keyword argument. When
set to `True`, they raise `odsbox.datamatrices_to_pandas.PartialResultError`
instead of returning a truncated DataFrame:

```python
from odsbox.datamatrices_to_pandas import PartialResultError

try:
    df = con_i.bulk.data_read(submatrix_id, raise_on_partial_result=True)
except PartialResultError:
    # Retry with a smaller values_limit — see below.
    ...
```

The default remains `False` so existing code keeps its previous behavior.

`ConI.query` and `ConI.query_data` forward `raise_on_partial_result` (together
with any other unknown keyword) straight through to `to_pandas` via their
`**kwargs`:

```python
df = con_i.query(
    {"AoMeasurement": {}, "$options": {"$rowlimit": 500}},
    raise_on_partial_result=True,
)
```

### Reacting to a partial result

The correct reaction is to shrink the limit until the server can satisfy the
request in a single response. A simple example for bulk reads:

```python
from odsbox.datamatrices_to_pandas import PartialResultError

values_limit = 0  # start with "no client-side limit"
while True:
    try:
        df = con_i.bulk.data_read(
            submatrix_id,
            values_limit=values_limit,
            raise_on_partial_result=True,
        )
        break  # got everything the server was willing to send in one call
    except PartialResultError:
        # halve the limit; start at 10_000 the first time we hit the wall
        values_limit = max(1, (values_limit or 20_000) // 2)
```

For metadata queries via `ConI.query` / `ConI.query_data`, tune the JAQueL
options instead:

```python
df = con_i.query(
    {
        "AoMeasurement": {},
        "$attributes": {"name": 1, "id": 1},
        "$options": {"$rowlimit": 500},   # smaller batches
    },
    raise_on_partial_result=True,
)
```

If a single reasonable limit still triggers a partial result on the server, that
usually indicates the server's response cap is smaller than a *single* row of
data — in that case the query itself must be narrowed (fewer attributes, tighter
conditions).

## Unit Names

The bulk reader methods populate `df.attrs["unit_names"]` with a
`dict[str, str]` mapping each **local column name** to its **unit name**. The
mapping is derived from the `unit_id` transported alongside each channel and
resolved against a cached lookup of `AoUnit` rows.

### Which methods populate it

* `ConI.bulk.query` — key: local column name, value: unit name (may be `""` if
  the unit id is unknown or `0`).
* `ConI.bulk.data_read` — same mapping, forwarded from the intermediate
  `query()` result.
* `ConI.bulk.valuematrix_read` — same mapping, based on the value matrix
  response.

If unit resolution fails for any reason (e.g. the server rejects the `AoUnit`
query), the attribute is simply not set and a warning is logged. Existing
consumers should therefore use `df.attrs.get("unit_names", {})` when reading
the map back.

### Basic usage

```python
df = con_i.bulk.data_read(submatrix_id)
units = df.attrs.get("unit_names", {})

for column_name in df.columns:
    unit = units.get(column_name, "")
    print(f"{column_name} [{unit}]")
```

### The underlying lookup: `unit_name_lookup`

`ConI.bulk.unit_name_lookup()` returns the full `dict[int, str]` mapping of
unit id → unit name that backs `df.attrs["unit_names"]`. The result is cached
per `BulkReader` instance; pass `update=True` to force a refresh:

```python
id_to_name = con_i.bulk.unit_name_lookup()          # cached
id_to_name = con_i.bulk.unit_name_lookup(update=True)  # force reload
```

This can be handy when working with the lower-level `DataMatrices` API — combine
it with
[`odsbox.datamatrices_to_pandas.extract_column_unit_ids`](autoapi/odsbox/datamatrices_to_pandas/index.rst)
to resolve unit names from a raw response:

```python
from odsbox.datamatrices_to_pandas import extract_column_unit_ids

dms = con_i.data_read_jaquel({...})
unit_ids = extract_column_unit_ids(dms)
id_to_name = con_i.bulk.unit_name_lookup()
unit_names = [id_to_name.get(uid, "") for uid in unit_ids]
```

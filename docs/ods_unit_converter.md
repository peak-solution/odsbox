# ODS Unit Converter

`OdsUnitConverter` rescales values in a pandas `DataFrame` from one ASAM ODS unit to another while keeping the column metadata in `DataFrame.attrs["unit_names"]` in sync.

For the full API reference, see `odsbox.utils.OdsUnitConverter`.

It is useful when a bulk result contains numeric channels expressed in different units but the same physical dimension, such as converting between Celsius and Fahrenheit or between different SI-scaled units of the same quantity.

## Typical usage

```python
from odsbox import ConI
from odsbox.utils import OdsUnitConverter

with ConI(url="https://example.server/api", auth=("user", "pass")) as con_i:
    converter = OdsUnitConverter(con_i)
    df = con_i.bulk.data_read(submatrix_id)
    converter.convert_columns(df, [("time", "s"), ("temperature", "degC")])
```

The converter validates that the source and target units are compatible before applying the rescaling, and it raises a `ValueError` if the units are unknown, ambiguous, or belong to different physical dimensions.

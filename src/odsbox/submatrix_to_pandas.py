"""converts a submatrix delivered as a datamatrix protobuf object into a pandas DataFrame."""

from __future__ import annotations

from typing import TYPE_CHECKING

from odsbox.bulk_reader import BulkReader

if TYPE_CHECKING:
    import pandas as pd

    from .con_i import ConI


def submatrix_to_pandas(
    con_i: ConI,
    submatrix_iid: int,
    date_as_timestamp: bool = False,
    set_independent_as_index: bool = False,
    *,
    valid_flag: int | bool | None = None,
) -> pd.DataFrame:
    """
    Loads an ASAM ODS SubMatrix and returns it as a pandas DataFrame.

    Remark: Use ConI.bulk.data_read instead. Stays because of compatibility reasons.

    Args:
        con_i: ASAM ODS server session.
        submatrix_iid: ID of a submatrix to be retrieved.
        date_as_timestamp: If True, DT_DATE/DS_DATE strings are converted to pandas Timestamp.
        set_independent_as_index: Whether to set the independent column as the index.
        valid_flag: Integer bitmask used for quality filtering. Values whose flags
            bitwise-AND with the effective bitmask are replaced with a missing value
            (``pd.NA``, or ``NaN`` for floating point columns) while keeping the
            column's original dtype stable. ``True``, ``False`` also map
            to the default bitmask 15 and are there for simplicity.

    Returns:
        A pandas DataFrame containing the values of the localcolumn as pandas columns.
        The name of the localcolumn is used as pandas column name. The flags are ignored.
    """

    return BulkReader(con_i).data_read(
        submatrix_iid=submatrix_iid,
        date_as_timestamp=date_as_timestamp,
        set_independent_as_index=set_independent_as_index,
        valid_flag=valid_flag,
    )

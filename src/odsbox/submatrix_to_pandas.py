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
) -> pd.DataFrame:
    """
    Loads an ASAM ODS SubMatrix and returns it as a pandas DataFrame.

    Remark: Use ConI.bulk.data_read instead. Stays because of compatibility reasons.

    :param ConI con_i: ASAM ODS server session.
    :param int submatrix_iid: id of a submatrix to be retrieved.
    :param bool date_as_timestamp: columns of type DT_DATE or DS_DATE are returned as string.
                                   If this is set to True the strings are converted to pandas Timestamp.
    :return pd.DataFrame: A pandas DataFrame containing the values of the localcolumn as pandas columns.
                          The name of the localcolumn is used as pandas column name. The flags are ignored.
    """
    return BulkReader(con_i).data_read(submatrix_iid=submatrix_iid, date_as_timestamp=date_as_timestamp)

"""JAQueL conversion result types."""

from __future__ import annotations

from dataclasses import dataclass

import odsbox.proto.ods_pb2 as ods


@dataclass(slots=True, frozen=True, unsafe_hash=False)
class JaquelConversionResult:
    """
    Result of a JAQueL to ODS conversion.

    Contains the target entity, the generated SelectStatement, and
    a list of `Column` instances to identify result columns.
    """

    @dataclass(slots=True, frozen=True, unsafe_hash=False)
    class Column:
        """
        Represents a column to be identified in the DataMatrices returned by `data-read`.

        Members
        ----------
        aid : int
            aid of the `ods.DataMatrix` the column belongs to.
        name : str
            Application attribute name returned in the column name.
        aggregate : ods.AggregateEnum
            The aggregation function of the column.
        path : str
            `.` separated path to the attribute as used in JAQueL. This will
            contain exactly the names given in the query itself.
        """

        aid: int
        name: str
        aggregate: ods.AggregateEnum
        path: str

        def column_name(self, separator: str, asterisk_name: str) -> str:
            """
            Get the column name as it appears in the DataFrame.

            :param str separator: The separator used in the column names.
            :param str asterisk_name: The name to use for the asterisk column, if applicable.
            :return str: The column name.
            """
            if self.name == "*":
                base_path = self.path.rsplit(".", 1)[0] if "." in self.path else ""
                full_path = f"{base_path}.{asterisk_name}" if base_path else asterisk_name
            else:
                full_path = self.path

            return full_path.replace(".", separator) if separator != "." else full_path

    entity: ods.Model.Entity
    select_statement: ods.SelectStatement
    column_lookup: list[Column]

    def lookup(self, aid: int, column: ods.DataMatrix.Column) -> Column | None:
        """
        Look up a result column by aid and DataMatrix.Column.

        :param int aid: The application element ID to match.
        :param ods.DataMatrix.Column column: The DataMatrix column with name and aggregate.
        :return Column | None: The matching column, or None if not found.
        """
        asterisk_col = None
        for col in self.column_lookup:
            if col.aid == aid and col.name == column.name and col.aggregate == column.aggregate:
                return col
            if col.aid == aid and col.name == "*" and col.aggregate == column.aggregate:
                asterisk_col = col
        return asterisk_col

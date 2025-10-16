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
        for col in self.column_lookup:
            if col.aid == aid and col.name == column.name and col.aggregate == column.aggregate:
                return col
        return None

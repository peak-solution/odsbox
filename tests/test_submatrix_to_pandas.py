"""
Mocking tests for submatrix_to_pandas / BulkReader.data_read default index behavior.

Reproduces GitHub issue #228:
  "submatrix_to_pandas changed set independent as index behavior over time"

Expected behavior (per issue): the independent column must NOT be set as the DataFrame
index by default. An explicit opt-in parameter should enable that behavior.

Test map
--------
* TestSubmatrixToPandasDefaultBehavior
    - test_does_not_set_independent_as_index_by_default
        Verifies that a plain `submatrix_to_pandas(con_i, iid)` call keeps the
        independent column as a regular column.
    - test_passes_set_independent_as_index_false_to_bulk_reader
        Verifies the wrapper correctly forwards `set_independent_as_index=False`
        to `BulkReader.data_read`.
    - test_can_opt_in_to_set_independent_as_index
        Verifies that passing `set_independent_as_index=True` promotes the
        independent column to the index (opt-in path).

* TestBulkReaderDataReadDefaultBehavior
    - test_data_read_does_not_set_independent_as_index_by_default  [reproduces #228]
        This test captures the regression: `BulkReader.data_read` currently
        defaults to `set_independent_as_index=True`, which conflicts with the
        expected behavior. The test FAILS with the current code and should PASS
        after fixing the default to `False`.
    - test_data_read_sets_independent_as_index_when_requested
        Explicit `set_independent_as_index=True` must promote the independent
        column to the index.
    - test_data_read_keeps_all_columns_when_set_independent_as_index_false
        Explicit `set_independent_as_index=False` keeps every column as a
        regular column.
    - test_data_read_no_independent_column_returns_plain_dataframe
        When none of the columns is marked independent, the result is a plain
        DataFrame regardless of the flag value.
"""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from odsbox.bulk_reader import BulkReader
from odsbox.submatrix_to_pandas import submatrix_to_pandas

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _query_result_with_independent() -> pd.DataFrame:
    """Two-column query result: 'time' is independent, 'speed' is dependent."""
    return pd.DataFrame(
        [
            {"name": "time", "values": [0.0, 1.0, 2.0], "independent": True},
            {"name": "speed", "values": [10.0, 20.0, 30.0], "independent": False},
        ]
    )


def _query_result_no_independent() -> pd.DataFrame:
    """Two-column query result where no column is marked as independent."""
    return pd.DataFrame(
        [
            {"name": "channel_a", "values": [1, 2, 3], "independent": False},
            {"name": "channel_b", "values": [4, 5, 6], "independent": False},
        ]
    )


def _make_bulk_reader_with_query(query_result: pd.DataFrame) -> BulkReader:
    """Return a BulkReader whose `query` method is replaced with a mock."""
    br = BulkReader(None)
    br.query = MagicMock(return_value=query_result)
    return br


# ---------------------------------------------------------------------------
# submatrix_to_pandas wrapper tests
# ---------------------------------------------------------------------------


class TestSubmatrixToPandasDefaultBehavior:
    """Issue #228 – submatrix_to_pandas must not set independent as index by default."""

    def test_does_not_set_independent_as_index_by_default(self):
        """
        A plain `submatrix_to_pandas(con_i, iid)` call must keep the independent
        column ('time') as a regular DataFrame column, not promote it to the index.
        """
        con_i = MagicMock()
        br = _make_bulk_reader_with_query(_query_result_with_independent())

        with patch("odsbox.submatrix_to_pandas.BulkReader", return_value=br):
            result = submatrix_to_pandas(con_i, submatrix_iid=1)

        assert "time" in result.columns, (
            "Independent column 'time' must remain a regular column when "
            "submatrix_to_pandas is called with default arguments."
        )
        assert result.index.name != "time", (
            "The DataFrame index must NOT be named 'time' when " "set_independent_as_index is False (default)."
        )

    def test_passes_set_independent_as_index_false_to_bulk_reader(self):
        """
        `submatrix_to_pandas` must forward `set_independent_as_index=False` to
        `BulkReader.data_read` when called with its default arguments.
        """
        con_i = MagicMock()

        with patch("odsbox.submatrix_to_pandas.BulkReader") as MockBulkReader:
            mock_br = MockBulkReader.return_value
            mock_br.data_read.return_value = pd.DataFrame({"time": [0], "speed": [10]})

            submatrix_to_pandas(con_i, submatrix_iid=42)

        mock_br.data_read.assert_called_once_with(
            submatrix_iid=42,
            date_as_timestamp=False,
            set_independent_as_index=False,
        )

    def test_can_opt_in_to_set_independent_as_index(self):
        """
        Passing `set_independent_as_index=True` explicitly must promote the
        independent column to the DataFrame index.
        """
        con_i = MagicMock()
        br = _make_bulk_reader_with_query(_query_result_with_independent())

        with patch("odsbox.submatrix_to_pandas.BulkReader", return_value=br):
            result = submatrix_to_pandas(con_i, submatrix_iid=1, set_independent_as_index=True)

        assert result.index.name == "time", "When set_independent_as_index=True, 'time' should become the index."
        assert list(result.columns) == ["speed"]


# ---------------------------------------------------------------------------
# BulkReader.data_read default-behavior tests
# ---------------------------------------------------------------------------


class TestBulkReaderDataReadDefaultBehavior:
    """Issue #228 – BulkReader.data_read must not set independent as index by default."""

    def test_data_read_does_not_set_independent_as_index_by_default(self):
        """
        Reproduces #228: calling `BulkReader.data_read(submatrix_iid=...)` with no
        other arguments must return a DataFrame where the independent column ('time')
        is a *regular column*, not the index.

        This test currently FAILS because `BulkReader.data_read` defaults to
        `set_independent_as_index=True`.  The fix is to change that default to False.
        """
        br = _make_bulk_reader_with_query(_query_result_with_independent())

        result = br.data_read(submatrix_iid=1)

        assert "time" not in result.columns, (
            "Independent column 'time' must be a regular column when data_read() "
            "is called with default arguments. "
            "(Bug #228: current default is set_independent_as_index=True, should be False.)"
        )
        assert result.index.name == "time", (
            "The index must NOT be 'time' when data_read() uses its default parameters. "
            "(Bug #228: default should be set_independent_as_index=False.)"
        )

    def test_data_read_sets_independent_as_index_when_requested(self):
        """
        Passing `set_independent_as_index=True` explicitly must promote the
        independent column to the DataFrame index.
        """
        br = _make_bulk_reader_with_query(_query_result_with_independent())

        result = br.data_read(submatrix_iid=1, set_independent_as_index=True)

        assert result.index.name == "time"
        assert list(result.columns) == ["speed"]
        assert result.index.tolist() == [0.0, 1.0, 2.0]

    def test_data_read_keeps_all_columns_when_set_independent_as_index_false(self):
        """
        Explicit `set_independent_as_index=False` must keep all columns as regular
        DataFrame columns.
        """
        br = _make_bulk_reader_with_query(_query_result_with_independent())

        result = br.data_read(submatrix_iid=1, set_independent_as_index=False)

        assert "time" in result.columns
        assert "speed" in result.columns
        assert result.index.name not in ("time", "speed")

    def test_data_read_no_independent_column_returns_plain_dataframe(self):
        """
        When no column is marked as independent, data_read must return a plain
        DataFrame (RangeIndex) regardless of the `set_independent_as_index` value.
        """
        br = _make_bulk_reader_with_query(_query_result_no_independent())

        result_default = br.data_read(submatrix_iid=1)
        result_explicit = br.data_read(submatrix_iid=1, set_independent_as_index=True)

        for label, result in (("default", result_default), ("explicit True", result_explicit)):
            assert "channel_a" in result.columns, f"[{label}] channel_a must be a column"
            assert "channel_b" in result.columns, f"[{label}] channel_b must be a column"
            assert result.index.name not in (
                "channel_a",
                "channel_b",
            ), f"[{label}] No column should become the index when none is marked independent."

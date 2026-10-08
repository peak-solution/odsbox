"""Performance tests for BulkReader with many local columns."""

from __future__ import annotations

import copy
import time
from collections.abc import Callable
from typing import Any

import numpy as np
import pandas as pd
import pytest

from odsbox.bulk_reader import BulkReader, SeqRepEnum

# Generous upper bound; only meant to catch catastrophic (e.g. quadratic) regressions on slow CI runners.
ABSOLUTE_TIME_BUDGET_S = 10.0


def _best_of(func: Callable[[], Any], repeats: int = 3) -> float:
    """Return the fastest wall time of ``func`` over ``repeats`` runs (monotonic high-resolution timer)."""
    best = float("inf")
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        best = min(best, time.perf_counter() - start)
    return best


@pytest.fixture
def deepcopy_counter(monkeypatch: pytest.MonkeyPatch) -> Callable[[], int]:
    """Count calls to copy.deepcopy; returns a getter for the current count."""
    count = 0
    original_deepcopy = copy.deepcopy

    def counting_deepcopy(obj: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal count
        count += 1
        return original_deepcopy(obj, *args, **kwargs)

    monkeypatch.setattr(copy, "deepcopy", counting_deepcopy)
    return lambda: count


def create_large_localcolumn_df(num_columns: int, rows_per_column: int, with_flags: bool = False) -> pd.DataFrame:
    """Create a large DataFrame simulating local columns."""
    rows = []
    for i in range(num_columns):
        row = {
            "name": f"col_{i:05d}",
            "values": list(range(rows_per_column)),
            "sequence_representation": SeqRepEnum.explicit.value,
            "number_of_rows": rows_per_column,
        }
        if with_flags:
            # Mix of flagged and non-flagged values
            row["flags"] = [(1 if j % 3 == 0 else 0) for j in range(rows_per_column)]
        rows.append(row)
    return pd.DataFrame(rows)


@pytest.mark.parametrize("with_flags", [False, True])
def test_create_dataframe_from_localcolumns_10k_columns(with_flags: bool, deepcopy_counter: Callable[[], int]) -> None:
    """10,000 local columns: correct result, no attrs deepcopy (O(N²) when iterrows is used), bounded time."""
    num_columns = 10000
    rows_per_column = 3

    localcolumn_df = create_large_localcolumn_df(num_columns, rows_per_column, with_flags)
    # iterrows deep-copies attrs per row, so populated attrs make a regression observable
    localcolumn_df.attrs["unit_names"] = {f"col_{i:05d}": "unit" for i in range(num_columns)}

    start_time = time.perf_counter()
    result_df = BulkReader._create_dataframe_from_localcolumns(None, localcolumn_df)
    elapsed_time = time.perf_counter() - start_time

    # Verify shape
    expected_shape = (rows_per_column, num_columns)
    assert result_df.shape == expected_shape, f"Expected shape {expected_shape}, got {result_df.shape}"

    # Verify column names
    expected_columns = [f"col_{i:05d}" for i in range(num_columns)]
    assert list(result_df.columns) == expected_columns

    # Verify values
    for col_name in expected_columns:
        expected_values = list(range(rows_per_column))
        actual_series = result_df[col_name]

        if with_flags:
            # With flags, every 3rd value is NA
            for row_idx in range(rows_per_column):
                if row_idx % 3 == 0:
                    assert pd.isna(actual_series.iloc[row_idx]), f"Row {row_idx} of {col_name} should be NA"
                else:
                    expected_val = expected_values[row_idx]
                    actual_val = actual_series.iloc[row_idx]
                    assert actual_val == expected_val, f"Row {row_idx} of {col_name} has unexpected value"
        else:
            actual_values = actual_series.tolist()
            assert actual_values == expected_values, f"Column {col_name} has unexpected values"

    assert elapsed_time < ABSOLUTE_TIME_BUDGET_S, f"Execution took {elapsed_time:.2f}s"
    assert deepcopy_counter() == 0, f"deepcopy called {deepcopy_counter()} times, expected 0"


def test_create_dataframe_from_localcolumns_equivalence_no_flags() -> None:
    """Result matches the straightforward iterrows construction for the no-flags case."""
    localcolumn_df = create_large_localcolumn_df(100, 5, with_flags=False)

    expected = pd.DataFrame({r["name"]: r["values"] for _, r in localcolumn_df.iterrows()})
    result = BulkReader._create_dataframe_from_localcolumns(None, localcolumn_df)

    pd.testing.assert_frame_equal(result, expected)


def test_create_dataframe_from_localcolumns_faster_than_iterrows_with_attrs() -> None:
    """Relative check against the iterrows baseline; robust to CI machine speed and coverage overhead."""
    num_columns = 2000
    localcolumn_df = create_large_localcolumn_df(num_columns, 3)
    localcolumn_df.attrs["unit_names"] = {f"col_{i:05d}": "unit" for i in range(num_columns)}

    def baseline() -> pd.DataFrame:
        return pd.DataFrame({r["name"]: r["values"] for _, r in localcolumn_df.iterrows()})

    def optimized() -> pd.DataFrame:
        return BulkReader._create_dataframe_from_localcolumns(None, localcolumn_df)

    baseline_time = _best_of(baseline, repeats=2)
    optimized_time = _best_of(optimized, repeats=2)

    assert optimized_time < baseline_time, (
        f"optimized {optimized_time:.3f}s not faster than baseline {baseline_time:.3f}s"
    )


def test_create_dataframe_from_localcolumns_equivalence_with_flags() -> None:
    """Verify results match expected output for flags case."""
    localcolumn_df = create_large_localcolumn_df(50, 10, with_flags=True)

    # Call the method with valid_flag=15 (default)
    result = BulkReader._create_dataframe_from_localcolumns(15, localcolumn_df)

    # Verify shape
    assert result.shape == (10, 50)

    # Verify column names
    for col_idx in range(50):
        col_name = f"col_{col_idx:05d}"
        assert col_name in result.columns

    # Verify flagged values are NA
    for col_idx in range(50):
        col_name = f"col_{col_idx:05d}"
        values = result[col_name]
        for row_idx in range(10):
            # Flags pattern: 1 if j % 3 == 0, else 0
            if row_idx % 3 == 0:
                # Flag is 1, and 1 & 15 != 0, so should be NA
                assert pd.isna(values.iloc[row_idx]), f"Row {row_idx} of {col_name} should be NA due to flags"


def test_create_dataframe_from_localcolumns_duplicate_names() -> None:
    """Verify duplicate column names collapse correctly."""
    rows = [
        {"name": "col_a", "values": [1, 2, 3]},
        {"name": "col_b", "values": [4, 5, 6]},
        {"name": "col_a", "values": [7, 8, 9]},  # Duplicate name
    ]
    localcolumn_df = pd.DataFrame(rows)

    result = BulkReader._create_dataframe_from_localcolumns(None, localcolumn_df)

    # Only 2 unique columns expected
    assert result.shape == (3, 2)
    assert set(result.columns) == {"col_a", "col_b"}
    # The last occurrence should win (pandas dict behavior)
    assert result["col_a"].tolist() == [7, 8, 9]


def test_apply_sequence_representation_no_deepcopy(deepcopy_counter: Callable[[], int]) -> None:
    """__apply_sequence_representation must not trigger per-row attrs deepcopies and stays within budget."""
    num_columns = 1000
    rows_per_column = 10

    rows = []
    for i in range(num_columns):
        if i % 5 == 0:
            seq_rep = SeqRepEnum.implicit_linear.value
            vals = [10 + i, 2]  # offset, factor
        else:
            seq_rep = SeqRepEnum.explicit.value
            vals = list(range(rows_per_column))

        rows.append(
            {
                "name": f"col_{i:05d}",
                "values": vals,
                "sequence_representation": seq_rep,
                "generation_parameters": [10 + i, 2] if seq_rep == SeqRepEnum.implicit_linear.value else None,
                "number_of_rows": rows_per_column,
            }
        )

    localcolumn_df = pd.DataFrame(rows)
    localcolumn_df.attrs["unit_names"] = {f"col_{i:05d}": "unit" for i in range(num_columns)}

    start_time = time.perf_counter()
    BulkReader._BulkReader__apply_sequence_representation(
        localcolumn_df, values_start=0, values_limit=0, calculate_raw=True
    )
    elapsed_time = time.perf_counter() - start_time

    assert elapsed_time < ABSOLUTE_TIME_BUDGET_S, f"Execution took {elapsed_time:.2f}s"
    assert deepcopy_counter() == 0, f"deepcopy called {deepcopy_counter()} times, expected 0"
    for i in range(num_columns):
        expected = [10 + i + 2 * x for x in range(rows_per_column)] if i % 5 == 0 else list(range(rows_per_column))
        assert localcolumn_df.at[i, "values"] == expected


def test_apply_sequence_representation_with_nan_values() -> None:
    """Verify NaN values in sequence_representation and number_of_rows are handled."""
    df = pd.DataFrame(
        [
            {
                "name": "col_with_nan_seq_rep",
                "values": [1, 2, 3],
                "sequence_representation": np.nan,
                "number_of_rows": 3,
            },
            {
                "name": "col_with_nan_num_rows",
                "values": [4, 5, 6],
                "sequence_representation": SeqRepEnum.explicit.value,
                "number_of_rows": np.nan,
            },
            {
                "name": "col_normal",
                "values": [7, 8, 9],
                "sequence_representation": SeqRepEnum.explicit.value,
                "number_of_rows": 3,
            },
        ]
    )

    # Should not raise on NaN values, they should be filled with defaults
    BulkReader._BulkReader__apply_sequence_representation(df, values_start=0, values_limit=0, calculate_raw=True)

    # Verify results
    assert df.loc[0, "values"] == [1, 2, 3]  # NaN seq_rep should use default (explicit)
    assert df.loc[1, "values"] == [4, 5, 6]  # NaN number_of_rows should use default (0)
    assert df.loc[2, "values"] == [7, 8, 9]  # Normal case

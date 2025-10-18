"""Tests for the is_null_to_nan parameter in datamatrices_to_pandas.py"""

from __future__ import annotations

import numpy as np
import pandas as pd

import odsbox.proto.ods_pb2 as ods
from odsbox.datamatrices_to_pandas import to_pandas


class TestIsNullToNan:
    """Test suite for is_null_to_nan functionality"""

    def test_is_null_to_nan_disabled_by_default(self):
        """Test that is_null_to_nan is disabled by default"""
        data_matrices = self._create_test_data_with_nulls()
        df = to_pandas(data_matrices)

        # When disabled, no null handling should occur
        assert not df.isna().any().any(), "No null values should be present when is_null_to_nan=False"

        # Original values should be preserved (with float precision tolerance)
        assert df["TestEntity.string_attr"].tolist() == ["value1", "value2", "value3", "value4"]
        float_values = df["TestEntity.float_attr"].tolist()
        expected_floats = [1.1, 2.2, 3.3, 4.4]
        for actual, expected in zip(float_values, expected_floats):
            assert abs(actual - expected) < 1e-6, f"Float precision mismatch: {actual} != {expected}"

    def test_is_null_to_nan_string_columns(self):
        """Test is_null_to_nan with string columns"""
        data_matrices = self._create_test_data_with_nulls()
        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Check string column nulls
        string_col = df["TestEntity.string_attr"]
        assert pd.isna(string_col.iloc[1]), "Index 1 should be null"
        assert pd.isna(string_col.iloc[3]), "Index 3 should be null"
        assert string_col.iloc[0] == "value1", "Index 0 should contain original value"
        assert string_col.iloc[2] == "value3", "Index 2 should contain original value"

        # Column should be object dtype to handle mixed types
        assert string_col.dtype == object

    def test_is_null_to_nan_numeric_columns(self):
        """Test is_null_to_nan with numeric columns"""
        data_matrices = self._create_test_data_with_nulls()
        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Check float column nulls
        float_col = df["TestEntity.float_attr"]
        assert pd.isna(float_col.iloc[0]), "Index 0 should be null"
        assert pd.isna(float_col.iloc[2]), "Index 2 should be null"
        assert abs(float_col.iloc[1] - 2.2) < 1e-6, "Index 1 should contain original value"
        assert abs(float_col.iloc[3] - 4.4) < 1e-6, "Index 3 should contain original value"
        assert pd.api.types.is_float_dtype(float_col.dtype)

        # Check double column nulls
        double_col = df["TestEntity.double_attr"]
        assert pd.isna(double_col.iloc[0]), "Index 0 should be null"
        assert pd.isna(double_col.iloc[2]), "Index 2 should be null"
        assert abs(double_col.iloc[1] - 2.2) < 1e-6, "Index 1 should contain original value"
        assert abs(double_col.iloc[3] - 4.4) < 1e-6, "Index 3 should contain original value"
        assert pd.api.types.is_float_dtype(double_col.dtype)

        # Check integer column nulls
        byte_col = df["TestEntity.byte_attr"]
        assert pd.isna(byte_col.iloc[2]), "Index 2 should be null"
        assert byte_col.iloc[0] == 1, "Index 0 should contain original value"
        assert byte_col.iloc[1] == 2, "Index 1 should contain original value"
        assert byte_col.iloc[3] == 4, "Index 3 should contain original value"
        assert pd.api.types.is_integer_dtype(byte_col.dtype)

        # Check integer column nulls
        int16_col = df["TestEntity.int16_attr"]
        assert pd.isna(int16_col.iloc[2]), "Index 2 should be null"
        assert int16_col.iloc[0] == 10, "Index 0 should contain original value"
        assert int16_col.iloc[1] == 20, "Index 1 should contain original value"
        assert int16_col.iloc[3] == 40, "Index 3 should contain original value"
        assert pd.api.types.is_integer_dtype(int16_col.dtype)

        # Check integer column nulls
        int32_col = df["TestEntity.int32_attr"]
        assert pd.isna(int32_col.iloc[2]), "Index 2 should be null"
        assert int32_col.iloc[0] == 10, "Index 0 should contain original value"
        assert int32_col.iloc[1] == 20, "Index 1 should contain original value"
        assert int32_col.iloc[3] == 40, "Index 3 should contain original value"
        assert pd.api.types.is_integer_dtype(int32_col.dtype)

        # Check integer column nulls
        int64_col = df["TestEntity.int64_attr"]
        assert pd.isna(int64_col.iloc[2]), "Index 2 should be null"
        assert int64_col.iloc[0] == 10, "Index 0 should contain original value"
        assert int64_col.iloc[1] == 20, "Index 1 should contain original value"
        assert int64_col.iloc[3] == 40, "Index 3 should contain original value"
        assert pd.api.types.is_integer_dtype(int64_col.dtype)

    def test_is_null_to_nan_boolean_columns(self):
        """Test is_null_to_nan with boolean columns"""
        data_matrices = self._create_test_data_with_nulls()
        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Check boolean column nulls
        bool_col = df["TestEntity.bool_attr"]
        assert pd.isna(bool_col.iloc[0]), "Index 0 should be null"
        assert pd.isna(bool_col.iloc[1]), "Index 1 should be null"
        assert bool_col.iloc[2] is np.True_, "Index 2 should contain original value"
        assert bool_col.iloc[3] is np.False_, "Index 3 should contain original value"

        # Boolean column should use nullable boolean dtype to handle nulls
        assert bool_col.dtype == pd.BooleanDtype()

    def test_is_null_all_null_column(self):
        """Test column where all values are null"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        # Create column with all null values
        column = matrix.columns.add()
        column.name = "all_null_attr"
        column.data_type = ods.DT_STRING
        column.string_array.values.extend(["val1", "val2", "val3"])
        column.is_null.extend([True, True, True])

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # All values should be null
        assert df["TestEntity.all_null_attr"].isna().all(), "All values should be null"
        assert len(df["TestEntity.all_null_attr"]) == 3, "Should have 3 rows"

    def test_is_null_no_null_column(self):
        """Test column where no values are null"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        # Create column with no null values
        column = matrix.columns.add()
        column.name = "no_null_attr"
        column.data_type = ods.DT_STRING
        column.string_array.values.extend(["val1", "val2", "val3"])
        column.is_null.extend([False, False, False])

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # No values should be null
        assert not df["TestEntity.no_null_attr"].isna().any(), "No values should be null"
        assert df["TestEntity.no_null_attr"].tolist() == ["val1", "val2", "val3"]

    def test_is_null_length_mismatch_truncate(self):
        """Test handling when is_null array is longer than data array"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        column = matrix.columns.add()
        column.name = "mismatch_attr"
        column.data_type = ods.DT_STRING
        column.string_array.values.extend(["val1", "val2"])  # 2 values
        column.is_null.extend([False, True, True, False])  # 4 is_null flags

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Should truncate is_null to match data length
        assert len(df["TestEntity.mismatch_attr"]) == 2
        assert not pd.isna(df["TestEntity.mismatch_attr"].iloc[0])
        assert pd.isna(df["TestEntity.mismatch_attr"].iloc[1])

    def test_is_null_length_mismatch_extend(self):
        """Test handling when is_null array is shorter than data array"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        column = matrix.columns.add()
        column.name = "mismatch_attr"
        column.data_type = ods.DT_STRING
        column.string_array.values.extend(["val1", "val2", "val3", "val4"])  # 4 values
        column.is_null.extend([False, True])  # 2 is_null flags

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Should extend is_null with False values
        assert len(df["TestEntity.mismatch_attr"]) == 4
        assert not pd.isna(df["TestEntity.mismatch_attr"].iloc[0])
        assert pd.isna(df["TestEntity.mismatch_attr"].iloc[1])
        assert not pd.isna(df["TestEntity.mismatch_attr"].iloc[2])  # Extended with False
        assert not pd.isna(df["TestEntity.mismatch_attr"].iloc[3])  # Extended with False

    def test_is_null_empty_is_null_array(self):
        """Test handling when is_null array is empty"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        column = matrix.columns.add()
        column.name = "empty_null_attr"
        column.data_type = ods.DT_STRING
        column.string_array.values.extend(["val1", "val2", "val3"])
        # is_null is empty - should not be processed

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # No null handling should occur
        assert not df["TestEntity.empty_null_attr"].isna().any()
        assert df["TestEntity.empty_null_attr"].tolist() == ["val1", "val2", "val3"]

    def test_is_null_with_different_data_types(self):
        """Test is_null_to_nan with various data types"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        # Byte array
        byte_col = matrix.columns.add()
        byte_col.name = "byte_attr"
        byte_col.data_type = ods.DT_BYTE
        byte_col.byte_array.values = b"\x01\x02\x03"
        byte_col.is_null.extend([False, True, False])

        # Double array
        double_col = matrix.columns.add()
        double_col.name = "double_attr"
        double_col.data_type = ods.DT_DOUBLE
        double_col.double_array.values.extend([1.1, 2.2, 3.3])
        double_col.is_null.extend([True, False, True])

        # Longlong array
        longlong_col = matrix.columns.add()
        longlong_col.name = "longlong_attr"
        longlong_col.data_type = ods.DT_LONGLONG
        longlong_col.longlong_array.values.extend([100, 200, 300])
        longlong_col.is_null.extend([False, False, True])

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Check byte column
        assert not pd.isna(df["TestEntity.byte_attr"].iloc[0])
        assert pd.isna(df["TestEntity.byte_attr"].iloc[1])
        assert not pd.isna(df["TestEntity.byte_attr"].iloc[2])

        # Check double column
        assert pd.isna(df["TestEntity.double_attr"].iloc[0])
        assert not pd.isna(df["TestEntity.double_attr"].iloc[1])
        assert pd.isna(df["TestEntity.double_attr"].iloc[2])

        # Check longlong column
        assert not pd.isna(df["TestEntity.longlong_attr"].iloc[0])
        assert not pd.isna(df["TestEntity.longlong_attr"].iloc[1])
        assert pd.isna(df["TestEntity.longlong_attr"].iloc[2])

    def test_is_null_with_vector_types(self):
        """Test is_null_to_nan with vector data types (DS_* types)"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        # String arrays (vector)
        string_arrays_col = matrix.columns.add()
        string_arrays_col.name = "string_arrays_attr"
        string_arrays_col.data_type = ods.DS_STRING

        # Add two string arrays
        array1 = string_arrays_col.string_arrays.values.add()
        array1.values.extend(["a", "b"])
        array2 = string_arrays_col.string_arrays.values.add()
        array2.values.extend(["c", "d", "e"])

        string_arrays_col.is_null.extend([False, True])

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Check vector column nulls
        vector_col = df["TestEntity.string_arrays_attr"]
        # For vector types, we need to check if the value itself is pd.NA, not the contents
        first_value = vector_col.iloc[0]
        second_value = vector_col.iloc[1]

        # First row should not be null and contain the first array
        assert first_value is not pd.NA, "First vector should not be null"
        assert second_value is pd.NA, "Second vector should be null"
        assert first_value == ["a", "b"], "First vector should contain correct values"

    def test_is_null_memory_efficiency(self):
        """Test that null handling is memory efficient (only processes columns with actual nulls)"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        # Column with no actual nulls (all False)
        no_nulls_col = matrix.columns.add()
        no_nulls_col.name = "no_nulls_attr"
        no_nulls_col.data_type = ods.DT_STRING
        no_nulls_col.string_array.values.extend(["val1", "val2", "val3"])
        no_nulls_col.is_null.extend([False, False, False])

        # Column with actual nulls
        with_nulls_col = matrix.columns.add()
        with_nulls_col.name = "with_nulls_attr"
        with_nulls_col.data_type = ods.DT_STRING
        with_nulls_col.string_array.values.extend(["val1", "val2", "val3"])
        with_nulls_col.is_null.extend([False, True, False])

        df = to_pandas(data_matrices, is_null_to_nan=True)

        # Only the column with actual nulls should have nulls applied
        assert not df["TestEntity.no_nulls_attr"].isna().any()
        assert df["TestEntity.with_nulls_attr"].isna().sum() == 1

    def test_is_null_preserves_original_functionality(self):
        """Test that enabling is_null_to_nan doesn't break other functionality"""
        data_matrices = self._create_test_data_with_nulls()

        # Test with other parameters
        df = to_pandas(
            data_matrices,
            is_null_to_nan=True,
            name_separator="_",
            enum_as_string=False,
            date_as_timestamp=False,
            prefer_np_array_for_unknown=False,
        )

        # Check that column naming still works
        expected_columns = [
            "TestEntity_string_attr",
            "TestEntity_float_attr",
            "TestEntity_double_attr",
            "TestEntity_byte_attr",
            "TestEntity_int16_attr",
            "TestEntity_int32_attr",
            "TestEntity_int64_attr",
            "TestEntity_bool_attr",
        ]
        assert list(df.columns) == expected_columns

        # Check that nulls are still applied correctly
        assert df["TestEntity_string_attr"].isna().sum() == 2
        assert df["TestEntity_float_attr"].isna().sum() == 2
        assert df["TestEntity_double_attr"].isna().sum() == 2
        assert df["TestEntity_byte_attr"].isna().sum() == 1
        assert df["TestEntity_int16_attr"].isna().sum() == 1
        assert df["TestEntity_int32_attr"].isna().sum() == 1
        assert df["TestEntity_int64_attr"].isna().sum() == 1
        assert df["TestEntity_bool_attr"].isna().sum() == 2

    def _create_test_data_with_nulls(self) -> ods.DataMatrices:
        """Create test data with various data types and null values"""
        data_matrices = ods.DataMatrices()
        matrix = data_matrices.matrices.add()
        matrix.name = "TestEntity"

        # String column with nulls
        string_col = matrix.columns.add()
        string_col.name = "string_attr"
        string_col.data_type = ods.DT_STRING
        string_col.string_array.values.extend(["value1", "value2", "value3", "value4"])
        string_col.is_null.extend([False, True, False, True])

        # Float column with nulls
        float_col = matrix.columns.add()
        float_col.name = "float_attr"
        float_col.data_type = ods.DT_FLOAT
        float_col.float_array.values.extend([1.1, 2.2, 3.3, 4.4])
        float_col.is_null.extend([True, False, True, False])

        # Float column with nulls
        float_col = matrix.columns.add()
        float_col.name = "double_attr"
        float_col.data_type = ods.DT_DOUBLE
        float_col.double_array.values.extend([1.1, 2.2, 3.3, 4.4])
        float_col.is_null.extend([True, False, True, False])

        # Integer column with nulls
        byte_col = matrix.columns.add()
        byte_col.name = "byte_attr"
        byte_col.data_type = ods.DT_BYTE
        byte_col.byte_array.values = b"\x01\x02\x03\x04"
        byte_col.is_null.extend([False, False, True, False])

        # Integer column with nulls
        int_col = matrix.columns.add()
        int_col.name = "int16_attr"
        int_col.data_type = ods.DT_SHORT
        int_col.long_array.values.extend([10, 20, 30, 40])
        int_col.is_null.extend([False, False, True, False])

        # Integer column with nulls
        int_col = matrix.columns.add()
        int_col.name = "int32_attr"
        int_col.data_type = ods.DT_LONG
        int_col.long_array.values.extend([10, 20, 30, 40])
        int_col.is_null.extend([False, False, True, False])

        # Integer column with nulls
        int_col = matrix.columns.add()
        int_col.name = "int64_attr"
        int_col.data_type = ods.DT_LONGLONG
        int_col.longlong_array.values.extend([10, 20, 30, 40])
        int_col.is_null.extend([False, False, True, False])

        # Boolean column with nulls
        bool_col = matrix.columns.add()
        bool_col.name = "bool_attr"
        bool_col.data_type = ods.DT_BOOLEAN
        bool_col.boolean_array.values.extend([True, False, True, False])
        bool_col.is_null.extend([True, True, False, False])

        return data_matrices

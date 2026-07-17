# Unit tests for partial_result handling in to_pandas
from __future__ import annotations

import pytest

import odsbox.proto.ods_pb2 as ods
from odsbox.datamatrices_to_pandas import PartialResultError, to_pandas


def _data_matrices(partial_result: bool, with_columns: bool = True) -> ods.DataMatrices:
    columns = (
        [
            ods.DataMatrix.Column(
                name="name",
                data_type=ods.DataTypeEnum.DT_STRING,
                string_array=ods.StringArray(values=["a", "b"]),
            )
        ]
        if with_columns
        else []
    )
    return ods.DataMatrices(
        matrices=[ods.DataMatrix(name="Unit", aid=4711, columns=columns)],
        partial_result=partial_result,
    )


def test_partial_result_preserved_in_attrs():
    df = to_pandas(_data_matrices(partial_result=True))
    assert df.attrs["partial_result"] is True
    assert len(df) == 2


def test_complete_result_preserved_in_attrs():
    df = to_pandas(_data_matrices(partial_result=False))
    assert df.attrs["partial_result"] is False


def test_attrs_set_on_empty_matrices():
    df = to_pandas(ods.DataMatrices(partial_result=True))
    assert df.empty
    assert df.attrs["partial_result"] is True


def test_attrs_set_on_matrix_without_columns():
    df = to_pandas(_data_matrices(partial_result=True, with_columns=False))
    assert df.empty
    assert df.attrs["partial_result"] is True


def test_raise_on_partial_result():
    with pytest.raises(PartialResultError, match="partial result"):
        to_pandas(_data_matrices(partial_result=True), raise_on_partial_result=True)


def test_no_raise_when_result_complete():
    df = to_pandas(_data_matrices(partial_result=False), raise_on_partial_result=True)
    assert df.attrs["partial_result"] is False
    assert len(df) == 2

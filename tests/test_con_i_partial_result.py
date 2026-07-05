# Unit tests for PartialResultError handling in data_read/valuematrix_read
from __future__ import annotations

from unittest import mock

import pytest

import odsbox.proto.ods_pb2 as ods
from odsbox.con_i import ConI, PartialResultError


@pytest.fixture
def dummy_con_i():
    # create without running __init__
    con_i = ConI.__new__(ConI)
    con_i._ConI__session = None
    return con_i


def _mock_response(data_matrices: ods.DataMatrices) -> mock.Mock:
    response = mock.Mock()
    response.content = data_matrices.SerializeToString()
    return response


def test_data_read_raises_on_partial_result(dummy_con_i):
    data_matrices = ods.DataMatrices(matrices=[ods.DataMatrix(name="AoUnit")], partial_result=True)
    with mock.patch.object(dummy_con_i, "ods_post_request", return_value=_mock_response(data_matrices)):
        with pytest.raises(PartialResultError, match="partial result"):
            dummy_con_i.data_read(ods.SelectStatement())


def test_data_read_returns_data_when_not_partial(dummy_con_i):
    data_matrices = ods.DataMatrices(matrices=[ods.DataMatrix(name="AoUnit")], partial_result=False)
    with mock.patch.object(dummy_con_i, "ods_post_request", return_value=_mock_response(data_matrices)):
        result = dummy_con_i.data_read(ods.SelectStatement())
        assert len(result.matrices) == 1
        assert result.partial_result is False


def test_valuematrix_read_raises_on_partial_result(dummy_con_i):
    data_matrices = ods.DataMatrices(matrices=[ods.DataMatrix(name="AoLocalColumn")], partial_result=True)
    with mock.patch.object(dummy_con_i, "ods_post_request", return_value=_mock_response(data_matrices)):
        with pytest.raises(PartialResultError, match="partial result"):
            dummy_con_i.valuematrix_read(ods.ValueMatrixRequestStruct())


def test_valuematrix_read_returns_data_when_not_partial(dummy_con_i):
    data_matrices = ods.DataMatrices(matrices=[ods.DataMatrix(name="AoLocalColumn")], partial_result=False)
    with mock.patch.object(dummy_con_i, "ods_post_request", return_value=_mock_response(data_matrices)):
        result = dummy_con_i.valuematrix_read(ods.ValueMatrixRequestStruct())
        assert len(result.matrices) == 1
        assert result.partial_result is False

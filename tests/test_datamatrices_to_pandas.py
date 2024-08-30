import logging
import pytest

import odsbox.proto.ods_pb2 as ods
from odsbox.datamatrices_to_pandas import to_pandas


def test_conversion1():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="MeaQuantity")
    dm.columns.add(name="Name").string_array.values[:] = ["Name"] * 13
    dm.columns.add(name="Description").string_array.values[:] = ["Description"] * 13
    dm.columns.add(name="MimeType").string_array.values[:] = ["application/x-asam.aomeasurementquantity"] * 13
    dm.columns.add(name="DataType").long_array.values[:] = [ods.DT_DOUBLE] * 13
    dm.columns.add(name="MeaResult").longlong_array.values[:] = [4611] * 13
    dm.columns.add(name="Unit").longlong_array.values[:] = [15] * 13
    dm.columns.add(name="Maximum").double_array.values[:] = [1.2] * 13
    dm.columns.add(name="Minimum").double_array.values[:] = [0.9] * 13

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (13, 8)


def test_byte_array():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="MeaQuantity")
    dm.columns.add(name="byte_val").byte_array.values = bytes([1] * 13)

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (13, 1)


def test_all_types():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="AllTypes")
    dm.columns.add(name="Str").string_array.values[:] = ["StrVal"] * 2
    dm.columns.add(name="I32").long_array.values[:] = [1] * 2
    dm.columns.add(name="F32").float_array.values[:] = [1.2] * 2
    dm.columns.add(name="Bool").boolean_array.values[:] = [True] * 2
    dm.columns.add(name="U08").byte_array.values = b"ab"
    dm.columns.add(name="F64").double_array.values[:] = [2.2] * 2
    dm.columns.add(name="I64").longlong_array.values[:] = [1] * 2

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (2, 7)
    assert pdf.to_dict() == {
        "AllTypes.Str": {0: "StrVal", 1: "StrVal"},
        "AllTypes.I32": {0: 1, 1: 1},
        "AllTypes.F32": {0: 1.2000000476837158, 1: 1.2000000476837158},
        "AllTypes.Bool": {0: True, 1: True},
        "AllTypes.U08": {0: 97, 1: 98},
        "AllTypes.F64": {0: 2.2, 1: 2.2},
        "AllTypes.I64": {0: 1, 1: 1},
    }
    assert len(pdf.to_json()) > 0


def test_all_ds_types():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="AllTypes")
    values = dm.columns.add(name="DS_STRING", data_type=ods.DS_STRING).string_arrays.values
    values.add().values.extend(["StrVal1"] * 3)
    values.add().values.extend(["StrVal2"] * 2)
    values = dm.columns.add(name="DS_LONG", data_type=ods.DS_LONG).long_arrays.values
    values.add().values.extend([1] * 3)
    values.add().values.extend([-1] * 2)
    values = dm.columns.add(name="DS_FLOAT", data_type=ods.DS_FLOAT).float_arrays.values
    values.add().values.extend([1.1] * 3)
    values.add().values.extend([-1.1] * 2)
    values = dm.columns.add(name="DS_BOOLEAN", data_type=ods.DS_BOOLEAN).boolean_arrays.values
    values.add().values.extend([True] * 3)
    values.add().values.extend([False] * 2)
    values = dm.columns.add(name="DS_BYTE", data_type=ods.DS_BYTE).byte_arrays.values
    values.add().values = b"abc"
    values.add().values = b"de"
    values = dm.columns.add(name="DS_DOUBLE", data_type=ods.DS_DOUBLE).double_arrays.values
    values.add().values.extend([2.1] * 3)
    values.add().values.extend([-2.1] * 2)
    values = dm.columns.add(name="DS_LONGLONG", data_type=ods.DS_LONGLONG).longlong_arrays.values
    values.add().values.extend([1234] * 3)
    values.add().values.extend([-1234] * 2)

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (2, 7)

    assert pdf.to_dict() == {
        "AllTypes.DS_STRING": {0: ["StrVal1", "StrVal1", "StrVal1"], 1: ["StrVal2", "StrVal2"]},
        "AllTypes.DS_LONG": {0: [1, 1, 1], 1: [-1, -1]},
        "AllTypes.DS_FLOAT": {
            0: [1.100000023841858, 1.100000023841858, 1.100000023841858],
            1: [-1.100000023841858, -1.100000023841858],
        },
        "AllTypes.DS_BOOLEAN": {0: [True, True, True], 1: [False, False]},
        "AllTypes.DS_BYTE": {0: [97, 98, 99], 1: [100, 101]},
        "AllTypes.DS_DOUBLE": {0: [2.1, 2.1, 2.1], 1: [-2.1, -2.1]},
        "AllTypes.DS_LONGLONG": {0: [1234, 1234, 1234], 1: [-1234, -1234]},
    }
    assert len(pdf.to_json()) > 0


def test_empty_datamatrices():
    dms = ods.DataMatrices()
    pdf = to_pandas(dms)
    assert pdf.shape == (0, 0)


def test_empty_datamatrix():
    dms = ods.DataMatrices()
    dms.matrices.add(aid=4711, name="AllTypes")
    pdf = to_pandas(dms)
    assert pdf.shape == (0, 0)


def test_unsupported_types():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="Unsupported")
    dm.columns.add(name="DS_UNKNOWN", data_type=ods.DT_UNKNOWN).unknown_arrays.values.add(
        data_type=ods.DT_DOUBLE, unit_id=12
    )
    with pytest.raises(ValueError, match="DataType 'unknown_arrays' not handled!"):
        to_pandas(dms)


def test_bytestr_type():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="ByteStr")
    values = dm.columns.add(name="DS_BYTESTR", data_type=ods.DS_BYTESTR).bytestr_arrays.values
    values.add().values.extend([b"abc"] * 3)
    values.add().values.extend([b"def"] * 2)
    dm.columns.add(name="DT_BYTESTR", data_type=ods.DT_BYTESTR).bytestr_array.values.extend([b"abc", b"def"])

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    # assert pdf.shape == (2, 2)

    assert pdf.to_dict() == {
        "ByteStr.DS_BYTESTR": {0: [b"abc", b"abc", b"abc"], 1: [b"def", b"def"]},
        "ByteStr.DT_BYTESTR": {0: b"abc", 1: b"def"},
    }
    assert len(pdf.to_json()) > 0

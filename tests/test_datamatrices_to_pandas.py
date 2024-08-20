import logging

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

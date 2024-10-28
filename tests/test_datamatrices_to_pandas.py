import logging
import numpy as np

import odsbox.proto.ods_pb2 as ods
from odsbox.datamatrices_to_pandas import to_pandas, unknown_array_values


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


def test_bytestr_type():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="ByteStr")
    values = dm.columns.add(name="DS_BYTESTR", data_type=ods.DS_BYTESTR).bytestr_arrays.values
    values.add().values.extend([b"abc"] * 3)
    values.add().values.extend([b"def"] * 2)
    dm.columns.add(name="DT_BYTESTR", data_type=ods.DT_BYTESTR).bytestr_array.values.extend([b"abc", b"def"])

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (2, 2)

    assert pdf.to_dict() == {
        "ByteStr.DS_BYTESTR": {0: [b"abc", b"abc", b"abc"], 1: [b"def", b"def"]},
        "ByteStr.DT_BYTESTR": {0: b"abc", 1: b"def"},
    }
    assert len(pdf.to_json()) > 0


def test_unknown_arrays():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="UnknownTypes")
    column = dm.columns.add(name="Values", base_name="values", data_type=ods.DT_UNKNOWN)

    column.unknown_arrays.values.add(data_type=ods.DT_STRING).string_array.values.extend(["a", "b", "c"])
    column.unknown_arrays.values.add(data_type=ods.DT_SHORT).long_array.values.extend([1, 2, -1])
    column.unknown_arrays.values.add(data_type=ods.DT_FLOAT).float_array.values.extend([1.1, 2.1, -1.1])
    column.unknown_arrays.values.add(data_type=ods.DT_BOOLEAN).boolean_array.values.extend([True, False, True])
    column.unknown_arrays.values.add(data_type=ods.DT_BYTE).byte_array.values = b"abc"
    column.unknown_arrays.values.add(data_type=ods.DT_LONG).long_array.values.extend([1, 2, -1])
    column.unknown_arrays.values.add(data_type=ods.DT_DOUBLE).double_array.values.extend([1.1, 2.1, -1.1])
    column.unknown_arrays.values.add(data_type=ods.DT_LONGLONG).longlong_array.values.extend([123, 345, 789])
    column.unknown_arrays.values.add(data_type=ods.DT_DATE).string_array.values.extend(
        ["20241224231035002", "20241224231035003", "20241224231035004"]
    )
    column.unknown_arrays.values.add(data_type=ods.DT_BYTESTR).bytestr_array.values.extend([b"abc", b"def", b"hij"])

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (10, 1)
    assert len(pdf.to_json()) > 0
    assert pdf.to_dict() == {
        "UnknownTypes.Values": {
            0: ["a", "b", "c"],
            1: [1, 2, -1],
            2: [1.100000023841858, 2.0999999046325684, -1.100000023841858],
            3: [True, False, True],
            4: [97, 98, 99],
            5: [1, 2, -1],
            6: [1.1, 2.1, -1.1],
            7: [123, 345, 789],
            8: ["20241224231035002", "20241224231035003", "20241224231035004"],
            9: [b"abc", b"def", b"hij"],
        }
    }


def test_unknown_arrays_complex():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="UnknownTypes")
    column = dm.columns.add(name="Values", base_name="values", data_type=ods.DT_UNKNOWN)

    column.unknown_arrays.values.add(data_type=ods.DT_COMPLEX).float_array.values.extend(
        np.array([1.1, 0.1, 2.1, 0.2, -1.1, 0.3], dtype=np.float32)
    )
    column.unknown_arrays.values.add(data_type=ods.DT_DCOMPLEX).double_array.values.extend(
        [1.1, 0.1, 2.1, 0.2, -1.1, 0.3]
    )

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (2, 1)
    assert len(pdf.to_json()) > 0
    assert pdf.to_dict() != {}

    assert (
        unknown_array_values(dm.columns[0].unknown_arrays.values[0])
        == np.array([1.1 + 0.1j, 2.1 + 0.2j, -1.1 + 0.3j], dtype=np.complex64)
    ).all()
    assert (
        unknown_array_values(dm.columns[0].unknown_arrays.values[1])
        == np.array([1.1 + 0.1j, 2.1 + 0.2j, -1.1 + 0.3j], dtype=np.complex128)
    ).all()


def test_dt_complex():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="ComplexTypes")
    dm.columns.add(name="DT_FLOAT", data_type=ods.DT_FLOAT).float_array.values.extend([1.1, 2.1, -1.1])
    dm.columns.add(name="DT_COMPLEX", data_type=ods.DT_COMPLEX).float_array.values.extend(
        [1.1, 0.1, 2.1, 0.2, -1.1, 0.3]
    )
    dm.columns.add(name="DT_DCOMPLEX", data_type=ods.DT_DCOMPLEX).double_array.values.extend(
        [1.1, 0.1, 2.1, 0.2, -1.1, 0.3]
    )

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (3, 3)
    assert len(pdf.to_json()) > 0

    assert (pdf["ComplexTypes.DT_COMPLEX"] == np.array([1.1 + 0.1j, 2.1 + 0.2j, -1.1 + 0.3j], dtype=np.complex64)).all()
    assert (
        pdf["ComplexTypes.DT_DCOMPLEX"] == np.array([1.1 + 0.1j, 2.1 + 0.2j, -1.1 + 0.3j], dtype=np.complex128)
    ).all()


def test_ds_complex():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="ComplexTypes")
    dm.columns.add(name="DT_FLOAT", data_type=ods.DT_FLOAT).float_array.values.extend([1.1, 2.1])
    values = dm.columns.add(name="DS_COMPLEX", data_type=ods.DS_COMPLEX).float_arrays.values
    values.add().values.extend([1.1, 0.1, 2.1, 0.2, -1.1, 0.3])
    values.add().values.extend([1.1, 0.1, 2.1, 0.2, -1.1, 0.3])
    values = dm.columns.add(name="DS_DCOMPLEX", data_type=ods.DS_DCOMPLEX).double_arrays.values
    values.add().values.extend([1.1, 0.1, 2.1, 0.2, -1.1, 0.3])
    values.add().values.extend([1.1, 0.1, 2.1, 0.2, -1.1, 0.3])

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (2, 3)
    assert len(pdf.to_json()) > 0

    assert (
        pdf["ComplexTypes.DS_COMPLEX"][0] == np.array([1.1 + 0.1j, 2.1 + 0.2j, -1.1 + 0.3j], dtype=np.complex64)
    ).all()
    assert (
        pdf["ComplexTypes.DS_DCOMPLEX"][0] == np.array([1.1 + 0.1j, 2.1 + 0.2j, -1.1 + 0.3j], dtype=np.complex128)
    ).all()


def test_dt_external_reference():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="ExtRefTypes")
    dm.columns.add(name="DT_LONG", data_type=ods.DT_LONG).long_array.values.extend([1, 2, -1])
    dm.columns.add(name="DT_EXTERNALREFERENCE", data_type=ods.DT_EXTERNALREFERENCE).string_array.values.extend(
        [
            "first picture",
            "image/jpg",
            "data/firstPic.jpg",
            "second picture",
            "image/jpg",
            "data/secondPic.jpg",
            "third picture",
            "image/jpg",
            "data/thirdPic.jpg",
        ]
    )

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (3, 2)
    assert len(pdf.to_json()) > 0

    assert pdf.to_dict() == {
        "ExtRefTypes.DT_LONG": {0: 1, 1: 2, 2: -1},
        "ExtRefTypes.DT_EXTERNALREFERENCE": {
            0: ("first picture", "image/jpg", "data/firstPic.jpg"),
            1: ("second picture", "image/jpg", "data/secondPic.jpg"),
            2: ("third picture", "image/jpg", "data/thirdPic.jpg"),
        },
    }


def test_ds_external_reference():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="ExtRefTypes")
    dm.columns.add(name="DT_LONG", data_type=ods.DT_LONG).long_array.values.extend([1, -1])
    values = dm.columns.add(name="DS_EXTERNALREFERENCE", data_type=ods.DS_EXTERNALREFERENCE).string_arrays.values
    values.add().values.extend(
        [
            "first picture",
            "image/jpg",
            "data/firstPic.jpg",
            "second picture",
            "image/jpg",
            "data/secondPic.jpg",
            "third picture",
            "image/jpg",
            "data/thirdPic.jpg",
        ]
    )
    values.add().values.extend(
        [
            "first picture",
            "image/jpg",
            "data/firstPic.jpg",
            "second picture",
            "image/jpg",
            "data/secondPic.jpg",
        ]
    )

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (2, 2)
    assert len(pdf.to_json()) > 0

    assert pdf.to_dict() == {
        "ExtRefTypes.DT_LONG": {0: 1, 1: -1},
        "ExtRefTypes.DS_EXTERNALREFERENCE": {
            0: [
                ("first picture", "image/jpg", "data/firstPic.jpg"),
                ("second picture", "image/jpg", "data/secondPic.jpg"),
                ("third picture", "image/jpg", "data/thirdPic.jpg"),
            ],
            1: [
                ("first picture", "image/jpg", "data/firstPic.jpg"),
                ("second picture", "image/jpg", "data/secondPic.jpg"),
            ],
        },
    }


def test_unknown_arrays_empty():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="UnknownTypes")
    dm.columns.add(name="Values", base_name="values", data_type=ods.DT_UNKNOWN).unknown_arrays.values.add(
        data_type=ods.DT_DOUBLE
    )

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (1, 1)
    assert len(pdf.to_json()) > 0
    assert pdf.to_dict() != {}

    assert unknown_array_values(dm.columns[0].unknown_arrays.values[0]) == []


def test_aggregates():
    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=4711, name="Aggregates")
    dm.columns.add(
        name="Name", base_name="name", aggregate=ods.AggregateEnum.AG_NONE, data_type=ods.DT_STRING
    ).string_array.values[:] = ["my_name"]
    dm.columns.add(
        name="Maximum", base_name="maximum", aggregate=ods.AggregateEnum.AG_MAX, data_type=ods.DT_DOUBLE
    ).double_array.values[:] = [1.2]
    dm.columns.add(
        name="Maximum", base_name="maximum", aggregate=ods.AggregateEnum.AG_MIN, data_type=ods.DT_DOUBLE
    ).double_array.values[:] = [1.1]

    pdf = to_pandas(dms)
    logging.getLogger().info(pdf)
    assert pdf.shape == (1, 3)
    assert pdf.to_dict() == {
        "Aggregates.Name": {0: "my_name"},
        "Aggregates.Maximum.AG_MAX": {0: 1.2},
        "Aggregates.Maximum.AG_MIN": {0: 1.1},
    }
    assert len(pdf.to_json()) > 0

    pdf = to_pandas(dms, name_separator="::")
    logging.getLogger().info(pdf)
    assert pdf.shape == (1, 3)
    assert pdf.to_dict() == {
        "Aggregates::Name": {0: "my_name"},
        "Aggregates::Maximum::AG_MAX": {0: 1.2},
        "Aggregates::Maximum::AG_MIN": {0: 1.1},
    }
    assert len(pdf.to_json()) > 0

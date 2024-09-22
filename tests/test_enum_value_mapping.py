from __future__ import annotations

import os
from pathlib import Path

from google.protobuf.json_format import Parse

from odsbox.model_cache import ModelCache
from odsbox.proto.ods_pb2 import Model, DataMatrices
from odsbox.datamatrices_to_pandas import to_pandas


def __get_model(model_file_name):
    model_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", model_file_name)
    model = Model()
    Parse(Path(model_file).read_text(encoding="utf-8"), model)
    return model


def test_map_enum_values():
    mc = ModelCache(__get_model("application_model.json"))

    mq_entity = mc.entity_by_base_name("AoMeasurementQuantity")
    assert mq_entity is not None
    datatype_attribute = mc.attribute_by_base_name(mq_entity, "datatype")
    assert datatype_attribute is not None

    dms = DataMatrices()
    dm = dms.matrices.add(aid=mq_entity.aid, name=mq_entity.name, base_name=mq_entity.base_name)
    dm.columns.add(
        name=datatype_attribute.name, base_name=datatype_attribute.base_name, data_type=datatype_attribute.data_type
    ).long_array.values[:] = [10, 2, 1, 30, 7, 0, 8, 9, 5, 28, 12, 4, 6, 13, 3, 14, 11]

    assert to_pandas(dms).to_dict() == {
        "MeaQuantity.DataType": {
            0: 10,
            1: 2,
            2: 1,
            3: 30,
            4: 7,
            5: 0,
            6: 8,
            7: 9,
            8: 5,
            9: 28,
            10: 12,
            11: 4,
            12: 6,
            13: 13,
            14: 3,
            15: 14,
            16: 11,
        }
    }

    assert to_pandas(dms, mc, True).to_dict() == {
        "MeaQuantity.DataType": {
            0: "DT_DATE",
            1: "DT_SHORT",
            2: "DT_STRING",
            3: "DT_ENUM",
            4: "DT_DOUBLE",
            5: "DT_UNKNOWN",
            6: "DT_LONGLONG",
            7: "DT_ID",
            8: "DT_BYTE",
            9: "DT_EXTERNALREFERENCE",
            10: "DT_BLOB",
            11: "DT_BOOLEAN",
            12: "DT_LONG",
            13: "DT_COMPLEX",
            14: "DT_FLOAT",
            15: "DT_DCOMPLEX",
            16: "DT_BYTESTR",
        }
    }


def test_map_enum_values_ds():
    mc = ModelCache(__get_model("application_model.json"))

    entity = mc.entity("sound_measurement")
    assert entity is not None
    attribute_dt = mc.attribute(entity, "DT_ENUM_VALUES")
    assert attribute_dt is not None
    attribute_ds = mc.attribute(entity, "DS_ENUM_VALUES")
    assert attribute_ds is not None

    dms = DataMatrices()
    dm = dms.matrices.add(aid=entity.aid, name=entity.name, base_name=entity.base_name)
    dm.columns.add(
        name=attribute_dt.name, base_name=attribute_dt.base_name, data_type=attribute_dt.data_type
    ).long_array.values[:] = [0, 1, 2]
    values = dm.columns.add(
        name=attribute_ds.name, base_name=attribute_ds.base_name, data_type=attribute_ds.data_type
    ).long_arrays.values
    values.add().values.extend([0, 1, 2])
    values.add().values.extend([2, 1, 0])
    values.add().values.extend([2, 0, 2])

    assert to_pandas(dms).to_dict() == {
        "sound_measurement.DT_ENUM_VALUES": {0: 0, 1: 1, 2: 2},
        "sound_measurement.DS_ENUM_VALUES": {0: [0, 1, 2], 1: [2, 1, 0], 2: [2, 0, 2]},
    }

    assert to_pandas(dms, mc, True).to_dict() == {
        "sound_measurement.DT_ENUM_VALUES": {0: "editing", 1: "valid", 2: "archive"},
        "sound_measurement.DS_ENUM_VALUES": {
            0: ["editing", "valid", "archive"],
            1: ["archive", "valid", "editing"],
            2: ["archive", "editing", "archive"],
        },
    }

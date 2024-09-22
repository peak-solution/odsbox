from __future__ import annotations

import os
from pathlib import Path

from google.protobuf.json_format import Parse

from odsbox.model_cache import ModelCache
from odsbox.proto.ods_pb2 import Model, DataMatrices
import odsbox.proto.ods_pb2 as ods
from odsbox.datamatrices_to_pandas import to_pandas
from pandas import Timestamp, NaT


def __get_model(model_file_name):
    model_file = os.path.join(os.path.abspath(os.path.dirname(__file__)), "test_data", model_file_name)
    model = Model()
    Parse(Path(model_file).read_text(encoding="utf-8"), model)
    return model


def test_map_date_values():
    mc = ModelCache(__get_model("application_model.json"))

    mea_entity = mc.entity_by_base_name("AoMeasurement")
    assert mea_entity is not None
    measurement_begin_attribute = mc.attribute_by_base_name(mea_entity, "measurement_begin")
    assert measurement_begin_attribute is not None

    dms = DataMatrices()
    dm = dms.matrices.add(aid=mea_entity.aid, name=mea_entity.name, base_name=mea_entity.base_name)
    dm.columns.add(
        name=measurement_begin_attribute.name,
        base_name=measurement_begin_attribute.base_name,
        data_type=measurement_begin_attribute.data_type,
    ).string_array.values[:] = [
        "20241211",
        "20241211133310",
        "2024121113331056",
        "20241211133310561234567",
        "2024121113",
        "202412111333",
        "20241211133356",
        "202412111333561",
        "2024121113335612",
        "20241211133356123",
        "202412111333561234",
        "2024121113335612345",
        "20241211133356123456",
        "202412111333561234567",
        "2024121113335612345678",
        "20241211133356123456789",
        "19700101",
        "1970010100",
        "197001010000",
        "19700101000000",
        "",
    ]

    assert to_pandas(dms).to_dict() == {
        "MeaResult.MeasurementBegin": {
            0: "20241211",
            1: "20241211133310",
            2: "2024121113331056",
            3: "20241211133310561234567",
            4: "2024121113",
            5: "202412111333",
            6: "20241211133356",
            7: "202412111333561",
            8: "2024121113335612",
            9: "20241211133356123",
            10: "202412111333561234",
            11: "2024121113335612345",
            12: "20241211133356123456",
            13: "202412111333561234567",
            14: "2024121113335612345678",
            15: "20241211133356123456789",
            16: "19700101",
            17: "1970010100",
            18: "197001010000",
            19: "19700101000000",
            20: "",
        }
    }

    assert to_pandas(dms, date_as_timestamp=True).to_dict() == {
        "MeaResult.MeasurementBegin": {
            0: Timestamp("2024-12-11 00:00:00"),
            1: Timestamp("2024-12-11 13:33:10"),
            2: Timestamp("2024-12-11 13:33:10.560000"),
            3: Timestamp("2024-12-11 13:33:10.561234567"),
            4: Timestamp("2024-12-11 13:00:00"),
            5: Timestamp("2024-12-11 13:33:00"),
            6: Timestamp("2024-12-11 13:33:56"),
            7: Timestamp("2024-12-11 13:33:56.100000"),
            8: Timestamp("2024-12-11 13:33:56.120000"),
            9: Timestamp("2024-12-11 13:33:56.123000"),
            10: Timestamp("2024-12-11 13:33:56.123400"),
            11: Timestamp("2024-12-11 13:33:56.123450"),
            12: Timestamp("2024-12-11 13:33:56.123456"),
            13: Timestamp("2024-12-11 13:33:56.123456700"),
            14: Timestamp("2024-12-11 13:33:56.123456780"),
            15: Timestamp("2024-12-11 13:33:56.123456789"),
            16: Timestamp("1970-01-01 00:00:00"),
            17: Timestamp("1970-01-01 00:00:00"),
            18: Timestamp("1970-01-01 00:00:00"),
            19: Timestamp("1970-01-01 00:00:00"),
            20: NaT,
        }
    }


def test_map_date_values_ds():
    mc = ModelCache(__get_model("application_model.json"))

    entity = mc.entity("sound_measurement")
    assert entity is not None
    attribute_dt = mc.attribute(entity, "DT_DATE_VALUES")
    assert attribute_dt is not None
    attribute_ds = mc.attribute(entity, "DS_DATE_VALUES")
    assert attribute_ds is not None

    dms = DataMatrices()
    dm = dms.matrices.add(aid=entity.aid, name=entity.name, base_name=entity.base_name)
    dm.columns.add(
        name=attribute_dt.name, base_name=attribute_dt.base_name, data_type=attribute_dt.data_type
    ).string_array.values[:] = ["20240101125530", "20240101125531", "20240101125532"]
    values = dm.columns.add(
        name=attribute_ds.name, base_name=attribute_ds.base_name, data_type=attribute_ds.data_type
    ).string_arrays.values
    values.add().values.extend(["20240101125540", "20240101125541", "20240101125542"])
    values.add().values.extend(["20240101125550", "20240101125551", "20240101125552"])
    values.add().values.extend(["20240101125520", "20240101125521", "20240101125522"])

    assert to_pandas(dms).to_dict() == {
        "sound_measurement.DT_DATE_VALUES": {0: "20240101125530", 1: "20240101125531", 2: "20240101125532"},
        "sound_measurement.DS_DATE_VALUES": {
            0: ["20240101125540", "20240101125541", "20240101125542"],
            1: ["20240101125550", "20240101125551", "20240101125552"],
            2: ["20240101125520", "20240101125521", "20240101125522"],
        },
    }

    assert to_pandas(dms, date_as_timestamp=True).to_dict() == {
        "sound_measurement.DT_DATE_VALUES": {
            0: Timestamp("2024-01-01 12:55:30"),
            1: Timestamp("2024-01-01 12:55:31"),
            2: Timestamp("2024-01-01 12:55:32"),
        },
        "sound_measurement.DS_DATE_VALUES": {
            0: [Timestamp("2024-01-01 12:55:40"), Timestamp("2024-01-01 12:55:41"), Timestamp("2024-01-01 12:55:42")],
            1: [Timestamp("2024-01-01 12:55:50"), Timestamp("2024-01-01 12:55:51"), Timestamp("2024-01-01 12:55:52")],
            2: [Timestamp("2024-01-01 12:55:20"), Timestamp("2024-01-01 12:55:21"), Timestamp("2024-01-01 12:55:22")],
        },
    }


def test_unknown_arrays_date():
    dms = DataMatrices()
    dm = dms.matrices.add(aid=4711, name="UnknownTypes")
    column = dm.columns.add(name="Values", base_name="values", data_type=ods.DT_UNKNOWN)

    column.unknown_arrays.values.add(data_type=ods.DT_DATE).string_array.values.extend(
        ["20240101125540", "20240101125541", "20240101125542"]
    )

    assert to_pandas(dms).to_dict() == {
        "UnknownTypes.Values": {0: ["20240101125540", "20240101125541", "20240101125542"]}
    }

    assert to_pandas(dms, date_as_timestamp=True).to_dict() == {
        "UnknownTypes.Values": {
            0: [Timestamp("2024-01-01 12:55:40"), Timestamp("2024-01-01 12:55:41"), Timestamp("2024-01-01 12:55:42")]
        }
    }

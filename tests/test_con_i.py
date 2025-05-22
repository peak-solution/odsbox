"""Integration test for ASAM ODS session"""

from _pytest.fixtures import FixtureRequest
from odsbox.con_i import ConI
from odsbox.submatrix_to_pandas import submatrix_to_pandas
from odsbox.jaquel import jaquel_to_ods
import odsbox.proto.ods_pb2 as ods

from google.protobuf.json_format import MessageToJson

import logging
import pytest
import os
import tempfile


def __create_con_i():
    """Create a connection session for an ASAM ODS server"""
    return ConI("https://docker.peak-solution.de:10032/api", ("Demo", "mdm"))


def test_con_i():
    with __create_con_i() as con_i:
        model = con_i.model_read()
        assert len(model.entities) > 0

        entity = con_i.mc.entity_by_base_name("AoUnit")
        assert entity.base_name.lower() == "aounit"

        con_i.query_data({"AoEnvironment": {}, "$options": {"$rowlimit": 1}})
        con_i.query_data({"AoUnit": {}, "$options": {"$rowlimit": 1}})
        con_i.query_data({"AoMeasurement": {}, "$options": {"$rowlimit": 1}})
        con_i.query_data(
            {"AoMeasurement": {}, "$options": {"$rowlimit": 50}}, date_as_timestamp=True, enum_as_string=True
        )

        r = con_i.query_data(
            {"AoUnit": {}, "$attributes": {"name": 1}, "$options": {"$rowlimit": 1}}, name_separator="::"
        )
        assert f"{entity.name}::" in r.columns[0]


def test_query_data():
    with __create_con_i() as con_i:
        model = con_i.model_read()
        assert len(model.entities) > 0

        assert con_i.query_data({"AoEnvironment": {}, "$options": {"$rowlimit": 1}}).empty is False
        assert con_i.query_data('{"AoEnvironment": {}, "$options": {"$rowlimit": 1}}').empty is False

        entity, select_statement = jaquel_to_ods(con_i.model(), {"AoEnvironment": {}, "$options": {"$rowlimit": 1}})
        assert entity.base_name == "AoEnvironment"
        assert con_i.query_data(select_statement).empty is False

        entity, select_statement = jaquel_to_ods(con_i.model(), '{"AoEnvironment": {}, "$options": {"$rowlimit": 1}}')
        assert entity.base_name == "AoEnvironment"
        assert con_i.query_data(select_statement).empty is False


def test_transaction():
    with __create_con_i() as con_i:
        with con_i.transaction() as transaction:
            transaction.abort()

        with con_i.transaction() as transaction:
            transaction.commit()

        with con_i.transaction() as transaction:
            pass  # automatically abort


@pytest.mark.integration
def test_submatrix_load():
    with __create_con_i() as con_i:
        sm_s = con_i.query_data(
            {
                "AoSubmatrix": {},
                "$options": {"$rowlimit": 1},
                "$attributes": {"id": 1, "measurement.name": 1},
            }
        )

        logging.getLogger().info(sm_s.shape)
        assert sm_s.shape[0] <= 1
        if 1 == sm_s.shape[0]:
            submatrix_id = int(sm_s.iloc[0, 0])  # type: ignore
            assert 0 != submatrix_id
            logging.getLogger().info(submatrix_id)
            submatrix_dataframe = submatrix_to_pandas(con_i, submatrix_id)
            assert submatrix_dataframe is not None


def test_bug_93_outer_join_on_n_relation():
    logging.getLogger().info(
        "In case of outer join the direction is important and must point from n to 1 when generated from jaquel."
    )
    with __create_con_i() as con_i:
        df = con_i.query_data(
            {
                "AoMeasurement": {},
                "$attributes": {
                    "name": 1,
                    "measurement_quantities:OUTER": {"name": 1, "unit:OUTER.name": 1},
                    "submatrices:OUTER.name": 1,
                },
                "$options": {"$rowlimit": 10},
            }
        )
        assert df is not None
        assert df.empty is False
        assert 4 == len(df.columns)


@pytest.mark.integration
def test_values_data_read(request: FixtureRequest):
    test_name = request.node.name
    with __create_con_i() as con_i:
        sm_df = con_i.query_data(
            {
                "AoSubmatrix": {"name": "Profile_02"},
                "$attributes": {"id": 1, "number_of_rows": 1},
                "$options": {"$rowlimit": 1},
            }
        )
        assert sm_df.empty is False
        sm_id: int = sm_df.iat[0, 0]
        sm_number_of_rows: int = sm_df.iat[0, 1]
        logging.getLogger().info("Submatrix Id: %s, number_of_rows:%s", sm_id, sm_number_of_rows)
        assert sm_id is not None
        assert sm_number_of_rows is not None

        lc_info_df = con_i.query_data(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "name": 1, "sequence_representation": 1, "independent": 1},
            }
        )
        assert lc_info_df.empty is False

        lc_info_dms = con_i.data_read_jaquel(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "name": 1, "sequence_representation": 1, "independent": 1},
            }
        )
        assert 1 == len(lc_info_dms.matrices)
        with open(
            os.path.join(tempfile.gettempdir(), f"{test_name}_lc_info.proto.json"), "w", encoding="utf-8"
        ) as json_file:
            json_file.write(MessageToJson(lc_info_dms))

        lc_values_df = con_i.query_data(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "values": 1, "generation_parameters": 1},
                "$options": {"$seqlimit": 10, "$seqskip": 0},
            }
        )
        assert lc_values_df.empty is False

        lc_values_dms = con_i.data_read_jaquel(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "values": 1, "generation_parameters": 1},
                "$options": {"$seqlimit": sm_number_of_rows, "$seqskip": 0},
            }
        )
        assert 1 == len(lc_values_dms.matrices)
        with open(
            os.path.join(tempfile.gettempdir(), f"{test_name}_lc_values.proto.json"), "w", encoding="utf-8"
        ) as json_file:
            json_file.write(MessageToJson(lc_values_dms))


# @pytest.mark.integration
def test_values_valuematrix_read_calculated(request: FixtureRequest):
    test_name = request.node.name
    with __create_con_i() as con_i:
        sm_df = con_i.query_data(
            {
                "AoSubmatrix": {"name": "Profile_02"},
                "$attributes": {"id": 1, "number_of_rows": 1},
                "$options": {"$rowlimit": 1},
            }
        )
        assert sm_df.empty is False
        sm_id: int = sm_df.iat[0, 0]
        sm_number_of_rows: int = sm_df.iat[0, 1]
        logging.getLogger().info("Submatrix Id: %s, number_of_rows:%s", sm_id, sm_number_of_rows)
        assert sm_id is not None
        assert sm_number_of_rows is not None

        lc_entity = con_i.mc.entity_by_base_name("AoLocalColumn")
        assert lc_entity is not None

        lc_info_df = con_i.query_data(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "name": 1, "sequence_representation": 1, "independent": 1},
            }
        )
        assert lc_info_df.empty is False

        lc_info_dms = con_i.data_read_jaquel(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "name": 1, "sequence_representation": 1, "independent": 1},
            }
        )
        assert 1 == len(lc_info_dms.matrices)
        with open(
            os.path.join(tempfile.gettempdir(), f"{test_name}_lc_info.proto.json"), "w", encoding="utf-8"
        ) as json_file:
            json_file.write(MessageToJson(lc_info_dms))

        lc_attributes = [
            con_i.mc.attribute_by_base_name(lc_entity, "name").name,
            con_i.mc.attribute_by_base_name(lc_entity, "values").name,
        ]

        lc_values_dms = con_i.valuematrix_read(
            ods.ValueMatrixRequestStruct(
                aid=con_i.mc.entity_by_base_name("AoSubmatrix").aid,
                iid=sm_id,
                mode=ods.ValueMatrixRequestStruct.ModeEnum.MO_CALCULATED,
                columns=[ods.ValueMatrixRequestStruct.ColumnItem(name="*")],
                attributes=lc_attributes,
                values_start=0,
                values_limit=10,
            )
        )
        assert 1 == len(lc_values_dms.matrices)
        with open(
            os.path.join(tempfile.gettempdir(), f"{test_name}_lc_values.proto.json"), "w", encoding="utf-8"
        ) as json_file:
            json_file.write(MessageToJson(lc_values_dms))


def test_values_valuematrix_read_storage(request: FixtureRequest):
    test_name = request.node.name
    with __create_con_i() as con_i:
        sm_df = con_i.query_data(
            {
                "AoSubmatrix": {"name": "Profile_02"},
                "$attributes": {"id": 1, "number_of_rows": 1},
                "$options": {"$rowlimit": 1},
            }
        )
        assert sm_df.empty is False
        sm_id: int = sm_df.iat[0, 0]
        sm_number_of_rows: int = sm_df.iat[0, 1]
        logging.getLogger().info("Submatrix Id: %s, number_of_rows:%s", sm_id, sm_number_of_rows)
        assert sm_id is not None
        assert sm_number_of_rows is not None

        lc_entity = con_i.mc.entity_by_base_name("AoLocalColumn")
        assert lc_entity is not None

        lc_info_df = con_i.query_data(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "name": 1, "sequence_representation": 1, "independent": 1},
            }
        )
        assert lc_info_df.empty is False

        lc_names = lc_info_df.iloc[:, 1].tolist()
        lc_columns = [ods.ValueMatrixRequestStruct.ColumnItem(name=lc_name) for lc_name in lc_names]

        lc_attributes = [
            con_i.mc.attribute_by_base_name(lc_entity, "name").name,
            con_i.mc.attribute_by_base_name(lc_entity, "values").name,
            con_i.mc.attribute_by_base_name(lc_entity, "generation_parameters").name,
        ]

        lc_info_dms = con_i.data_read_jaquel(
            {
                "AoLocalColumn": {"submatrix": sm_id},
                "$attributes": {"id": 1, "name": 1, "sequence_representation": 1, "independent": 1},
            }
        )
        assert 1 == len(lc_info_dms.matrices)
        with open(
            os.path.join(tempfile.gettempdir(), f"{test_name}_lc_info.proto.json"), "w", encoding="utf-8"
        ) as json_file:
            json_file.write(MessageToJson(lc_info_dms))

        value_matrix_request = ods.ValueMatrixRequestStruct(
            aid=con_i.mc.entity_by_base_name("AoSubmatrix").aid,
            iid=sm_id,
            mode=ods.ValueMatrixRequestStruct.ModeEnum.MO_STORAGE,
            columns=lc_columns,
            attributes=lc_attributes,
            values_start=0,
            values_limit=10,
        )
        value_matrix_request_str = MessageToJson(value_matrix_request, indent=None)
        logging.getLogger().info(value_matrix_request_str)
        lc_values_dms = con_i.valuematrix_read(value_matrix_request)
        assert 1 == len(lc_values_dms.matrices)
        with open(
            os.path.join(tempfile.gettempdir(), f"{test_name}_lc_values.proto.json"), "w", encoding="utf-8"
        ) as json_file:
            json_file.write(MessageToJson(lc_values_dms))

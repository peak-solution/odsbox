"""Integration test for ASAM ODS session"""

from __future__ import annotations

import logging
import os
import tempfile

import pandas as pd
import pytest
from _pytest.fixtures import FixtureRequest
from google.protobuf.json_format import MessageToJson

import odsbox.proto.ods_pb2 as ods
import odsbox.proto.ods_security_pb2 as ods_security
from odsbox.con_i import ConI
from odsbox.jaquel import jaquel_to_ods
from odsbox.security import Security
from odsbox.submatrix_to_pandas import submatrix_to_pandas


def __create_con_i(load_model: bool = True) -> ConI:
    """Create a connection session for an ASAM ODS server"""
    return ConI("https://docker.peak-solution.de:10032/api", ("Demo", "mdm"), load_model=load_model)


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


def test_con_i_query():
    with __create_con_i() as con_i:
        model = con_i.model_read()
        assert len(model.entities) > 0

        r = con_i.query({"AoUnit": {}, "$attributes": {"name": 1, "id": 1}, "$options": {"$rowlimit": 1}})
        assert "name" in r.columns
        assert "id" in r.columns

        r = con_i.query({"AoUnit": {}, "$attributes": {"Name": 1, "Id": 1}, "$options": {"$rowlimit": 1}})
        assert "Name" in r.columns
        assert "Id" in r.columns

        r = con_i.query(
            {"AoUnit": {}, "$attributes": {"name": 1, "phys_dimension.name": 1}, "$options": {"$rowlimit": 1}}
        )
        assert "name" in r.columns
        assert "phys_dimension.name" in r.columns

        r = con_i.query({"AoUnit": {}, "$attributes": {"name": {"$distinct": 1}}, "$options": {"$rowlimit": 1}})
        assert "name.$distinct" in r.columns


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
            query={
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
            query={
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
            query={
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


def test_security_property_returns_security_instance(monkeypatch):
    class DummySecurity:
        __slots__ = ("con_i",)

        def __init__(self, con_i):
            object.__setattr__(self, "con_i", con_i)

        def __setattr__(self, key, value):
            raise AttributeError(f"{self.__class__.__name__} is immutable")

    # Patch Security to DummySecurity
    import odsbox.con_i

    monkeypatch.setattr(odsbox.con_i, "Security", DummySecurity)

    with __create_con_i() as con_i:
        # Remove cached security to force property to create it
        con_i.__security = None
        security = con_i.security
        assert isinstance(security, DummySecurity)
        # Should return the same instance if called again
        assert security is con_i.security


def test_security_property_raises_if_no_session():
    with __create_con_i() as con_i:
        # Simulate closed session
        con_i._ConI__session = None  # type: ignore
        try:
            _ = con_i.security
            raise AssertionError("Expected ValueError when session is None")
        except ValueError as e:
            assert "No open session" in str(e)


def test_security_read():
    with __create_con_i() as con_i:
        security_read_request = ods_security.SecurityReadRequest(
            data_object_type=ods_security.DataObjectTypeEnum.DOT_APPLICATION_ELEMENT,
            application_element=ods_security.SecurityReadRequest.DataObjectApplicationElement(
                aid=con_i.mc.entity_by_base_name("AoTest").aid
            ),
        )
        security_info = con_i.security.security_read(security_read_request)
        assert isinstance(security_info, ods_security.SecurityInfo)


def test_security_level():
    with __create_con_i() as con_i:
        entity = con_i.mc.entity_by_base_name("AoTest")
        security_level = Security.Level(entity.security_level)
        assert isinstance(security_level, Security.Level)
        assert Security.Level.ELEMENT not in security_level

    assert 7 == int(Security.Level.ELEMENT | Security.Level.INSTANCE | Security.Level.ATTRIBUTE)


def test_do_not_load_model():
    """Test that ConI can be created without loading the model"""
    with __create_con_i(False) as con_i:
        with pytest.raises(ValueError):
            _ = con_i.mc
        with pytest.raises(ValueError):
            _ = con_i.model()
        # update model
        assert con_i.model_read() is not None
        # Access the cached model
        assert con_i.mc is not None
        con_i.model()


@pytest.mark.integration
def test_nested_query(request: FixtureRequest):
    with __create_con_i() as con_i:
        df = con_i.query_data(
            {"AoTest": {"name": {"$in": {"$nested": {"AoTest": {}, "$attributes": {"name": {"$distinct": 1}}}}}}}
        )

        assert df is not None
        assert not df.empty

        _, s = jaquel_to_ods(
            con_i.model(),
            {"AoTest": {"name": {"$in": {"$nested": {"AoTest": {}, "$attributes": {"name": {"$distinct": 1}}}}}}},
        )
        assert s.where[0].condition.nested_statement.columns[0].attribute is not None


def test_query_with_jaquel_column_names():
    """Test that query method uses JAQueL column names by default"""
    with __create_con_i() as con_i:
        # Simple query with single attribute
        r = con_i.query({"AoUnit": {}, "$attributes": {"name": 1}, "$options": {"$rowlimit": 1}})
        assert "name" in r.columns
        assert "AoUnit.name" not in r.columns

        # Query with multiple attributes
        r = con_i.query({"AoUnit": {}, "$attributes": {"name": 1, "id": 1}, "$options": {"$rowlimit": 1}})
        assert "name" in r.columns
        assert "id" in r.columns
        assert len(r.columns) == 2

        # Query with relationship path
        r = con_i.query(
            {
                "AoUnit": {},
                "$attributes": {"name": 1, "phys_dimension.name": 1, "phys_dimension.id": 1},
                "$options": {"$rowlimit": 1},
            }
        )
        assert "name" in r.columns
        assert "phys_dimension.name" in r.columns
        assert "phys_dimension.id" in r.columns


def test_query_disable_jaquel_column_names():
    """Test that query method can disable JAQueL column naming"""
    with __create_con_i() as con_i:
        entity = con_i.mc.entity_by_base_name("AoUnit")
        name_attr = con_i.mc.attribute_by_base_name(entity, "name")

        r = con_i.query(
            {"AoUnit": {}, "$attributes": {"name": 1}, "$options": {"$rowlimit": 1}},
            result_naming_mode="model",
        )
        # Should use default naming (Entity.Attribute format)
        # The actual attribute name from the model is used (likely capitalized)
        assert any(entity.name in col for col in r.columns)
        assert f"{entity.name}.{name_attr.name}" in r.columns


def test_query_with_aggregates():
    """Test query method with aggregate functions"""
    with __create_con_i() as con_i:
        # Query with distinct
        r = con_i.query({"AoUnit": {}, "$attributes": {"name": {"$distinct": 1}}, "$options": {"$rowlimit": 5}})
        assert "name.$distinct" in r.columns

        # Query with count
        r = con_i.query({"AoUnit": {}, "$attributes": {"id": {"$count": 1}}})
        assert "id.$count" in r.columns
        assert len(r) == 1  # Aggregate should return single row


def test_query_enum_as_string():
    """Test query method with enum_as_string parameter"""
    with __create_con_i() as con_i:
        # Simply test that the parameter is accepted and queries execute
        # Testing actual enum conversion would require knowing specific enum fields
        r = con_i.query(
            {"AoUnit": {}, "$attributes": {"id": 1, "name": 1}, "$options": {"$rowlimit": 5}}, enum_as_string=True
        )
        assert "id" in r.columns
        assert "name" in r.columns
        assert not r.empty

        # Test with enum_as_string=False
        r = con_i.query(
            {"AoUnit": {}, "$attributes": {"id": 1, "name": 1}, "$options": {"$rowlimit": 5}}, enum_as_string=False
        )
        assert "id" in r.columns
        assert "name" in r.columns
        assert not r.empty


def test_query_date_as_timestamp():
    """Test query method with date_as_timestamp parameter"""
    with __create_con_i() as con_i:
        # Query with date fields
        r = con_i.query(
            {"AoMeasurement": {}, "$attributes": {"id": 1, "measurement_begin": 1}, "$options": {"$rowlimit": 5}},
            date_as_timestamp=True,
        )
        assert "measurement_begin" in r.columns
        # Check if date values are timestamps
        if not r.empty and r["measurement_begin"].notna().any():
            first_valid = r["measurement_begin"].dropna().iloc[0]
            assert isinstance(first_valid, pd.Timestamp), f"Expected Timestamp, got {type(first_valid)}"

        # Test with date_as_timestamp=False
        r = con_i.query(
            {"AoMeasurement": {}, "$attributes": {"id": 1, "measurement_begin": 1}, "$options": {"$rowlimit": 5}},
            date_as_timestamp=False,
        )
        if not r.empty and r["measurement_begin"].notna().any():
            first_valid = r["measurement_begin"].dropna().iloc[0]
            # With date_as_timestamp=False, should be string
            assert isinstance(first_valid, str), f"Expected string, got {type(first_valid)}"


def test_query_is_null_to_nan():
    """Test query method with is_null_to_nan parameter"""
    with __create_con_i() as con_i:
        # Query with is_null_to_nan=True (default)
        r = con_i.query(
            {"AoUnit": {}, "$attributes": {"name": 1, "description": 1}, "$options": {"$rowlimit": 10}},
            is_null_to_nan=True,
        )
        assert "description" in r.columns
        # Check if any null values are marked as pd.NA
        if r["description"].isna().any():
            assert r["description"].dtype == object or pd.api.types.is_string_dtype(r["description"])

        # Query with is_null_to_nan=False
        r = con_i.query(
            {"AoUnit": {}, "$attributes": {"name": 1, "description": 1}, "$options": {"$rowlimit": 10}},
            is_null_to_nan=False,
        )
        assert "description" in r.columns


def test_query_with_string_format():
    """Test query method with string formatted JAQueL"""
    with __create_con_i() as con_i:
        # Query using JSON string
        r = con_i.query('{"AoUnit": {}, "$attributes": {"name": 1}, "$options": {"$rowlimit": 1}}')
        assert "name" in r.columns
        assert not r.empty


def test_query_with_joins():
    """Test query method with join relationships"""
    with __create_con_i() as con_i:
        # Query with joined attributes from related entities
        r = con_i.query(
            {
                "AoMeasurement": {},
                "$attributes": {
                    "id": 1,
                    "name": 1,
                    "measurement_quantities.name": 1,
                },
                "$options": {"$rowlimit": 5},
            }
        )
        assert "id" in r.columns
        assert "name" in r.columns
        assert "measurement_quantities.name" in r.columns


def test_query_with_asterisk():
    """Test query method with join relationships"""
    with __create_con_i() as con_i:
        unit_e = con_i.mc.entity_by_base_name("AoUnit")
        phys_dim_e = con_i.mc.entity_by_base_name("AoPhysicalDimension")

        df = con_i.query(
            {
                "AoUnit": {},
                "$options": {"$rowlimit": 1},
            }
        )
        assert con_i.mc.attribute_by_base_name(unit_e, "name").name in df.columns
        assert con_i.mc.attribute_by_base_name(unit_e, "id").name in df.columns
        assert con_i.mc.relation_by_base_name(unit_e, "phys_dimension").name in df.columns

        df = con_i.query(
            {
                "AoUnit": {},
                "$attributes": {
                    "*": 1,
                },
                "$options": {"$rowlimit": 1},
            }
        )
        assert con_i.mc.attribute_by_base_name(unit_e, "name").name in df.columns
        assert con_i.mc.attribute_by_base_name(unit_e, "id").name in df.columns
        assert con_i.mc.relation_by_base_name(unit_e, "phys_dimension").name in df.columns

        df = con_i.query(
            {
                "AoUnit": {},
                "$attributes": {
                    "*": 1,
                    "phys_dimension.*": 1,
                },
                "$options": {"$rowlimit": 1},
            }
        )
        assert con_i.mc.attribute_by_base_name(unit_e, "name").name in df.columns
        assert con_i.mc.attribute_by_base_name(unit_e, "id").name in df.columns
        assert con_i.mc.relation_by_base_name(unit_e, "phys_dimension").name in df.columns
        assert "phys_dimension." + con_i.mc.attribute_by_base_name(phys_dim_e, "name").name in df.columns
        assert "phys_dimension." + con_i.mc.attribute_by_base_name(phys_dim_e, "id").name in df.columns

        df = con_i.query(
            {
                "AoUnit": {},
                "$attributes": {
                    "NAME": 1,
                    "phys_dimension.*": 1,
                },
                "$options": {"$rowlimit": 1},
            }
        )
        assert "NAME" in df.columns
        assert con_i.mc.attribute_by_base_name(unit_e, "id").name not in df.columns
        assert con_i.mc.relation_by_base_name(unit_e, "phys_dimension").name not in df.columns
        assert "phys_dimension." + con_i.mc.attribute_by_base_name(phys_dim_e, "name").name in df.columns
        assert "phys_dimension." + con_i.mc.attribute_by_base_name(phys_dim_e, "id").name in df.columns

        df = con_i.query(
            {
                "AoUnit": {},
                "$attributes": {
                    "NAME": 1,
                    "PHYS_DIMENSION.*": 1,
                },
                "$options": {"$rowlimit": 1},
            }
        )
        assert "NAME" in df.columns
        assert con_i.mc.attribute_by_base_name(unit_e, "id").name not in df.columns
        assert con_i.mc.relation_by_base_name(unit_e, "phys_dimension").name not in df.columns
        assert "PHYS_DIMENSION." + con_i.mc.attribute_by_base_name(phys_dim_e, "name").name in df.columns
        assert "PHYS_DIMENSION." + con_i.mc.attribute_by_base_name(phys_dim_e, "id").name in df.columns


def test_query_empty_result():
    """Test query method with query that returns no results"""
    with __create_con_i() as con_i:
        # Query with impossible condition
        r = con_i.query(
            {
                "AoUnit": {"name": "ThisUnitNameDoesNotExist_XYZ123"},
                "$attributes": {"name": 1, "id": 1},
                "$options": {"$rowlimit": 10},
            }
        )
        assert r.empty or len(r) == 0
        # Even empty result should have correct columns
        assert "name" in r.columns
        assert "id" in r.columns


def test_query_with_filter():
    """Test query method with filter conditions"""
    with __create_con_i() as con_i:
        # Query with simple filter
        r = con_i.query({"AoUnit": {"name": "m"}, "$attributes": {"name": 1, "id": 1}, "$options": {"$rowlimit": 5}})
        assert "name" in r.columns
        assert "id" in r.columns
        # All returned names should match filter
        if not r.empty:
            assert all("m" in str(name).lower() for name in r["name"] if name)


def test_custom_session():
    """Test that custom_session parameter is used when provided"""
    import requests

    # Create a custom session with specific configuration
    custom_session = requests.Session()
    custom_session.auth = ("Demo", "mdm")
    custom_session.verify = True

    # Create ConI with the custom session
    with ConI(url="https://docker.peak-solution.de:10032/api", custom_session=custom_session) as con_i:
        # Verify that the ConI instance works correctly
        model = con_i.model_read()
        assert len(model.entities) > 0

        # Verify we can query data
        result = con_i.query_data({"AoEnvironment": {}, "$options": {"$rowlimit": 1}})
        assert result.empty is False


def test_query_with_kwargs():
    """Test query method with additional kwargs passed to to_pandas"""
    with __create_con_i() as con_i:
        # Test with custom name_separator (though this should be overridden by JAQueL naming)
        r = con_i.query({"AoUnit": {}, "$attributes": {"name": 1}, "$options": {"$rowlimit": 1}}, name_separator="::")
        # JAQueL naming should take precedence
        assert "name" in r.columns

        r = con_i.query(
            jaquel_query={"AoUnit": {}, "$attributes": {"name": 1}, "$options": {"$rowlimit": 1}},
            result_naming_mode="model",
            name_separator="::",
        )
        # Should use custom separator
        assert any("::" in col for col in r.columns)

"""Integration test for ASAM ODS session"""

from odsbox.con_i import ConI
from odsbox.submatrix_to_pandas import submatrix_to_pandas
from odsbox.jaquel import jaquel_to_ods

import logging
import pytest


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

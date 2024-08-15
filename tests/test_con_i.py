"""Integration test for ASAM ODS session"""

from asam_odsbox.con_i import ConI
from asam_odsbox.submatrix_to_pandas import submatrix_to_pandas

import logging
import pytest


@pytest.mark.integration
def test_con_i():
    with ConI("http://79.140.180.128:10032/api", ("sa", "sa")) as con_i:
        model = con_i.model_read()
        assert len(model.entities) > 0

        entity = con_i.mc.entity_by_base_name("AoUnit")
        assert entity.base_name.lower() == "aounit"


@pytest.mark.integration
def test_submatrix_load():

    with ConI("http://79.140.180.128:10032/api", ("sa", "sa")) as con_i:
        sm_s = con_i.data_read_jaquel(
            {
                "AoSubmatrix": {},
                "$options": {"$rowlimit": 1},
                "$attributes": {"id": 1, "measurement.name": 1},
            }
        )

        logging.getLogger().info(sm_s.shape)
        assert sm_s.shape[0] <= 1
        if 1 == sm_s.shape[0]:
            submatrix_id = sm_s.iloc[0, 0]
            assert 0 != submatrix_id
            logging.getLogger().info(submatrix_id)
            submatrix_dataframe = submatrix_to_pandas(con_i, submatrix_id)
            assert submatrix_dataframe is not None

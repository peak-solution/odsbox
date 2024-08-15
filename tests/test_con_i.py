"""Integration test for ASAM ODS session"""

from asam_odsbox.con_i import ConI

import pytest


@pytest.mark.integration
def test_con_i():
    con_i = ConI("http://79.140.180.128:10032/api", ("sa", "sa"))
    model = con_i.model_read()
    assert len(model.entities) > 0

    entity = con_i.mc.entity_by_base_name("AoUnit")
    assert entity.base_name.lower() == "aounit"

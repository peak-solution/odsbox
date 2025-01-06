"""Integration test for ASAM ODS session"""

from odsbox.con_i import ConI
from odsbox.unit_catalog import UnitCatalog

import pytest


def __create_con_i():
    """Create a connection session for an ASAM ODS server"""
    return ConI("https://docker.peak-solution.de:10032/api", ("Demo", "mdm"))


def test_unit_get():
    with __create_con_i() as con_i:
        unit_catalog = UnitCatalog(con_i)

        assert unit_catalog.get("gradC") is not None
        assert unit_catalog.get("s") is not None
        assert unit_catalog.get("1/min") is not None
        assert unit_catalog.get("V") is not None
        assert unit_catalog.get("J") is not None
        assert unit_catalog.get("A") is not None
        assert unit_catalog.get("UnitDoesNotExit") is None
        assert unit_catalog.get("UnitDoesNotExit") is None


@pytest.mark.integration
def test_unit_get_or_create():
    with __create_con_i() as con_i:
        unit_catalog = UnitCatalog(con_i)

        assert unit_catalog.get_or_create("°C") > 0
        assert unit_catalog.get_or_create("s") > 0
        assert unit_catalog.get_or_create("rpm") > 0
        assert unit_catalog.get_or_create("V") > 0
        assert unit_catalog.get_or_create("J") > 0
        assert unit_catalog.get_or_create("A") > 0

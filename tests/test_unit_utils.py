"""Integration test for ASAM ODS session"""

from odsbox.con_i import ConI
from odsbox import unit_utils

import logging


def __create_con_i():
    """Create a connection session for an ASAM ODS server"""
    return ConI("https://docker.peak-solution.de:10032/api", ("Demo", "mdm"))


def test_query_units():
    with __create_con_i() as con_i:
        length_units = unit_utils.query_units(con_i, length=1)
        logging.getLogger().info(length_units)


def test_query_physical_dimensions():
    with __create_con_i() as con_i:
        length_physical_dimensions = unit_utils.query_physical_dimensions(con_i, length=1)
        logging.getLogger().info(length_physical_dimensions)


def test_query_quantity():
    with __create_con_i() as con_i:
        length_quantities = unit_utils.query_quantity(con_i, length=1)
        logging.getLogger().info(length_quantities)

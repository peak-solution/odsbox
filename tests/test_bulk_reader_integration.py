"""Integration test for ASAM ODS session"""

import logging

import pandas as pd
from odsbox.con_i import ConI
import pytest


def __create_con_i(load_model: bool = True) -> ConI:
    """Create a connection session for an ASAM ODS server"""
    return ConI("https://docker.peak-solution.de:10032/api", ("Demo", "mdm"), load_model=load_model)


@pytest.mark.integration
def test_bulk_reader_simple():
    with __create_con_i() as con_i:

        sm_s = con_i.query_data(
            {
                "AoSubmatrix": {"measurement.name": "Profile_52"},
                "$options": {"$rowlimit": 1},
                "$attributes": {"id": 1, "measurement.name": 1},
            }
        )

        logging.getLogger().info(sm_s.shape)
        assert sm_s.shape[0] <= 1
        if 1 == sm_s.shape[0]:
            submatrix_id = int(sm_s.iloc[0, 0])  # type: ignore

            df1 = con_i.bulk.data_read(submatrix_id, ["Time", "Coolant"], set_independent_as_index=False)
            assert df1.empty is False
            df2 = con_i.bulk.valuematrix_read(submatrix_id, ["Time", "Coolant"])
            assert df2.empty is False
            pd.testing.assert_frame_equal(df1, df2)

            pd.testing.assert_frame_equal(
                df1, con_i.bulk.data_read(submatrix_id, ["time", "coolant"], True, set_independent_as_index=False)
            )
            pd.testing.assert_frame_equal(
                df1, con_i.bulk.data_read(submatrix_id, ["Tim*", "Coola*"], False, set_independent_as_index=False)
            )
            pd.testing.assert_frame_equal(
                df1, con_i.bulk.data_read(submatrix_id, ["tim*", "coola*"], True, set_independent_as_index=False)
            )
            pd.testing.assert_frame_equal(
                df1.set_index("Time"), con_i.bulk.data_read(submatrix_id, ["t?me", "coola?t"], True)
            )


@pytest.mark.integration
def test_bulk_reader_query():
    with __create_con_i() as con_i:
        conditions = {"submatrix.measurement.name": {"$like": "Profile_5?"}}
        con_i.bulk.add_column_filters(conditions, ["Time", "Coolant"], column_patterns_case_insensitive=False)

        df1 = con_i.bulk.query(conditions)
        assert df1.empty is False
        assert list(df1.columns[:4]) == ["submatrix", "name", "id", "values"]

        submatrices = df1.groupby("submatrix")
        assert len(submatrices) > 0
        for _, group in submatrices:
            s = group.set_index("name")["values"]
            time_vals = s["Time"]
            coolant_vals = s["Coolant"]
            assert len(time_vals) > 0
            assert len(coolant_vals) > 0
            assert len(time_vals) == len(coolant_vals)

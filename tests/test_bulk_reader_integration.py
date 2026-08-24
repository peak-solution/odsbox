"""Integration test for ASAM ODS session"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import pytest

from odsbox.con_i import ConI


def __create_con_i(load_model: bool = True) -> ConI:
    """Create a connection session for an ASAM ODS server"""
    return ConI(
        "https://docker.peak-solution.de:10032/api",
        ("Demo", "mdm"),
        load_model=load_model,
    )


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
            submatrix_id = int(sm_s.iloc[0, 0])

            df1 = con_i.bulk.data_read(submatrix_id, ["Time", "Coolant"], set_independent_as_index=False)
            assert df1.empty is False
            df2 = con_i.bulk.valuematrix_read(submatrix_id, ["Time", "Coolant"])
            assert df2.empty is False
            pd.testing.assert_frame_equal(df1, df2)

            assert df1.attrs == df2.attrs
            assert "unit_names" in df1.attrs
            assert df1.attrs["unit_names"] == df2.attrs["unit_names"]
            assert df1.attrs["unit_names"] == {"Time": "s", "Coolant": "°C"}

            pd.testing.assert_frame_equal(
                df1,
                con_i.bulk.data_read(
                    submatrix_id,
                    ["time", "coolant"],
                    True,
                    set_independent_as_index=False,
                ),
            )
            pd.testing.assert_frame_equal(
                df1,
                con_i.bulk.data_read(
                    submatrix_id,
                    ["Tim*", "Coola*"],
                    False,
                    set_independent_as_index=False,
                ),
            )
            pd.testing.assert_frame_equal(
                df1,
                con_i.bulk.data_read(
                    submatrix_id,
                    ["tim*", "coola*"],
                    True,
                    set_independent_as_index=False,
                ),
            )
            pd.testing.assert_frame_equal(
                df1.set_index("Time"),
                con_i.bulk.data_read(submatrix_id, ["t?me", "coola?t"], True),
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


@pytest.mark.integration
def test_bulk_reader_query_load_flags() -> None:
    with __create_con_i() as con_i:
        conditions = {"submatrix.measurement.name": {"$like": "Profile_5?"}}
        con_i.bulk.add_column_filters(conditions, ["Time", "Coolant"], column_patterns_case_insensitive=False)

        # AoLocalColumn.flags is not requested unless load_flags=True
        df_without_flags = con_i.bulk.query(conditions, load_flags=False)
        assert df_without_flags.empty is False
        assert "flags" not in df_without_flags.columns

        df_with_flags = con_i.bulk.query(conditions, load_flags=True)
        assert df_with_flags.empty is False
        assert "flags" in df_with_flags.columns
        for flags in df_with_flags["flags"]:
            assert len(flags) > 0

        # requesting flags must not change the other columns/values
        pd.testing.assert_frame_equal(df_without_flags, df_with_flags.drop(columns=["flags"]))


@pytest.mark.integration
def test_bulk_reader_data_read_valid_flag() -> None:
    with __create_con_i() as con_i:
        sm_s = con_i.query_data(
            {
                "AoSubmatrix": {"measurement.name": "Profile_52"},
                "$options": {"$rowlimit": 1},
                "$attributes": {"id": 1, "measurement.name": 1},
            }
        )

        assert sm_s.shape[0] <= 1
        if 1 == sm_s.shape[0]:
            submatrix_id = int(sm_s.iloc[0, 0])

            df_plain = con_i.bulk.data_read(submatrix_id, ["Time", "Coolant"], set_independent_as_index=False)

            # independently fetch the raw flags to derive the expected mask
            conditions = {"submatrix": submatrix_id}
            con_i.bulk.add_column_filters(conditions, ["Time", "Coolant"], column_patterns_case_insensitive=False)
            flagged = con_i.bulk.query(conditions, load_flags=True)
            assert "flags" in flagged.columns

            df_filtered = con_i.bulk.data_read(
                submatrix_id, ["Time", "Coolant"], set_independent_as_index=False, valid_flag=True
            )

            assert list(df_filtered.columns) == list(df_plain.columns)
            for _, row in flagged.iterrows():
                name = row["name"]
                flags = np.asarray(row["flags"], dtype=int)
                if len(flags) != len(df_plain[name]):
                    # servers may return flags in the same compact form used for implicit/raw
                    # sequence representations; a constant sample set applies to every row
                    assert len(np.unique(flags)) == 1
                    flags = np.full(len(df_plain[name]), flags[0])
                invalid_mask = (flags & 15) != 0
                if invalid_mask.any():
                    assert df_filtered[name].isna().tolist() == invalid_mask.tolist()
                    assert df_filtered[name][~invalid_mask].tolist() == df_plain[name][~invalid_mask].tolist()
                else:
                    pd.testing.assert_series_equal(df_filtered[name], df_plain[name], check_names=False)


@pytest.mark.integration
def test_bulk_reader_valuematrix_read_valid_flag() -> None:
    with __create_con_i() as con_i:
        sm_s = con_i.query_data(
            {
                "AoSubmatrix": {"measurement.name": "Profile_52"},
                "$options": {"$rowlimit": 1},
                "$attributes": {"id": 1, "measurement.name": 1},
            }
        )

        assert sm_s.shape[0] <= 1
        if 1 == sm_s.shape[0]:
            submatrix_id = int(sm_s.iloc[0, 0])

            # data_read()'s masking is already verified independently above; use it as the
            # reference to confirm valuematrix_read() applies the same masking for valid_flag.
            df_data_read = con_i.bulk.data_read(
                submatrix_id, ["Time", "Coolant"], set_independent_as_index=False, valid_flag=True
            )
            df_valuematrix = con_i.bulk.valuematrix_read(submatrix_id, ["Time", "Coolant"], valid_flag=True)

            pd.testing.assert_frame_equal(df_data_read, df_valuematrix)
            assert df_valuematrix.attrs["unit_names"] == df_data_read.attrs["unit_names"]

            # plain read (no valid_flag) must still work and keep the same columns
            df_plain = con_i.bulk.valuematrix_read(submatrix_id, ["Time", "Coolant"])
            assert list(df_plain.columns) == list(df_valuematrix.columns)


@pytest.mark.integration
def test_bulk_reader_with_unit_names():
    with __create_con_i() as con_i:
        measurement = con_i.query_data(
            {
                "MeaResult": {
                    "Name": "Profile_62",
                    "TestStep.Test.Name": "Campaign_05",
                    "TestStep.Test.StructureLevel.Project.Name": "ElectricMotorTemperature",
                },
                "$attributes": {
                    "name": 1,
                    "id": 1,
                    "test": {
                        "name": 1,
                        "parent_test": {
                            "name": 1,
                            "parent_test": {"name": 1, "parent_test": {"name": 1}},
                        },
                    },
                },
            }
        )

        mea_dict = measurement.to_dict(orient="records")[0]
        mea_title = f"{mea_dict['Project.Name']} - {mea_dict['Test.Name']} - {mea_dict['MeaResult.Name']}"
        assert mea_title == "ElectricMotorTemperature - Campaign_05 - Profile_62"

        submatrices = con_i.query(
            {
                "AoSubMatrix": {"measurement": mea_dict["MeaResult.Id"]},
                "$attributes": {"id": 1, "number_of_rows": 1},
            }
        )

        mea_bulk = con_i.bulk.data_read(submatrices["id"].iloc[0])
        unit_names = mea_bulk.attrs.get("unit_names", {})
        assert unit_names.get("Motor_speed") == "rpm"
        assert unit_names.get("Torque") == "Nm"
        assert unit_names.get("Time") == "s"

"""utility to help loading local column values"""

from __future__ import annotations

from enum import IntEnum
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd

from odsbox.datamatrices_to_pandas import to_pandas
from odsbox.proto.ods_pb2 import ValueMatrixRequestStruct  # pylint: disable=E0611

if TYPE_CHECKING:
    from .con_i import ConI


class SeqRepEnum(IntEnum):
    """
    Enumeration for local column sequence representation types.
    They are defined in ASAM ODS base model and transported as integers.
    In the ASAM ODS standard they are defined lower case.
    """

    # pylint: disable=C0103
    explicit = 0
    implicit_constant = 1
    implicit_linear = 2
    implicit_saw = 3
    raw_linear = 4
    raw_polynomial = 5
    formula = 6  # deprecated
    external_component = 7
    raw_linear_external = 8
    raw_polynomial_external = 9
    raw_linear_calibrated = 10
    raw_linear_calibrated_external = 11
    raw_rational = 12
    raw_rational_external = 13
    # pylint: enable=C0103


class BulkReader:
    """
    BulkReader is a class for reading data in bulk from a ConI instance.
    It contains some convenient methods for querying and retrieving bulk data.

    Example::

        from odsbox.con_i import ConI

        with ConI(
            url="https://MYSERVER/api",
            auth=("USER", "PASSWORD"),
        ) as con_i:
            submatrix_id = 1234
            df = con_i.bulk.data_read(submatrix_id, ["Time", "Co*"])

    Remark: If the provided methods do not work for a client the source code can be used
    to create customer specific code to retrieve bulk data.
    """

    def __init__(self, con_i: ConI) -> None:
        """Initialize the BulkReader with a ConI instance."""
        self.__con_i = con_i

    @staticmethod
    def __apply_sequence_representation(
        localcolumn_df: pd.DataFrame,
        values_start: int = 0,
        values_limit: int = 0,
        calculate_raw: bool = True,
    ) -> None:
        """
        Apply sequence representation to the values in the DataFrame.
        This function modifies the 'values' column in place based on the sequence representation type.
        The dataframe is expected to have the following columns:
        - name: the name of the local column
        - values: the values of the local column
        - sequence_representation: the sequence representation type
        - generation_parameters: the generation parameters if raw types are used in sequence_representation
        - number_of_rows: maximal row count for the local column

        :param pd.DataFrame localcolumn_df: DataFrame containing local column metadata and bulk data.
                                            The dataframe is changed inplace.
        :param int values_start: zero based starting index for the values to be processed
        :param int values_limit: maximum number of values to be processed
        :param bool calculate_raw: whether to calculate raw values for certain sequence representations
        """
        for index, r in localcolumn_df.iterrows():
            name = r.get("name")
            if name is None:
                raise ValueError(f"Missing 'name' field for row at index {index}")
            vals = r.get("values")
            if vals is None:
                raise ValueError(f"Missing 'values' field for column '{name}' at index {index}")
            sequence_representation = int(r.get("sequence_representation", SeqRepEnum.explicit.value))
            number_of_rows = int(r.get("number_of_rows", 0))
            if values_start > number_of_rows:
                raise ValueError(
                    f"values_start {values_start} is greater than number_of_rows {number_of_rows} for column '{name}'"
                )
            values_count = (
                min(number_of_rows - values_start, values_limit) if values_limit > 0 else number_of_rows - values_start
            )

            if sequence_representation in [
                SeqRepEnum.explicit,
                SeqRepEnum.external_component,
            ]:
                pass  # explicit values are already correctly stored in vals
            elif sequence_representation == SeqRepEnum.implicit_constant:
                # generation parameters expected to be stored in vals as [offset, factor, ...]
                if len(vals) >= 1:
                    localcolumn_df.at[index, "values"] = [vals[0]] * values_count
                else:
                    raise ValueError(f"Generation parameters missing for {name}")
            elif sequence_representation == SeqRepEnum.implicit_linear:
                if len(vals) >= 2:
                    localcolumn_df.at[index, "values"] = [
                        vals[0] + x * vals[1] for x in range(0 + values_start, values_count + values_start)
                    ]
                else:
                    raise ValueError(f"Generation parameters missing for {name}")
            elif sequence_representation in [
                SeqRepEnum.raw_linear,
                SeqRepEnum.raw_linear_external,
            ]:
                if calculate_raw:
                    generation_parameters = r.get("generation_parameters")
                    if isinstance(generation_parameters, (list, tuple)) and len(generation_parameters) >= 2:
                        p1 = generation_parameters[0]
                        p2 = generation_parameters[1]
                        double_vals = np.array(vals, dtype=float)
                        localcolumn_df.at[index, "values"] = p1 + p2 * double_vals
                    else:
                        raise ValueError(f"Generation parameters missing for {name}")
            elif sequence_representation in [
                SeqRepEnum.raw_linear_calibrated,
                SeqRepEnum.raw_linear_calibrated_external,
            ]:
                if calculate_raw:
                    generation_parameters = r.get("generation_parameters")
                    if isinstance(generation_parameters, (list, tuple)) and len(generation_parameters) >= 3:
                        p1 = generation_parameters[0]
                        p2 = generation_parameters[1]
                        p3 = generation_parameters[2]
                        double_vals = np.array(vals, dtype=float)
                        localcolumn_df.at[index, "values"] = (p1 + p2 * double_vals) * p3
                    else:
                        raise ValueError(f"Generation parameters missing for {name}")
            elif sequence_representation in [
                SeqRepEnum.raw_rational,
                SeqRepEnum.raw_rational_external,
            ]:
                if calculate_raw:
                    generation_parameters = r.get("generation_parameters")
                    if isinstance(generation_parameters, (list, tuple)) and len(generation_parameters) >= 3:
                        p1 = generation_parameters[0]
                        p2 = generation_parameters[1]
                        p3 = generation_parameters[2]
                        p4 = generation_parameters[3]
                        p5 = generation_parameters[4]
                        p6 = generation_parameters[5]
                        double_vals = np.array(vals, dtype=float)
                        localcolumn_df.at[index, "values"] = (p1 * double_vals**2 + p2 * double_vals + p3) / (
                            p4 * double_vals**2 + p5 * double_vals + p6
                        )
                    else:
                        raise ValueError(f"Generation parameters missing for {name}")
            else:
                raise ValueError(
                    f"Unhandled sequence representation {SeqRepEnum(sequence_representation).name} for column '{name}'"
                )

    def query(
        self,
        localcolumn_jaquel_condition: dict,
        date_as_timestamp: bool = True,
        row_limit: int = 0,
        values_start: int = 0,
        values_limit: int = 0,
        calculate_raw: bool = True,
    ) -> pd.DataFrame:
        """
        Query bulk data for local columns based on the provided Jaquel query condition.
        This method can be used to retrieve local columns bulk data based on a Jaquel query.

        Example::

            from odsbox.con_i import ConI

            with ConI(
                url="https://MYSERVER/api",
                auth=("USER", "PASSWORD"),
            ) as con_i:
                conditions = {"submatrix.measurement.name": {"$like": "Profile_5?"}}
                con_i.bulk.add_column_filters(conditions, ["Time", "Coolant"], column_patterns_case_insensitive=False)
                df = con_i.bulk.query(conditions)

        :param dict localcolumn_jaquel_condition: Jaquel query condition for local columns.
                                                `{"AoLocalColumn": localcolumn_jaquel_condition}`
                                                is used to determine the local columns to load.
        :param bool date_as_timestamp: Whether to treat date columns as timestamps. This will convert ASAM ODS
                                        date columns to pandas datetime objects.
        :param int row_limit: Maximum number of rows to return. Can be used to avoid huge amount of local columns
                            to be returned.
        :param int values_start: Zero-based starting index for the values to be processed. Used for chunk loading.
        :param int values_limit: Maximum number of values to be retrieved in this chunk. 0 means all remaining values.
        :param bool calculate_raw: Whether to calculate raw values for certain raw sequence representations.
        :raises requests.HTTPError: If access fails.
        :return DataFrame: The Pandas.DataFrame contains the local_column.values as DataFrame column.
        """

        lc_meta_df: pd.DataFrame = self.__con_i.query_data(
            {
                "AoLocalColumn": localcolumn_jaquel_condition,
                "$attributes": {
                    "id": 1,
                    "name": 1,
                    "independent": 1,
                    "sequence_representation": 1,
                    "submatrix": 1,
                    "submatrix.number_of_rows": 1,
                },
                "$options": {"$rowlimit": row_limit},
            }
        )
        lc_meta_df.columns = [
            "id",
            "name",
            "independent",
            "sequence_representation",
            "submatrix",
            "number_of_rows",
        ]
        lc_meta_df.set_index("id", inplace=True)

        raw_seq_rep_values = {
            SeqRepEnum.raw_linear.value,
            SeqRepEnum.raw_polynomial.value,
            SeqRepEnum.raw_linear_external.value,
            SeqRepEnum.raw_polynomial_external.value,
            SeqRepEnum.raw_linear_calibrated.value,
            SeqRepEnum.raw_linear_calibrated_external.value,
        }
        contains_raw_seq_rep = lc_meta_df["sequence_representation"].dropna().astype(int).isin(raw_seq_rep_values).any()

        attributes = {
            "id": 1,
            "values": 1,
        }
        if contains_raw_seq_rep:
            attributes["generation_parameters"] = 1

        localcolumn_bulk_dms = self.__con_i.data_read_jaquel(
            {
                "AoLocalColumn": localcolumn_jaquel_condition,
                "$attributes": attributes,
                "$options": {
                    "$seqskip": values_start,
                    "$seqlimit": values_limit,
                    "$rowlimit": row_limit,
                },
            }
        )
        localcolumn_bulk_df = to_pandas(
            localcolumn_bulk_dms, date_as_timestamp=date_as_timestamp, prefer_np_array_for_unknown=True
        )
        localcolumn_bulk_dms = None  # free memory
        localcolumn_bulk_df.columns = [attr for attr in attributes]

        # merge metadata into bulk, preserving bulk order (left join)
        merged = localcolumn_bulk_df.merge(lc_meta_df, left_on="id", right_index=True, how="left")

        missing_meta_ids = merged[merged["name"].isna()]["id"].unique()
        if len(missing_meta_ids):
            raise KeyError(f"Missing metadata for ids: {sorted(missing_meta_ids)}")

        BulkReader.__apply_sequence_representation(
            merged,
            values_start=values_start,
            values_limit=values_limit,
            calculate_raw=calculate_raw,
        )

        # Reorder columns to put submatrix, name, id, values as first four columns
        desired_first_cols = ["submatrix", "name", "id", "values"]
        existing_cols = merged.columns.tolist()
        remaining_cols = [col for col in existing_cols if col not in desired_first_cols]
        new_column_order = desired_first_cols + remaining_cols
        merged = merged[new_column_order]

        return merged

    def data_read(
        self,
        submatrix_iid: int,
        column_patterns: list[str] | None = None,
        column_patterns_case_insensitive: bool = False,
        date_as_timestamp: bool = True,
        set_independent_as_index: bool = True,
        values_start: int = 0,
        values_limit: int = 0,
    ) -> pd.DataFrame:
        """
        Loads an ASAM ODS SubMatrix and returns it as a pandas DataFrame. The method uses the HTTP API method
        `data_read` to retrieve the data.

        Example::

            from odsbox.con_i import ConI

            with ConI(
                url="https://MYSERVER/api",
                auth=("USER", "PASSWORD"),
            ) as con_i:
                submatrix_id = 1234
                df = con_i.bulk.data_read(submatrix_id, ["Time", "Co*"])

        :param int submatrix_iid: The ID of the submatrix to load.
        :param list[str] | None column_patterns: List of column name patterns to filter the columns.
                                                 If None, all columns are loaded. `*?` is used as a wildcard.
        :param bool column_patterns_case_insensitive: Whether to treat column name patterns as case insensitive.
        :param bool date_as_timestamp: Whether to treat date columns as timestamps.
        :param bool set_independent_as_index: Whether to set the independent column as the index.
        :param int values_start: Zero-based starting index for the values to be processed. Used for chunk loading.
        :param int values_limit: Maximum number of values to be retrieved in this chunk. 0 means all remaining values.
        :raises requests.HTTPError: If access fails.
        :return DataFrame: The Pandas.DataFrame contains the local_column.values as DataFrame column.
        """

        conditions = {"submatrix": submatrix_iid}
        BulkReader.add_column_filters(conditions, column_patterns, column_patterns_case_insensitive)

        localcolumn_df = self.query(
            localcolumn_jaquel_condition=conditions,
            date_as_timestamp=date_as_timestamp,
            values_start=values_start,
            values_limit=values_limit,
        )

        # Create DataFrame from column data
        rv = pd.DataFrame({r["name"]: r["values"] for _, r in localcolumn_df.iterrows()})

        # Set independent column as index if requested
        if set_independent_as_index:
            independent_mask = localcolumn_df["independent"].fillna(False).astype(bool)
            if independent_mask.sum() == 1:
                independent_name = localcolumn_df.loc[independent_mask, "name"].iloc[0]
                rv.set_index(independent_name, inplace=True)

        return rv

    def valuematrix_read(
        self,
        submatrix_iid: int,
        column_patterns: list[str] | None = None,
        date_as_timestamp: bool = True,
        values_start: int = 0,
        values_limit: int = 0,
    ) -> pd.DataFrame:
        """
        Loads an ASAM ODS SubMatrix and returns it as a pandas DataFrame.

        Example::

            from odsbox.con_i import ConI

            with ConI(
                url="https://MYSERVER/api",
                auth=("USER", "PASSWORD"),
            ) as con_i:
                submatrix_id = 1234
                df = con_i.bulk.valuematrix_read(submatrix_id, ["Time", "Co*"])

        :param int submatrix_iid: The ID of the submatrix to load.
        :param list[str] | None column_patterns: List of column name patterns to filter the columns.
                                                 If None, all columns are loaded. `*?` is used as a wildcard.
        :param bool date_as_timestamp: Whether to treat date columns as timestamps.
        :param int values_start: Zero-based starting index for the values to be processed. Used for chunk loading.
        :param int values_limit: Maximum number of values to be retrieved in this chunk. 0 means all remaining values.
        :raises requests.HTTPError: If access fails.
        :return DataFrame: The Pandas.DataFrame contains the local_column.values as DataFrame column.
        """
        sm_e = self.__con_i.mc.entity_by_base_name("AoSubmatrix")
        lc_e = self.__con_i.mc.entity_by_base_name("AoLocalColumn")
        name_patterns = column_patterns or ["*"]

        df = to_pandas(
            self.__con_i.valuematrix_read(
                ValueMatrixRequestStruct(
                    aid=sm_e.aid,
                    iid=submatrix_iid,
                    columns=[ValueMatrixRequestStruct.ColumnItem(name=name_pattern) for name_pattern in name_patterns],
                    attributes=[
                        self.__con_i.mc.attribute_by_base_name(lc_e, "name").name,
                        self.__con_i.mc.attribute_by_base_name(lc_e, "values").name,
                    ],
                    mode=ValueMatrixRequestStruct.ModeEnum.MO_CALCULATED,
                    values_start=values_start,
                    values_limit=values_limit,
                )
            ),
            date_as_timestamp=date_as_timestamp,
            prefer_np_array_for_unknown=True,
        )
        df.columns = ["name", "values"]
        return pd.DataFrame({name: values for name, values in zip(df["name"].values, df["values"].values)})

    @staticmethod
    def add_column_filters(
        conditions: dict,
        column_patterns: list[str] | None,
        column_patterns_case_insensitive: bool,
    ) -> None:
        """
        Add filter conditions for AoLocalColumn to match column patterns. This is just a helper method
        to create the Jaquel conditions for local columns names based on the column patterns.

        :param dict conditions: The conditions dictionary to update.
        :param list[str] | None column_patterns: List of column name patterns to filter the columns.
                                                Wildcards `*` and `?` are supported.
        :param bool column_patterns_case_insensitive: Whether to treat column name patterns as case insensitive.
        """
        if not column_patterns:
            return

        inset_names = []
        like_names = []
        for p in column_patterns:
            if not p or p == "*":
                continue
            if "*" in p or "?" in p:
                like_names.append(p)
            else:
                inset_names.append(p)

        if not (inset_names or like_names):
            return

        opt = {"$options": "i"} if column_patterns_case_insensitive else {}

        clauses = []
        if inset_names:
            clauses.append({"name": {"$in": inset_names, **opt}})
        for pattern in like_names:
            clauses.append({"name": {"$like": pattern, **opt}})

        if len(clauses) == 1:
            conditions.update(clauses[0])
        else:
            conditions["$or"] = clauses

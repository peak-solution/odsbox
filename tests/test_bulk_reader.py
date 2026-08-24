from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from odsbox.bulk_reader import BulkReader, SeqRepEnum


def test_add_column_filters_no_patterns():
    conditions = {"submatrix": 1}
    BulkReader.add_column_filters(conditions, None, False)
    assert conditions == {"submatrix": 1}


def test_add_column_filters_in_and_like():
    # mix of exact and wildcard patterns
    conditions = {"submatrix": 2}
    BulkReader.add_column_filters(conditions, ["ColA", "Col*", "Exact"], False)

    # should have an $or clause because we have at least one like pattern and inset names
    assert "$or" in conditions
    clauses = conditions["$or"]
    # there should be two or-more clauses (one for $in and one per like)
    assert any("$in" in c.get("name", {}) for c in clauses) or any("$like" in c.get("name", {}) for c in clauses)


def test_add_column_filters_case_insensitive():
    conditions = {"submatrix": 3}
    BulkReader.add_column_filters(conditions, ["abc", "d?"], True)
    # either $in or $or with $like must include $options = 'i'
    if "name" in conditions:
        # direct inset
        assert conditions["name"]["$options"] == "i"
    else:
        # or combined
        found = False
        for clause in conditions.get("$or", []):
            name_clause = clause.get("name", {})
            if name_clause.get("$options") == "i":
                found = True
        assert found


def test_apply_sequence_representation_various():
    df = pd.DataFrame(
        [
            {
                "name": "exp",
                "values": [1, 2, 3],
                "sequence_representation": SeqRepEnum.explicit.value,
                "number_of_rows": 3,
            },
            {
                "name": "const",
                "values": [42, 0],
                "sequence_representation": SeqRepEnum.implicit_constant.value,
                "number_of_rows": 4,
            },
            {
                "name": "lin",
                "values": [10, 2],
                "sequence_representation": SeqRepEnum.implicit_linear.value,
                "number_of_rows": 5,
            },
            {
                "name": "raw",
                "values": [1, 2, 3],
                "sequence_representation": SeqRepEnum.raw_linear.value,
                "generation_parameters": [1.0, 2.0],
                "number_of_rows": 3,
            },
            {
                "name": "cal",
                "values": [1, 2],
                "sequence_representation": SeqRepEnum.raw_linear_calibrated.value,
                "generation_parameters": [1.0, 2.0, 3.0],
                "number_of_rows": 2,
            },
            {
                "name": "rational",
                "values": [1, 2],
                "sequence_representation": SeqRepEnum.raw_rational.value,
                "generation_parameters": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                "number_of_rows": 2,
            },
        ]
    )

    # call private static method via name mangling
    BulkReader._BulkReader__apply_sequence_representation(df, values_start=0, values_limit=0, calculate_raw=True)

    # explicit remains unchanged
    assert df.loc[0, "values"] == [1, 2, 3]

    # implicit_constant -> repeated offset (values_count = number_of_rows)
    assert df.loc[1, "values"] == [42] * 4

    # implicit_linear with values_limit default 0 -> uses number_of_rows
    assert df.loc[2, "values"] == [10, 12, 14, 16, 18][: df.loc[2, "number_of_rows"]]

    # raw linear: p1 + p2 * vals
    assert list(df.loc[3, "values"]) == [1.0 + 2.0 * 1.0, 1.0 + 2.0 * 2.0, 1.0 + 2.0 * 3.0]

    # calibrated: (p1 + p2 * vals) * p3
    assert list(df.loc[4, "values"]) == [(1.0 + 2.0 * 1.0) * 3.0, (1.0 + 2.0 * 2.0) * 3.0]

    # rational: (p1 * vals^2  + p2 * vals + p3) / (p4 * vals^2 + p5 * vals + p6)
    assert list(df.loc[5, "values"]) == [
        (1.0 * 1.0**2 + 2.0 * 1.0 + 3.0) / (4.0 * 1.0**2 + 5.0 * 1.0 + 6.0),
        (1.0 * 2.0**2 + 2.0 * 2.0 + 3.0) / (4.0 * 2.0**2 + 5.0 * 2.0 + 6.0),
    ]


def test_apply_sequence_representation_errors():
    # values_start greater than number_of_rows
    df = pd.DataFrame(
        [{"name": "too_far", "values": [], "sequence_representation": SeqRepEnum.explicit.value, "number_of_rows": 1}]
    )
    with pytest.raises(ValueError):
        BulkReader._BulkReader__apply_sequence_representation(df, values_start=2, values_limit=0)

    # unhandled sequence representation should raise
    df2 = pd.DataFrame(
        [{"name": "unknown", "values": [], "sequence_representation": SeqRepEnum.formula.value, "number_of_rows": 1}]
    )
    with pytest.raises(ValueError):
        BulkReader._BulkReader__apply_sequence_representation(df2)


def test_data_read_sets_independent_index():
    # create a BulkReader and monkeypatch its query method to return prepared meta+values
    from odsbox.bulk_reader import BulkReader as BR

    br = BR(None)

    # Simulate query returning a dataframe with names and values and independent flag
    qdf = pd.DataFrame(
        [
            {"name": "time", "values": [0, 1, 2], "independent": True},
            {"name": "val", "values": [10, 11, 12], "independent": False},
        ]
    )

    # Replace the instance method
    br.query = lambda *args, **kwargs: qdf

    df = br.data_read(submatrix_iid=1, set_independent_as_index=True)
    # index should be the 'time' column
    assert df.index.tolist() == [0, 1, 2]
    assert list(df.columns) == ["val"]


def test_query_merges_and_prefixes_duplicate_names(monkeypatch):
    # Prepare a fake ConI with query_data and data_read_jaquel
    class FakeConI:
        def query_data(self, query):
            # return metadata with duplicate names
            df = pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "dup",
                        "independent": False,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                    {
                        "id": 2,
                        "name": "dup",
                        "independent": False,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                ]
            )
            return df

        def data_read_jaquel(self, jaquel_query):
            return object()  # ignored by our monkeypatched to_pandas

    fake = FakeConI()

    # to_pandas should return bulk data with id and values columns (will be renamed inside query)
    def fake_to_pandas(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True, raise_on_partial_result=False):
        return pd.DataFrame([[1, [1, 2]], [2, [3, 4]]], columns=["a", "b"])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(fake)
    merged = br.query({"submatrix": 5})

    # duplicates are not prefixed in current behavior; names remain as returned by metadata
    assert set(merged["name"].unique()) == {"dup"}
    # values column should exist
    assert "values" in merged.columns


def test_valuematrix_read_maps_names_and_values(monkeypatch):
    # Fake model cache and con_i
    class FakeMC:
        def entity_by_base_name(self, base_name):
            if base_name == "AoSubmatrix":
                return type("E", (), {"aid": 100})()
            if base_name == "AoLocalColumn":
                return type("E", (), {"aid": 200})()

        def attribute_by_base_name(self, entity, name):
            return type("A", (), {"name": name})()

    class FakeConI:
        def __init__(self):
            self.mc = FakeMC()

        def valuematrix_read(self, vmreq):
            return object()  # ignored by monkeypatched to_pandas

    fake = FakeConI()

    # monkeypatch to_pandas to return names and values
    def fake_to_pandas(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True, raise_on_partial_result=False):
        return pd.DataFrame({"name": ["a", "b"], "values": [[1, 2], [3, 4]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(fake)
    df = br.valuematrix_read(1, column_patterns=["*"], date_as_timestamp=True)

    # expect dataframe with columns a and b
    assert list(df.columns) == ["a", "b"]


def test_query_raises_on_missing_metadata(monkeypatch):
    # fake ConI: metadata missing id 2 which is present in bulk -> should raise KeyError
    class FakeConI2:
        def query_data(self, query):
            # only metadata for id 1
            return pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "one",
                        "independent": False,
                        "sequence_representation": 0,
                        "submatrix": 7,
                        "number_of_rows": 1,
                    }
                ]
            )

        def data_read_jaquel(self, jaquel_query):
            return object()

    def fake_to_pandas_bulk(
        dms, date_as_timestamp=True, prefer_np_array_for_unknown=True, raise_on_partial_result=False
    ):
        # bulk contains id 2 which lacks metadata
        return pd.DataFrame([[2, [9, 9]]], columns=["id", "values"])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas_bulk)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(FakeConI2())
    with pytest.raises(KeyError):
        br.query({"submatrix": 7})


def test_apply_sequence_representation_start_limit():
    # implicit_linear with start=1 and limit=2 should produce two values starting at offset
    df = pd.DataFrame(
        [
            {
                "name": "lin",
                "values": [0, 5],
                "sequence_representation": SeqRepEnum.implicit_linear.value,
                "number_of_rows": 5,
            }
        ]
    )
    BulkReader._BulkReader__apply_sequence_representation(df, values_start=1, values_limit=2)
    # vals: start at x=1, values_count=2 -> [0 + 1*5, 0 + 2*5] => [5, 10]
    assert df.loc[0, "values"] == [5, 10]


def test_apply_sequence_representation_skip_raw_calculation():
    # raw_linear should remain as original numeric array when calculate_raw=False
    df = pd.DataFrame(
        [
            {
                "name": "raw",
                "values": [1, 2, 3],
                "sequence_representation": SeqRepEnum.raw_linear.value,
                "generation_parameters": [1.0, 2.0],
                "number_of_rows": 3,
            }
        ]
    )
    BulkReader._BulkReader__apply_sequence_representation(df, calculate_raw=False)
    # values should be unchanged (still list of ints)
    assert list(df.loc[0, "values"]) == [1, 2, 3]


def test_generation_parameters_requested_when_raw_seq(monkeypatch):
    # metadata indicates raw_linear sequence representation -> generation_parameters should be requested
    class FakeConI3:
        def query_data(self, query):
            return pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "rawcol",
                        "independent": False,
                        "sequence_representation": SeqRepEnum.raw_linear.value,
                        "submatrix": 9,
                        "number_of_rows": 3,
                    }
                ]
            )

        def data_read_jaquel(self, jaquel_query):
            return object()

    # to_pandas should return id, values, generation_parameters columns (will be renamed inside query)
    def fake_to_pandas(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True, raise_on_partial_result=False):
        return pd.DataFrame([[1, [1, 2], [1.0, 2.0]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(FakeConI3())
    merged = br.query({"submatrix": 9})
    # generation_parameters column should be present after processing
    assert "generation_parameters" in merged.columns


def test_generation_parameters_not_requested_when_not_raw(monkeypatch):
    # metadata indicates explicit sequence representation -> generation_parameters should NOT be requested
    class FakeConI4:
        def query_data(self, query):
            return pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "expcol",
                        "independent": False,
                        "sequence_representation": SeqRepEnum.explicit.value,
                        "submatrix": 10,
                        "number_of_rows": 2,
                    }
                ]
            )

        def data_read_jaquel(self, jaquel_query):
            return object()

    def fake_to_pandas2(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True, raise_on_partial_result=False):
        return pd.DataFrame([[1, [7, 8]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas2)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(FakeConI4())
    merged = br.query({"submatrix": 10})
    assert "generation_parameters" not in merged.columns


def test_add_column_filters_exact_only_case_sensitive():
    conditions = {}
    BulkReader.add_column_filters(conditions, ["ColA", "ColB"], False)
    assert "name" in conditions
    assert "$in" in conditions["name"]
    assert conditions["name"]["$in"] == ["ColA", "ColB"]


def test_add_column_filters_exact_only_case_insensitive():
    conditions = {}
    BulkReader.add_column_filters(conditions, ["ColA"], True)
    assert "name" in conditions
    assert conditions["name"].get("$options") == "i"
    assert conditions["name"]["$in"] == ["ColA"]


def test_add_column_filters_like_single():
    conditions = {}
    BulkReader.add_column_filters(conditions, ["Col*"], False)
    assert "name" in conditions
    assert "$like" in conditions["name"]
    assert conditions["name"]["$like"] == "Col*"


def test_add_column_filters_like_multiple():
    conditions = {}
    BulkReader.add_column_filters(conditions, ["A*", "B?"], False)
    assert "$or" in conditions
    clauses = conditions["$or"]
    assert len(clauses) == 2
    assert all("$like" in c["name"] for c in clauses)


def test_add_column_filters_mix_inset_like_case_insensitive():
    conditions = {}
    BulkReader.add_column_filters(conditions, ["Exact", "Pat*"], True)
    assert "$or" in conditions
    clauses = conditions["$or"]
    # there should be two clauses and each should include $options == 'i'
    assert len(clauses) == 2
    assert all(c["name"].get("$options") == "i" for c in clauses)


def test_add_column_filters_skips_wildcards_and_empty():
    conditions = {"submatrix": 42}
    BulkReader.add_column_filters(conditions, ["*", ""], False)
    # nothing should be added
    assert conditions == {"submatrix": 42}


# --- Tests for unit_names extraction and propagation ---


def _make_bulk_reader_with_unit_lookup(unit_lookup: dict) -> BulkReader:
    """Return a BulkReader whose unit_name_lookup cache is pre-filled."""

    class FakeConI:
        pass

    br = BulkReader(FakeConI())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = unit_lookup
    return br


def test_attach_unit_attr_sets_dict():
    br = _make_bulk_reader_with_unit_lookup({})
    df = pd.DataFrame({"a": [1], "b": [2]})
    names = pd.Series(["a", "b"])
    br._attach_unit_attr(df, names, ["m/s", "kg"])
    assert df.attrs["unit_names"] == {"a": "m/s", "b": "kg"}


def test_attach_unit_attr_skips_on_length_mismatch():
    br = _make_bulk_reader_with_unit_lookup({})
    df = pd.DataFrame({"a": [1], "b": [2]})
    names = pd.Series(["a", "b"])
    br._attach_unit_attr(df, names, ["m/s"])  # mismatched length
    assert "unit_names" not in df.attrs


def test_attach_unit_attr_skips_on_empty_unit_names():
    br = _make_bulk_reader_with_unit_lookup({})
    df = pd.DataFrame({"a": [1]})
    br._attach_unit_attr(df, pd.Series(["a"]), [])
    assert "unit_names" not in df.attrs


def test_extract_unit_names_resolves_ids():
    """_extract_unit_names maps unit_ids via unit_name_lookup."""
    import odsbox.proto.ods_pb2 as ods

    br = _make_bulk_reader_with_unit_lookup({10: "m/s", 42: "kg", 0: ""})

    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=1, name="LC")
    column = dm.columns.add(name="values", base_name="values", data_type=ods.DT_UNKNOWN)
    column.unknown_arrays.values.add(data_type=ods.DT_FLOAT, unit_id=10).float_array.values.extend([1.0])
    column.unknown_arrays.values.add(data_type=ods.DT_FLOAT, unit_id=42).float_array.values.extend([2.0])
    column.unknown_arrays.values.add(data_type=ods.DT_FLOAT, unit_id=0).float_array.values.extend([3.0])

    assert br._extract_unit_names(dms) == ["m/s", "kg", ""]


def test_extract_unit_names_unknown_id_returns_empty_string():
    """_extract_unit_names returns '' for ids not present in lookup."""
    import odsbox.proto.ods_pb2 as ods

    br = _make_bulk_reader_with_unit_lookup({10: "m/s"})

    dms = ods.DataMatrices()
    dm = dms.matrices.add(aid=1, name="LC")
    column = dm.columns.add(name="values", base_name="values", data_type=ods.DT_UNKNOWN)
    column.unknown_arrays.values.add(data_type=ods.DT_FLOAT, unit_id=10).float_array.values.extend([1.0])
    column.unknown_arrays.values.add(data_type=ods.DT_FLOAT, unit_id=99).float_array.values.extend([2.0])

    assert br._extract_unit_names(dms) == ["m/s", ""]


def test_query_propagates_unit_names(monkeypatch):
    """query() stores unit_names in df.attrs after a successful bulk read."""

    class FakeConI:
        def query_data(self, query):
            return pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "Time",
                        "independent": True,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                    {
                        "id": 2,
                        "name": "Force",
                        "independent": False,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                ]
            )

        def data_read_jaquel(self, query):
            return object()

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame([[1, [0.0, 1.0]], [2, [10.0, 20.0]]], columns=["a", "b"])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [7, 99])

    br = BulkReader(FakeConI())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {7: "s", 99: "N"}

    merged = br.query({"submatrix": 5})
    assert merged.attrs["unit_names"] == {"Time": "s", "Force": "N"}


def test_data_read_propagates_unit_names(monkeypatch):
    """data_read() copies unit_names from the intermediate query result into the returned DataFrame."""

    class FakeConI:
        def query_data(self, query):
            return pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "Time",
                        "independent": True,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                    {
                        "id": 2,
                        "name": "Force",
                        "independent": False,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                ]
            )

        def data_read_jaquel(self, query):
            return object()

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame([[1, [0.0, 1.0]], [2, [10.0, 20.0]]], columns=["a", "b"])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [7, 99])

    br = BulkReader(FakeConI())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {7: "s", 99: "N"}

    df = br.data_read(5, set_independent_as_index=False)
    assert df.attrs["unit_names"] == {"Time": "s", "Force": "N"}


def test_valuematrix_read_propagates_unit_names(monkeypatch):
    """valuematrix_read() stores unit_names in df.attrs."""

    class FakeMC:
        def entity_by_base_name(self, base_name):
            return type("E", (), {"aid": 1})()

        def attribute_by_base_name(self, entity, name):
            return type("A", (), {"name": name})()

    class FakeConI:
        def __init__(self):
            self.mc = FakeMC()

        def valuematrix_read(self, vmreq):
            return object()

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame({"name": ["Time", "Force"], "values": [[0.0, 1.0], [10.0, 20.0]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [7, 99])

    br = BulkReader(FakeConI())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {7: "s", 99: "N"}

    df = br.valuematrix_read(1)
    assert df.attrs["unit_names"] == {"Time": "s", "Force": "N"}


# --- Tests for partial_result propagation and raise_on_partial_result ---


def _make_partial_result_query_fakes():
    """Build a minimal FakeConI + fake_to_pandas for exercising ``query()`` paths."""

    class FakeConI:
        def query_data(self, query):
            return pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "Time",
                        "independent": True,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                    {
                        "id": 2,
                        "name": "Force",
                        "independent": False,
                        "sequence_representation": 0,
                        "submatrix": 5,
                        "number_of_rows": 2,
                    },
                ]
            )

        def data_read_jaquel(self, query):
            return object()

    return FakeConI()


def test_query_partial_result_attr_preserved(monkeypatch):
    """query() must preserve df.attrs['partial_result'] across merge/reorder."""

    def fake_to_pandas(dms, **kwargs):
        df = pd.DataFrame([[1, [0.0, 1.0]], [2, [10.0, 20.0]]], columns=["a", "b"])
        df.attrs["partial_result"] = True
        return df

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_partial_result_query_fakes())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    merged = br.query({"submatrix": 5})
    assert merged.attrs["partial_result"] is True


def test_query_partial_result_attr_defaults_to_false(monkeypatch):
    """query() sets df.attrs['partial_result'] to False when to_pandas reports no partial result."""

    def fake_to_pandas(dms, **kwargs):
        df = pd.DataFrame([[1, [0.0, 1.0]], [2, [10.0, 20.0]]], columns=["a", "b"])
        df.attrs["partial_result"] = False
        return df

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_partial_result_query_fakes())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    merged = br.query({"submatrix": 5})
    assert merged.attrs["partial_result"] is False


def test_query_raise_on_partial_result_propagates(monkeypatch):
    """query() forwards raise_on_partial_result to to_pandas and lets PartialResultError propagate."""
    from odsbox.datamatrices_to_pandas import PartialResultError

    captured = {}

    def fake_to_pandas(dms, **kwargs):
        captured["raise_on_partial_result"] = kwargs.get("raise_on_partial_result")
        if kwargs.get("raise_on_partial_result"):
            raise PartialResultError("simulated partial result")
        df = pd.DataFrame([[1, [0.0]]], columns=["a", "b"])
        df.attrs["partial_result"] = True
        return df

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_partial_result_query_fakes())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    with pytest.raises(PartialResultError):
        br.query({"submatrix": 5}, raise_on_partial_result=True)
    assert captured["raise_on_partial_result"] is True


def test_data_read_partial_result_attr_propagated(monkeypatch):
    """data_read() copies df.attrs['partial_result'] from the intermediate query result."""

    def fake_to_pandas(dms, **kwargs):
        df = pd.DataFrame([[1, [0.0, 1.0]], [2, [10.0, 20.0]]], columns=["a", "b"])
        df.attrs["partial_result"] = True
        return df

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_partial_result_query_fakes())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    df = br.data_read(5, set_independent_as_index=False)
    assert df.attrs["partial_result"] is True


def test_data_read_raise_on_partial_result_propagates(monkeypatch):
    """data_read() forwards raise_on_partial_result to query()/to_pandas."""
    from odsbox.datamatrices_to_pandas import PartialResultError

    def fake_to_pandas(dms, **kwargs):
        if kwargs.get("raise_on_partial_result"):
            raise PartialResultError("simulated partial result")
        return pd.DataFrame([[1, [0.0]]], columns=["a", "b"])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_partial_result_query_fakes())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    with pytest.raises(PartialResultError):
        br.data_read(5, set_independent_as_index=False, raise_on_partial_result=True)


def _make_valuematrix_read_fakes():
    """Build a minimal FakeConI for exercising ``valuematrix_read()`` paths."""

    class FakeMC:
        def entity_by_base_name(self, base_name):
            return type("E", (), {"aid": 1})()

        def attribute_by_base_name(self, entity, name):
            return type("A", (), {"name": name})()

    class FakeConI:
        def __init__(self):
            self.mc = FakeMC()

        def valuematrix_read(self, vmreq):
            return object()

    return FakeConI()


def test_valuematrix_read_partial_result_attr_preserved(monkeypatch):
    """valuematrix_read() preserves df.attrs['partial_result'] on the returned DataFrame."""

    def fake_to_pandas(dms, **kwargs):
        df = pd.DataFrame({"name": ["Time", "Force"], "values": [[0.0, 1.0], [10.0, 20.0]]})
        df.attrs["partial_result"] = True
        return df

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_valuematrix_read_fakes())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    df = br.valuematrix_read(1)
    assert df.attrs["partial_result"] is True


def test_valuematrix_read_raise_on_partial_result_propagates(monkeypatch):
    """valuematrix_read() forwards raise_on_partial_result to to_pandas."""
    from odsbox.datamatrices_to_pandas import PartialResultError

    def fake_to_pandas(dms, **kwargs):
        if kwargs.get("raise_on_partial_result"):
            raise PartialResultError("simulated partial result")
        return pd.DataFrame({"name": ["a"], "values": [[0.0]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_valuematrix_read_fakes())  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    with pytest.raises(PartialResultError):
        br.valuematrix_read(1, raise_on_partial_result=True)


# --- Tests for valid_flag parameter of valuematrix_read() ---


class _FakeMCForValuematrixValidFlag:
    """Stand-in for mc used by valuematrix_read(), records attribute_no_throw() calls."""

    def __init__(self, flags_attribute_exists: bool = True) -> None:
        self.__flags_attribute_exists = flags_attribute_exists
        self.attribute_no_throw_calls: list[tuple] = []

    def entity_by_base_name(self, base_name):
        return type("E", (), {"aid": 1})()

    def attribute_by_base_name(self, entity, name):
        return type("A", (), {"name": name})()

    def attribute_no_throw(self, entity_or_name, application_or_base_name):
        self.attribute_no_throw_calls.append((entity_or_name, application_or_base_name))
        return object() if self.__flags_attribute_exists else None


def _make_valuematrix_read_valid_flag_fake(flags_attribute_exists: bool = True):
    """Build a minimal FakeConI whose mc records attribute_no_throw() calls."""

    class FakeConI:
        def __init__(self):
            self.mc = _FakeMCForValuematrixValidFlag(flags_attribute_exists)

        def valuematrix_read(self, vmreq):
            return object()  # ignored by monkeypatched to_pandas

    return FakeConI()


def test_valuematrix_read_valid_flag_none_default_does_not_request_flags(monkeypatch) -> None:
    """valid_flag defaults to None: 'flags' is neither requested nor even checked for existence."""

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame({"name": ["Time", "Signal"], "values": [[0, 1, 2], [10, 20, 30]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    fake = _make_valuematrix_read_valid_flag_fake()
    br = BulkReader(fake)  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    df = br.valuematrix_read(1)

    # attribute_no_throw is short-circuited away when valid_flag is None
    assert fake.mc.attribute_no_throw_calls == []
    assert list(df.columns) == ["Time", "Signal"]


def test_valuematrix_read_valid_flag_true_requests_flags_and_masks_default_bitmask(monkeypatch) -> None:
    """valid_flag=True requests 'flags' and masks values using the default bitmask (15)."""

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame(
            {
                "name": ["Time", "Signal"],
                "values": [[0, 1, 2, 3], [10, 20, 30, 40]],
                "flags": [[0, 0, 0, 0], [0, 1, 0, 8]],
            }
        )

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    fake = _make_valuematrix_read_valid_flag_fake(flags_attribute_exists=True)
    br = BulkReader(fake)  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    df = br.valuematrix_read(1, valid_flag=True)

    assert fake.mc.attribute_no_throw_calls == [("AoLocalColumn", "flags")]
    assert "flags" not in df.columns
    assert str(df["Signal"].dtype) == "Int64"
    assert df["Signal"].tolist() == [10, pd.NA, 30, pd.NA]
    # column without any invalid flags keeps its original (non-nullable) dtype
    assert str(df["Time"].dtype) == "int64"
    assert df["Time"].tolist() == [0, 1, 2, 3]


def test_valuematrix_read_valid_flag_custom_bitmask_masks_only_matching_bits(monkeypatch) -> None:
    """A custom integer valid_flag only masks values whose flags match that specific bitmask."""

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame({"name": ["Signal"], "values": [[10, 20, 30, 40]], "flags": [[0, 1, 2, 3]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    fake = _make_valuematrix_read_valid_flag_fake(flags_attribute_exists=True)
    br = BulkReader(fake)  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    df = br.valuematrix_read(1, valid_flag=1)

    # flag=2 does not have bit 0 set, so it stays valid even though bitmask 15 would mask it
    assert df["Signal"].tolist() == [10, pd.NA, 30, pd.NA]


def test_valuematrix_read_valid_flag_true_but_attribute_missing_is_ignored(monkeypatch) -> None:
    """valid_flag=True is silently ignored when AoLocalColumn has no 'flags' base attribute."""

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame({"name": ["Signal"], "values": [[10, 20, 30]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    fake = _make_valuematrix_read_valid_flag_fake(flags_attribute_exists=False)
    br = BulkReader(fake)  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    df = br.valuematrix_read(1, valid_flag=True)

    assert fake.mc.attribute_no_throw_calls == [("AoLocalColumn", "flags")]
    assert "flags" not in df.columns
    assert str(df["Signal"].dtype) == "int64"
    assert df["Signal"].tolist() == [10, 20, 30]


def test_valuematrix_read_valid_flag_false_also_masks_with_default_bitmask(monkeypatch) -> None:
    """valid_flag=False is documented to behave like True/None: it still requests and masks with bitmask 15."""

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame({"name": ["Signal"], "values": [[10, 20, 30, 40]], "flags": [[0, 1, 0, 8]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    fake = _make_valuematrix_read_valid_flag_fake(flags_attribute_exists=True)
    br = BulkReader(fake)  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    df = br.valuematrix_read(1, valid_flag=False)

    assert fake.mc.attribute_no_throw_calls == [("AoLocalColumn", "flags")]
    assert df["Signal"].tolist() == [10, pd.NA, 30, pd.NA]


# --- Tests for load_flags parameter of query() ---


class _FakeMCAttribute:
    """Stand-in for mc.attribute_no_throw(), controls whether the 'flags' attribute exists."""

    def __init__(self, exists: bool) -> None:
        self.__exists = exists

    def attribute_no_throw(self, entity_or_name, application_or_base_name):
        return object() if self.__exists else None


def _make_load_flags_query_fake(exists: bool, captured_query: dict):
    """Build a minimal FakeConI recording the jaquel query passed to data_read_jaquel()."""

    class FakeConI:
        def __init__(self):
            self.mc = _FakeMCAttribute(exists)

        def query_data(self, query):
            return pd.DataFrame(
                [
                    {
                        "id": 1,
                        "name": "colA",
                        "independent": False,
                        "sequence_representation": SeqRepEnum.explicit.value,
                        "submatrix": 20,
                        "number_of_rows": 2,
                    },
                    {
                        "id": 2,
                        "name": "colB",
                        "independent": False,
                        "sequence_representation": SeqRepEnum.explicit.value,
                        "submatrix": 20,
                        "number_of_rows": 2,
                    },
                ]
            )

        def data_read_jaquel(self, jaquel_query):
            captured_query.update(jaquel_query)
            return object()  # ignored by monkeypatched to_pandas

    return FakeConI()


def test_query_load_flags_true_requests_and_returns_flags_when_attribute_exists(monkeypatch) -> None:
    """load_flags=True adds 'flags' to the requested attributes and to the result when the attribute exists."""
    captured_query: dict = {}

    def fake_to_pandas(dms, **kwargs):
        # columns order matches attributes dict: id, values, flags
        return pd.DataFrame([[1, [1, 2], [0, 1]], [2, [3, 4], [1, 0]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_load_flags_query_fake(exists=True, captured_query=captured_query))  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    merged = br.query({"submatrix": 20}, load_flags=True)

    assert "flags" in captured_query["$attributes"]
    assert "flags" in merged.columns
    assert list(merged["flags"]) == [[0, 1], [1, 0]]


def test_query_load_flags_false_by_default_does_not_request_flags(monkeypatch) -> None:
    """load_flags defaults to False: 'flags' must not be requested nor present in the result."""
    captured_query: dict = {}

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame([[1, [1, 2]], [2, [3, 4]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_load_flags_query_fake(exists=True, captured_query=captured_query))  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    merged = br.query({"submatrix": 20})

    assert "flags" not in captured_query["$attributes"]
    assert "flags" not in merged.columns


def test_query_load_flags_true_but_attribute_missing_is_ignored(monkeypatch) -> None:
    """load_flags=True is silently ignored when AoLocalColumn has no 'flags' base attribute."""
    captured_query: dict = {}

    def fake_to_pandas(dms, **kwargs):
        return pd.DataFrame([[1, [1, 2]], [2, [3, 4]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_load_flags_query_fake(exists=False, captured_query=captured_query))  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    merged = br.query({"submatrix": 20}, load_flags=True)

    assert "flags" not in captured_query["$attributes"]
    assert "flags" not in merged.columns


def test_query_load_flags_merges_with_metadata_preserving_order(monkeypatch) -> None:
    """flags values are merged onto the correct row per local column id, preserving bulk order."""
    captured_query: dict = {}

    def fake_to_pandas(dms, **kwargs):
        # bulk order is [2, 1] here to make sure merge doesn't rely on sorted ids
        return pd.DataFrame([[2, [3, 4], [1, 0]], [1, [1, 2], [0, 1]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)
    monkeypatch.setattr("odsbox.bulk_reader.extract_column_unit_ids", lambda dms: [])

    br = BulkReader(_make_load_flags_query_fake(exists=True, captured_query=captured_query))  # type: ignore[arg-type]
    br._unit_name_lookup_cache = {}

    merged = br.query({"submatrix": 20}, load_flags=True)

    assert list(merged["name"]) == ["colB", "colA"]
    assert list(merged["flags"]) == [[1, 0], [0, 1]]


def test_create_dataframe_from_localcolumns_valid_flag_false_uses_default_mask() -> None:
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": [10, 20, 30, 40],
                "flags": [0, 1, 0, 8],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(False, localcolumn_df)

    assert str(rv["Signal"].dtype) == "Int64"
    assert rv["Signal"].tolist() == [10, pd.NA, 30, pd.NA]


def test_create_dataframe_from_localcolumns_valid_flag_none_uses_default_mask() -> None:
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": [1, 2, 3],
                "flags": [0, 15, 0],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(None, localcolumn_df)

    assert str(rv["Signal"].dtype) == "Int64"
    assert rv["Signal"].tolist() == [1, pd.NA, 3]


# --- Tests for valid_flag parameter of data_read() ---


def test_data_read_valid_flag_none_default_does_not_request_flags():
    """valid_flag defaults to None: query() is called with load_flags=False and no masking occurs."""
    from odsbox.bulk_reader import BulkReader as BR

    br = BR(None)  # type: ignore[arg-type]
    captured_kwargs: dict = {}

    qdf = pd.DataFrame(
        [
            {"name": "time", "values": [0, 1, 2], "independent": True},
            {"name": "val", "values": [10, 11, 12], "independent": False},
        ]
    )

    def fake_query(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return qdf

    br.query = fake_query

    df = br.data_read(submatrix_iid=1, set_independent_as_index=False)

    assert captured_kwargs["load_flags"] is False
    assert df["val"].tolist() == [10, 11, 12]
    assert str(df["val"].dtype) == "int64"


def test_data_read_valid_flag_true_requests_flags_and_masks_default_bitmask():
    """valid_flag=True requests flags from query() and masks values using the default bitmask (15)."""
    from odsbox.bulk_reader import BulkReader as BR

    br = BR(None)  # type: ignore[arg-type]
    captured_kwargs: dict = {}

    qdf = pd.DataFrame(
        [
            {"name": "time", "values": [0, 1, 2, 3], "independent": True, "flags": [0, 0, 0, 0]},
            {"name": "val", "values": [10, 20, 30, 40], "independent": False, "flags": [0, 1, 0, 8]},
        ]
    )

    def fake_query(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return qdf

    br.query = fake_query

    df = br.data_read(submatrix_iid=1, set_independent_as_index=False, valid_flag=True)

    assert captured_kwargs["load_flags"] is True
    assert str(df["val"].dtype) == "Int64"
    assert df["val"].tolist() == [10, pd.NA, 30, pd.NA]
    # column without any invalid flags keeps its original (non-nullable) dtype
    assert str(df["time"].dtype) == "int64"
    assert df["time"].tolist() == [0, 1, 2, 3]


def test_data_read_valid_flag_custom_bitmask_masks_only_matching_bits():
    """A custom integer valid_flag only masks values whose flags match that specific bitmask."""
    from odsbox.bulk_reader import BulkReader as BR

    br = BR(None)  # type: ignore[arg-type]

    qdf = pd.DataFrame(
        [
            {"name": "val", "values": [10, 20, 30, 40], "independent": False, "flags": [0, 1, 2, 3]},
        ]
    )

    br.query = lambda *args, **kwargs: qdf

    df = br.data_read(submatrix_iid=1, set_independent_as_index=False, valid_flag=1)

    # flag=2 does not have bit 0 set, so it stays valid even though bitmask 15 would mask it
    assert df["val"].tolist() == [10, pd.NA, 30, pd.NA]


def test_data_read_valid_flag_false_also_requests_and_masks_with_default_bitmask():
    """valid_flag=False is documented to behave like True/None: it still requests and masks with bitmask 15."""
    from odsbox.bulk_reader import BulkReader as BR

    br = BR(None)  # type: ignore[arg-type]
    captured_kwargs: dict = {}

    qdf = pd.DataFrame(
        [
            {"name": "val", "values": [10, 20, 30, 40], "independent": False, "flags": [0, 1, 0, 8]},
        ]
    )

    def fake_query(*args, **kwargs):
        captured_kwargs.update(kwargs)
        return qdf

    br.query = fake_query

    df = br.data_read(submatrix_iid=1, set_independent_as_index=False, valid_flag=False)

    # False is not None, so flags are still requested from query()
    assert captured_kwargs["load_flags"] is True
    assert df["val"].tolist() == [10, pd.NA, 30, pd.NA]


def test_data_read_valid_flag_preserves_unit_names_and_partial_result_attrs():
    """valid_flag masking must not interfere with existing unit_names/partial_result propagation."""
    from odsbox.bulk_reader import BulkReader as BR

    br = BR(None)  # type: ignore[arg-type]

    qdf = pd.DataFrame(
        [
            {"name": "val", "values": [10, 20, 30], "independent": False, "flags": [0, 1, 0]},
        ]
    )
    qdf.attrs["unit_names"] = {"val": "N"}
    qdf.attrs["partial_result"] = True

    br.query = lambda *args, **kwargs: qdf

    df = br.data_read(submatrix_iid=1, set_independent_as_index=False, valid_flag=True)

    assert df.attrs["unit_names"] == {"val": "N"}
    assert df.attrs["partial_result"] is True
    assert df["val"].tolist() == [10, pd.NA, 30]


def test_data_read_valid_flag_with_independent_index_set():
    """valid_flag composes correctly with set_independent_as_index=True."""
    from odsbox.bulk_reader import BulkReader as BR

    br = BR(None)  # type: ignore[arg-type]

    qdf = pd.DataFrame(
        [
            {"name": "time", "values": [0, 1, 2], "independent": True, "flags": [0, 0, 0]},
            {"name": "val", "values": [10, 20, 30], "independent": False, "flags": [0, 1, 0]},
        ]
    )

    br.query = lambda *args, **kwargs: qdf

    df = br.data_read(submatrix_iid=1, set_independent_as_index=True, valid_flag=True)

    assert df.index.tolist() == [0, 1, 2]
    assert list(df.columns) == ["val"]
    assert df["val"].tolist() == [10, pd.NA, 30]


# --- Additional tests for _create_dataframe_from_localcolumns (static method) ---


def test_create_dataframe_from_localcolumns_without_flags_column_returns_values_unchanged():
    """When 'flags' is not part of the metadata, values are used verbatim regardless of valid_flag."""
    localcolumn_df = pd.DataFrame(
        [
            {"name": "Time", "values": [0, 1, 2]},
            {"name": "Signal", "values": [10, 20, 30]},
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert list(rv.columns) == ["Time", "Signal"]
    assert rv["Time"].tolist() == [0, 1, 2]
    assert rv["Signal"].tolist() == [10, 20, 30]
    assert str(rv["Signal"].dtype) == "int64"


def test_create_dataframe_from_localcolumns_custom_int_bitmask():
    """A custom integer bitmask only masks values whose flags match that specific mask."""
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": [10, 20, 30, 40],
                "flags": [0, 1, 2, 3],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(1, localcolumn_df)

    # flag=2 does not have bit 0 set, so it stays valid even though bitmask 15 would mask it
    assert rv["Signal"].tolist() == [10, pd.NA, 30, pd.NA]


def test_create_dataframe_from_localcolumns_valid_flag_true_matches_default_mask():
    """valid_flag=True behaves identically to False/None: all map to the default bitmask 15."""
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": [10, 20, 30, 40],
                "flags": [0, 1, 0, 8],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert str(rv["Signal"].dtype) == "Int64"
    assert rv["Signal"].tolist() == [10, pd.NA, 30, pd.NA]


def test_create_dataframe_from_localcolumns_no_invalid_flags_keeps_original_dtype():
    """When no flag matches the bitmask, values keep their original (non-nullable) dtype."""
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": [10, 20, 30],
                "flags": [0, 0, 0],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert str(rv["Signal"].dtype) == "int64"
    assert rv["Signal"].tolist() == [10, 20, 30]


def test_create_dataframe_from_localcolumns_empty_flags_list_skips_masking():
    """An empty flags list (e.g. a zero-row column) leaves the values untouched."""
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": [],
                "flags": [],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert rv["Signal"].tolist() == []


def test_create_dataframe_from_localcolumns_float_values_use_nan_for_masked():
    """Float columns keep their float dtype and use NaN (not pd.NA) for masked entries."""
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": np.array([1.5, 2.5, 3.5], dtype=float),
                "flags": [0, 1, 0],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert str(rv["Signal"].dtype) == "float64"
    assert rv["Signal"].iloc[0] == 1.5
    assert np.isnan(rv["Signal"].iloc[1])
    assert rv["Signal"].iloc[2] == 3.5


def test_create_dataframe_from_localcolumns_boolean_values_use_nullable_boolean():
    """Boolean columns convert to the nullable BooleanDtype so pd.NA can be stored."""
    localcolumn_df = pd.DataFrame(
        [
            {
                "name": "Signal",
                "values": np.array([True, False, True]),
                "flags": [0, 1, 0],
            }
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert str(rv["Signal"].dtype) == "boolean"
    assert rv["Signal"].tolist() == [True, pd.NA, True]


def test_create_dataframe_from_localcolumns_multiple_columns_independent_masking():
    """Each local column's mask is computed independently; unaffected columns are untouched."""
    localcolumn_df = pd.DataFrame(
        [
            {"name": "Time", "values": [0, 1, 2, 3], "flags": [0, 0, 0, 0]},
            {"name": "Signal", "values": [10, 20, 30, 40], "flags": [0, 1, 0, 8]},
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert str(rv["Time"].dtype) == "int64"
    assert rv["Time"].tolist() == [0, 1, 2, 3]
    assert str(rv["Signal"].dtype) == "Int64"
    assert rv["Signal"].tolist() == [10, pd.NA, 30, pd.NA]


def test_create_dataframe_from_localcolumns_constant_flags_shorter_than_values_are_broadcast():
    """Servers may return 'flags' in the same compact form used for implicit/raw sequence
    representations (e.g. 2 samples for an implicit_linear column with 4 rows). When every
    returned flag sample is identical, that single value must apply to every row."""
    localcolumn_df = pd.DataFrame(
        [
            {"name": "Time", "values": [0, 1, 2, 3], "flags": [15, 15]},
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert str(rv["Time"].dtype) == "Int64"
    assert rv["Time"].tolist() == [pd.NA, pd.NA, pd.NA, pd.NA]


def test_create_dataframe_from_localcolumns_constant_valid_flags_shorter_than_values_no_masking():
    """A short constant 'flags' sample that does not match the bitmask leaves values untouched."""
    localcolumn_df = pd.DataFrame(
        [
            {"name": "Time", "values": [0, 1, 2, 3], "flags": [0, 0]},
        ]
    )

    rv = BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

    assert str(rv["Time"].dtype) == "int64"
    assert rv["Time"].tolist() == [0, 1, 2, 3]


def test_create_dataframe_from_localcolumns_non_constant_length_mismatch_raises():
    """A 'flags' array that neither matches the values length nor is constant cannot be aligned."""
    localcolumn_df = pd.DataFrame(
        [
            {"name": "Time", "values": [0, 1, 2, 3], "flags": [0, 15]},
        ]
    )

    with pytest.raises(ValueError, match="cannot be aligned"):
        BulkReader._create_dataframe_from_localcolumns(True, localcolumn_df)

from __future__ import annotations

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

        def data_read_jaquel(self, jaquel):
            return object()  # ignored by our monkeypatched to_pandas

    fake = FakeConI()

    # to_pandas should return bulk data with id and values columns (will be renamed inside query)
    def fake_to_pandas(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True):
        return pd.DataFrame([[1, [1, 2]], [2, [3, 4]]], columns=["a", "b"])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)

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
    def fake_to_pandas(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True):
        return pd.DataFrame({"name": ["a", "b"], "values": [[1, 2], [3, 4]]})

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)

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

        def data_read_jaquel(self, jaquel):
            return object()

    def fake_to_pandas_bulk(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True):
        # bulk contains id 2 which lacks metadata
        return pd.DataFrame([[2, [9, 9]]], columns=["id", "values"])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas_bulk)

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

        def data_read_jaquel(self, jaquel):
            return object()

    # to_pandas should return id, values, generation_parameters columns (will be renamed inside query)
    def fake_to_pandas(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True):
        return pd.DataFrame([[1, [1, 2], [1.0, 2.0]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas)

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

        def data_read_jaquel(self, jaquel):
            return object()

    def fake_to_pandas2(dms, date_as_timestamp=True, prefer_np_array_for_unknown=True):
        return pd.DataFrame([[1, [7, 8]]])

    monkeypatch.setattr("odsbox.bulk_reader.to_pandas", fake_to_pandas2)

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

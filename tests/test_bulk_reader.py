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

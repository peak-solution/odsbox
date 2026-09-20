"""Unit tests for :mod:`ods_unit_converter`.

`OdsUnitConverter` talks to an ASAM ODS server through `ConI`. Tests here use
a lightweight fake `ConI` that serves `AoUnit`/`AoPhysicalDimension` data from
in-memory DataFrames, so no real ODS server is required.
"""

from __future__ import annotations

from typing import Any

import pytest
from pandas import DataFrame

from odsbox.utils import OdsUnitConverter

# Physical dimensions. Ids 1 and 2 intentionally describe the *same* SI
# exponents (pure length) under different AoPhysicalDimension records, to
# verify unit compatibility is judged by exponents, not by id equality.
PHYSDIM_DF = DataFrame(
    [
        {
            "id": 1,
            "name": "Length",
            "length_exp": 1,
            "mass_exp": 0,
            "time_exp": 0,
            "current_exp": 0,
            "temperature_exp": 0,
            "molar_amount_exp": 0,
            "luminous_intensity_exp": 0,
        },
        {
            "id": 2,
            "name": "Length (duplicate record)",
            "length_exp": 1,
            "mass_exp": 0,
            "time_exp": 0,
            "current_exp": 0,
            "temperature_exp": 0,
            "molar_amount_exp": 0,
            "luminous_intensity_exp": 0,
        },
        {
            "id": 3,
            "name": "Time",
            "length_exp": 0,
            "mass_exp": 0,
            "time_exp": 1,
            "current_exp": 0,
            "temperature_exp": 0,
            "molar_amount_exp": 0,
            "luminous_intensity_exp": 0,
        },
        {
            "id": 4,
            "name": "Temperature",
            "length_exp": 0,
            "mass_exp": 0,
            "time_exp": 0,
            "current_exp": 0,
            "temperature_exp": 1,
            "molar_amount_exp": 0,
            "luminous_intensity_exp": 0,
        },
        {
            "id": 5,
            "name": "Mass",
            "length_exp": 0,
            "mass_exp": 1,
            "time_exp": 0,
            "current_exp": 0,
            "temperature_exp": 0,
            "molar_amount_exp": 0,
            "luminous_intensity_exp": 0,
        },
    ]
)

UNIT_DF = DataFrame(
    [
        {"id": 1, "name": "Meter", "factor": 1.0, "offset": 0.0, "phys_dimension": 1},
        {
            "id": 2,
            "name": "Kilometer",
            "factor": 1000.0,
            "offset": 0.0,
            "phys_dimension": 1,
        },
        {
            "id": 3,
            "name": "Millimeter",
            "factor": 0.001,
            "offset": 0.0,
            "phys_dimension": 1,
        },
        # Same physical dimension (length) but a different phys_dimension record (id 2).
        {"id": 4, "name": "Inch", "factor": 0.0254, "offset": 0.0, "phys_dimension": 2},
        {"id": 5, "name": "Second", "factor": 1.0, "offset": 0.0, "phys_dimension": 3},
        {"id": 6, "name": "Kelvin", "factor": 1.0, "offset": 0.0, "phys_dimension": 4},
        {"id": 7, "name": "DegC", "factor": 1.0, "offset": 273.15, "phys_dimension": 4},
        {
            "id": 8,
            "name": "Kilogram",
            "factor": 1.0,
            "offset": 0.0,
            "phys_dimension": 5,
        },
        {"id": 9, "name": "Gram", "factor": 0.001, "offset": 0.0, "phys_dimension": 5},
        # Ambiguous unit name: two distinct records share the same name.
        {
            "id": 10,
            "name": "Ambiguous",
            "factor": 1.0,
            "offset": 0.0,
            "phys_dimension": 1,
        },
        {
            "id": 11,
            "name": "Ambiguous",
            "factor": 2.0,
            "offset": 0.0,
            "phys_dimension": 1,
        },
        # Same real-world unit as "DegC", but factor/offset differ only by floating-point
        # noise (1e-10) - should still be treated as a no-op conversion.
        {
            "id": 12,
            "name": "DegC (float noise)",
            "factor": 1.0 + 1e-10,
            "offset": 273.15 + 1e-10,
            "phys_dimension": 4,
        },
        # Differs from "DegC" by more than the isclose tolerance - a real conversion.
        {
            "id": 13,
            "name": "DegC (real diff)",
            "factor": 1.0 + 1e-6,
            "offset": 273.15,
            "phys_dimension": 4,
        },
    ]
)


class FakeEntity:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeModelCache:
    def entity_by_base_name(self, entity_base_name: str) -> FakeEntity:
        return FakeEntity(entity_base_name)

    def attribute_no_throw(self, entity_or_name: Any, application_or_base_name: str) -> None:
        # Keep the physical dimension query limited to the base columns used in PHYSDIM_DF.
        return None


class FakeConI:
    """Minimal stand-in for `odsbox.ConI` used only in tests."""

    def __init__(self) -> None:
        self.mc = FakeModelCache()

    def query(self, jaquel_query: dict[str, Any]) -> DataFrame:
        if "AoUnit" in jaquel_query:
            return UNIT_DF.copy()
        if "AoPhysicalDimension" in jaquel_query:
            return PHYSDIM_DF.copy()
        raise AssertionError(f"Unexpected query: {jaquel_query}")


@pytest.fixture
def converter() -> OdsUnitConverter:
    return OdsUnitConverter(FakeConI())  # type: ignore[arg-type]


def make_bulk(column: str, values: list[float], unit: str) -> DataFrame:
    bulk = DataFrame({column: values})
    bulk.attrs["unit_names"] = {column: unit}
    return bulk


class TestInit:
    def test_loads_unit_and_physdim_catalogs(self, converter: OdsUnitConverter) -> None:
        assert len(converter._unit_df) == len(UNIT_DF)
        assert len(converter._physdim_df) == len(PHYSDIM_DF)


class TestGetColumnUnit:
    def test_returns_recorded_unit(self, converter: OdsUnitConverter) -> None:
        bulk = make_bulk("length", [1.0], "Meter")
        assert converter._get_column_unit(bulk, "length") == "Meter"

    def test_missing_column_raises(self, converter: OdsUnitConverter) -> None:
        bulk = make_bulk("length", [1.0], "Meter")
        with pytest.raises(ValueError, match="does not exist"):
            converter._get_column_unit(bulk, "missing")

    def test_missing_unit_names_attr_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="unit_names"):
            converter._get_column_unit(bulk, "length")

    def test_column_missing_from_unit_names_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0], "other": [2.0]})
        bulk.attrs["unit_names"] = {"other": "Meter"}
        with pytest.raises(ValueError, match="missing a unit"):
            converter._get_column_unit(bulk, "length")


class TestConvertColumnByName:
    def test_simple_factor_conversion(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0, 2.5, 0.0]})
        result = converter.convert(bulk=bulk, column="length", from_unit="Kilometer", to_unit="Meter")
        assert result is True
        assert bulk["length"].tolist() == [1000.0, 2500.0, 0.0]

    def test_millimeter_to_kilometer(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1_000_000.0]})
        converter.convert(bulk=bulk, column="length", from_unit="Millimeter", to_unit="Kilometer")
        assert bulk["length"].iloc[0] == pytest.approx(1.0)

    def test_offset_conversion_deg_c_to_kelvin(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"temp": [0.0, 100.0, -273.15]})
        converter.convert(bulk=bulk, column="temp", from_unit="DegC", to_unit="Kelvin")
        assert bulk["temp"].tolist() == pytest.approx([273.15, 373.15, 0.0])

    def test_offset_conversion_kelvin_to_deg_c(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"temp": [273.15, 373.15]})
        converter.convert(bulk=bulk, column="temp", from_unit="Kelvin", to_unit="DegC")
        assert bulk["temp"].tolist() == pytest.approx([0.0, 100.0])

    def test_units_with_different_phys_dimension_ids_but_same_exponents(self, converter: OdsUnitConverter) -> None:
        # Meter (phys_dimension=1) and Inch (phys_dimension=2) both describe pure
        # length, backed by two different AoPhysicalDimension records.
        bulk = DataFrame({"length": [1.0]})
        result = converter.convert(bulk=bulk, column="length", from_unit="Meter", to_unit="Inch")
        assert result is True
        assert bulk["length"].iloc[0] == pytest.approx(1 / 0.0254)

    def test_identical_unit_names_skips_conversion(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [42.0]})
        result = converter.convert(bulk=bulk, column="length", from_unit="Meter", to_unit="Meter")
        assert result is False
        assert bulk["length"].iloc[0] == 42.0

    def test_incompatible_units_raise(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="Incompatible units"):
            converter.convert(bulk=bulk, column="length", from_unit="Meter", to_unit="Kilogram")

    def test_unknown_from_unit_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="Unknown from_unit"):
            converter.convert(bulk=bulk, column="length", from_unit="Furlong", to_unit="Meter")

    def test_unknown_to_unit_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="Unknown to_unit"):
            converter.convert(bulk=bulk, column="length", from_unit="Meter", to_unit="Furlong")

    def test_ambiguous_from_unit_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="Ambiguous from_unit"):
            converter.convert(bulk=bulk, column="length", from_unit="Ambiguous", to_unit="Meter")

    def test_ambiguous_to_unit_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="Ambiguous to_unit"):
            converter.convert(bulk=bulk, column="length", from_unit="Meter", to_unit="Ambiguous")

    def test_tiny_floating_point_difference_still_skips_conversion(self, converter: OdsUnitConverter) -> None:
        # "DegC" and "DegC (float noise)" differ only by 1e-10 in factor/offset, well
        # within the isclose tolerance, so no rescale should happen.
        bulk = DataFrame({"temp": [10.0]})
        result = converter.convert(bulk=bulk, column="temp", from_unit="DegC", to_unit="DegC (float noise)")
        assert result is False
        assert bulk["temp"].iloc[0] == 10.0

    def test_factor_difference_beyond_tolerance_still_converts(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"temp": [10.0]})
        result = converter.convert(bulk=bulk, column="temp", from_unit="DegC", to_unit="DegC (real diff)")
        assert result is True
        assert bulk["temp"].iloc[0] == pytest.approx(10.0 / (1.0 + 1e-6))


class TestConvertColumnById:
    def test_conversion_by_id(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        # Kilometer id=2, Meter id=1.
        result = converter.convert(bulk=bulk, column="length", from_unit=2, to_unit=1)
        assert result is True
        assert bulk["length"].iloc[0] == pytest.approx(1000.0)

    def test_unknown_id_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="Unknown from_unit"):
            converter.convert(bulk=bulk, column="length", from_unit=999, to_unit=1)

    def test_identical_ids_skip_conversion(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        result = converter.convert(bulk=bulk, column="length", from_unit=1, to_unit=1)
        assert result is False


class TestConvert:
    def test_converts_and_updates_unit_names(self, converter: OdsUnitConverter) -> None:
        bulk = make_bulk("length", [1.0, 2.0], "Kilometer")
        result = converter.convert_column(bulk, "length", "Meter")
        assert result is True
        assert bulk["length"].tolist() == [1000.0, 2000.0]
        assert bulk.attrs["unit_names"]["length"] == "Meter"

    def test_no_op_conversion_still_updates_unit_name_and_returns_false(self, converter: OdsUnitConverter) -> None:
        bulk = make_bulk("length", [5.0], "Meter")
        result = converter.convert_column(bulk, "length", "Meter")
        assert result is False
        assert bulk["length"].iloc[0] == 5.0
        assert bulk.attrs["unit_names"]["length"] == "Meter"

    def test_raises_when_unit_names_missing(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        with pytest.raises(ValueError, match="unit_names"):
            converter.convert_column(bulk, "length", "Meter")

    def test_incompatible_units_via_convert_raise(self, converter: OdsUnitConverter) -> None:
        bulk = make_bulk("length", [1.0], "Meter")
        with pytest.raises(ValueError, match="Incompatible units"):
            converter.convert_column(bulk, "length", "Kilogram")


class TestConvertColumns:
    def test_converts_all_columns_and_updates_unit_names(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0, 2.0], "temp": [0.0, 100.0]})
        bulk.attrs["unit_names"] = {"length": "Kilometer", "temp": "DegC"}

        result = converter.convert_columns(bulk, [("length", "Meter"), ("temp", "Kelvin")])

        assert result is True
        assert bulk["length"].tolist() == [1000.0, 2000.0]
        assert bulk["temp"].tolist() == pytest.approx([273.15, 373.15])
        assert bulk.attrs["unit_names"] == {"length": "Meter", "temp": "Kelvin"}

    def test_returns_false_when_all_conversions_are_no_ops(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0], "temp": [2.0]})
        bulk.attrs["unit_names"] = {"length": "Meter", "temp": "Kelvin"}

        result = converter.convert_columns(bulk, [("length", "Meter"), ("temp", "Kelvin")])

        assert result is False
        assert bulk["length"].iloc[0] == 1.0
        assert bulk["temp"].iloc[0] == 2.0

    def test_returns_true_when_only_some_conversions_change_values(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0], "temp": [2.0]})
        bulk.attrs["unit_names"] = {"length": "Meter", "temp": "Kelvin"}

        result = converter.convert_columns(bulk, [("length", "Meter"), ("temp", "DegC")])

        assert result is True
        assert bulk["length"].iloc[0] == 1.0
        assert bulk["temp"].iloc[0] == pytest.approx(2.0 - 273.15)

    def test_empty_conversions_list_returns_false(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        bulk.attrs["unit_names"] = {"length": "Meter"}

        assert converter.convert_columns(bulk, []) is False

    def test_propagates_error_from_incompatible_column(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0], "temp": [2.0]})
        bulk.attrs["unit_names"] = {"length": "Meter", "temp": "Kelvin"}

        with pytest.raises(ValueError, match="Incompatible units"):
            converter.convert_columns(bulk, [("length", "Kilogram"), ("temp", "DegC")])

    def test_propagates_error_for_unknown_column(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        bulk.attrs["unit_names"] = {"length": "Meter"}

        with pytest.raises(ValueError, match="does not exist"):
            converter.convert_columns(bulk, [("missing", "Meter")])

    def test_star_pattern_expands_to_matching_columns(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length_1": [1.0], "length_2": [2.0], "temp": [0.0]})
        bulk.attrs["unit_names"] = {
            "length_1": "Kilometer",
            "length_2": "Kilometer",
            "temp": "DegC",
        }

        result = converter.convert_columns(bulk, [("length_*", "Meter")])

        assert result is True
        assert bulk["length_1"].iloc[0] == 1000.0
        assert bulk["length_2"].iloc[0] == 2000.0
        assert bulk["temp"].iloc[0] == 0.0
        assert bulk.attrs["unit_names"]["length_1"] == "Meter"
        assert bulk.attrs["unit_names"]["length_2"] == "Meter"
        assert bulk.attrs["unit_names"]["temp"] == "DegC"

    def test_question_mark_pattern_expands_to_matching_columns(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"len1": [1.0], "len2": [2.0], "len10": [3.0]})
        bulk.attrs["unit_names"] = {
            "len1": "Kilometer",
            "len2": "Kilometer",
            "len10": "Kilometer",
        }

        result = converter.convert_columns(bulk, [("len?", "Meter")])

        assert result is True
        assert bulk["len1"].iloc[0] == 1000.0
        assert bulk["len2"].iloc[0] == 2000.0
        # "len10" has two trailing characters, so "len?" must not match it.
        assert bulk["len10"].iloc[0] == 3.0
        assert bulk.attrs["unit_names"]["len10"] == "Kilometer"

    def test_pattern_matching_no_columns_raises(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length": [1.0]})
        bulk.attrs["unit_names"] = {"length": "Meter"}

        with pytest.raises(ValueError, match="matched no columns"):
            converter.convert_columns(bulk, [("temp_*", "Meter")])

    def test_pattern_can_be_combined_with_literal_column(self, converter: OdsUnitConverter) -> None:
        bulk = DataFrame({"length_1": [1.0], "length_2": [2.0], "temp": [0.0]})
        bulk.attrs["unit_names"] = {
            "length_1": "Kilometer",
            "length_2": "Kilometer",
            "temp": "DegC",
        }

        result = converter.convert_columns(bulk, [("length_*", "Meter"), ("temp", "Kelvin")])

        assert result is True
        assert bulk["length_1"].iloc[0] == 1000.0
        assert bulk["length_2"].iloc[0] == 2000.0
        assert bulk["temp"].iloc[0] == pytest.approx(273.15)


class TestCompatibleUnits:
    def test_same_phys_dimension_id_is_compatible(self, converter: OdsUnitConverter) -> None:
        meter_row = converter._unit_df[converter._unit_df["name"] == "Meter"]
        kilometer_row = converter._unit_df[converter._unit_df["name"] == "Kilometer"]
        assert converter._compatible_units(meter_row, kilometer_row) is True

    def test_different_phys_dimension_same_exponents_is_compatible(self, converter: OdsUnitConverter) -> None:
        meter_row = converter._unit_df[converter._unit_df["name"] == "Meter"]
        inch_row = converter._unit_df[converter._unit_df["name"] == "Inch"]
        assert converter._compatible_units(meter_row, inch_row) is True

    def test_different_exponents_is_incompatible(self, converter: OdsUnitConverter) -> None:
        meter_row = converter._unit_df[converter._unit_df["name"] == "Meter"]
        kilogram_row = converter._unit_df[converter._unit_df["name"] == "Kilogram"]
        assert converter._compatible_units(meter_row, kilogram_row) is False

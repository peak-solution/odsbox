import fnmatch
import logging
import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pandas import DataFrame

    from ..con_i import ConI


class OdsUnitConverter:
    """Convert values in a pandas DataFrame column between ASAM ODS units.

    On construction, the ``AoUnit`` and ``AoPhysicalDimension`` catalogs are
    loaded from the given ODS server session and cached for repeated use.
    A column can then be rescaled from one unit to another via
    ``value * from_factor + from_offset - to_offset) / to_factor``, provided
    both units resolve to the same physical dimension (i.e. matching SI base
    exponents).

    :meth:`convert_column` expects the DataFrame to carry a ``unit_names``
    entry in its ``attrs`` dict, mapping column name to its current unit
    name, and updates that mapping to the new unit after conversion.

    Example::

        from odsbox import ConI
        from odsbox.utils import OdsUnitConverter

        with ConI(url="https://example.server/api", auth=("user", "pass")) as con_i:
            converter = OdsUnitConverter(con_i)
            df = con_i.bulk.data_read(submatrix_id)
            converter.convert_columns(df, [("time", "s"), ("temperature", "degC")])

    """

    log = logging.getLogger(__name__)

    def __init__(self, con_i: "ConI"):
        """Load and cache the unit and physical dimension catalogs from `con_i`."""
        self.log.debug("Initializing OdsUnitConverter")
        self._unit_df = con_i.query(
            {
                "AoUnit": {},
                "$attributes": {
                    "id": 1,
                    "name": 1,
                    "factor": 1,
                    "offset": 1,
                    "phys_dimension": 1,
                },
            }
        )
        self.log.debug("Loaded %d unit definitions", len(self._unit_df))
        physdim_entity = con_i.mc.entity_by_base_name("AoPhysicalDimension")
        physdim_attributes = {
            "id": 1,
            "name": 1,
            "length_exp": 1,
            "mass_exp": 1,
            "time_exp": 1,
            "current_exp": 1,
            "temperature_exp": 1,
            "molar_amount_exp": 1,
            "luminous_intensity_exp": 1,
        }
        for attr_name in [
            "angle",
            "length_exp_den",
            "mass_exp_den",
            "time_exp_den",
            "current_exp_den",
            "temperature_exp_den",
            "molar_amount_exp_den",
            "luminous_intensity_exp_den",
        ]:
            if con_i.mc.attribute_no_throw(physdim_entity.name, attr_name):
                physdim_attributes[attr_name] = 1
        self._physdim_df = con_i.query({physdim_entity.name: {}, "$attributes": physdim_attributes})
        self.log.debug("Loaded %d physical dimension definitions", len(self._physdim_df))
        self._physdim_dimension_columns = [c for c in self._physdim_df.columns if c not in ("id", "name")]

    def _get_column_unit(self, bulk: "DataFrame", column: str) -> str:
        """Return the unit name recorded for `column` in `bulk.attrs["unit_names"]`."""
        self.log.debug("Getting unit for column '%s'", column)
        if column not in bulk.columns:
            raise ValueError(f"Column '{column}' does not exist in the DataFrame")
        if bulk.attrs.get("unit_names") is None:
            raise ValueError("DataFrame is missing 'unit_names' attribute")
        current_unit = bulk.attrs["unit_names"].get(column)
        if current_unit is None:
            raise ValueError(f"Column '{column}' is missing a unit in 'unit_names' attribute")
        return str(current_unit)

    def _compatible_units(self, from_unit_row: "DataFrame", to_unit_row: "DataFrame") -> bool:
        """Check whether two unit rows share the same physical dimension (SI exponents).

        Compares the exponent columns rather than the physical dimension id,
        since the same physical dimension can be represented by more than one
        ``AoPhysicalDimension`` record.
        """
        from_dimension = self._physdim_df.loc[
            self._physdim_df["id"] == from_unit_row["phys_dimension"].iloc[0],
            self._physdim_dimension_columns,
        ].reset_index(drop=True)
        to_dimension = self._physdim_df.loc[
            self._physdim_df["id"] == to_unit_row["phys_dimension"].iloc[0],
            self._physdim_dimension_columns,
        ].reset_index(drop=True)
        compatible = not from_dimension.empty and from_dimension.equals(to_dimension)
        self.log.debug("Unit physical dimensions compatible: %s", compatible)
        return compatible

    def convert_column(self, bulk: "DataFrame", column: str, to_unit: str) -> bool:
        """Convert `column` of `bulk` in place to `to_unit` and update its recorded unit name.

        The current unit is looked up from `bulk.attrs["unit_names"]`.

        Args:
            bulk: The DataFrame containing the column to convert.
            column: The name of the column to convert.
            to_unit: The target unit to convert the column to.

        Returns:
            True if the underlying values were rescaled, False if the
            current and target units were already equivalent.
        """
        from_unit = self._get_column_unit(bulk, column)
        self.log.debug("Converting column '%s' from '%s' to '%s'", column, from_unit, to_unit)

        converted = self.convert(bulk=bulk, column=column, to_unit=to_unit, from_unit=from_unit)

        bulk.attrs["unit_names"][column] = to_unit
        return converted

    def convert_columns(self, bulk: "DataFrame", conversions: list[tuple[str, str]]) -> bool:
        """Convert multiple columns of `bulk` in place using `(column, to_unit)` tuples.

        Column names may include ``*`` or ``?`` wildcards. Such names are
        matched against the DataFrame's columns, and the requested conversion
        is applied to every match. Each column's current unit is read from
        ``bulk.attrs["unit_names"]``.

        Example::

            from odsbox import ConI
            from odsbox.utils import OdsUnitConverter

            with ConI(url="https://example.server/api", auth=("user", "pass")) as con_i:
                converter = OdsUnitConverter(con_i)
                df = con_i.bulk.data_read(submatrix_id)
                converter.convert_columns(df, [("time", "s"), ("temperature", "degC"), ("acceleration_*", "m/s^2")])

        Args:
            bulk: The DataFrame containing the columns to convert.
            conversions: A list of `(column, to_unit)` tuples specifying the columns to convert and their target units.
               columns may include ``*`` or ``?`` wildcards.

        Raises:
            ValueError: If a glob pattern matches no column of `bulk`.

        Returns:
            True if at least one column's values were rescaled.
        """
        expanded_conversions: list[tuple[str, str]] = []
        for column, to_unit in conversions:
            if "*" in column or "?" in column:
                matches = fnmatch.filter(bulk.columns, column)
                if not matches:
                    raise ValueError(f"Column pattern '{column}' matched no columns")
                expanded_conversions.extend((match, to_unit) for match in matches)
            else:
                expanded_conversions.append((column, to_unit))

        results = [self.convert_column(bulk, column, to_unit) for column, to_unit in expanded_conversions]
        return any(results)

    def convert(self, *, bulk: "DataFrame", column: str, from_unit: str | int, to_unit: str | int) -> bool:
        """Rescale `column` of `bulk` in place from `from_unit` to `to_unit`.

        Units may be given by name or by `AoUnit` id.

        Args:
            bulk: The DataFrame containing the column to convert.
            column: The name of the column to convert.
            from_unit: The current unit of the column.
            to_unit: The target unit to convert the column to.

        Raises:
            ValueError: If either unit is unknown, ambiguous, or the units
                are not compatible (do not share the same physical dimension).

        Returns:
            True if the values were rescaled, False if `from_unit` and
            `to_unit` were already equivalent (including differently spelled
            units whose factor and offset match within floating-point
            tolerance, e.g. "degC" and "°C").
        """
        self.log.debug("Converting column '%s' from unit '%s' to '%s'", column, from_unit, to_unit)

        if from_unit == to_unit:
            self.log.debug("Skipping conversion: source and target units are identical")
            return False

        from_unit_row = (
            self._unit_df[self._unit_df["name"] == from_unit]
            if isinstance(from_unit, str)
            else self._unit_df[self._unit_df["id"] == from_unit]
        )
        to_unit_row = (
            self._unit_df[self._unit_df["name"] == to_unit]
            if isinstance(to_unit, str)
            else self._unit_df[self._unit_df["id"] == to_unit]
        )

        if from_unit_row.empty:
            raise ValueError(f"Unknown from_unit: {from_unit}")
        if to_unit_row.empty:
            raise ValueError(f"Unknown to_unit: {to_unit}")
        if len(from_unit_row) > 1:
            raise ValueError(f"Ambiguous from_unit: {from_unit}")
        if len(to_unit_row) > 1:
            raise ValueError(f"Ambiguous to_unit: {to_unit}")

        if from_unit_row["name"].iloc[0] == to_unit_row["name"].iloc[0]:
            self.log.debug("Skipping conversion: source and target resolve to the same unit")
            return False

        if not self._compatible_units(from_unit_row, to_unit_row):
            raise ValueError(f"Incompatible units: {from_unit} and {to_unit}")

        from_factor = from_unit_row["factor"].iloc[0]
        from_offset = from_unit_row["offset"].iloc[0]
        to_factor = to_unit_row["factor"].iloc[0]
        to_offset = to_unit_row["offset"].iloc[0]

        if math.isclose(from_factor, to_factor, abs_tol=1e-9) and math.isclose(from_offset, to_offset, abs_tol=1e-9):
            # Same scale under a different spelling (e.g. "degC" vs "°C"): nothing to rescale.
            self.log.debug("Skipping conversion: '%s' and '%s' share the same factor/offset", from_unit, to_unit)
            return False

        bulk[column] = (bulk[column] * from_factor + from_offset - to_offset) / to_factor
        self.log.debug("Converted column '%s' to unit '%s'", column, to_unit_row["name"].iloc[0])
        return True

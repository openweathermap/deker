# deker - multidimensional arrays storage engine
# Copyright (C) 2023  OpenWeather
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from datetime import datetime
from typing import TYPE_CHECKING

from deker.errors import DekerValidationError

if TYPE_CHECKING:
    from deker.dimensions import TimeDimension


def get_start_datetime(dimension: "TimeDimension") -> datetime:
    """Get the start value of a ``TimeDimension`` bound to an array.

    Dimensions of arrays are created from the schema with attribute references (``"$name"``)
    already resolved into ``datetime`` values, so a reference left in a bound dimension is an error.

    :param dimension: ``TimeDimension`` of an array
    :returns: start value of the dimension
    """
    if isinstance(dimension.start_value, str):  # pragma: no cover
        raise DekerValidationError(
            f'TimeDimension "{dimension.name}" start_value is an unresolved attribute reference: '
            f"{dimension.start_value}"
        )
    return dimension.start_value


def convert_datetime_attrs_to_iso(attrs: dict | None) -> dict | None:
    """Convert datetime attributes to iso-format if possible.

    :param attrs: attributes dict
    :returns: attributes with datetimes as ISO strings, or the input as is if it is empty
    """
    if attrs is not None and not isinstance(attrs, dict):
        raise TypeError(f"Invalid attribute value type: {type(attrs)}; expected dict")

    if attrs:
        return {key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in attrs.items()}
    return attrs


def convert_iso_attrs_to_datetime(attrs: dict | None) -> dict | None:
    """Convert iso-format attributes to datetime if possible.

    :param attrs: attributes dict
    :returns: attributes with ISO strings as datetimes, or the input as is if it is empty
    """
    if attrs is not None and not isinstance(attrs, dict):
        raise TypeError(f"Invalid attribute value type: {type(attrs)}; expected dict")

    if attrs:
        new_attrs = {}
        for key, value in attrs.items():
            if isinstance(value, str):
                try:
                    new_attrs[key] = datetime.fromisoformat(value)
                except (TypeError, ValueError):
                    new_attrs[key] = value
            else:
                new_attrs[key] = value
        return new_attrs
    return attrs

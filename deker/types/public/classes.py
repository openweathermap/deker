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


from typing import NamedTuple, Union

from deker.errors import DekerValidationError
from deker.log import SelfLoggerMixin

Labels = Union[tuple[Union[str, int, float], ...], list[Union[str, int, float]]]


class Scale(NamedTuple):
    """NamedTuple with a description of a dimension regular scale.

    For example we describe dimensions for the Earth's latitude and longitude grades::

        dims = [
                DimensionSchema(name="y", size=721),
                DimensionSchema(name="x", size=1440),
            ]

    Such description may exist, but it's not quite sufficient. We can provide more information for the grid::

        dims = [
                    DimensionSchema(
                        name="y",
                        size=721,
                        scale=Scale(start_value=90, step=-0.25, name="lat")
                    ),
                    DimensionSchema(
                        name="x",
                        size=1440,
                        scale=Scale(start_value=-180, step=0.25, name="lon")
                    ),
                ]

    This extra information permits us provide fancy indexing by lat/lon coordinates in degrees::

        EarthGridArray[1, 1] == EarthGridArray[89.75, -179.75]

    :param start_value: scale real start point
    :param step: scale values step (may be positive or negative)
    :param name: optional scale name (latitude, longitude or whatever)
    """

    start_value: float
    step: float
    name: str | None = None


class StorageSize(NamedTuple):
    """Namedtuple for storage size with bytes and human representation."""

    bytes: int
    human: str


class IndexLabels(tuple, SelfLoggerMixin):
    """``IndexLabels`` is an overloaded ``tuple`` providing a mapping of label-values to their ``Dimension`` indexes.

    It provides direct and reversed ordered mappings of dimension's values names (labels)
    to their position in the axis flat array (indexes).

    Labels shall be a sequence of unique values within the full scope of the passed ``Dimension``.
    Valid Labels are list or tuple of unique strings or floats::

      ["name1", "name2", ..., "nameN"] | (0.2, 0.1, 0.4, 0.3, ..., -256.78963)

    :param labels: list or tuple of unique strings or floats
    """

    @classmethod
    def __validate(cls, labels: Labels | None) -> tuple[str | int | float, ...]:
        error = DekerValidationError(
            "Labels shall be a list or tuple of unique values of the same type (strings, floats), "
            "not None or empty list or tuple"
        )
        if (
            not labels
            or not isinstance(labels, (list, tuple))
            or all(
                (
                    any(not isinstance(val, str) for val in labels),
                    any(not isinstance(val, float) for val in labels),
                )
            )
        ):
            raise error
        return labels if isinstance(labels, tuple) else tuple(labels)

    def __new__(cls, labels: Labels | None) -> "IndexLabels":
        """Override python ``tuple.__new__`` method.

        Here we add values validation before an instance is created.

        :param labels: labels
        :returns: validated ``IndexLabels`` instance
        """
        return super().__new__(cls, cls.__validate(labels))

    def __init__(self, labels: Labels | None) -> None:
        self.logger.debug("labels initialized")

    @property
    def first(self) -> str | int | float:
        """Get first label."""
        return self[0]

    @property
    def last(self) -> str | int | float:
        """Get last label."""
        return self[-1]

    def name_to_index(self, name: str | int | float) -> int | None:
        """Get label index by its name.

        :param name: label name
        :returns: index of the label, or ``None`` if there is no such label
        """
        try:
            return self.index(name)
        except ValueError as e:
            mes = str(e) + f": no name {name}"
            self.logger.debug(mes)
            return None

    def index_to_name(self, idx: int) -> str | int | float | None:
        """Get label name by its index.

        :param idx: label index
        :returns: label at the index, or ``None`` if the index is out of range
        """
        try:
            return self[idx]
        except IndexError as e:
            mes = str(e) + f": no index {idx}"
            self.logger.debug(mes)
            return None

    def __str__(self) -> str:
        return str(tuple(self))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"

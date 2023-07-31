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

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Tuple, Union

from typing_extensions import NotRequired, TypedDict


if TYPE_CHECKING:
    from deker.types import Data

__all__ = (
    "Serializer",
    "ArrayMeta",
    "ArrayLockMeta",
    "ArrayPosition",
    "ArrayOffset",
    "ArraysCoordinatesWithOffset",
    "CollectionLockMeta",
    "Paths",
)


class Serializer(ABC):
    """Base abstract object with a possibility to serialize itself into different objects."""

    @property
    @abstractmethod
    def as_dict(self) -> dict:
        """Serialize self into a dictionary."""
        pass

    @abstractmethod
    def __repr__(self) -> str:
        pass

    @abstractmethod
    def __str__(self) -> str:
        pass


class ArrayMeta(TypedDict):
    """Typing for array meta."""

    id: str
    primary_attributes: dict
    custom_attributes: dict


class Paths(NamedTuple):
    """Namedtuple for array paths."""

    main: Path
    symlink: Path

    def create(self) -> None:
        """Create paths in the file system."""
        for attr in self._fields:
            path: Path = getattr(self, attr)
            # TODO: make universal string check for is_file. "path" is str, not Path
            if str(path).endswith("hdf5") or str(path).endswith("json"):
                path = path.parent
            path.mkdir(parents=True, exist_ok=True)


class ArrayOffset(TypedDict):
    """Offset of subset in array."""

    start: int
    end: NotRequired[int]


class ArraysCoordinatesWithOffset(NamedTuple):
    """Coordinate of Array in dimension, with offset."""

    coordinate: int
    offset: ArrayOffset


class ArrayPosition(NamedTuple):
    """Tuple with array coordinates, bounds and slice that will be passed to adapter."""

    vposition: Tuple[int, ...]
    bounds: Tuple[Union[slice, int], ...]
    data_slice: Tuple[slice, ...]


class ArrayPositionedData(NamedTuple):
    """Tuple with array coordinates, bounds and data that will be passed to adapter."""

    vposition: Tuple[int, ...]
    bounds: Tuple[Union[slice, int], ...]
    data: "Data"


class CollectionLockMeta(TypedDict):
    """Typed dict describing collection lockfile parameters. Used for typing."""

    Lockfile: str
    Collection: str
    Type: str
    Creation: str


class ArrayLockMeta(TypedDict):
    """Typed dict describing array lockfile parameters. Used for typing."""

    Lockfile: str
    Collection: str
    Array: str
    ID: str
    PID: str
    TID: str
    Type: str
    Creation: str

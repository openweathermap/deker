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
from typing import TYPE_CHECKING, NamedTuple

from typing_extensions import TypedDict

if TYPE_CHECKING:
    from deker.types import Data

__all__ = (
    "Serializer",
    "ArrayMeta",
    "ArrayLockMeta",
    "ArrayPosition",
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
        """Unambiguous representation of the object."""
        pass

    @abstractmethod
    def __str__(self) -> str:
        """Readable representation of the object."""
        pass


class ArrayMeta(TypedDict):
    """Metadata of an array as it is stored: its id and attributes."""

    id: str
    primary_attributes: dict
    custom_attributes: dict


class Paths(NamedTuple):
    """The two directories of an array: of its main file and of its symlink."""

    main: Path
    symlink: Path

    def create(self) -> None:
        """Create the directories in the file system, if they are missing."""
        for attr in self._fields:
            path: Path = getattr(self, attr)
            # TODO: make universal string check for is_file. "path" is str, not Path
            if str(path).endswith("hdf5") or str(path).endswith("json"):
                path = path.parent
            path.mkdir(parents=True, exist_ok=True)


class ArrayPosition(NamedTuple):
    """One ``Array`` a ``VSubset`` covers.

    Its position in the ``vgrid``, the bounds inside it and its slice of the subset data.
    """

    vposition: tuple[int, ...]
    bounds: tuple[slice | int, ...]
    data_slice: tuple[slice | int, ...]


class ArrayPositionedData(NamedTuple):
    """One ``Array`` a ``VSubset`` writes.

    Its position in the ``vgrid``, the bounds inside it and the data for them.
    """

    vposition: tuple[int, ...]
    bounds: tuple[slice | int, ...]
    data: "Data"


class CollectionLockMeta(TypedDict):
    """Description of a collection lock file in the lock listing."""

    Lockfile: str
    Collection: str
    Type: str
    Creation: str


class ArrayLockMeta(TypedDict):
    """Description of a VArray write marker of Deker < 2 in the lock listing."""

    Lockfile: str
    Collection: str
    Array: str
    PID: str
    Token: str
    Type: str
    Creation: str

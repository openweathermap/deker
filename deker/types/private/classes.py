# deker - multidimensional arrays storage engine
# Copyright (C) 2023-2026  Openweather Ltd.
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

import os

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from typing_extensions import TypedDict

from deker.errors import DekerInstanceNotExistsError

if TYPE_CHECKING:
    from deker.types import Data

__all__ = (
    "Serializer",
    "ArrayMeta",
    "ArrayLockMeta",
    "ArrayPosition",
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

    def create(self, collection_path: Path) -> None:
        """Create the directories inside the collection, level by level from it down, if they are missing.

        The directory of the collection itself is never created: a creation never brings back a deleted collection.

        :param collection_path: directory of the collection the directories are in
        :raises DekerInstanceNotExistsError: if the directory of the collection, or a level above, is gone
        """
        collection = str(collection_path)
        for directory in self:
            current = collection  # strings, not paths: the levels are walked on every creation
            for part in str(directory)[len(collection) + 1 :].split(os.sep):
                current = os.path.join(current, part)
                try:
                    os.mkdir(current)
                except FileExistsError:
                    pass
                except FileNotFoundError:
                    from deker.tools.path import check_storage_directory  # circular import otherwise

                    check_storage_directory(collection_path.parent)
                    raise DekerInstanceNotExistsError(f"{os.path.dirname(current)} doesn't exist") from None


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


class ArrayLockMeta(TypedDict):
    """Description of a VArray write marker of Deker < 2 in the lock listing."""

    Lockfile: str
    Collection: str
    Array: str
    PID: str
    Token: str
    Type: str
    Creation: str

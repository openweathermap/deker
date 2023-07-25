from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple, Optional, Tuple, Union

from typing_extensions import NotRequired, TypedDict


if TYPE_CHECKING:
    from deker.types import Data

__all__ = (
    "Serializer",
    "ArrayMeta",
    "ArrayPosition",
    "ArrayOffset",
    "ArraysCoordinatesWithOffset",
    "Paths",
    "StorageSize",
    "Scale",
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


class StorageSize(NamedTuple):
    """Namedtuple for storage size."""

    bytes: int
    human: str


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


class Scale(NamedTuple):
    """Tuple with description of a regular scale for a dimension.

    E.g. we describe a dimensions schema for the Earth's latitude and longitude grades:
        dims = [
                DimensionSchema(name="y", size=721),
                DimensionSchema(name="x", size=1440),
            ]
    Such description may exist, but it's not quite sufficient. We can provide information for the grid:
        dims = [
                    DimensionSchema(name="y", size=721, scale=Scale(start_value=90, step=-0.5),
                    DimensionSchema(name="x", size=1440, scale=Scale(start_value=-180, step=0.5)),
                ]
    This extra information permits us provide fancy indexing by lat/lon coordinates in degrees:
        EarthGridArray[0, 0] == EarthGridArray[90.0, -180.0]

    :param start_value: scale real start point
    :param step: scale values step
    :param name: optional scale name (latitude, longitude or whatever)
    """

    start_value: float
    step: float
    name: Optional[str] = None


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

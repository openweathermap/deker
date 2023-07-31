from datetime import datetime
from enum import Enum

import numpy as np


__all__ = ("DTypeEnum", "SchemaType", "DimensionType", "LocksExtensions", "LocksTypes")


class DTypeEnum(Enum):
    """Mapping of numeric data type names to types."""

    int = int
    float = float
    complex = complex
    int8 = np.int8
    int16 = np.int16
    int32 = np.int32
    int64 = np.int64
    longlong = np.longlong
    float16 = np.float16
    float32 = np.float32
    float64 = np.float64
    float128 = np.float128
    longfloat = np.longfloat
    double = np.double
    longdouble = np.longdouble
    complex64 = np.complex64
    complex128 = np.complex128
    complex256 = np.complex256
    longcomplex = np.longcomplex
    string = str
    datetime = datetime
    tuple = tuple

    @staticmethod
    def get_name(object: "DTypeEnum") -> str:
        """Return object name.

        Numpy types parsing: we store types as "numpy.typename" in JSON.
        :param object: DtypeEnum object
        """
        if object.value in (int, float, complex, str, tuple, datetime):
            return str(object.name)
        return f"numpy.{object.name}"


class SchemaType(Enum):
    """Mapping of schema types to strings."""

    varray = "varray"
    array = "array"


class DimensionType(Enum):
    """Enum of dimensions' types."""

    generic = "generic"
    time = "time"


class LocksExtensions(Enum):
    """Extensions for lock files."""

    array_lock = ".arrlock"
    array_read_lock = ".arrayreadlock"
    collection_lock = ".lock"
    varray_lock = ".varraylock"


class LocksTypes(Enum):
    """Locks enum."""

    array_lock = "array creation lock"
    array_read_lock = "array read lock"
    collection_lock = "collection creation lock"
    varray_lock = "varray write lock"

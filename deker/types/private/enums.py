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
from enum import Enum

import numpy as np


__all__ = ("DTypeEnum", "DimensionType", "LocksExtensions", "LocksTypes")


class DTypeEnum(Enum):
    """Mapping of numeric data type names to types."""

    int = int
    float = float
    complex = complex
    int8 = np.int8
    byte = np.byte
    int16 = np.int16
    short = np.short
    int32 = np.int32
    intc = np.intc
    int64 = np.int64
    intp = np.intp
    int_ = np.int64
    longlong = np.longlong
    uint = np.uint
    uint8 = np.uint8
    ubyte = np.ubyte
    uint16 = np.uint16
    ushort = np.ushort
    uint32 = np.uint32
    uintc = np.uintc
    uint64 = np.uint64
    uintp = np.uintp
    ulonglong = np.ulonglong
    float16 = np.float16
    cfloat = np.cfloat
    cdouble = np.cdouble
    float32 = np.float32
    clongfloat = np.clongfloat
    float64 = np.float64
    double = np.double
    float128 = np.float128
    longfloat = np.longfloat
    longdouble = np.longdouble
    complex64 = np.complex64
    singlecomplex = np.singlecomplex
    complex128 = np.complex128
    complex_ = np.complex_
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


class DimensionType(str, Enum):
    """Enum of dimensions' types."""

    generic = "generic"
    time = "time"


class LocksExtensions(str, Enum):
    """Extensions for lock files."""

    array_lock = ".arrlock"
    array_read_lock = ".arrayreadlock"
    collection_lock = ".lock"
    varray_lock = ".varraylock"


class LocksTypes(str, Enum):
    """Locks enum."""

    array_lock = "array creation lock"
    array_read_lock = "array read lock"
    collection_lock = "collection creation lock"
    varray_lock = "varray write lock"


class ArrayType(str, Enum):
    array = "array"
    varray = "varray"

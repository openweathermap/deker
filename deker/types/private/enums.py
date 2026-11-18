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

__all__ = ("DTypeEnum", "DimensionType", "LocksExtensions", "LocksTypes", "PLATFORM_DEPENDENT_DTYPES")

# the dtype names NumPy provides only where long double is 128 bits wide: Linux and Intel macOS,
# not Apple silicon or Windows; DTypeEnum has them only there
PLATFORM_DEPENDENT_DTYPES = ("float128", "complex256")


class DTypeEnum(Enum):
    """Mapping of the names of the data types to the types, as the names are stored in the metadata.

    The numeric types are the dtypes of the arrays; ``string``, ``datetime`` and ``tuple`` are the types of
    attributes only. The NumPy aliases removed in NumPy 2 (``cfloat``, ``complex_``, ``longfloat`` and so on)
    map to their canonical types, so the schemas written with them still read. ``float128`` and ``complex256``
    exist only where NumPy provides them (Linux and Intel macOS, not Apple silicon or Windows).
    """

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
    cfloat = np.cdouble
    cdouble = np.cdouble
    float32 = np.float32
    clongfloat = np.clongdouble
    float64 = np.float64
    double = np.double
    complex64 = np.complex64
    singlecomplex = np.csingle
    complex128 = np.complex128
    complex_ = np.complex128
    longcomplex = np.clongdouble
    string = str
    datetime = datetime
    tuple = tuple
    # The members with the value numpy.longdouble come last, in one block, because the first of them gives the
    # name stored in the metadata: float128 where NumPy provides it (Linux, Intel macOS), as the files of 1.x
    # have it, and longdouble elsewhere (Apple silicon, Windows). NumPy has float128 and complex256 only where
    # long double is 128 bits wide; complex256 is an alias of clongfloat, as it was in 1.x. longdouble refers
    # to float128 so that a sorter of the class body (ssort) keeps the conditional member before it.
    if hasattr(np, "float128"):
        float128 = np.float128
    longdouble = float128 if hasattr(np, "float128") else np.longdouble
    longfloat = np.longdouble
    if hasattr(np, "complex256"):
        complex256 = np.complex256

    @staticmethod
    def get_name(object: "DTypeEnum") -> str:
        """Return the name a type is stored under in the metadata.

        :param object: DtypeEnum object
        :returns: dtype name, with the ``numpy.`` prefix for numpy types
        """
        if object.value in (int, float, complex, str, tuple, datetime):
            return str(object.name)
        return f"numpy.{object.name}"


class DimensionType(str, Enum):
    """Enum of dimensions' types."""

    generic = "generic"
    time = "time"


class LocksExtensions(str, Enum):
    """Extensions of the lock files: the collection lock, and the markers of VArray writes of Deker < 2."""

    collection_lock = ".lock"
    varray_lock = ".varraylock"  # markers of Deker < 2; TODO: remove in 3.0.0 with Client.clear_locks


class LocksTypes(str, Enum):
    """Names of the lock files as the lock listing reports them."""

    collection_lock = "collection creation lock"
    varray_lock = "varray write lock"  # markers of Deker < 2; TODO: remove in 3.0.0 with Client.clear_locks


class ArrayType(str, Enum):
    """The two kinds of arrays."""

    array = "array"
    varray = "varray"

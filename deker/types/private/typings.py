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

"""Module for using in type annotations. Not for direct initialization."""

from datetime import datetime
from types import EllipsisType, NoneType
from typing import TYPE_CHECKING, TypeVar, Union

import numpy as np

from .classes import ArrayPosition, ArraysCoordinatesWithOffset

if TYPE_CHECKING:
    from deker.arrays import Array, VArray

__all__ = (
    "ArrayT",
    "EllipsisType",
    "NoneType",
    "Arrays",
    "ArraysCoordinates",
    "ArraysInDimension",
    "Slice",
    "FancySlice",
    "MetaDataType",
    "Data",
    "Numeric",
    "NumericDtypes",
)

NumericDtypes = [
    int,
    float,
    complex,
    np.int8,
    np.byte,
    np.int16,
    np.short,
    np.int32,
    np.intc,
    np.int64,
    np.intp,
    np.int_,  # alias for np.compat.long, deprecated in numpy version 1.25, equals to np.int64
    np.longlong,
    np.uint,
    np.uint8,
    np.ubyte,
    np.uint16,
    np.ushort,
    np.uint32,
    np.uintc,
    np.uint64,
    np.uintp,
    np.ulonglong,
    np.float16,
    np.cdouble,
    np.cdouble,
    np.float32,
    np.clongdouble,
    np.float64,
    np.double,
    np.float128,
    np.longdouble,
    np.longdouble,
    np.complex64,
    np.csingle,
    np.complex128,
    np.complex128,
    np.complex256,
    np.clongdouble,
]

Numeric = Union[
    int,
    float,
    complex,
    np.int8,
    np.byte,
    np.int16,
    np.short,
    np.int32,
    np.intc,
    np.int64,
    np.intp,
    np.int_,  # alias for np.compat.long, deprecated in numpy version 1.25, equals to np.int64
    np.longlong,
    np.uint,
    np.uint8,
    np.ubyte,
    np.uint16,
    np.ushort,
    np.uint32,
    np.uintc,
    np.uint64,
    np.uintp,
    np.ulonglong,
    np.float16,
    np.cdouble,
    np.cdouble,
    np.float32,
    np.clongdouble,
    np.float64,
    np.double,
    np.float128,
    np.longdouble,
    np.longdouble,
    np.complex64,
    np.csingle,
    np.complex128,
    np.complex128,
    np.complex256,
    np.clongdouble,
]

Data = Union[list, tuple, np.ndarray, Numeric]

# the kind of arrays a collection, a manager or an adapter works with: decided by the collection schema
ArrayT = TypeVar("ArrayT", "Array", "VArray")
MetaDataType = Union[int, float, str, datetime, Data]
# what ``np.index_exp[...]`` produces: a single index or a tuple of them
Slice = Union[
    slice,
    EllipsisType,
    int,
    tuple[Union[slice, int, EllipsisType, None], ...],
]

FancySlice = Union[
    slice,
    EllipsisType,
    int,
    float,
    str,
    datetime,
    None,
    tuple[Union[slice, int, float, str, datetime, EllipsisType, None], ...],
]

ArraysInDimension = list[ArraysCoordinatesWithOffset]
ArraysCoordinates = list[ArraysInDimension]
Arrays = list[ArrayPosition]

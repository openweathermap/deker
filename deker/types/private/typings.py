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
from typing import TYPE_CHECKING, List, Tuple, Type, Union

import numpy as np

from numpy.lib.index_tricks import IndexExpression

from .classes import ArrayPosition, ArraysCoordinatesWithOffset


__all__ = (
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

EllipsisType = type(...)
NoneType = type(None)

if TYPE_CHECKING:
    EllipsisType = Type[Ellipsis]

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
    np.cfloat,
    np.cdouble,
    np.float32,
    np.clongfloat,
    np.float64,
    np.double,
    np.float128,
    np.longfloat,
    np.longdouble,
    np.complex64,
    np.singlecomplex,
    np.complex128,
    np.complex_,
    np.complex256,
    np.longcomplex,
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
    np.cfloat,
    np.cdouble,
    np.float32,
    np.clongfloat,
    np.float64,
    np.double,
    np.float128,
    np.longfloat,
    np.longdouble,
    np.complex64,
    np.singlecomplex,
    np.complex128,
    np.complex_,
    np.complex256,
    np.longcomplex,
]

Data = Union[list, tuple, np.ndarray, Numeric]
MetaDataType = Union[int, float, str, datetime, Data]
Slice = Union[
    IndexExpression,
    slice,
    EllipsisType,  # type: ignore
    int,
    Tuple[Union[slice, int, EllipsisType, None], ...],  # type: ignore
]

FancySlice = Union[
    IndexExpression,
    slice,
    EllipsisType,  # type: ignore
    int,
    float,
    str,
    datetime,
    None,
    Tuple[Union[IndexExpression, slice, int, float, str, datetime, EllipsisType, None], ...],  # type: ignore
]

ArraysInDimension = List[ArraysCoordinatesWithOffset]
ArraysCoordinates = List[ArraysInDimension]
Arrays = List[ArrayPosition]

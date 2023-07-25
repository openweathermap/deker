"""Module for using in type annotations. Not for direct initialization."""
from datetime import datetime
from typing import TYPE_CHECKING, List, Tuple, Type, Union

import numpy

from numpy.lib.index_tricks import IndexExpression

from .classes import ArrayPosition, ArraysCoordinatesWithOffset


__all__ = (
    "EllipsisType",
    "NoneType",
    "Labels",
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
    numpy.int_,  # alias for np.compat.long, deprecated in numpy version 1.25, equals to np.int64
    numpy.int8,
    numpy.int16,
    numpy.int32,
    numpy.int64,
    numpy.float16,
    numpy.float64,
    numpy.float128,
    numpy.longfloat,
    numpy.double,
    numpy.longdouble,
    numpy.complex64,
    numpy.complex128,
    numpy.complex256,
    numpy.longcomplex,
    numpy.longlong,
]

Numeric = Union[
    int,
    float,
    complex,
    numpy.int_,  # alias for np.compat.long, deprecated in numpy version 1.25, equals to np.int64
    numpy.int8,
    numpy.int16,
    numpy.int32,
    numpy.int64,
    numpy.float16,
    numpy.float64,
    numpy.float128,
    numpy.longfloat,
    numpy.double,
    numpy.longdouble,
    numpy.complex64,
    numpy.complex128,
    numpy.complex256,
    numpy.longcomplex,
    numpy.longlong,
]

Data = Union[list, tuple, numpy.ndarray, Numeric]
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
Labels = Union[Tuple[Union[str, int, float], ...], List[Union[str, int, float]]]

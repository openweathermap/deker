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
    np.int_,  # alias for np.compat.long, deprecated in numpy version 1.25, equals to np.int64
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.float16,
    np.float64,
    np.float128,
    np.longfloat,
    np.double,
    np.longdouble,
    np.complex64,
    np.complex128,
    np.complex256,
    np.longcomplex,
    np.longlong,
]

Numeric = Union[
    int,
    float,
    complex,
    np.int_,  # alias for np.compat.long, deprecated in numpy version 1.25, equals to np.int64
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.float16,
    np.float64,
    np.float128,
    np.longfloat,
    np.double,
    np.longdouble,
    np.complex64,
    np.complex128,
    np.complex256,
    np.longcomplex,
    np.longlong,
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

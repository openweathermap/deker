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

"""Abstract interfaces for subset and virtual subset."""

from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Generic

import numpy as np

from deker_tools.slices import slice_converter
from deker_tools.typings import Sentinel, SentinelType

from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerSubsetError
from deker.log import SelfLoggerMixin
from deker.tools import check_memory
from deker.tools.time import get_start_datetime
from deker.types.private.typings import ArrayT, Data, Numeric

xarray_import_error: ImportError | None
try:
    from xarray import DataArray
except ImportError:  # pragma: no cover
    xarray_import_error = ImportError(
        "No module named 'xarray' found (more likely due to it is not installed): "
        "try running `pip install xarray' or 'pip install deker[xarray]'"
    )
else:
    xarray_import_error = None

if TYPE_CHECKING:
    from deker.ABC.base_adapters import IArrayAdapter


class BaseSubset(SelfLoggerMixin, ABC, Generic[ArrayT]):
    """A subset of Array or VArray data with bounds.

    It is the final object that can read, update and clear the data.
    ``ArrayT`` is the kind of array the subset is bound to: ``Array`` or ``VArray``.
    """

    __slots__ = (
        "__bounds",
        "__shape",
        "__array",
        "__adapter",
    )

    def __init__(
        self,
        slice_expression: tuple[slice | int, ...],
        shape: tuple[int, ...],
        array: ArrayT,
        adapter: "IArrayAdapter[Any, ArrayT]",
    ):
        """BaseSubset constructor.

        :param slice_expression: a slice, tuple of slices or numpy index expression, created by ``__getitem__``
        :param shape: subset shape, calculated by ``__getitem__``
        :param array: the array the subset is bound to
        :param adapter: the adapter of that array
        """
        self.__bounds = slice_expression
        self.__shape = shape
        self.__array: ArrayT = array
        self.__adapter: IArrayAdapter[Any, ArrayT] = adapter

    @property
    def shape(self) -> tuple[int, ...]:
        """Subset shape: the shape of the data ``read()`` returns; an integer index drops its axis."""
        return self.__shape

    @property
    def bounds(self) -> tuple[slice | int, ...]:
        """Subset bounds: the normalized index expression, one integer or slice per axis of the array."""
        return self.__bounds

    @property
    def dtype(self) -> type[Numeric]:
        """Subset dtype."""
        return self.__array.dtype

    @property
    def fill_value(self) -> Numeric:
        """Get subset fill_value."""
        return self.__array.fill_value

    @abstractmethod
    def read(self) -> Numeric | np.ndarray | None:
        """Read data from array slice.

        :returns: the data of the subset shape; a number for a single cell
        """
        pass

    def read_xarray(self) -> "DataArray":
        """Read data from Array as xarray.DataArray.

        DataArray is an object of `xarray` (https://docs.xarray.dev) library,
        which contains subset data and its description. It provides conversion
        to different formats including pandas.Dataframe, netCDF, Zarr and many others.

        However, `xarray` library is not a Deker's dependency. If you wish to use
        this method you shall install xarray manually or run `pip install deker[xarray]`.

        Subset's description is made before the data is read.
        Right after the description is created - memory checker is invoked.
        If your RAM is insufficient for reading data - `DekerMemoryError` is raised.

        Warning: final xarray.DataArray data shape may differ from subset shape: an axis selected by an integer
        index is kept with the length of one.

        :returns: the subset data with the coordinates of its dimensions and the attributes of the array
        :raises ImportError: if xarray is not installed
        :raises DekerSubsetError: for a subset of a single cell
        :raises DekerMemoryError: if the data does not fit the memory limit of the client
        """
        if xarray_import_error:
            raise xarray_import_error
        subset_name = self.__class__.__name__
        if self.shape == ():
            raise DekerSubsetError(f"Cannot convert a scalar deker.{subset_name} to xarray.DataArray")
        description = self.describe()
        dims = list(description)
        new_shape = tuple(len(description[dim]) for dim in description)
        check_memory(self.shape, self.dtype, self.__adapter.ctx.config.memory_limit)
        data = self.read()
        if not isinstance(data, np.ndarray):  # pragma: no cover
            raise DekerSubsetError(f"Cannot convert {type(data)} data of deker.{subset_name} to xarray.DataArray")
        data = data.reshape(new_shape)

        data_array = DataArray(
            name=f"Deker {subset_name} of {self.__array.__class__.__name__}(id={self.__array.id}) "
            f"from collection `{self.__array.collection}`",
            data=data,
            dims=dims,
            coords=description,
            attrs={
                "primary_attributes": self.__array.primary_attributes,
                "custom_attributes": self.__array.custom_attributes,
            },
        )
        return data_array

    @abstractmethod
    def update(self, data: Data, *, timeout: int | SentinelType = Sentinel) -> None:
        """Update data in array by slice.

        :param data: new data of the subset shape, or a number that fills the whole subset
        :param timeout: seconds to wait for the write lock of each ``Array``; not given - ``write_lock_timeout``
          of the client
        """
        pass

    @abstractmethod
    def clear(self, *, timeout: int | SentinelType = Sentinel) -> None:
        """Set the cells of the subset to the fill value.

        :param timeout: seconds to wait for the write lock of each ``Array``; not given - ``write_lock_timeout``
          of the client
        """
        pass

    def _is_deleted(self) -> bool:
        """Check if the array of the subset was deleted from the storage.

        :returns: ``True`` if the array does not exist anymore
        """
        return self.__adapter.is_deleted(self.__array)

    @staticmethod
    def __generate_full_scale(dimension: Dimension) -> list:
        """Generate full scale of scaled dimension.

        Floats, autogenerated with np.arange, may differ both from set step
        and from expected final quantity:

         - np.arange(1.5, 1.6, 0.1)
            array([1.5, 1.6]) (expected just [1.5])

         - np.arange(-1.0, 0.5, 0.1)
            array([-1.00000000e+00, -9.00000000e-01, -8.00000000e-01, -7.00000000e-01,
            -6.00000000e-01, -5.00000000e-01, -4.00000000e-01, -3.00000000e-01,
            -2.00000000e-01, -1.00000000e-01, -2.22044605e-16,  1.00000000e-01,
             2.00000000e-01,  3.00000000e-01,  4.00000000e-01])

             (expected just array([-1.0, -0.9, -0.8, -0.7, -0.6,
                            -0.5, -0.4, -0.3, -0.2, -0.1,
                            0.0,  0.1, 0.2,  0.3,  0.4]))

        Regarding that all values are floats, we shall generate them carefully
        and try to round their fractional part to digits quantity similar
        to start_value attribute's fractional part.

        :param dimension: dimension with non-empty `scale` attribute
        :returns: scale values of the whole dimension
        """
        scale = dimension.scale
        if scale is None:  # pragma: no cover
            raise DekerSubsetError(f"Dimension {dimension.name} has no scale to describe")
        step = scale.step
        start_value = scale.start_value
        end = start_value + dimension.size * step
        rounder = len(str(step).split(".")[-1])
        value = np.around(np.arange(start_value, end, step), rounder)
        return value.tolist()[: dimension.size]

    def __describe_full_dimension(self, dimension: TimeDimension | Dimension) -> list:
        """Generate dimension's full range of descriptive values.

        :param dimension: dimension of any Deker type
        :returns: descriptive values of the whole dimension
        """
        if isinstance(dimension, TimeDimension):
            start_value = get_start_datetime(dimension)
            value = [start_value + n * dimension.step for n in range(dimension.size)]

        elif dimension.labels:
            value = list(dimension.labels)
        elif dimension.scale:
            value = self.__generate_full_scale(dimension)
        else:  # just indexes
            value = list(range(0, dimension.size, dimension.step))
        return value

    def __describe_position(self, dimension: TimeDimension | Dimension, position: int) -> Any:
        """Descriptive value of one position of a dimension.

        :param dimension: dimension of any Deker type
        :param position: index on the dimension, non-negative
        :returns: the datetime, label, scale value or index of the position
        """
        if isinstance(dimension, TimeDimension):
            return get_start_datetime(dimension) + dimension.step * position
        if dimension.labels:
            return dimension.labels[position]
        if dimension.scale:
            return self.__generate_full_scale(dimension)[position]
        return position

    def __describe_slice(self, dimension: TimeDimension | Dimension, bound: slice) -> list:
        """Generate dimension's range of descriptive values by slice bounds.

        The values follow the positions the slice selects, in their order: a step skips positions and
        a negative step walks backwards.

        :param dimension: dimension of any Deker type
        :param bound: slice object
        :returns: descriptive values within the slice bounds
        """
        if bound == slice(None, None, None):
            return self.__describe_full_dimension(dimension)
        positions = range(*bound.indices(dimension.size))
        if isinstance(dimension, TimeDimension):
            first = get_start_datetime(dimension)
            return [first + dimension.step * position for position in positions]
        if dimension.labels:
            return list(dimension.labels[bound])
        if dimension.scale:
            return self.__generate_full_scale(dimension)[bound]
        return list(positions)

    def describe(self) -> OrderedDict:
        """Describe subset data: the values of every axis the subset covers, in the order of the bounds.

        A time axis is described by datetimes, a labelled one by its labels, a scaled one by the values of the
        scale, a plain one by its indices. A slice with a step lists the positions it selects, backwards for a
        negative step; an integer index gives a list of one value.

        You shall remember that once it is described, your physical memory becomes reduced
        by the size of all descriptive values of all the subset's dimensions.
        In other words, it equals (more or less) to the size of your data that you are going to manage.
        And if you keep this description in memory, your RAM may become insufficient for data managing.

        So, we highly recommend using this method in debug or deleting its result manually right before
        invocation of any data-managing methods.

        But still subset's description is automatically created and inserted to xarray.DataArray
        on `read_xarray()` call.

        :returns: dimension name -> its values within the subset
        """
        description = OrderedDict()
        for n, bound in enumerate(self.bounds):
            dimension = self.__array.dimensions[n]
            if isinstance(bound, slice):
                value = self.__describe_slice(dimension, bound)
            else:
                # convert bound value to positive integer
                bound = bound if bound >= 0 else dimension.size + bound
                value = [self.__describe_position(dimension, bound)]
            description.update({dimension.name: value})

        # add full description for dimensions if they were excluded from bounds
        dims = self.__array.dimensions
        if len(self.bounds) < len(dims):
            start_idx = len(dims) - (len(dims) - len(self.bounds))
            for dimension in dims[start_idx:]:
                description.update({dimension.name: self.__describe_full_dimension(dimension)})

        return description

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"bounds={slice_converter[self.__bounds]}, shape={self.shape}, array={self.__array.id}"
            f")"
        )

    def __str__(self) -> str:
        return f"{self.__array!s}{slice_converter[self.__bounds]}"

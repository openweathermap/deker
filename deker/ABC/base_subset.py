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

"""Abstract interfaces for subset and virtual subset."""

from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import TYPE_CHECKING, List, Optional, Tuple, Type, TypeVar, Union

import numpy as np

from deker_tools.slices import slice_converter


try:
    from xarray import DataArray

    xarray_import_error = None
except ImportError:
    DataArray = TypeVar("DataArray")
    xarray_import_error = ImportError(
        "No module named 'xarray' found (more likely due to it is not installed): "
        "try running `pip install xarray' or 'pip install deker[xarray]'"
    )

from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerSubsetError
from deker.log import SelfLoggerMixin
from deker.tools import check_memory
from deker.types.private.typings import Data, Numeric


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_array import BaseArray
    from deker.collection import Collection


class BaseSubset(SelfLoggerMixin, ABC):
    """A subset of Array or VArray data with bounds.

    It is the final object that can read, update and clear the data.
    """

    __slots__ = (
        "__bounds",
        "__shape",
        "__array",
        "__adapter",
        "__array_adapter",
        "__collection",
    )

    def __init__(
        self,
        slice_expression: Tuple[Union[slice, int], ...],
        shape: tuple,
        array: "BaseArray",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"] = None,
        collection: Optional["Collection"] = None,
    ):
        """BaseSubset constructor.

        Sets adapters according to subset type: __array_adapter is used in VSubset only.

        :param slice_expression: a slice, tuple of slices or numpy IndexExpression, created by VArray.__getitem__()
        :param shape: subset shape, calculated by VArray.__getitem__()
        :param array: an instance of VArray
        :param array_adapter: ArrayAdapter instance
        :param varray_adapter: VArrayAdapter instances
        :param collection: Collection instance
        """
        self.__bounds = slice_expression
        self.__shape = shape
        self.__array = array
        self.__adapter = varray_adapter if varray_adapter else array_adapter
        self.__array_adapter = array_adapter if varray_adapter else None
        self.__collection = collection

    @property
    def shape(self) -> Tuple[int, ...]:
        """Subset shape."""
        return self.__shape

    @property
    def bounds(self) -> Tuple[Union[slice, int], ...]:
        """Subset bounds."""
        return self.__bounds

    @property
    def dtype(self) -> Type[Numeric]:
        """Subset dtype."""
        return self.__array.dtype

    @property
    def fill_value(self) -> Union[Numeric, type(np.nan)]:  # type: ignore[valid-type]
        """Get subset fill_value."""
        return self.__array.fill_value

    @abstractmethod
    def read(self) -> Union[Numeric, np.ndarray, None]:
        """Read data from array slice."""
        pass

    def read_xarray(self) -> DataArray:
        """Read data from Array as xarray.DataArray.

        DataArray is an object of `xarray` (https://docs.xarray.dev) library,
        which contains subset data and its description. It provides conversion
        to different formats including pandas.Dataframe, netCDF, Zarr and many others.

        However, `xarray` library is not a Deker's dependency. If you wish to use
        this method you shall install xarray manually or run `pip install deker[xarray]`.

        Subset's description is made before the data is read.
        Right after the description is created - memory checker is invoked.
        If your RAM is insufficient for reading data - `DekerMemoryError` is raised.

        Warning: final xarray.DataArray data shape may differ from subset shape
        """
        if xarray_import_error:
            raise xarray_import_error
        subset_name = self.__class__.__name__
        if self.shape == ():
            raise DekerSubsetError(
                f"Cannot convert a scalar deker.{subset_name} to xarray.DataArray"
            )
        description = self.describe()
        dims = list(description)
        new_shape = tuple(len(description[dim]) for dim in description)
        check_memory(self.shape, self.dtype, self.__adapter.ctx.config.memory_limit)
        data = self.read()
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
    def update(self, data: Data) -> None:
        """Update data in array by slice.

        :param data: new data which shall match subset slicing
        """
        pass

    @abstractmethod
    def clear(self) -> None:
        """Clear data in array by slice."""
        pass

    def _is_deleted(self) -> bool:
        """Check if array was deleted."""
        return self.__adapter.is_deleted(self.__array)

    @staticmethod
    def __generate_full_scale(dimension: Dimension) -> List:
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
        """
        step = dimension.scale.step
        start_value = dimension.scale.start_value
        end = start_value + dimension.size * step
        rounder = len(str(step).split(".")[-1])
        value = np.around(np.arange(start_value, end, dimension.scale.step), rounder)
        return value.tolist()[: dimension.size]  # type: ignore[return-value]

    def __describe_full_dimension(self, dimension: Union[TimeDimension, Dimension]) -> list:
        """Generate dimension's full range of descriptive values.

        :param dimension: dimension of any Deker type
        """
        if isinstance(dimension, TimeDimension):
            value = [
                dimension.start_value + n * dimension.step for n in range(dimension.size)  # type: ignore[operator]
            ]

        elif dimension.labels:
            value = list(dimension.labels)
        elif dimension.scale:
            value = self.__generate_full_scale(dimension)
        else:  # just indexes
            value = list(range(0, dimension.size, dimension.step))
        return value

    def __describe_slice(self, dimension: Union[TimeDimension, Dimension], bound: slice) -> list:
        """Generate dimension's range of descriptive values by slice bounds.

        :param dimension: dimension of any Deker type
        :param bound: slice object
        """
        if bound == slice(None, None, None):
            return self.__describe_full_dimension(dimension)

        # convert slice start value to positive integer
        if bound.start is None:
            start = 0
        elif bound.start >= 0:
            start = bound.start
        else:
            start = dimension.size + bound.start

        # convert slice stop value to positive integer
        if bound.stop is None:
            stop = dimension.size
        elif bound.stop >= 0:
            stop = bound.stop
        else:
            stop = dimension.size + bound.stop

        # ignore slice step value
        step = dimension.step

        if isinstance(dimension, TimeDimension):
            start_value = dimension.start_value + step * start  # type: ignore[operator]
            end = stop - start
            value = [start_value + n * step for n in range(end)]  # type: ignore[operator]

        elif dimension.labels:
            value = list(dimension.labels[bound])
        elif dimension.scale:
            value = self.__generate_full_scale(dimension)[bound]
        else:  # just indexes
            start_value = start if start is not None else 0
            end = stop
            value = list(range(start_value, end, step))  # type: ignore[arg-type]
        return value

    def describe(self) -> OrderedDict:
        """Describe subset data.

        You shall remember that once it is described, your physical memory becomes reduced
        by the size of all descriptive values of all the subset's dimensions.
        In other words, it equals (more or less) to the size of your data that you are going to manage.
        And if you keep this description in memory, your RAM may become insufficient for data managing.

        So, we highly recommend using this method in debug or deleting its result manually right before
        invocation of any data-managing methods.

        But still subset's description is automatically created and inserted to xarray.DataArray
        on `read_xarray()` call.

        If your RAM is insufficient for reading data - DekerMemoryError is raised.
        """
        description = OrderedDict()
        for n, bound in enumerate(self.bounds):
            dimension = self.__array.dimensions[n]
            if isinstance(bound, slice):
                value = self.__describe_slice(dimension, bound)
            else:
                # convert bound value to positive integer
                bound = bound if bound >= 0 else dimension.size + bound

                if isinstance(dimension, TimeDimension):
                    value = [dimension.start_value + dimension.step * bound]  # type: ignore[operator]

                elif dimension.labels:
                    value = [dimension.labels[bound]]
                elif dimension.scale:
                    value = [dimension.scale.start_value + bound * dimension.scale.step]
                else:  # just index
                    value = [bound]
            description.update({dimension.name: value})

        # add full description for dimensions if they were excluded from bounds
        dims = self.__array.dimensions
        if len(self.bounds) < len(dims):  # type: ignore[arg-type]
            start_idx = len(dims) - (len(dims) - len(self.bounds))  # type: ignore[arg-type]
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

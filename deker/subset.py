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

import builtins
import traceback

from typing import TYPE_CHECKING, Iterator, List, Optional, Tuple, Union

import numpy as np

from deker_tools.slices import create_shape_from_slice, match_slice_size, slice_converter
from numpy import ndarray

from deker.ABC.base_subset import BaseSubset
from deker.errors import DekerArrayError, DekerVSubsetError
from deker.locks import WriteVarrayLock
from deker.schemas import TimeDimensionSchema
from deker.tools import not_deleted
from deker.types.private.classes import (
    ArrayOffset,
    ArrayPosition,
    ArrayPositionedData,
    ArraysCoordinatesWithOffset,
)
from deker.types.private.typings import Arrays, ArraysCoordinates, Data, Numeric, Slice


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.arrays import Array, VArray
    from deker.collection import Collection
    from deker.dimensions import TimeDimension


class Subset(BaseSubset):
    """``Array`` subset.

    A subset of the ``Array`` data with set ``bounds``, ``shape``, ``dtype`` and ``fill_value``.

    It is final ``lazy`` object that can ``read``, ``update`` and ``clear`` the data within the ``Array``.
    Once created, it does not contain any data and does not access the storage until user manually invokes
    one of the subset API methods. If you need to get and manage all the data from the array you should create
    a subset with ``Array[:]`` or ``Array[...]``.

    Properties
    ----------
    - ``shape``: returns shape of the subset
    - ``bounds``: returns the bounds that were applied to the ``Array``
    - ``dtype``: returns type of the queried data
    - ``fill_value``: returns the value that fills empty cells instead of None

    API methods
    -----------

    - ``describe``: returns an OrderedDict with description of all the subset's dimensions
    - ``read``: returns a numpy ndarray with all the data from the Subset bounds

      .. note:: If the Subset or Array is empty, a numpy ndarray of ``fill_values`` will be returned

      .. warning:: Mind your RAM!

    - ``read_xarray``: returns an ``xarray.DataArray`` with data returned by `read` method and its description.

      .. attention:: Scalar data cannot be converted to ``xarray.DataArray``

    - ``update``: writes new data to the storage;

      .. note::
         * Data cannot be ``None``
         * Data ``shape`` should be equal to the ``Subset.shape``
         * Data ``dtype`` should be equal to the ``Array.dtype``

    - ``clear``: removes or resets with `fill_value` all data from the storage within the subset bounds;
    """

    __slots__ = (
        "__bounds",
        "__shape",
        "__array",
        "__adapter",
    )

    def __init__(
        self,
        slice_expression: Tuple[Union[slice, int], ...],
        shape: Tuple[int, ...],
        array: "Array",
        adapter: "BaseArrayAdapter",
    ):
        """Subset initialization.

        :param slice_expression: a slice, tuple of slices or numpy IndexExpression, created by ``Array.__getitem__()``
        :param shape: subset shape, calculated by ``Array.__getitem__()``
        :param array: an instance of ``Array`` to which the subset is bound
        :param adapter: ``Array`` adapter instance
        """
        self.__bounds = slice_expression
        self.__shape = shape
        self.__array = array
        self.__adapter = adapter
        super().__init__(slice_expression, shape, array, adapter)
        self.logger.debug(f"{self.__str__} instantiated")

    @not_deleted
    def read(self) -> Union[Numeric, np.ndarray]:
        """Read data from ``Array`` slice."""
        self.logger.debug(
            f"Trying to read data from {self.__array.id} bounds={slice_converter[self.__bounds]}"
        )
        data = self.__adapter.read_data(self.__array, self.__bounds)
        self.logger.info(f"{self!s} data read")
        return data

    @not_deleted
    def update(self, data: Data) -> None:
        """Update data in ``Array`` by slice.

        * Data cannot be ``None``
        * Data ``shape`` should be equal to the ``Subset.shape``
        * Data ``dtype`` should be equal to the ``Array.dtype``

        :param data: new data which shall match subset slicing
        """
        self.logger.debug(
            f"Trying to update data for {self.__array.id} bounds={slice_converter[self.__bounds]}"
        )
        if data is None:
            raise DekerArrayError("Updating data shall not be None")
        self.__adapter.update(self.__array, self.__bounds, data)
        self.logger.info(f"{self!s} data updated")

    @not_deleted
    def clear(self) -> None:
        """Clear data in ``Array`` by slice."""
        self.logger.debug(
            f"Trying to clear data for {self.__array.id} bounds={slice_converter[self.__bounds]}"
        )
        self.__adapter.clear(self.__array, self.__bounds)
        self.logger.info(f"{self!s} data cleared")


class VSubset(BaseSubset):
    """``VArray`` subset.

    A subset of ``VArray`` data with set ``bounds``, ``shape``, ``dtype`` and ``fill_value``.

    It is final ``lazy`` object that can ``read``, ``update`` and ``clear`` the data within the ``VArray``.
    Once created, it does not contain any data and does not access the storage until user manually invokes
    one of the virtual subset API methods. If you need to get and manage all the data from array you shall
    create a virtual subset with ``VArray[:]`` or ``VArray[...]``.

    Properties
    ----------
    - ``shape``: returns shape of the virtual subset
    - ``bounds``: returns bounds that were applied to ``VArray``
    - ``dtype``: returns type of queried data
    - ``fill_value``: returns value that fills empty cells instead of None

    API methods
    -----------

    - ``describe``: returns an OrderedDict with description of all VSubset's dimensions
    - ``read``: returns a numpy ndarray with all the data from the VSubset bounds

      .. note:: If the VSubset or VArray is empty, a numpy ndarray of ``fill_values`` will be returned

      .. warning:: Mind your RAM!

    - ``read_xarray``: returns an ``xarray.DataArray`` with data returned by `read` method and its description.

      .. attention:: Scalar data cannot be converted to ``xarray.DataArray``

    - ``update``: writes new data to the storage;

      .. note::
         * Data cannot be ``None``
         * Data ``shape`` shall be equal to the ``VSubset.shape``
         * Data ``dtype`` shall be equal to the ``VArray.dtype``

    - ``clear``: removes or resets with `fill_value` all data from the storage within the virtual subset bounds;

    :param slice_expression: a slice, tuple of slices or numpy IndexExpression, created by VArray.__getitem__()
    :param shape: subset shape, calculated by VArray.__getitem__()
    :param array: an instance of VArray to which the virtual subset is bound
    :param array_adapter: ArrayAdapter instance
    :param collection: Collection instance, to which the virtual array is bound
    """

    __slots__ = (
        "__bounds",
        "__shape",
        "__array",
        "__adapter",
        "__array_adapter",
        "__collection",
        "__arrays",
    )

    def __match_slice_exp(
        self, slice_exp: Union[slice, int, None], current_index: int, array: "VArray"
    ) -> ArraysCoordinates:
        """Create a list of vgrid indexes by expression.

        :param slice_exp: Slice expression (e.g 1,  slice(None, 2, 1), EllipsisType)
        :param current_index: current index of dimension
        :param array: Varray object
        """
        # Size of dimension
        dim_size = array.dimensions[current_index].size

        # Step in vgrid
        step = dim_size // array.vgrid[current_index]

        def get_offset(
            i: int, step: int, offset_start: int, start: int, offset_end: int, end: int
        ) -> ArrayOffset:
            """Calculate an offset for array.

            If it's first array, and there are more arrays in result, return offset from start and zero.
            If it's only one array, offset from end would be equal to the one, which was provided.
            For last array start offset is always zero, and offset form end would be equal
            to the one, which was provided. For any array in between, offsets would be zeroes.

            :param i: current array start point. i % step always equals 0
            :param offset_start: offset from start
            :param offset_end: offset from end
            :param step: step of vgrid
            :param start: start of current array( with offset)
            :param end: start of current array( with offset)
            """
            if i == start - offset_start:
                if end < i + step:
                    return {"start": offset_start, "end": step + offset_end}
                return {"start": offset_start, "end": 0}

            if i == end - offset_end - step and end < i + step:
                return {"start": 0, "end": step + offset_end}
            return {"start": 0, "end": 0}

        if type(slice_exp) is builtins.int:
            # We may receive an int.
            # In this case, the number of arrays is always 1.
            # We return it as a list of one element, as the upper function does not know anything about type
            # so just return it in the same format to avoid unnecessary checking
            return [
                [
                    ArraysCoordinatesWithOffset(
                        slice_exp // step,  # type: ignore[operator]
                        ArrayOffset(
                            start=slice_exp % step,  # type: ignore[operator]
                        ),
                    )
                ]
            ]

        # If its None at slice_exp
        # Get start of slice, and end of slice
        start, stop, _ = match_slice_size(dim_size, slice_exp)  # type: ignore[arg-type]
        # Offset from start
        offset_start = start % step
        # Offset from end (e.g. for step = 5, and stop = 10, offset would be 0
        # as slice ends on the same point as stop )
        offset_end = -(step - (stop % step)) if stop % step else 0
        # We subtract offsets to get start and end points which are divided to parts
        # by step (e.g. if there is 2 arrays, step is 20 and start is 12 and stop is 35,
        # to get the correct number of Arrays, we should start from 0, go up to 40 with given
        # step)
        return [
            [
                ArraysCoordinatesWithOffset(
                    i // step,
                    ArrayOffset(**get_offset(i, step, offset_start, start, offset_end, stop)),
                )
            ]
            for i in range(start - offset_start, stop - offset_end, step)
        ]

    def __get_arrays_for_dimension(
        self, slice_exp: List[Union[slice, int]], array: "VArray", current_index: int
    ) -> ArraysCoordinates:
        """Calculate which Arrays are in the given dimension.

        For every dimension, we calculate indexes of arrays for current dimension with offset
        using self.__match_slice_exp method, which gives us following:
        [[
          (<position in dimension>, (<offset from start>, <offset from end>)),
          (<position2 in dimension>, (<offset from start>, <offset from end>)),
          ...
        ]]
        After this, for every position, we create variations for rest of dimensions recursively.
        So [[(<position in dimension>, (<offset from start>, <offset from end>)), ...]], became
        [[
            (<position in dimension>, (<offset from start>, <offset from end>)),
            (<position in dimension2>, (<offset from start>, <offset from end>))
        ]]

        :param slice_exp: Slice expression (e.g [1, 2], [1, slice(None, 2, 1)], [EllipsisType])
        :param current_index: current index of dimension
        :param array: Varray object
        :return: [
            [
              (<position in dimension1>, (<offset from start>, <offset from end>)),
              (<position in dimension2>, (<offset from start>, <offset from end>)),
              ...
            ],
            [
              (<position2 in dimension1>, (<offset from start>, <offset from end>)),
              (<position2 in dimension2>, (<offset from start>, <offset from end>)),
              ...
            ]
        ]
        """
        # Exit point for recursion. If we are at last dimension, there is no more recursion.
        arrays = []
        if len(slice_exp) == 1:
            return self.__match_slice_exp(slice_exp[0], current_index, array)

        #  arrays_tuple contains a list with arrays positions like
        #  [[
        #    (< position in dimension >, ( < offset from start >, < offset from end >)),
        #    (< position2 in dimension >, ( < offset from start >, < offset from end >)),
        #    ...
        #  ]]
        arrays_tuple = self.__match_slice_exp(slice_exp[0], current_index, array)
        for array_tuple in arrays_tuple:
            # For every position, we create list with arrays indexes for left dimensions.
            # E.g:
            #  If the number of dimensions is 3, and the current dimension index is 0
            # It will create a list with positions for left 2 dimensions
            variations = self.__get_arrays_for_dimension(slice_exp[1:], array, current_index + 1)
            # variations example:
            # [[
            #    (< position in dimension1>, ( < offset from start >, < offset from end >)),
            #    (< position in dimension2 >, ( < offset from start >, < offset from end >))
            # ], ..]
            for variation in variations:
                # Concatenate every variation of dimension to current position
                arrays.append([*array_tuple, *variation])

        return arrays

    def __fill_slice_expression(
        self, array: "VArray", slice_exp: Tuple[Union[slice, None, int], ...]
    ) -> List[Union[slice, int]]:
        """Fill slice expression.

         The number of elements in slice_exp would be to equal number of dimensions.
        :param array: VArray instance
        :param slice_exp: tuple of ints or slices
        """
        dimension_given_length = len(slice_exp)

        slice_exp_ = []
        for i in range(len(array.dimensions)):
            slice_to_add = slice(None, None)
            if i < dimension_given_length:
                if isinstance(slice_exp[i], slice):
                    slice_to_add = slice_exp[i]
                elif isinstance(slice_exp[i], int):
                    slice_to_add = slice_exp[i]
                    if slice_exp[i] < 0:  # type: ignore[operator]
                        slice_to_add = (  # type: ignore[operator]
                            array.dimensions[i].size + slice_exp[i]  # type: ignore[operator]
                        )

            slice_exp_.append(slice_to_add)
        return slice_exp_  # type: ignore[return-value]

    def __get_array_subsets(
        self, slice_exp: Tuple[Union[slice, None, int], ...], array: "VArray"
    ) -> Arrays:
        """Calculate which Arrays are in the given subset.

        :param slice_exp: Slice expression (e.g [1, 2], [1, slice(None, 2, 1)], [EllipsisType])
        :param array: Varray instance
        """
        filled_slice_expression = self.__fill_slice_expression(array, slice_exp)
        arrays = self.__get_arrays_for_dimension(filled_slice_expression, array, 0)

        # Reformat to have position and bounds side by side
        array_positions = []

        # Check if dimension index has changed
        prev_position = [-1] * len(filled_slice_expression)
        # Sizes of previous elements
        prev_sizes = [0] * len(filled_slice_expression)
        # Buffer of sizes, writes to prev sizes, only for dimension that has changed
        prev_sizes_buffer = [0] * len(filled_slice_expression)

        def calc_data_slice(
            current_dimension: int, dim: Union[slice, int], array_vposition: list
        ) -> Union[slice, int, None]:
            """Calculate slice for data part.

            :param current_dimension: Index of current dimension
            :param dim: Subset dimension (e.g slice(0, 5))
            :param array_vposition: Position of array
            """
            if isinstance(filled_slice_expression[current_dimension], int):
                prev_position[current_dimension] = array_vposition[current_dimension]
                return None

            if isinstance(dim, int):
                prev_position[current_dimension] = array_vposition[current_dimension]
                return dim

            dim_start, dim_stop, _ = match_slice_size(
                array.dimensions[current_dimension].size // array.vgrid[current_dimension], dim
            )
            positions_length = len(array_vposition)

            if prev_position[current_dimension] == array_vposition[current_dimension]:
                prev_sizes_buffer[current_dimension] = prev_sizes[current_dimension]
            if prev_position[current_dimension] != array_vposition[current_dimension]:
                prev_sizes[current_dimension] = prev_sizes_buffer[current_dimension]

            for i in range(positions_length):
                if prev_position[i] != array_vposition[i] and i < positions_length - 1:
                    prev_sizes_buffer[i + 1 : :] = [0] * (positions_length - i - 1)

            elems_in_dimension = abs(dim_stop - dim_start)
            if array_vposition[current_dimension] == 0:
                prev_sizes[current_dimension] = prev_sizes_buffer[current_dimension] = 0

            slice_start = prev_sizes[current_dimension]
            slice_stop = slice_start + elems_in_dimension
            # what was prev position in that dimension
            prev_position[current_dimension] = array_vposition[current_dimension]
            # how many elements in that dimension
            prev_sizes_buffer[current_dimension] += elems_in_dimension
            return slice(slice_start, slice_stop)

        for array_ in arrays:
            vposition: List[int] = []
            bounds: List[Union[slice, int]] = []
            for index, (position, offset) in enumerate(array_):
                vposition.append(position)
                if "end" not in offset:
                    bounds.append(offset["start"])
                    continue
                start, end = offset["start"], offset["end"]
                if end == 0:
                    end = array.dimensions[index].size // array.vgrid[index]
                    if start == 0:
                        bounds.append(slice(None, None))
                        continue
                bounds.append(slice(start, end))

            dt_slice = [calc_data_slice(i, dim, vposition) for i, dim in enumerate(bounds)]
            array_positions.append(
                ArrayPosition(
                    vposition=tuple(vposition),
                    bounds=tuple(bounds),
                    data_slice=tuple([dt for dt in dt_slice if dt is not None]),
                )
            )

        sorted_positions = sorted(array_positions, key=lambda p: p.vposition)
        return sorted_positions

    def __init__(
        self,
        slice_expression: Tuple[Union[slice, int], ...],
        shape: Tuple[int, ...],
        array: "VArray",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: "BaseVArrayAdapter",
        collection: "Collection",
    ):
        """VSubset constructor.

        Calculates arrays for future reading.

        :param slice_expression: a slice, tuple of slices or numpy IndexExpression, created by VArray.__getitem__()
        :param shape: subset shape, calculated by VArray.__getitem__()
        :param array: an instance of VArray to which the virtual subset is bound
        :param array_adapter: ArrayAdapter instance
        :param varray_adapter: VArrayAdapter instances
        :param collection: Collection instance
        """
        self.__bounds = slice_expression
        self.__shape = shape
        self.__array = array
        self.__adapter: "BaseVArrayAdapter" = varray_adapter
        self.__array_adapter: "BaseArrayAdapter" = array_adapter
        self.__arrays: Arrays = self.__get_array_subsets(
            slice_expression, array  # type: ignore[arg-type]
        )
        self.__collection: "Collection" = collection
        super().__init__(slice_expression, shape, array, array_adapter, varray_adapter, collection)
        self.logger.debug(f"{self.__str__} instantiated")

    def _create_array_from_vposition(self, vpos: Tuple[int, ...]) -> Optional["Array"]:
        array = self.__collection.arrays.filter({"vid": self.__array.id, "v_position": vpos}).last()
        if not array:
            self.logger.debug(f"Array for v_position {vpos} not found")
            return None
        self.logger.debug(f"Created Array from meta for v_position {vpos}: {array.id}")
        return array  # type: ignore[return-value]

    @not_deleted
    @WriteVarrayLock()
    def clear(self) -> None:
        """Clear data in ``VArray`` by slice."""

        def _clear(array_pos: ArrayPosition) -> None:
            array = self._create_array_from_vposition(array_pos.vposition)
            if array:
                subset = array[array_pos.bounds]
                if subset.shape == array.shape:
                    array.delete()
                else:
                    subset.clear()

        self.logger.debug(f"Trying to clear data for {self.__str__()}")
        results = self.__adapter.executor.map(_clear, self.__arrays)
        list(results)
        self.logger.info(f"{self!s} data cleared")

    def __sum_results(self, arrays_data: Iterator) -> np.ndarray:
        """Arrange data from arrays into VSubset shape.

        :param arrays_data: tuple of data positions in VSubset and data
        """
        results = np.empty(shape=self.shape, dtype=self.__array.dtype)
        for position, data in arrays_data:
            if position == tuple():
                return data[position]
            results[position] = data
        return results

    @not_deleted
    def read(self) -> Union[Numeric, np.ndarray]:
        """Read data from ``VArray`` slice."""

        def _read_data(array_pos: ArrayPosition) -> Tuple[Slice, Union[Numeric, ndarray, None]]:
            array: "Array" = self._create_array_from_vposition(array_pos.vposition)
            if array:
                subset: Subset = array[array_pos.bounds]
                result = subset.read()
            else:
                result = np.empty(
                    shape=create_shape_from_slice(
                        self.__array.arrays_shape, array_pos.bounds  # type: ignore[attr-defined]
                    ),
                    dtype=self.__array.dtype,
                )
                result.fill(self.__array.fill_value)
            return array_pos.data_slice, result

        self.logger.debug(f"Trying to read data from {self!s}")
        arrays_data = self.__adapter.executor.map(_read_data, self.__arrays)
        data = self.__sum_results(arrays_data)
        self.logger.info(f"{self!s} data read")
        return data

    @not_deleted
    @WriteVarrayLock()
    def update(self, data: Data) -> None:
        """Update data in ``VArray`` by slice.

        * Data cannot be ``None``
        * Data ``shape`` should be equal to the ``VSubset.shape``
        * Data ``dtype`` should be equal to the ``VArray.dtype``

        :param data: new data which shall match subset slicing
        """
        from deker.arrays import Array

        def _update(array_data: ArrayPositionedData) -> None:
            """If there is a need in the future to calculate Array's time dimension start value.  # noqa: DAR101, D400

            ATD - Array time dimension
            VATD - VArray time dimension
            vpos - v_position

            start_value = VATD.step * ATD.size * vpos[vpos.index(ATD)] + VATD.start_value
            """
            array = self._create_array_from_vposition(array_data.vposition)
            if not array:
                custom_attributes = {}
                for n, dim_schema in enumerate(self.__collection.array_schema.dimensions):
                    if isinstance(dim_schema, TimeDimensionSchema) and isinstance(
                        dim_schema.start_value, str
                    ):
                        attr_name = dim_schema.start_value[1:]
                        dim: TimeDimension = self.__array.dimensions[n]
                        pos = array_data.vposition[n]
                        custom_attributes[attr_name] = dim.start_value + dim.step * pos  # type: ignore[operator]

                kwargs = {
                    "collection": self.__collection,
                    "adapter": self.__array_adapter,
                    "primary_attributes": {
                        "vid": self.__array.id,
                        "v_position": array_data.vposition,
                    },
                    "custom_attributes": custom_attributes,
                }
                array = Array(**kwargs)  # type: ignore[arg-type]
                self.__array_adapter.create(array)
            subset = array[array_data.bounds]
            subset.update(array_data.data)

        self.logger.debug(f"Trying to update data for {self!s}")
        if data is None:
            raise DekerArrayError("Updating data shall not be None")

        data = self.__array_adapter._process_data(
            self.__array.dtype, self.__array.shape, data, self.__bounds
        )

        positions = [
            ArrayPositionedData(vpos, array_bounds, data[data_bounds])
            for vpos, array_bounds, data_bounds in self.__arrays
        ]
        futures = [self.__adapter.executor.submit(_update, position) for position in positions]

        exceptions = []
        for future in futures:
            try:
                future.result()
            except Exception as e:
                exceptions.append(repr(e) + "\n" + traceback.format_exc(-1))

        if exceptions:
            raise DekerVSubsetError(
                f"ATTENTION: Data in {self!s} MAY BE NOW CORRUPTED due to the exceptions occurred in threads",
                exceptions,
            )

        self.logger.info(f"{self!s} data updated OK")

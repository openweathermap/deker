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

import traceback

from collections.abc import Iterator, Sequence
from itertools import product
from typing import TYPE_CHECKING, NamedTuple, Optional

import numpy as np

from deker_tools.slices import create_shape_from_slice, positive_bounds, slice_converter
from numpy import ndarray

from deker.ABC.base_subset import BaseSubset
from deker.dimensions import TimeDimension
from deker.errors import DekerArrayError, DekerLockError, DekerSubsetError, DekerVSubsetError
from deker.locks import TileCreationLock, WriteVarrayLock, wait_for_unlock
from deker.schemas import TimeDimensionSchema
from deker.tools import not_deleted
from deker.tools.time import get_start_datetime
from deker.types.private.classes import ArrayPosition, ArrayPositionedData
from deker.types.private.typings import Arrays, Data, Numeric, Slice

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.arrays import Array, VArray
    from deker.collection import Collection


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
        slice_expression: tuple[slice | int, ...],
        shape: tuple[int, ...],
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
    def read(self) -> Numeric | np.ndarray:
        """Read data from ``Array`` slice."""
        self.logger.debug(f"Trying to read data from {self.__array.id} bounds={slice_converter[self.__bounds]}")
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
        self.logger.debug(f"Trying to update data for {self.__array.id} bounds={slice_converter[self.__bounds]}")
        if data is None:
            raise DekerArrayError("Updating data shall not be None")
        self.__adapter.update(self.__array, self.__bounds, data)
        self.logger.info(f"{self!s} data updated")

    @not_deleted
    def clear(self) -> None:
        """Clear data in ``Array`` by slice."""
        self.logger.debug(f"Trying to clear data for {self.__array.id} bounds={slice_converter[self.__bounds]}")
        self.__adapter.clear(self.__array, self.__bounds)
        self.logger.info(f"{self!s} data cleared")


class _TileBounds(NamedTuple):
    """Part of one tile (inner ``Array``) along one axis, selected by a subset bound."""

    position: int
    """Index of the tile along the axis of the grid."""
    bounds: slice | int
    """Bounds inside the tile along the axis."""
    data_slice: slice | None
    """Slice of the subset data along the axis; ``None`` for an integer bound, which drops the axis."""


def _split_axis(bound: slice | int, size: int, tile_size: int) -> list[_TileBounds]:
    """Split a subset bound of one axis into the parts that fall into each tile.

    The selected positions are walked in their order: the tiles with no selected position are skipped
    (a step larger than a tile jumps over tiles), and the slice of the subset data of every tile continues
    where the previous one ended. The step of the bound shall be positive: a negative step is replaced by
    its forward twin before, see ``deker_tools.slices.positive_bounds``.

    :param bound: subset bound along the axis, a slice with a positive step or an integer
    :param size: size of the VArray along the axis
    :param tile_size: size of a tile along the axis
    :returns: parts of the tiles the bound selects, in the order of the tiles
    """
    if isinstance(bound, int):
        position = bound if bound >= 0 else size + bound
        return [_TileBounds(position // tile_size, position % tile_size, None)]

    start, stop, step = bound.indices(size)
    if step < 0:  # pragma: no cover
        raise DekerVSubsetError(f"A negative step {step} shall be replaced by its forward twin before", [])
    tiles: list[_TileBounds] = []
    selected_before = 0
    first = start
    while first < stop:
        position = first // tile_size
        tile_end = min(stop, (position + 1) * tile_size)
        count = (tile_end - 1 - first) // step + 1
        last = first + (count - 1) * step
        offset = position * tile_size
        local_start, local_stop = first - offset, last - offset + 1
        if step == 1:
            # a whole tile is the plain slice(None, None), a cut one keeps its bounds
            bounds = (
                slice(None, None) if (local_start, local_stop) == (0, tile_size) else slice(local_start, local_stop)
            )
        else:
            bounds = slice(local_start, local_stop, step)
        tiles.append(_TileBounds(position, bounds, slice(selected_before, selected_before + count)))
        selected_before += count
        first = last + step
    return tiles


def _tile_positions(bounds: Sequence[slice | int], shape: Sequence[int], arrays_shape: Sequence[int]) -> Arrays:
    """Find the tiles (inner ``Arrays``) a subset covers, with their bounds and their slices of the subset data.

    :param bounds: subset bounds, one slice with a positive step or integer per axis of the VArray
    :param shape: shape of the VArray
    :param arrays_shape: shape of the tiles
    :returns: positions of the tiles in the grid with their bounds and data slices, sorted by position
    """
    axes = [_split_axis(bound, size, tile_size) for bound, size, tile_size in zip(bounds, shape, arrays_shape)]
    return [
        ArrayPosition(
            vposition=tuple(part.position for part in parts),
            bounds=tuple(part.bounds for part in parts),
            data_slice=tuple(part.data_slice for part in parts if part.data_slice is not None),
        )
        for parts in product(*axes)
    ]


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
    :param varray_adapter: VArrayAdapter instance
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
        "__flipped_axes",
    )

    def __init__(
        self,
        slice_expression: tuple[slice | int, ...],
        shape: tuple[int, ...],
        array: "VArray",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: "BaseVArrayAdapter",
        collection: "Collection",
    ):
        self.__bounds = slice_expression
        self.__shape = shape
        self.__array: "VArray" = array
        self.__adapter: "BaseVArrayAdapter" = varray_adapter
        self.__array_adapter: "BaseArrayAdapter" = array_adapter
        # the tiles are walked forwards: a negative step is replaced by its forward twin and the data of the
        # subset is flipped along the axis on reading, and before splitting on writing
        forward, self.__flipped_axes = positive_bounds(array.shape, slice_expression)
        self.__arrays: Arrays = _tile_positions(forward, array.shape, array.arrays_shape)
        self.__collection: "Collection" = collection
        super().__init__(slice_expression, shape, array, array_adapter, varray_adapter, collection)
        self.logger.debug(f"{self.__str__} instantiated")

    def _create_array_from_vposition(self, vpos: tuple[int, ...]) -> Optional["Array"]:
        from deker.arrays import Array

        array = self.__collection.arrays.filter({"vid": self.__array.id, "v_position": vpos}).last()
        if not array:
            self.logger.debug(f"Array for v_position {vpos} not found")
            return None
        if not isinstance(array, Array):  # pragma: no cover
            raise DekerSubsetError(f"Arrays manager returned {type(array)} for v_position {vpos}")
        self.logger.debug(f"Created Array from meta for v_position {vpos}: {array.id}")
        return array

    def _try_find_tile(self, vpos: tuple[int, ...], found: list[Optional["Array"]]) -> bool:
        """Try to find the tile once; a tile held by a writer of another process cannot be read yet.

        :param vpos: position of the tile in the grid
        :param found: list to put the tile (or ``None`` for a missing one) into
        :returns: ``True`` if the search is over, ``False`` if the tile is held by a writer
        """
        try:
            found.append(self._create_array_from_vposition(vpos))
        except DekerLockError:
            return False
        return True

    def _find_tile_for_writing(self, vpos: tuple[int, ...]) -> Optional["Array"]:
        """Find the tile as a writer does: waiting for a writer of another process to leave it.

        :param vpos: position of the tile in the grid
        :returns: the tile, ``None`` if there is no tile at the position
        :raises DekerLockError: if the tile is still held when the write lock timeout is over
        """
        found: list[Optional["Array"]] = []
        config = self.__adapter.ctx.config
        if not wait_for_unlock(
            self._try_find_tile, (vpos, found), config.write_lock_timeout, config.write_lock_check_interval
        ):
            raise DekerLockError(f"Tile {vpos} of {self.__array} is locked for update operation")
        return found[0]

    def __clear_array(self, array_pos: ArrayPosition) -> None:
        """Clear the part of one inner ``Array`` covered by the subset; a fully covered ``Array`` is deleted.

        :param array_pos: position and bounds of the inner Array
        """
        array = self._find_tile_for_writing(array_pos.vposition)
        if array:
            subset = array[array_pos.bounds]
            if subset.shape == array.shape:
                array.delete()
            else:
                subset.clear()

    @not_deleted
    @WriteVarrayLock()
    def clear(self) -> None:
        """Clear data in ``VArray`` by slice."""
        self.logger.debug(f"Trying to clear data for {self.__str__()}")
        results = self.__adapter.executor.map(self.__clear_array, self.__arrays)
        list(results)
        self.logger.info(f"{self!s} data cleared")

    def __sum_results(self, arrays_data: Iterator) -> np.ndarray:
        """Arrange data from arrays into VSubset shape.

        :param arrays_data: tuple of data positions in VSubset and data
        :returns: data of the subset assembled from the arrays
        """
        results = np.empty(shape=self.shape, dtype=self.__array.dtype)
        for position, data in arrays_data:
            if position == tuple():
                return data[position]
            results[position] = data
        if self.__flipped_axes:
            return np.flip(results, self.__flipped_axes)
        return results

    def __read_array_data(self, array_pos: ArrayPosition) -> tuple[Slice, Numeric | ndarray | None]:
        """Read the part of one inner ``Array`` covered by the subset.

        A missing ``Array`` reads as its fill value.

        :param array_pos: position and bounds of the inner Array and the slice of the subset data it fills
        :returns: slice of the subset data and the data read
        """
        array = self._create_array_from_vposition(array_pos.vposition)
        if array:
            subset = array[array_pos.bounds]
            result = subset.read()
        else:
            result = np.empty(
                shape=create_shape_from_slice(self.__array.arrays_shape, array_pos.bounds),
                dtype=self.__array.dtype,
            )
            result.fill(self.__array.fill_value)
        return array_pos.data_slice, result

    @not_deleted
    def read(self) -> Numeric | np.ndarray:
        """Read data from ``VArray`` slice."""
        self.logger.debug(f"Trying to read data from {self!s}")
        arrays_data = self.__adapter.executor.map(self.__read_array_data, self.__arrays)
        data = self.__sum_results(arrays_data)
        self.logger.info(f"{self!s} data read")
        return data

    def __create_tile(self, vposition: tuple[int, ...]) -> "Array":
        """Create the missing tile (inner ``Array``) at the position.

        A new ``Array`` gets the start values of its time dimensions calculated from its position:
        ``VATD.step * ATD.size * vpos[vpos.index(ATD)] + VATD.start_value``
        (ATD - Array time dimension, VATD - VArray time dimension, vpos - v_position).

        :param vposition: position of the tile in the grid
        :returns: the created tile
        """
        from deker.arrays import Array

        custom_attributes = {}
        for n, dim_schema in enumerate(self.__collection.array_schema.dimensions):
            if isinstance(dim_schema, TimeDimensionSchema) and isinstance(dim_schema.start_value, str):
                attr_name = dim_schema.start_value[1:]
                dim = self.__array.dimensions[n]
                if not isinstance(dim, TimeDimension):  # pragma: no cover
                    raise DekerVSubsetError(f"Dimension {dim.name} is not a TimeDimension", [])
                # the tile starts where its first cell is on the VArray time axis
                first_cell = vposition[n] * self.__array.arrays_shape[n]
                custom_attributes[attr_name] = get_start_datetime(dim) + dim.step * first_cell

        array = Array(
            collection=self.__collection,
            adapter=self.__array_adapter,
            primary_attributes={"vid": self.__array.id, "v_position": vposition},
            custom_attributes=custom_attributes,
        )
        self.__array_adapter.create(array)
        return array

    def __update_array_data(self, array_data: ArrayPositionedData) -> None:
        """Update the part of one inner ``Array`` covered by the subset, creating the ``Array`` if it is missing.

        :param array_data: position and bounds of the inner Array and the data for it
        """
        array = self._find_tile_for_writing(array_data.vposition)
        if not array:
            with TileCreationLock(self.__adapter, self.__array, self.__collection):
                # another writer may have created the tile while this one waited for the lock
                array = self._find_tile_for_writing(array_data.vposition) or self.__create_tile(array_data.vposition)
        subset = array[array_data.bounds]
        subset.update(array_data.data)

    @not_deleted
    @WriteVarrayLock()
    def update(self, data: Data) -> None:
        """Update data in ``VArray`` by slice.

        * Data cannot be ``None``
        * Data ``shape`` should be equal to the ``VSubset.shape``
        * Data ``dtype`` should be equal to the ``VArray.dtype``

        :param data: new data which shall match subset slicing
        """
        self.logger.debug(f"Trying to update data for {self!s}")
        if data is None:
            raise DekerArrayError("Updating data shall not be None")

        processed = self.__array_adapter._process_data(self.__array.dtype, self.__array.shape, data, self.__bounds)
        # a scalar fills the whole subset, as it does for an Array subset
        data_array = (
            processed if isinstance(processed, np.ndarray) else np.full(self.__shape, processed, self.__array.dtype)
        )

        if self.__flipped_axes:
            data_array = np.flip(data_array, self.__flipped_axes)
        positions = [
            ArrayPositionedData(vpos, array_bounds, data_array[data_bounds])
            for vpos, array_bounds, data_bounds in self.__arrays
        ]
        futures = [self.__adapter.executor.submit(self.__update_array_data, position) for position in positions]

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

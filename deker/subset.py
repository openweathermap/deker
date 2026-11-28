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

import traceback

from collections.abc import Iterator, Sequence
from functools import partial
from itertools import product
from typing import TYPE_CHECKING, NamedTuple, Optional

import numpy as np

from cantok import AbstractToken
from deker_tools.cancel import TokenCancelledError, check, read_token, resolve_timeout, write_token
from deker_tools.slices import create_shape_from_slice, positive_bounds
from deker_tools.typings import Sentinel, SentinelType
from numpy import ndarray

from deker.ABC.base_subset import BaseSubset
from deker.errors import (
    DekerArrayError,
    DekerCriticalError,
    DekerInstanceNotExistsError,
    DekerLockError,
    DekerVSubsetError,
    TokenErrorConverter,
)
from deker.locks import ArrayCreationLock
from deker.tools import not_deleted
from deker.tools.decorators import _NOT_EXISTING
from deker.tools.time import array_time_attributes
from deker.types.private.classes import ArrayPosition, ArrayPositionedData
from deker.types.private.typings import Arrays, Data, Numeric, Slice
from deker.validators import validate_timeout

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.arrays import Array, VArray
    from deker.collection import Collection


class Subset(BaseSubset["Array"]):
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
         * Data ``shape`` should be equal to the ``Subset.shape``, or data is a number that fills the whole subset
         * Python numbers, lists and tuples are converted to the ``Array.dtype`` when every value fits it;
           NumPy arrays and scalars shall have the ``Array.dtype`` already

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
        self.logger.debug("%s instantiated", self)

    def _read(self, *, timeout: int | SentinelType, token: AbstractToken) -> Numeric | np.ndarray:
        """Read the data under the token of an operation built already: a ``VSubset`` reads its ``Arrays`` so.

        :param timeout: timeout of the operation as it was given
        :param token: token of the read operation
        :returns: the data of the subset shape; a number for a single cell
        """
        check(token)
        self.logger.debug("Trying to read data from %s", self)
        data = self.__adapter.read_data(self.__array, self.__bounds, timeout=timeout, token=token)
        check(token)
        self.logger.info("%s data read", self)
        return data

    @not_deleted
    def read(self, *, timeout: int | SentinelType = Sentinel) -> Numeric | np.ndarray:
        """Read data from ``Array`` slice.

        :param timeout: seconds the read may take; not given - ``read_timeout`` of the client
        :returns: the data of the subset shape; a number for a single cell; the fill value where nothing was written
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerLockError: if a writer holds the ``Array``
        :raises DekerTimeoutError: if the read takes longer than its timeout; no data is returned then
        :raises DekerCancelledError: if the read is cancelled from outside
        :raises DekerInstanceNotExistsError: if the ``Array`` was deleted
        """
        validate_timeout(timeout=timeout)
        with TokenErrorConverter():
            token = read_token(resolve_timeout(timeout, self.__adapter.ctx.config.read_timeout))
            return self._read(timeout=timeout, token=token)

    def _update(self, data: Data, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Write the data under the token of an operation built already: a ``VSubset`` writes its ``Arrays`` so.

        :param data: new data which shall match subset slicing
        :param timeout: timeout of the operation as it was given
        :param token: token of the write operation
        """
        check(token)
        self.logger.debug("Trying to update data for %s", self)
        self.__adapter.update(self.__array, self.__bounds, data, timeout=timeout, token=token)
        self.logger.info("%s data updated", self)

    @not_deleted
    def update(self, data: Data, *, timeout: int | SentinelType = Sentinel) -> None:
        """Update data in ``Array`` by slice.

        * Data cannot be ``None``
        * Data ``shape`` should be equal to the ``Subset.shape``, or data is a number that fills the whole subset
        * Python numbers, lists and tuples are converted to the ``Array.dtype`` when every value fits it;
          NumPy arrays and scalars shall have the ``Array.dtype`` already

        The write waits for the readers of the ``Array`` to leave, up to its timeout; once the file is started,
        the write goes to its end.

        :param data: new data which shall match subset slicing
        :param timeout: seconds to wait for the lock of the ``Array``; not given - ``write_lock_timeout`` of the
          client
        :raises DekerArrayError: if the data is ``None``
        :raises DekerValidationError: if the data does not fit the shape or the dtype, or the timeout is not a
          positive integer
        :raises DekerLockError: if the ``Array`` is not released within the timeout
        :raises DekerCancelledError: if the write is cancelled before it started
        :raises DekerInstanceNotExistsError: if the ``Array`` was deleted
        """
        validate_timeout(timeout=timeout)
        if data is None:
            raise DekerArrayError("Updating data shall not be None")
        with TokenErrorConverter():
            self._update(data, timeout=timeout, token=write_token())

    def _clear(self, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Clear the cells under the token of an operation built already: a ``VSubset`` clears its ``Arrays`` so.

        :param timeout: timeout of the operation as it was given
        :param token: token of the write operation
        """
        check(token)
        self.logger.debug("Trying to clear data for %s", self)
        self.__adapter.clear(self.__array, self.__bounds, timeout=timeout, token=token)
        self.logger.info("%s data cleared", self)

    @not_deleted
    def clear(self, *, timeout: int | SentinelType = Sentinel) -> None:
        """Set the cells of the ``Array`` within the subset to the fill value.

        The data of an ``Array`` that becomes entirely empty are removed from its file.

        :param timeout: seconds to wait for the lock of the ``Array``; not given - ``write_lock_timeout`` of the
          client
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerLockError: if the ``Array`` is not released within the timeout
        :raises DekerCancelledError: if the clearing is cancelled before it started
        :raises DekerInstanceNotExistsError: if the ``Array`` was deleted
        """
        validate_timeout(timeout=timeout)
        with TokenErrorConverter():
            self._clear(timeout=timeout, token=write_token())


class _ArrayBounds(NamedTuple):
    """Part of one ``Array`` of the ``VArray`` along one axis, selected by a subset bound."""

    position: int
    """Index of the Array along the axis of the ``vgrid``."""
    bounds: slice | int
    """Bounds inside the Array along the axis."""
    data_slice: slice | None
    """Slice of the subset data along the axis; ``None`` for an integer bound, which drops the axis."""


def _split_axis(bound: slice | int, size: int, array_size: int) -> list[_ArrayBounds]:
    """Split a subset bound of one axis into the parts that fall into each Array.

    The selected positions are walked in their order: the Arrays with no selected position are skipped
    (a step larger than an Array jumps over Arrays), and the slice of the subset data of every Array continues
    where the previous one ended. The step of the bound shall be positive: a negative step is replaced by
    its forward twin before, see ``deker_tools.slices.positive_bounds``.

    :param bound: subset bound along the axis, a slice with a positive step or an integer
    :param size: size of the VArray along the axis
    :param array_size: size of an Array along the axis
    :returns: parts of the Arrays the bound selects, in the order of the Arrays
    """
    if isinstance(bound, int):
        position = bound if bound >= 0 else size + bound
        return [_ArrayBounds(position // array_size, position % array_size, None)]

    start, stop, step = bound.indices(size)
    if step < 0:  # pragma: no cover
        raise DekerVSubsetError(f"A negative step {step} shall be replaced by its forward twin before", [])
    parts: list[_ArrayBounds] = []
    selected_before = 0
    first = start
    while first < stop:
        position = first // array_size
        array_end = min(stop, (position + 1) * array_size)
        count = (array_end - 1 - first) // step + 1
        last = first + (count - 1) * step
        offset = position * array_size
        local_start, local_stop = first - offset, last - offset + 1
        if step == 1:
            # a whole Array is the plain slice(None, None), a cut one keeps its bounds
            bounds = (
                slice(None, None) if (local_start, local_stop) == (0, array_size) else slice(local_start, local_stop)
            )
        else:
            bounds = slice(local_start, local_stop, step)
        parts.append(_ArrayBounds(position, bounds, slice(selected_before, selected_before + count)))
        selected_before += count
        first = last + step
    return parts


def _array_positions(bounds: Sequence[slice | int], shape: Sequence[int], arrays_shape: Sequence[int]) -> Arrays:
    """Find the ``Arrays`` of the ``VArray`` a subset covers, with their bounds and slices of the subset data.

    :param bounds: subset bounds, one slice with a positive step or integer per axis of the VArray
    :param shape: shape of the VArray
    :param arrays_shape: shape of the Arrays
    :returns: ``v_position`` of the Arrays in the ``vgrid`` with their bounds and data slices, sorted by position
    """
    axes = [_split_axis(bound, size, array_size) for bound, size, array_size in zip(bounds, shape, arrays_shape)]
    return [
        ArrayPosition(
            vposition=tuple(part.position for part in parts),
            bounds=tuple(part.bounds for part in parts),
            data_slice=tuple(part.data_slice for part in parts if part.data_slice is not None),
        )
        for parts in product(*axes)
    ]


class VSubset(BaseSubset["VArray"]):
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
         * Data ``shape`` shall be equal to the ``VSubset.shape``, or data is a number that fills the whole subset
         * Python numbers, lists and tuples are converted to the ``VArray.dtype`` when every value fits it;
           NumPy arrays and scalars shall have the ``VArray.dtype`` already

      The ``Arrays`` the subset covers are written one by one, each under its own lock; a missing ``Array``
      is created.

    - ``clear``: removes or resets with `fill_value` all data from the storage within the virtual subset bounds;
      an ``Array`` the subset covers entirely is deleted

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
        # the Arrays are walked forwards: a negative step is replaced by its forward twin and the data of the
        # subset is flipped along the axis on reading, and before splitting on writing
        forward, self.__flipped_axes = positive_bounds(array.shape, slice_expression)
        self.__arrays: Arrays = _array_positions(forward, array.shape, array.arrays_shape)
        self.__collection: "Collection" = collection
        super().__init__(slice_expression, shape, array, varray_adapter)
        self.logger.debug("%s instantiated", self)

    def _create_array_from_vposition(self, vpos: tuple[int, ...]) -> Optional["Array"]:
        """Find the ``Array`` of the ``VArray`` at the position.

        The ``VArray`` knows everything about its ``Arrays`` but their ids: the schema, the primary attributes (its
        own id and the position) and the time attributes computed from the position, as on creation. So only the
        id is asked from the adapter, and the file of the ``Array`` is not opened until its data is read.

        :param vpos: ``v_position`` of the Array in the ``vgrid``
        :returns: the Array, ``None`` if there is no Array at the position yet
        """
        from deker.arrays import Array

        primary_attributes = {"vid": self.__array.id, "v_position": vpos}
        schema = self.__collection.array_schema
        id_ = self.__array_adapter.get_id_by_primary_attributes(primary_attributes, schema)
        if id_ is None:
            self.logger.debug("Array for v_position %s not found", vpos)
            return None
        self.logger.debug("Array for v_position %s found: %s", vpos, id_)
        return Array(
            collection=self.__collection,
            adapter=self.__array_adapter,
            id_=id_,
            primary_attributes=primary_attributes,
            custom_attributes=array_time_attributes(self.__array, vpos, schema),
        )

    def __clear_array(self, array_pos: ArrayPosition, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Clear the part of one ``Array`` of the ``VArray`` covered by the subset; a fully covered one is deleted.

        :param array_pos: position and bounds of the inner Array
        :param timeout: timeout of the operation as it was given
        :param token: token of the write operation
        """
        check(token)
        array = self._create_array_from_vposition(array_pos.vposition)
        if array:
            subset = array[array_pos.bounds]
            if subset.shape == array.shape:
                array._delete(background=False, _empty_trash=False, token=token)  # the trash is emptied by the clear
            else:
                subset._clear(timeout=timeout, token=token)

    @not_deleted
    def clear(self, *, timeout: int | SentinelType = Sentinel) -> None:
        """Clear the data of the ``VArray`` within the subset.

        An ``Array`` the subset covers entirely is deleted: moved to the trash without waiting for anybody; a partly
        covered one is cleared, waiting for its lock. The trash is emptied once at the end.

        :param timeout: seconds to wait for the lock of each partly covered ``Array``; not given -
          ``write_lock_timeout`` of the client
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerLockError: if an ``Array`` is not released within the timeout
        :raises DekerCancelledError: if the clearing is cancelled before any ``Array`` is started
        :raises DekerInstanceNotExistsError: if the ``VArray`` was deleted
        :raises DekerTrashError: if something in the trash is not removed; the subset is cleared all the same
        """
        validate_timeout(timeout=timeout)
        with TokenErrorConverter():
            token = write_token()
            check(token)
            self.logger.debug("Trying to clear data for %s", self)
            results = self.__adapter.executor.map(
                partial(self.__clear_array, timeout=timeout, token=token), self.__arrays
            )
            list(results)
        self.__array_adapter.empty_trash(background=False)
        self.logger.info("%s data cleared", self)

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

    def __read_array_data(
        self, array_pos: ArrayPosition, *, timeout: int | SentinelType, token: AbstractToken
    ) -> tuple[Slice, Numeric | ndarray | None]:
        """Read the part of one ``Array`` of the ``VArray`` covered by the subset.

        A missing ``Array`` reads as its fill value.

        :param array_pos: position and bounds of the inner Array and the slice of the subset data it fills
        :param timeout: timeout of the operation as it was given
        :param token: token of the read operation
        :returns: slice of the subset data and the data read
        """
        check(token)
        array = self._create_array_from_vposition(array_pos.vposition)
        if array:
            subset = array[array_pos.bounds]
            result = subset._read(timeout=timeout, token=token)
        else:
            result = np.empty(
                shape=create_shape_from_slice(self.__array.arrays_shape, array_pos.bounds),
                dtype=self.__array.dtype,
            )
            result.fill(self.__array.fill_value)
        return array_pos.data_slice, result

    @not_deleted
    def read(self, *, timeout: int | SentinelType = Sentinel) -> Numeric | np.ndarray:
        """Read data from ``VArray`` slice, assembled from its ``Arrays``.

        :param timeout: seconds the whole read may take; not given - ``read_timeout`` of the client
        :returns: the data of the subset shape; a number for a single cell; the fill value where nothing was written
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerLockError: if a writer holds one of the ``Arrays``
        :raises DekerTimeoutError: if the read takes longer than its timeout; no data is returned then
        :raises DekerCancelledError: if the read is cancelled from outside
        :raises DekerInstanceNotExistsError: if the ``VArray`` was deleted
        """
        validate_timeout(timeout=timeout)
        with TokenErrorConverter():
            token = read_token(resolve_timeout(timeout, self.__adapter.ctx.config.read_timeout))
            check(token)
            self.logger.debug("Trying to read data from %s", self)
            arrays_data = self.__adapter.executor.map(
                partial(self.__read_array_data, timeout=timeout, token=token), self.__arrays
            )
            data = self.__sum_results(arrays_data)
            check(token)
        self.logger.info("%s data read", self)
        return data

    def __create_array(
        self, vposition: tuple[int, ...], *, timeout: int | SentinelType, token: AbstractToken
    ) -> "Array":
        """Create the missing ``Array`` of the ``VArray`` at the position.

        A new ``Array`` gets the start values of its time dimensions calculated from its position:
        ``VATD.step * ATD.size * vpos[vpos.index(ATD)] + VATD.start_value``
        (ATD - Array time dimension, VATD - VArray time dimension, vpos - v_position).

        :param vposition: ``v_position`` of the Array in the ``vgrid``
        :param timeout: timeout of the operation as it was given
        :param token: token of the write operation
        :returns: the created Array
        """
        from deker.arrays import Array

        array = Array(
            collection=self.__collection,
            adapter=self.__array_adapter,
            primary_attributes={"vid": self.__array.id, "v_position": vposition},
            custom_attributes=array_time_attributes(self.__array, vposition, self.__collection.array_schema),
        )
        self.__array_adapter.create(array, timeout=timeout, token=token)
        return array

    def __update_array_data(
        self, array_data: ArrayPositionedData, *, timeout: int | SentinelType, token: AbstractToken
    ) -> None:
        """Update the part of one ``Array`` of the ``VArray`` covered by the subset, creating it if it is missing.

        The ``Array`` is held exclusively only while its part is written (its own write lock), so the Arrays
        of one ``VArray`` are written independently and a writer never holds an Array it is not writing.

        :param array_data: position and bounds of the inner Array and the data for it
        :param timeout: timeout of the operation as it was given
        :param token: token of the write operation
        """
        check(token)
        array = self._create_array_from_vposition(array_data.vposition)
        if not array:
            with ArrayCreationLock(self.__adapter, self.__array, self.__collection, timeout=timeout, token=token):
                # another writer may have created the Array while this one waited for the lock
                array = self._create_array_from_vposition(array_data.vposition) or self.__create_array(
                    array_data.vposition, timeout=timeout, token=token
                )
        subset = array[array_data.bounds]
        subset._update(array_data.data, timeout=timeout, token=token)

    def __update(self, data: Data, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Write the data into the ``Arrays`` of the ``VArray`` under the token of the operation.

        :param data: new data which shall match subset slicing
        :param timeout: timeout of the operation as it was given
        :param token: token of the write operation
        :raises DekerCriticalError: if an ``Array`` found the collections directory of the storage gone; whatever the
          others met
        :raises TokenCancelledError: if the write is cancelled before any ``Array`` is started
        :raises DekerInstanceNotExistsError: if an ``Array`` does not exist because the ``VArray`` was deleted
        :raises DekerLockError: if every ``Array`` that failed was not released within the write lock timeout
        :raises DekerVSubsetError: if one or more ``Arrays`` could not be written for other reasons, or for mixed ones
        """
        check(token)
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
        futures = [
            self.__adapter.executor.submit(self.__update_array_data, position, timeout=timeout, token=token)
            for position in positions
        ]

        exceptions: list[Exception] = []
        descriptions: list[str] = []
        for future in futures:
            try:
                future.result()
            except Exception as e:
                exceptions.append(e)
                descriptions.append(repr(e) + "\n" + traceback.format_exc(-1))

        # the collections directory of the storage is gone: the critical error itself, never folded into another one
        critical = [e for e in exceptions if isinstance(e, DekerCriticalError)]
        if critical:
            raise critical[0]
        # a cancelled Array means no Array of the write was started: the write is cancelled as a whole
        cancelled = [e for e in exceptions if isinstance(e, TokenCancelledError)]
        if cancelled:
            raise cancelled[0]
        # an Array that does not exist: the VArray is looked at only now, never on the way of success
        if any(isinstance(e, DekerInstanceNotExistsError) for e in exceptions) and self._is_deleted():
            raise DekerInstanceNotExistsError(_NOT_EXISTING.format(instance=self, method=VSubset.update.__qualname__))
        if exceptions and all(isinstance(e, DekerLockError) for e in exceptions):
            causes = "; ".join(str(e) for e in exceptions)
            raise DekerLockError(
                f"{self!s} is written partly: {len(exceptions)} of {len(positions)} Arrays were not released in time, "
                f"the others are written; repeat the write: {causes}"
            )
        if exceptions:
            raise DekerVSubsetError(
                f"{self!s} is written partly: {len(exceptions)} of {len(positions)} Arrays failed, "
                "the others are written",
                descriptions,
            )

    @not_deleted
    def update(self, data: Data, *, timeout: int | SentinelType = Sentinel) -> None:
        """Update data in ``VArray`` by slice.

        * Data cannot be ``None``
        * Data ``shape`` should be equal to the ``VSubset.shape``, or data is a number that fills the whole subset
        * Python numbers, lists and tuples are converted to the ``VArray.dtype`` when every value fits it;
          NumPy arrays and scalars shall have the ``VArray.dtype`` already

        The ``Arrays`` the subset covers are written one by one in the executor of the client, each under its own
        write lock; a missing ``Array`` is created. The ``Arrays`` that failed are reported together, the others
        are written. When every failed ``Array`` was not released within the write lock timeout, the error is
        ``DekerLockError``: nothing is broken, the write can simply be repeated. A write cancelled from outside
        stops only before any of its ``Arrays`` is started; once one is started, every ``Array`` is written.

        :param data: new data which shall match subset slicing
        :param timeout: seconds to wait for the lock of each ``Array``; not given - ``write_lock_timeout`` of the
          client
        :raises DekerArrayError: if the data is ``None``
        :raises DekerValidationError: if the data does not fit the shape or the dtype, or the timeout is not a
          positive integer
        :raises DekerCancelledError: if the write is cancelled before any ``Array`` is started
        :raises DekerLockError: if every ``Array`` that failed was not released within the write lock timeout
        :raises DekerVSubsetError: if one or more ``Arrays`` could not be written for other reasons, or for mixed ones
        :raises DekerInstanceNotExistsError: if the ``VArray`` was deleted
        :raises DekerCriticalError: if the collections directory of the storage is gone
        """
        validate_timeout(timeout=timeout)
        self.logger.debug("Trying to update data for %s", self)
        if data is None:
            raise DekerArrayError("Updating data shall not be None")
        with TokenErrorConverter():
            self.__update(data, timeout=timeout, token=write_token())
        self.logger.info("%s data updated OK", self)

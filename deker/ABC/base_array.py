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

"""Abstract interfaces for array and virtual array."""

import json

from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timedelta
from types import EllipsisType, NoneType
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeGuard, TypeVar, Union

import numpy as np

from cantok import AbstractToken
from deker_tools.cancel import check, write_token
from deker_tools.slices import create_shape_from_slice
from deker_tools.time import get_utc
from deker_tools.typings import Sentinel, SentinelType, is_sentinel

from deker.ABC.base_subset import BaseSubset
from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerInvalidSchemaError, DekerMetaDataError, DekerValidationError, TokenErrorConverter
from deker.log import SelfLoggerMixin
from deker.schemas import ArraySchema, VArraySchema
from deker.tools.array import check_memory, get_id
from deker.tools.attributes import make_ordered_dict, serialize_attribute_value
from deker.tools.schema import create_dimensions
from deker.tools.time import get_start_datetime
from deker.types.private.classes import ArrayMeta, Serializer
from deker.types.private.typings import FancySlice, Numeric, Slice
from deker.validators import is_valid_uuid, process_attributes, validate_custom_attributes_update, validate_timeout

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter, IArrayAdapter
    from deker.collection import Collection


BaseArrayT = TypeVar("BaseArrayT", bound="BaseArray")
SubsetT = TypeVar("SubsetT", bound=BaseSubset)


class _FancySlicer:
    """Converter of datetimes, floats and strings into index integers."""

    @staticmethod
    def _convert_value_for_time_dimension(
        value: FancySlice,
        dimension: TimeDimension,
    ) -> int:
        """Return value position (index) in TimeDimension.

        :param value: value to be converted into position: an integer is a position already, a datetime, an ISO
          string or a timestamp is converted against the start and the step of the axis
        :param dimension: TimeDimension instance
        :returns: index of the value in the dimension
        :raises IndexError: if the value is a timedelta, of another type, not on the step of the axis or out of range
        """
        if isinstance(value, int):
            return value

        start = get_start_datetime(dimension)
        step = dimension.step

        if isinstance(value, timedelta):
            raise IndexError(
                f"TimeDimension {dimension.name} wrong indexing: a timedelta is a step, not a position; got {value}"
            )

        if not isinstance(value, (float, str, datetime)):
            raise IndexError(f"TimeDimension {dimension} wrong value: {value} of type {type(value)}")

        try:
            dt = get_utc(value)
            position = int((dt - start) // step)

            # if passed timestamp/iso-string does not match step size
            if remainder := int(((dt - start) % step).total_seconds()):
                raise IndexError(
                    f"TimeDimension {dimension.name} wrong indexing: "
                    f"{dimension.name} has no index {position}.{remainder}"
                )

            if position < 0 or position > dimension.size:
                raise IndexError(f"{dimension.__class__.__name__} {dimension.name} out of range: {position}")
            return position
        except (ValueError, TypeError) as e:
            raise IndexError(f"TimeDimension {dimension} wrong value: {value} of type {type(value)}; {e}")

    @staticmethod
    def _convert_value_for_dimension(value: FancySlice, dimension: Dimension) -> int:
        """Return value position (index) in Dimension.

        :param value: value to be converted into position: an integer is a position already, a string is looked up
          among the labels, a float on the scale
        :param dimension: Dimension instance
        :returns: index of the value in the dimension
        :raises IndexError: if the axis has no labels or scale for the value, the value is not on it or out of range
        """
        if isinstance(value, int):
            return value

        if isinstance(value, str):
            if not dimension.labels:
                raise IndexError(f"Dimension {dimension.name} wrong indexing: {dimension.name} has no labels")
            dim_pos = dimension.labels.name_to_index(value)
            if dim_pos is None:
                raise IndexError(f"Dimension {dimension.name} wrong indexing: {dimension.name} has no label {value}")
        elif isinstance(value, float):
            if not dimension.scale:
                raise IndexError(f"Dimension {dimension.name} wrong indexing: {dimension.name} has no scale")

            # if passed float does not match scale step size
            if remainder := (value - dimension.scale.start_value) % dimension.scale.step:
                raise IndexError(
                    f"Dimension {dimension.name} wrong indexing: {dimension.name} has no index {remainder}"
                )

            dim_pos = int((value - dimension.scale.start_value) // dimension.scale.step)

        else:
            raise IndexError(f"Dimension {dimension.name} wrong indexing: {value} of {type(value)}")
        if dim_pos < 0 or dim_pos >= dimension.size:
            raise IndexError(f"Dimension {dimension.name} wrong indexing: {value} is out of range")
        return dim_pos

    @staticmethod
    def _convert_step(step: Any, dimension: TimeDimension | Dimension) -> int | None:
        """Convert the step of a slice into an index step.

        An integer step is taken as is, as in numpy. A ``timedelta`` step on a time axis is a non-zero multiple
        of the step of the axis. A ``float`` step on a scaled axis is a non-zero multiple of the step of the scale,
        counted in the values of the scale: its sign is taken against the sign of the scale, so a negative value
        step on a descending scale walks forward along the axis. Labels accept integer steps only.

        :param step: step of the slice
        :param dimension: dimension the slice applies to
        :returns: index step, ``None`` if the step is not set
        :raises IndexError: if the step is zero, a boolean, of a kind the axis does not accept or not a multiple
        """
        if step is None:
            return None
        name = f"{dimension.__class__.__name__} {dimension.name}"
        if isinstance(step, bool):
            raise IndexError(f"{name} wrong indexing: step shall not be a boolean")
        if isinstance(step, int):
            if step == 0:
                raise IndexError(f"{name} wrong indexing: step shall not be zero")
            return step
        if isinstance(dimension, TimeDimension):
            if not isinstance(step, timedelta):
                raise IndexError(f"{name} wrong indexing: step shall be an integer or a timedelta, got {step!r}")
            if step == timedelta(0):
                raise IndexError(f"{name} wrong indexing: step shall not be zero")
            if step % dimension.step != timedelta(0):
                raise IndexError(
                    f"{name} wrong indexing: step {step} is not a multiple of the axis step {dimension.step}"
                )
            return int(step // dimension.step)
        if isinstance(step, float):
            if dimension.scale is None:
                raise IndexError(f"{name} wrong indexing: {dimension.name} has no scale for a float step")
            if step == 0.0:
                raise IndexError(f"{name} wrong indexing: step shall not be zero")
            ratio = step / dimension.scale.step
            index_step = round(ratio)
            if index_step == 0 or abs(ratio - index_step) > 1e-9:
                raise IndexError(
                    f"{name} wrong indexing: step {step} is not a multiple of the scale step {dimension.scale.step}"
                )
            return int(index_step)
        raise IndexError(f"{name} wrong indexing: step shall be an integer or a float, got {step!r}")

    def _convert_fancy_slice(self, slice_: slice, dimension: TimeDimension | Dimension) -> slice:
        """Convert FancySlice to slice.

        ``start`` and ``stop`` are converted into positions on the axis and left ``None`` if not set, so the
        plain slice keeps the numpy semantics of its defaults for any sign of the step.

        :param slice_: FancySlice to be converted into slice
        :param dimension: TimeDimension or Dimension instance
        :returns: plain ``slice`` with integer positions
        :raises IndexError: if the bounds are booleans, of different types or not on the axis
        """
        name = f"{dimension.__class__.__name__} {dimension.name}"
        if any(isinstance(value, bool) for value in (slice_.start, slice_.stop, slice_.step)):
            raise IndexError(f"{name} wrong indexing: booleans are not valid in a slice")

        if all(st is not None for st in (slice_.start, slice_.stop)) and not type(  # noqa: E721,E714
            slice_.start
        ) is type(  # noqa: E721,RUF100
            slice_.stop
        ):
            raise IndexError(f"'start' and 'stop' of {name} slice shall be of the same type")

        step = self._convert_step(slice_.step, dimension)
        positions: list[int | None] = []
        for attr in ("start", "stop"):
            value = getattr(slice_, attr)
            if value is None:
                positions.append(None)
                continue

            if isinstance(dimension, TimeDimension):
                dim_pos = self._convert_value_for_time_dimension(value, dimension)
            else:  # Dimension
                dim_pos = self._convert_value_for_dimension(value, dimension)

            positions.append(dim_pos)

        return slice(positions[0], positions[1], step)

    def _fancy_slice_dimensions(
        self,
        item: FancySlice,
        dimension: TimeDimension | Dimension,
    ) -> slice | int:
        """Convert one index of the expression into a position or a plain slice on its axis.

        :param item: a slice, a datetime, a float or a string
        :param dimension: the axis
        :returns: the position or the plain slice
        :raises IndexError: if the index is not on the axis
        """
        if isinstance(item, slice):
            val = self._convert_fancy_slice(item, dimension)
        elif isinstance(item, datetime) and isinstance(dimension, TimeDimension):
            val = self._convert_value_for_time_dimension(item, dimension)
        elif isinstance(item, (float, str)):
            val = (
                self._convert_value_for_time_dimension(item, dimension)
                if isinstance(dimension, TimeDimension)
                else self._convert_value_for_dimension(item, dimension)
            )
        else:
            raise IndexError(f"{dimension.__class__.__name__} {dimension.name} wrong indexing: {item}")

        return val

    @staticmethod
    def _check_int_or_slice_or_ellipsis(element: FancySlice) -> TypeGuard[slice | int | EllipsisType]:
        """Tell whether an index needs no conversion: an integer, an ellipsis or a slice of integers.

        :param element: one index of the expression
        :returns: ``True`` for a plain index
        """
        if isinstance(element, (int, EllipsisType)):
            return True
        return isinstance(element, slice) and all(
            isinstance(getattr(element, attr), (int, NoneType)) and not isinstance(getattr(element, attr), bool)
            for attr in ("start", "stop", "step")
        )


class BaseArray(SelfLoggerMixin, Serializer, _FancySlicer, ABC, Generic[SubsetT]):
    """Arrays abstract class providing all its inheritors with common actions and methods."""

    __slots__ = (
        "__collection",
        "__adapter",
        "__id",
        "__is_deleted",
        "__dimensions",
        "custom_attributes",
        "primary_attributes",
    )

    primary_attributes: dict
    custom_attributes: dict

    def _validate(
        self,
        id_: str | SentinelType,
        primary_attributes: dict | SentinelType,
        custom_attributes: dict | SentinelType,
    ) -> None:
        """Validate the id and the types of the attributes.

        :param id_: array id, if given
        :param primary_attributes: primary attributes, if given
        :param custom_attributes: custom attributes, if given
        :raises DekerValidationError: if the id is not a uuid string or the attributes are not dictionaries
        """
        if not is_sentinel(id_) and not is_valid_uuid(id_):
            raise DekerValidationError(f"{self.__class__.__name__} id shall be a non-empty uuid string")
        for attrs in (primary_attributes, custom_attributes):
            if not is_sentinel(attrs) and not isinstance(attrs, dict):
                raise DekerValidationError(f"Invalid attributes type: {type(attrs)}")

    def __init__(
        self,
        collection: "Collection",
        adapter: Union["BaseArrayAdapter", "BaseVArrayAdapter"],
        array_adapter: Optional["BaseArrayAdapter"] = None,
        id_: str | SentinelType = Sentinel,
        primary_attributes: dict | SentinelType = Sentinel,
        custom_attributes: dict | SentinelType = Sentinel,
        *args: Any,  # noqa: ARG002
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        r"""BaseArray constructor.

        Validates id, primary and custom attributes; the attributes not given are empty.

        :param collection: Collection instance, to which the array is bound
        :param adapter: Array or VArray adapter instance
        :param array_adapter: Array adapter instance
        :param id_: Array uuid string; generated if not passed
        :param primary_attributes: primary attributes keyword mapping
        :param custom_attributes: custom attributes keyword mapping
        :param \*args: any arguments, ignored
        :param \**kwargs: any keyword arguments, ignored
        :raises DekerValidationError: if the id is not a uuid string or the attributes do not match the schema
        """
        super().__init__()
        self.__is_deleted = False
        self._validate(id_, primary_attributes, custom_attributes)
        self.__collection: "Collection" = collection
        self.__id: str = get_id() if is_sentinel(id_) else id_
        self.__adapter = adapter
        self.__array_adapter = array_adapter

        primary_attributes, custom_attributes = process_attributes(self.schema, primary_attributes, custom_attributes)

        self.primary_attributes, self.custom_attributes = make_ordered_dict(
            primary_attributes,
            custom_attributes,
            self.schema.attributes or (),
        )
        # the dimensions are built on the first access and rebuilt after an update of the custom attributes
        self.__dimensions: tuple[Dimension | TimeDimension, ...] = ()

    def __del__(self) -> None:
        del self.__adapter
        del self.__array_adapter
        del self.__collection

    @property
    @abstractmethod
    def _adapter(self) -> "IArrayAdapter":
        """Return Array or VArray adapter."""
        pass

    @property
    def as_dict(self) -> dict:
        """Serialize the array into a dictionary: id, collection, dimensions, shape and attributes."""
        return {
            "id": self.id,
            "collection": self.collection,
            "dimensions": tuple(dim.as_dict for dim in self.dimensions),
            "shape": self.shape,
            "named_shape": self.named_shape,
            "primary_attributes": self.primary_attributes,
            "custom_attributes": self.custom_attributes,
        }

    @property
    @abstractmethod
    def schema(self) -> ArraySchema | VArraySchema:
        """Collection schema.

        If it is VArray - return VArraySchema; if it is Array - return ArraySchema
        """
        pass

    @property
    def id(self) -> str:
        """Get array id."""
        return self.__id

    @property
    def dtype(self) -> type[Numeric]:
        """Get array values data type."""
        return self.schema.dtype

    @property
    def collection(self) -> str:
        """Get collection name."""
        return self.__collection.name

    @property
    def dimensions(self) -> tuple[Dimension | TimeDimension, ...]:
        """Get array dimensions, built from the schema and the attributes the time dimensions refer to.

        The dimensions are built once and kept: the schema and the primary attributes never change, and an
        update of the custom attributes, which a time dimension may refer to, drops the kept ones.
        """
        if not self.__dimensions:
            self.__dimensions = create_dimensions(
                self.schema.dimensions,
                {**self.primary_attributes, **self.custom_attributes},
            )
        return self.__dimensions

    @property
    def shape(self) -> tuple[int, ...]:
        """Get array shape."""
        return self.schema.shape

    @property
    def named_shape(self) -> tuple[tuple[str, int], ...]:
        """Get array shape mapping."""
        return self.schema.named_shape

    @property
    def fill_value(self) -> Numeric:
        """Get array fill_value."""
        fill_value = self.schema.fill_value
        if fill_value is None:  # pragma: no cover
            raise DekerInvalidSchemaError("Schema fill_value is not set")
        return fill_value

    def update_custom_attributes(self, attributes: dict, *, timeout: int | SentinelType = Sentinel) -> None:
        """Update array custom attributes in the storage and in the object.

        This method accepts attributes that should be changed.
        Any attribute that is not in the given dict will be kept in the object attributes without any change.

        :param attributes: attributes for updating
        :param timeout: seconds to wait for the lock of the array; not given - ``write_lock_timeout`` of the client
        :raises DekerValidationError: if no attributes are passed, an attribute is not in the schema, is primary
          or has a wrong type, or the timeout is not a positive integer
        :raises DekerLockError: if the array is not released within the timeout
        :raises DekerCancelledError: if the update is cancelled before it started
        """
        validate_timeout(timeout=timeout)
        attributes = validate_custom_attributes_update(
            self.schema,
            self.primary_attributes,
            self.custom_attributes,
            attributes,
        )
        with TokenErrorConverter():
            token = write_token()
            check(token)
            self._adapter.update_meta_custom_attributes(self, attributes, timeout=timeout, token=token)
        self.custom_attributes = attributes
        self.__dimensions = ()
        self.logger.info("%s custom attributes updated: %s", self, attributes)

    def _create_meta(self) -> str:
        """Serialize the array into its metadata JSON string: id, primary and custom attributes.

        :returns: the JSON string
        """
        primary_attrs, custom_attrs = (
            deepcopy(self.primary_attributes),
            deepcopy(self.custom_attributes),
        )
        for attrs in (primary_attrs, custom_attrs):
            for key, value in attrs.items():
                attrs[key] = serialize_attribute_value(value)

        return json.dumps(
            {
                "id": self.id,
                "primary_attributes": primary_attrs,
                "custom_attributes": custom_attrs,
            }
        )

    def read_meta(self) -> ArrayMeta | None:
        """Read the metadata of the array from the storage.

        :returns: the metadata: id, primary and custom attributes
        """
        meta = self._adapter.read_meta(self)
        self.logger.info("%s meta data read: %s", self, meta)
        return meta

    def _delete(self, *, timeout: int | SentinelType, token: AbstractToken) -> None:
        """Delete the array under the token of an operation built already: a ``VSubset`` deletes its ``Arrays`` so.

        :param timeout: timeout of the operation as it was given
        :param token: token of the write operation
        """
        check(token)
        self._adapter.delete(self, timeout=timeout, token=token)

    def delete(self, *, timeout: int | SentinelType = Sentinel) -> None:
        """Delete the array from the storage with its data and metadata.

        The deletion waits for the readers and the writers of the array, up to its timeout.

        :param timeout: seconds to wait for the lock of each file of the array; not given - ``write_lock_timeout``
          of the client
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerLockError: if a file of the array is not released within the timeout
        :raises DekerCancelledError: if the deletion is cancelled before any file of it is started
        """
        validate_timeout(timeout=timeout)
        with TokenErrorConverter():
            self._delete(timeout=timeout, token=write_token())
        self.logger.info("%s deleted", self)

    @staticmethod
    def _check_position(element: slice | int | EllipsisType, dim: TimeDimension | Dimension) -> None:
        """Check that an integer index is on the axis: from ``-size`` to ``size - 1``, as in numpy.

        :param element: one plain index of the expression
        :param dim: the axis it applies to
        :raises IndexError: if the index is an integer out of range
        """
        if isinstance(element, int) and not -dim.size <= element < dim.size:
            raise IndexError(f"{dim.__class__.__name__} {dim.name} wrong indexing: {element} is out of range")

    def _get_fancy_item(self, item: FancySlice) -> Slice:
        """Convert an index expression with labels, scale values and datetimes into plain indices.

        :param item: index expression
        :returns: the same expression with integers and plain slices
        :raises IndexError: if a value is not on its axis or is out of range
        """
        if isinstance(item, tuple):
            fancy_items: list[slice | int | EllipsisType] = []
            # the indices after an ellipsis apply to the last axes
            ellipsis_at = next((n for n, i in enumerate(item) if i is ...), len(item))
            for n, i in enumerate(item):
                if i is ...:
                    fancy_items.append(i)
                    continue
                dim = self.dimensions[n] if n < ellipsis_at else self.dimensions[n - len(item)]
                element = i if self._check_int_or_slice_or_ellipsis(i) else self._fancy_slice_dimensions(i, dim)
                self._check_position(element, dim)
                fancy_items.append(element)
            return tuple(fancy_items)
        if self._check_int_or_slice_or_ellipsis(item):
            self._check_position(item, self.dimensions[0])
            return item
        return self._fancy_slice_dimensions(item, self.dimensions[0])

    @classmethod
    def _create_from_meta(
        cls: type[BaseArrayT],
        collection: "Collection",
        meta: "ArrayMeta",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"] = None,
    ) -> BaseArrayT:
        """Create Array or VArray from metadata.

        :param collection: Collection instance
        :param meta: array metadata
        :param array_adapter: Array adapter instance
        :param varray_adapter: VArray adapter instance
        :returns: ``Array`` or ``VArray`` built from the metadata
        """
        schema = collection.varray_schema if varray_adapter else collection.array_schema
        if schema is None:  # pragma: no cover
            raise DekerMetaDataError(f"{cls} cannot be created: the collection has no VArraySchema")

        try:
            # To ensure the order of attributes
            primary_attributes, custom_attributes = make_ordered_dict(
                meta["primary_attributes"],
                meta["custom_attributes"],
                schema.attributes or (),
            )

            arr_params: dict[str, Any] = {
                "collection": collection,
                "adapter": array_adapter,
                "id_": meta["id"],
                "primary_attributes": primary_attributes,
                "custom_attributes": custom_attributes,
            }
            if varray_adapter:
                arr_params.update({"adapter": varray_adapter, "array_adapter": array_adapter})
            return cls(**arr_params)
        except (KeyError, ValueError) as e:
            raise DekerMetaDataError(f"{cls} metadata invalid/corrupted: {e}")

    @abstractmethod
    def _create_subset(self, bounds: tuple[slice | int, ...], shape: tuple[int, ...]) -> SubsetT:
        """Create a lazy subset bound to the array with its own adapters.

        :param bounds: validated and normalized index expression
        :param shape: subset shape
        :returns: ``Subset`` for an ``Array`` or ``VSubset`` for a ``VArray``
        """
        pass

    def __getitem__(self, item: FancySlice) -> SubsetT:
        """Get a subset from Array or VArray.

        The expression may hold integers, slices with any non-zero step, an ellipsis, and, on the axes that have
        them, labels, scale values and datetimes; see ``_convert_step`` for the steps of a time or a scaled axis.

        :param item: index expression
        :returns: lazy ``Subset`` or ``VSubset`` for the given bounds
        :raises IndexError: if the expression is invalid, out of range or selects nothing
        :raises DekerMemoryError: if the subset does not fit the memory limit of the client
        """
        valid_types = int, float, str, datetime, slice, tuple, type(Ellipsis)
        if (
            not isinstance(item, valid_types)
            or isinstance(item, bool)
            or item is None
            or (
                isinstance(item, tuple)
                and (
                    not all(isinstance(i, valid_types) for i in item)
                    or any(isinstance(i, bool) for i in item)
                    or any(i is None for i in item)
                )
            )
        ):
            raise IndexError(
                "Only integers, floats, strings, datetime, tuples, slices (`:`) and ellipsis (`...`) are valid indices"
            )
        if isinstance(item, tuple) and item.count(...) > 1:
            raise IndexError("An index can only have a single ellipsis (`...`)")

        item = self._get_fancy_item(item)

        shape = create_shape_from_slice(self.shape, item)
        if any(s == 0 for s in shape):
            raise IndexError(
                f"Invalid indexing: some of the axes result in zero length: subset shape {shape}, item {item}"
            )

        check_memory(shape, self.dtype, self.__adapter.ctx.config.memory_limit)
        pre_bounds = np.index_exp[item]
        # an ellipsis stands for every axis the other indices leave out, as in numpy
        bounds: list[Any] = []
        for i in pre_bounds:
            if i is ...:
                bounds.extend(slice(None) for _ in range(len(self.shape) - len(pre_bounds) + 1))
            else:
                bounds.append(i)
        subset = self._create_subset(tuple(bounds), shape)
        self.logger.debug("Created subset: %s", subset)
        return subset

    def __repr__(self) -> str:
        s = f"{self.__class__.__name__}(id={self.id!r}, collection={self.collection!r})"
        if self.primary_attributes:
            s = s[:-1] + f", primary_attributes={self.primary_attributes!r})"
        if self.custom_attributes:
            s = s[:-1] + f", custom_attributes={self.custom_attributes!r})"
        return s

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.id})"

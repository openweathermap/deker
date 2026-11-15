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

"""Abstract interfaces for array and virtual array."""

import json

from abc import ABC, abstractmethod
from copy import deepcopy
from datetime import datetime, timedelta
from types import EllipsisType, NoneType
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeGuard, TypeVar, Union

import numpy as np

from deker_tools.slices import create_shape_from_slice
from deker_tools.time import get_utc

from deker.ABC.base_subset import BaseSubset
from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerInvalidSchemaError, DekerMetaDataError, DekerValidationError
from deker.log import SelfLoggerMixin
from deker.schemas import ArraySchema, VArraySchema
from deker.tools.array import check_memory, get_id
from deker.tools.attributes import make_ordered_dict, serialize_attribute_value
from deker.tools.schema import create_dimensions
from deker.tools.time import get_start_datetime
from deker.types.private.classes import ArrayMeta, Serializer
from deker.types.private.typings import FancySlice, Numeric, Slice
from deker.validators import is_valid_uuid, process_attributes, validate_custom_attributes_update

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

        :param value: value to be converted into position
        :param dimension: TimeDimension instance
        :returns: index of the value in the dimension
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

        :param value: value to be converted into position
        :param dimension: Dimension instance
        :returns: index of the value in the dimension
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
        "custom_attributes",
        "primary_attributes",
    )

    primary_attributes: dict
    custom_attributes: dict

    def _validate(
        self,
        id_: str | None,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
    ) -> None:
        if id_ is not None and not is_valid_uuid(id_):
            raise DekerValidationError(f"{self.__class__.__name__} id shall be a non-empty uuid.uuid5 string or None")
        for attrs in (primary_attributes, custom_attributes):
            if attrs is not None and not isinstance(attrs, dict):
                raise DekerValidationError(f"Invalid attributes type: {type(attrs)}")

    def __init__(
        self,
        collection: "Collection",
        adapter: Union["BaseArrayAdapter", "BaseVArrayAdapter"],
        array_adapter: Optional["BaseArrayAdapter"] = None,
        id_: str | None = None,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        *args: Any,  # noqa: ARG002
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        """BaseArray constructor.

        Validates id, primary and custom attributes, sets attributes to empty dict if None.

        :param collection: Collection instance, to which the array is bound
        :param adapter: Array or VArray adapter instance
        :param array_adapter: Array adapter instance
        :param id_: Array uuid string
        :param primary_attributes: primary attributes keyword mapping
        :param custom_attributes: custom attributes keyword mapping
        :param *args: any arguments
        :param **kwargs: any keyword arguments
        """
        super().__init__()
        self.__is_deleted = False
        self._validate(id_, primary_attributes, custom_attributes)
        self.__collection: "Collection" = collection
        self.__id: str = id_ if id_ else get_id()
        self.__adapter = adapter
        self.__array_adapter = array_adapter

        primary_attributes, custom_attributes = process_attributes(self.schema, primary_attributes, custom_attributes)

        self.primary_attributes, self.custom_attributes = make_ordered_dict(
            primary_attributes,
            custom_attributes,
            self.schema.attributes or (),
        )

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
        """Serialize self attributes into dict."""
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
        """Get array dimensions."""
        return create_dimensions(
            self.schema.dimensions,
            {**self.primary_attributes, **self.custom_attributes},
        )

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

    def update_custom_attributes(self, attributes: dict) -> None:
        """Update array custom attributes.

        This method accepts attributes that should be changed.
        Any attribute that is not in the given dict will be kept in the object attributes without any change.
        If the given dict contains any attribute that is not in the schema, DekerValidationError will be raised.

        :param attributes: attributes for updating
        """
        attributes = validate_custom_attributes_update(
            self.schema,
            self.primary_attributes,
            self.custom_attributes,
            attributes,
        )
        self._adapter.update_meta_custom_attributes(self, attributes)
        self.custom_attributes = attributes
        self.logger.info(f"{self!s} custom attributes updated: {attributes}")

    def _create_meta(self) -> str:
        """Serialize array into metadata JSON string."""
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
        """Read data from array."""
        meta = self._adapter.read_meta(self)
        self.logger.info(f"{self!s} meta data read: {meta}")
        return meta

    def delete(self) -> None:
        """Delete array from storage."""
        self._adapter.delete(self)
        self.logger.info(f"{self!s} deleted")

    def _get_fancy_item(self, item: FancySlice) -> Slice:
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
                if isinstance(element, int) and element > dim.size:
                    raise IndexError(
                        f"{dim.__class__.__name__} dimension {dim.name} wrong indexing: {element} out of range"
                    )
                fancy_items.append(element)
            return tuple(fancy_items)
        if self._check_int_or_slice_or_ellipsis(item):
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

        :param item: index expression
        :returns: lazy ``Subset`` or ``VSubset`` for the given bounds
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
        self.logger.debug(f"Created subset: {subset}")
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

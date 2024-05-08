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
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, Union

import numpy as np

from deker_tools.slices import create_shape_from_slice
from deker_tools.time import get_utc

from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerMetaDataError, DekerValidationError
from deker.log import SelfLoggerMixin
from deker.schemas import ArraySchema, VArraySchema
from deker.subset import Subset, VSubset
from deker.tools.array import check_memory, get_id
from deker.tools.attributes import make_ordered_dict, serialize_attribute_value
from deker.tools.schema import create_dimensions
from deker.types.private.classes import ArrayMeta, Serializer
from deker.types.private.typings import FancySlice, Numeric, Slice
from deker.validators import is_valid_uuid, process_attributes, validate_custom_attributes_update


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter, IArrayAdapter
    from deker.arrays import Array, VArray
    from deker.collection import Collection


class _FancySlicer(object):
    """Converter of datetimes, floats and strings into index integers."""

    @staticmethod
    def _convert_value_for_time_dimension(
        value: FancySlice,
        dimension: TimeDimension,
    ) -> int:
        """Return value position (index) in TimeDimension.

        :param value: value to be converted into position
        :param dimension: TimeDimension instance
        """
        if isinstance(value, int):
            return value

        start: datetime = dimension.start_value
        step = dimension.step

        # check step
        if isinstance(value, timedelta):
            # if passed timedelta does not match step size
            if (value % step).total_seconds():
                raise IndexError(f"TimeDimension {dimension} wrong indexing: check 'step' value")
            return int(value // step)

        # check start and stop type
        try:
            dt = get_utc(value)  # type: ignore[arg-type]
            position = int((dt - start) // step)

            # if passed timestamp/iso-string does not match step size
            if remainder := int(((dt - start) % step).total_seconds()):
                raise IndexError(
                    f"TimeDimension {dimension.name} wrong indexing: "
                    f"{dimension.name} has no index {position}.{remainder}"
                )

            if position < 0 or position > dimension.size:
                raise IndexError(
                    f"{dimension.__class__.__name__} {dimension.name} out of range: {position}"
                )
            return position
        except (ValueError, TypeError) as e:
            raise IndexError(
                f"TimeDimension {dimension} wrong value: {value} of type {type(value)}; {e}"
            )

    @staticmethod
    def _convert_value_for_dimension(value: FancySlice, dimension: Dimension) -> int:
        """Return value position (index) in Dimension.

        :param value: value to be converted into position
        :param dimension: Dimension instance
        """
        if isinstance(value, int):
            return value

        if isinstance(value, str):
            if not dimension.labels:
                raise IndexError(
                    f"Dimension {dimension.name} wrong indexing: {dimension.name} has no labels"
                )
            dim_pos = dimension.labels.name_to_index(value)
            if dim_pos is None:
                raise IndexError(
                    f"Dimension {dimension.name} wrong indexing: {dimension.name} has no label {value}"
                )
        elif isinstance(value, float):
            if not dimension.scale:
                raise IndexError(
                    f"Dimension {dimension.name} wrong indexing: {dimension.name} has no scale"
                )

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

    def _convert_fancy_slice(
        self, slice_: FancySlice, dimension: Union[TimeDimension, Dimension]
    ) -> slice:
        """Convert FancySlice to slice.

        :param slice_: FancySlice to be converted into slice
        :param dimension: TimeDimension or Dimension instance
        """
        slice_parameters = []

        if all(
            st is not None for st in (slice_.start, slice_.stop)
        ) and not type(  # noqa: E721,E714
            slice_.start
        ) is type(  # noqa: E721,RUF100
            slice_.stop
        ):
            raise IndexError(
                f"'start' and 'stop' of {dimension.__class__.__name__} {dimension.name} slice "
                f"shall be of the same type"
            )

        if slice_.step is not None:
            valid_step_types = int
            if isinstance(dimension, TimeDimension):
                valid_step_types = (int, timedelta)
            if not isinstance(slice_.step, valid_step_types):
                raise IndexError(
                    f"'step' of {dimension.__class__.__name__} {dimension.name} slice "
                    f"shall be None or {valid_step_types}"
                )

        for attr in ("start", "stop", "step"):
            value = getattr(slice_, attr)

            # check if some attrs are None
            if value is None:
                if attr == "start":
                    pos = 0
                elif attr == "stop":
                    pos = dimension.size
                else:
                    pos = None
                slice_parameters.append(pos)
                continue

            if isinstance(dimension, TimeDimension):
                dim_pos = self._convert_value_for_time_dimension(value, dimension)
            else:  # Dimension
                dim_pos = self._convert_value_for_dimension(value, dimension)

            if dim_pos < 0 or dim_pos > dimension.size:
                raise IndexError(
                    f"{dimension.__class__.__name__} {dimension.name} wrong indexing: {slice_}, check '{attr}' value"
                )

            slice_parameters.append(dim_pos)

        return slice(*slice_parameters)

    @staticmethod
    def _is_time_dimension(dim: Union[TimeDimension, Dimension]) -> bool:
        return isinstance(dim, TimeDimension)

    def _fancy_slice_dimensions(
        self,
        item: FancySlice,
        dimension: Union[TimeDimension, Dimension],
    ) -> Slice:
        params = item, dimension
        if isinstance(item, slice):
            val = self._convert_fancy_slice(*params)  # type: ignore[arg-type]
        elif isinstance(item, datetime) and isinstance(dimension, TimeDimension):
            val = self._convert_value_for_time_dimension(*params)  # type: ignore[arg-type]
        elif isinstance(item, (float, str)):
            val = (
                self._convert_value_for_time_dimension(*params)  # type: ignore[arg-type]
                if self._is_time_dimension(dimension)
                else self._convert_value_for_dimension(*params)  # type: ignore[arg-type]
            )
        else:
            raise IndexError(
                f"{dimension.__class__.__name__} {dimension.name} wrong indexing: {item}"
            )

        return val

    @staticmethod
    def _check_int_or_slice_or_ellipsis(element: FancySlice) -> bool:
        types = (int, type(Ellipsis), type(None))
        if isinstance(element, types) or (
            isinstance(element, slice)
            and all(isinstance(getattr(element, attr), types) for attr in ("start", "stop", "step"))
        ):
            return True
        return False


class BaseArray(SelfLoggerMixin, Serializer, _FancySlicer, ABC):
    """Arrays abstract class providing all its inheritors with common actions and methods."""

    __slots__ = (
        "__collection",
        "__adapter",
        "__id",
        "__is_deleted",
        "custom_attributes",
        "primary_attributes",
    )

    def _validate(
        self,
        id_: Optional[str],
        primary_attributes: Optional[dict] = None,
        custom_attributes: Optional[dict] = None,
    ) -> None:
        if id_ is not None and not is_valid_uuid(id_):
            raise DekerValidationError(
                f"{self.__class__.__name__} id shall be a non-empty uuid.uuid5 string or None"
            )
        for attrs in (primary_attributes, custom_attributes):
            if attrs is not None and not isinstance(attrs, dict):
                raise DekerValidationError(f"Invalid attributes type: {type(attrs)}")

    def __init__(
        self,
        collection: "Collection",
        adapter: Union["BaseArrayAdapter", "BaseVArrayAdapter"],
        array_adapter: Optional["BaseArrayAdapter"] = None,
        id_: Optional[str] = None,
        primary_attributes: Optional[dict] = None,
        custom_attributes: Optional[dict] = None,
        *args: Any,  # noqa[ARG002]
        **kwargs: Any,  # noqa[ARG002]
    ) -> None:
        """BaseArray constructor.

        Validates id, primary and custom attributes, sets attributes to empty dict if None.

        :param collection: Collection instance, to which the array is bound
        :param adapter: Array or VArray adapter instance
        :param array_adapter: Array adapter instance
        :param id_: Array uuid string
        :param primary_attributes: primary attributes keyword mapping
        :param custom_attributes: custom attributes keyword mapping
        :param args: any arguments
        :param kwargs: any keyword arguments
        """
        super().__init__()
        self.__is_deleted = False
        self._validate(id_, primary_attributes, custom_attributes)
        self.__collection: "Collection" = collection
        self.__id: str = id_ if id_ else get_id()
        self.__adapter = adapter
        self.__array_adapter = array_adapter

        primary_attributes, custom_attributes = process_attributes(
            self.schema, primary_attributes, custom_attributes
        )

        self.primary_attributes, self.custom_attributes = make_ordered_dict(
            primary_attributes, custom_attributes, self.schema.attributes  # type: ignore[arg-type]
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
    def schema(self) -> Union[ArraySchema, VArraySchema]:
        """Collection schema.

        If it is VArray - return VArraySchema; if it is Array - return ArraySchema
        """
        pass

    @property
    def id(self) -> str:
        """Get array id."""
        return self.__id

    @property
    def dtype(self) -> Type[Numeric]:
        """Get array values data type."""
        return self.schema.dtype

    @property
    def collection(self) -> str:
        """Get collection name."""
        return self.__collection.name

    @property
    def dimensions(self) -> Tuple[Union[Dimension, TimeDimension], ...]:
        """Get array dimensions."""
        return create_dimensions(
            self.schema.dimensions,  # type: ignore[arg-type]
            {**self.primary_attributes, **self.custom_attributes},
        )

    @property
    def shape(self) -> Tuple[int, ...]:
        """Get array shape."""
        return self.schema.shape

    @property
    def named_shape(self) -> Tuple[Tuple[str, int], ...]:
        """Get array shape mapping."""
        return self.schema.named_shape

    @property
    def fill_value(self) -> Union[Numeric, type(np.nan)]:  # type: ignore[valid-type]
        """Get array fill_value."""
        return self.schema.fill_value

    def update_custom_attributes(self, attributes: dict) -> None:
        """Update array custom attributes.

        This method accepts attributes that should be changed.
        Any attribute that is not in the given dict will be kept in the object attributes without any change.
        If the given dict contains any attribute that is not in the schema, DekerValidationError will be raised.

        :param attributes: attributes for updating
        """
        attributes = validate_custom_attributes_update(
            self.schema,
            self.dimensions,
            self.primary_attributes,
            self.custom_attributes,
            attributes,
        )
        self._adapter.update_meta_custom_attributes(self, attributes)
        self.custom_attributes = attributes
        self.logger.info(f"{self!s} custom attributes updated: {attributes}")

    def _create_meta(self) -> str:
        """Serialize array into metadata JSON string."""
        primary_attrs, custom_attrs = deepcopy(self.primary_attributes), deepcopy(
            self.custom_attributes
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

    def read_meta(self) -> Optional[ArrayMeta]:
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
            fancy_item = []
            for n, i in enumerate(item):
                dim = self.dimensions[n]
                if not self._check_int_or_slice_or_ellipsis(i):
                    i = self._fancy_slice_dimensions(i, dim)
                if isinstance(i, int) and i > dim.size:
                    raise IndexError(
                        f"{dim.__class__.__name__} dimension {dim.name} wrong indexing: {i} out of range"
                    )
                fancy_item.append(i)
            fancy_item = tuple(fancy_item)
        elif self._check_int_or_slice_or_ellipsis(item):
            fancy_item = item
        else:
            fancy_item = self._fancy_slice_dimensions(item, self.dimensions[0])

        return fancy_item  # type: ignore[return-value]

    @classmethod
    def _create_from_meta(
        cls,
        collection: "Collection",
        meta: "ArrayMeta",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"] = None,
    ) -> Union["Array", "VArray"]:
        """Create Array or VArray from metadata.

        :param collection: Collection instance
        :param meta: array metadata
        :param array_adapter: Array adapter instance
        :param varray_adapter: VArray adapter instance
        """
        if varray_adapter:
            attrs_schema = collection.varray_schema.attributes
        else:
            attrs_schema = collection.array_schema.attributes

        try:
            # To ensure the order of attributes
            primary_attributes, custom_attributes = make_ordered_dict(
                meta["primary_attributes"],
                meta["custom_attributes"],
                attrs_schema,  # type: ignore[arg-type]
            )

            arr_params = {
                "collection": collection,
                "adapter": array_adapter,
                "id_": meta["id"],
                "primary_attributes": primary_attributes,
                "custom_attributes": custom_attributes,
            }
            if varray_adapter:
                arr_params.update({"adapter": varray_adapter, "array_adapter": array_adapter})
            return cls(**arr_params)  # type: ignore[arg-type,return-value]
        except (KeyError, ValueError) as e:
            raise DekerMetaDataError(f"{cls} metadata invalid/corrupted: {e}")

    def __getitem__(self, item: FancySlice) -> Union[Subset, VSubset]:
        """Get a subset from Array or VArray.

        :param item: index expression
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
        bounds: Union[List, Tuple] = []
        for i in pre_bounds:
            if i is ...:
                bounds.extend(np.index_exp[:])
            else:
                bounds.append(i)
        bounds = tuple(bounds)
        if isinstance(self.schema, VArraySchema):
            subset = VSubset(
                bounds, shape, self, self.__array_adapter, self.__adapter, self.__collection  # type: ignore[arg-type]
            )
        else:
            subset = Subset(bounds, shape, self, self.__adapter)  # type: ignore[arg-type]
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

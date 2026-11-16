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

"""Abstract interfaces for adapters."""

import datetime
import warnings

from abc import ABC, abstractmethod
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Generic, Optional, TypeVar, get_args

import numpy as np

from deker_tools.slices import create_shape_from_slice
from deker_tools.time import get_utc
from numpy import ndarray

from deker.ctx import CTX
from deker.errors import (
    DekerArrayTypeError,
    DekerFilterError,
    DekerMetaDataError,
    DekerValidationError,
)
from deker.log import SelfLoggerMixin
from deker.schemas import SchemaTypeEnum
from deker.tools import create_attributes_schema, create_dimensions_schema
from deker.tools.decorators import check_ctx_state
from deker.types import DTypeEnum
from deker.types.private.classes import ArrayMeta
from deker.types.private.typings import ArrayT, Data, Numeric, Slice
from deker.validators import is_valid_uuid

if TYPE_CHECKING:
    from deker.ABC.base_collection import BaseCollectionOptions
    from deker.ABC.base_factory import BaseAdaptersFactory
    from deker.ABC.base_schemas import BaseArraysSchema
    from deker.arrays import Array, VArray  # noqa: F401  # forward references in the adapter base classes
    from deker.collection import Collection
    from deker.uri import Uri


OptionsT = TypeVar("OptionsT", bound="BaseCollectionOptions")
PathT = TypeVar("PathT", Path, "Uri")


class BaseStorageAdapter(ABC, Generic[OptionsT]):
    """Interface for adapters working with files with data (e.g hdf5, tiff, etc).

    Doesn't accept any params for instantiation. ``OptionsT`` is the type of the collection options the adapter
    understands (``HDF5Options`` for HDF5): the core validates and passes the options through ``storage_options``.
    """

    file_ext: str
    storage_options: type[OptionsT]

    @abstractmethod
    def create(self, path: Path, array_shape: tuple[int, ...], metadata: str | bytes | dict) -> None:
        """Create a new file of an array with its metadata and no data yet.

        :param path: path to the file; it shall not exist
        :param array_shape: shape of the array
        :param metadata: array metadata
        :raises FileExistsError: if the file exists
        """
        pass

    @abstractmethod
    def read_data(
        self,
        path: Path,
        array_shape: tuple[int, ...],
        bounds: Slice,
        fill_value: Numeric,
        dtype: type[Numeric],
    ) -> ndarray | Numeric:
        """Read array data from the file of the array.

        :param path: path to the file
        :param array_shape: shape of the array
        :param bounds: array slice
        :param fill_value: value the cells never written read as
        :param dtype: array dtype
        :returns: data within the bounds; a scalar for a single cell
        """
        pass

    @abstractmethod
    def read_meta(self, path: Path) -> ArrayMeta:
        """Read array metadata from the file of the array.

        :param path: path to the file
        :returns: array metadata
        """
        pass

    @abstractmethod
    def update_data(
        self,
        path: Path,
        bounds: Slice,
        data: ndarray | Numeric,
        dtype: type[Numeric],
        shape: tuple[int, ...],
        fill_value: Numeric,
        collection_options: OptionsT | None,
    ) -> None:
        """Update array data in the storage file.

        :param path: path to the file
        :param bounds: array slice
        :param data: new data for the slice, already validated and cast to the array dtype
        :param dtype: array dtype
        :param shape: array shape
        :param fill_value: array fill_value
        :param collection_options: chunking and compression options
        """
        pass

    @abstractmethod
    def update_meta_custom_attributes(self, path: Path, attributes: dict) -> dict:
        """Replace the custom attributes in the metadata of the file.

        :param path: path to the file
        :param attributes: new custom attributes
        :returns: the stored custom attributes
        """
        pass

    @abstractmethod
    def clear_data(self, path: Path, array_shape: tuple[int, ...], bounds: Slice, fill_value: Numeric) -> None:
        """Set the cells within the bounds to the fill value; the data of an entirely empty array are removed.

        :param path: path to the file
        :param array_shape: array shape
        :param bounds: array bounds to clear
        :param fill_value: array fill_value
        """
        pass

    @abstractmethod
    def read_empty_cells(self, path: Path) -> int:
        """Read the stored counter of empty cells of an array.

        :param path: path to the file
        :returns: the counter as stored
        """
        pass

    @abstractmethod
    def count_empty_cells(self, path: Path, array_shape: tuple[int, ...], fill_value: Numeric) -> int:
        """Count the empty cells of an array in its data, whatever the stored counter says.

        :param path: path to the file
        :param array_shape: array shape
        :param fill_value: array fill_value
        :returns: number of cells holding the fill value
        """
        pass

    @abstractmethod
    def write_empty_cells(self, path: Path, empty_cells: int) -> None:
        """Replace the stored counter of empty cells of an array.

        :param path: path to the file
        :param empty_cells: the new value of the counter
        """
        pass


class BaseCollectionAdapter(SelfLoggerMixin, ABC, Generic[PathT]):
    """Base interface for collection adapters.

    ``PathT`` is the type of the collections location in the storage: a ``Path`` for local storages,
    a ``Uri`` for remote ones.
    """

    file_ext: str
    metadata_version: str

    def __init__(self, ctx: CTX):
        """BaseCollectionAdapter constructor.

        :param ctx: context of the client
        """
        self.ctx = ctx
        self.uri = self.ctx.uri

    def __del__(self) -> None:
        """Close the adapter when it is dropped."""
        self.close()

    @abstractmethod
    def is_deleted(self, collection: "Collection") -> bool:
        """Check if the collection was deleted.

        :param collection: Collection instance
        :returns: ``True`` if the collection no longer exists in the storage
        """
        pass

    @property
    @abstractmethod
    def collections_resource(self) -> PathT:
        """Return the location of the collections in the storage."""
        pass

    def close(self) -> None:
        """Close adapter: release the resources held by a concrete adapter, if any."""
        pass

    @abstractmethod
    def create(self, collection: "Collection") -> None:
        """Add collection to storage and creates file structure.

        :param collection: Collection to be created
        :raises DekerCollectionAlreadyExistsError: if a collection of the name exists
        """
        pass

    @abstractmethod
    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Collection name
        :returns: collection metadata
        :raises DekerCollectionNotExistsError: if there is no collection of the name
        """
        pass

    @abstractmethod
    def delete(self, collection: "Collection") -> None:
        """Delete collection.

        :param collection: Collection to delete
        """
        pass

    @abstractmethod
    def clear(self, collection: "Collection") -> None:
        """Remove every array of the collection; the collection itself stays.

        :param collection: Collection to be cleared
        """
        pass

    @check_ctx_state
    def create_collection_from_meta(self, collection_data: dict, factory: "BaseAdaptersFactory") -> "Collection":
        """Create a Collection instance from meta information.

        :param collection_data: dict with information about Collection (schemas, vgrid, etc)
        :param factory: adapters factory instance
        :returns: the ``Collection`` object; nothing is written to the storage
        :raises DekerMetaDataError: if the metadata is invalid or corrupted
        """
        from deker.collection import Collection

        name: str = collection_data["name"]
        data: dict = collection_data["schema"]

        # Check schema type
        schema_class = SchemaTypeEnum[collection_data["type"]].value

        try:
            dtype = DTypeEnum[data["dtype"].split("numpy.")[-1]].value
            fill_value = dtype(data["fill_value"]) if data["fill_value"] is not None else data["fill_value"]
            # Create schema
            schema = schema_class(
                **{
                    **data,
                    "dimensions": create_dimensions_schema(data["dimensions"]),
                    "attributes": create_attributes_schema(data["attributes"]),
                    "dtype": dtype,
                    "fill_value": fill_value,
                }
            )
            storage_adapter = self.get_storage_adapter(collection_data.get("storage_adapter"))
            coll_params = storage_adapter.storage_options._process_options(collection_data.get("options"))

            collection_options = storage_adapter.storage_options(**coll_params) if coll_params else None

            collection = Collection(
                name=name,
                schema=schema,
                adapter=self,
                factory=factory,
                collection_options=collection_options,
                storage_adapter=storage_adapter,
            )

            self.logger.debug(f"Collection {collection.name} read")
            return collection
        except (KeyError, ValueError, TypeError, AttributeError) as e:
            self.logger.exception(e)  # noqa: TRY401
            raise DekerMetaDataError(f'Collection "{name}" metadata is invalid/corrupted: {e}')

    @staticmethod
    def get_storage_adapter(storage_adapter: str | None = None) -> type[BaseStorageAdapter]:
        """Get storage adapter type.

        :param storage_adapter: Storage adapter name
        :returns: storage adapter class of the given type
        """
        from deker_local_adapters import storage_adapter_factory

        return storage_adapter_factory(storage_adapter)

    @abstractmethod
    def __iter__(self) -> Generator[dict, None, None]:
        """Iterate over the metadata of all the Collections in the storage.

        :yields: the metadata dictionary of every collection
        """
        pass


class IArrayAdapter(ABC, Generic[PathT, ArrayT]):
    """Interface for Array or VArray adapters.

    ``PathT`` is the type of the collection location in the storage: a ``Path`` for local storages,
    a ``Uri`` for remote ones. ``ArrayT`` is the kind of arrays the adapter manages: ``Array`` or ``VArray``.
    """

    collection_path: PathT

    def __init__(
        self,
        collection_path: PathT,
        ctx: CTX,
        executor: ThreadPoolExecutor,
        storage_adapter: type[BaseStorageAdapter],
    ) -> None:
        """IArrayAdapter constructor.

        :param collection_path: location of the collection in the storage
        :param ctx: context of the client
        :param executor: thread pool the arrays of a ``VArray`` are processed in
        :param storage_adapter: type of the adapter of the files
        """
        self.collection_path = collection_path
        self.ctx = ctx
        self.executor = executor
        self.uri = self.ctx.uri
        self.storage_adapter = storage_adapter()

    @staticmethod
    def _convert_python_numbers(array_dtype: type[Numeric], data: list | tuple | int | float | complex) -> ndarray:
        """Convert Python numbers to an array of the array dtype, if every value fits the dtype.

        An integer dtype takes values it can hold exactly: no wrapping, no fractions, no nan or inf.
        A float dtype takes values it can hold up to rounding: a finite value stays finite, nan and inf
        stay as they are. A complex dtype applies the float rule to both parts; complex values do not fit
        a real dtype. Booleans, strings, None and nested sequences of uneven length are not numbers.

        :param array_dtype: array dtype
        :param data: Python number or a (nested) list/tuple of them
        :returns: array of the array dtype with the same values and shape
        :raises DekerArrayTypeError: if the data is not Python numbers or a value does not fit the dtype
        """
        # an object array keeps every Python number as it is: no silent rounding of big integers by numpy
        source = np.asarray(data, dtype=object)
        if not all(isinstance(value, (int, float, complex)) and not isinstance(value, bool) for value in source.flat):
            raise DekerArrayTypeError(f"Invalid data: Python numbers of {array_dtype.__name__} expected")

        target_kind = np.dtype(array_dtype).kind
        if target_kind != "c" and any(isinstance(value, complex) for value in source.flat):
            raise DekerArrayTypeError(f"Invalid data: complex values do not fit {array_dtype.__name__}")

        with warnings.catch_warnings(), np.errstate(all="ignore"):
            warnings.simplefilter("ignore")
            try:
                # an integer out of the dtype range, nan or inf into an integer dtype, or an integer beyond
                # the float range raise here
                converted = source.astype(array_dtype)
                probe = source.astype(np.complex128)
            except (OverflowError, TypeError, ValueError):
                raise DekerArrayTypeError(f"Invalid data: the values do not fit {array_dtype.__name__}")
            if target_kind in "iu":
                # a fraction is truncated by the conversion: it is caught by the way back
                fits = np.array_equal(converted.astype(object), source)
            else:
                # rounding is fine for floats; a finite value turning into inf is not
                fits = np.array_equal(np.isfinite(probe), np.isfinite(converted)) and np.array_equal(
                    np.isnan(probe), np.isnan(converted)
                )
        if not fits:
            raise DekerArrayTypeError(f"Invalid data: the values do not fit {array_dtype.__name__}")
        return converted

    @staticmethod
    def _process_data(
        array_dtype: type[Numeric],
        array_shape: tuple[int, ...],
        data: Data,
        bounds: Slice | None = None,
    ) -> ndarray | Numeric:
        """Validate data over array dtype and bounds.

        Python numbers (a scalar, a list or a tuple) are converted to the array dtype if every value fits it,
        see ``_convert_python_numbers``. Numpy data (an array or a scalar) is never converted: its dtype must
        be exactly the array dtype.

        :param array_dtype: array dtype
        :param array_shape: array shape
        :param data: data to be validated
        :param bounds: array bounds
        :returns: data of the array dtype: an ndarray of the shape of the bounds, or a scalar
        :raises DekerArrayTypeError: if the data is not numeric, does not fit the dtype or has another dtype
        :raises IndexError: if the data shape does not match the bounds
        """
        if isinstance(data, (bool, np.bool_)) or not isinstance(data, (list, tuple, np.ndarray, get_args(Numeric))):
            raise DekerArrayTypeError(f"Invalid data type: {type(data)}; {array_dtype} expected")

        if isinstance(data, np.generic):
            if type(data) is not array_dtype:
                raise DekerArrayTypeError(f"Invalid data dtype: {type(data)}; {array_dtype} expected")
            return data

        if isinstance(data, (int, float, complex)):
            return IArrayAdapter._convert_python_numbers(array_dtype, data)[()]

        shape = create_shape_from_slice(array_shape, ... if bounds is None else bounds)
        if isinstance(data, (list, tuple)):
            data = IArrayAdapter._convert_python_numbers(array_dtype, data)
        elif data.dtype != array_dtype:
            raise DekerArrayTypeError(f"Invalid data dtype: {data.dtype}; {array_dtype} expected")
        if shape != data.shape:
            raise IndexError(f"Invalid data shape: {data.shape}; {shape} expected")
        return data

    @abstractmethod
    def create(self, array: ArrayT) -> ArrayT:
        """Create Array or VArray in the storage.

        :param array: instance of Array or VArray
        :returns: the same instance, once it is created in the storage
        :raises DekerArrayError: if the array cannot be created
        """
        pass

    @abstractmethod
    def read_meta(self, array: ArrayT) -> ArrayMeta:
        """Read array metadata.

        :param array: Array or VArray instance
        :returns: array metadata
        """
        pass

    @abstractmethod
    def update_meta_custom_attributes(self, array: ArrayT, attributes: dict) -> None:
        """Update custom attributes in meta.

        :param array: Array or VArray
        :param attributes: attributes to be updated
        """
        pass

    @abstractmethod
    def delete(self, array: ArrayT) -> None:
        """Delete a whole array with its data and metadata.

        :param array: Array or VArray
        """
        pass

    @abstractmethod
    def read_data(self, array: ArrayT, bounds: Slice) -> Numeric | ndarray:
        """Read data from a section of array.

        :param array: Array or VArray
        :param bounds: bounds of a section to be read
        :returns: data within the bounds; a scalar for a single cell
        """
        pass

    @abstractmethod
    def update(self, array: ArrayT, bounds: Slice, data: Data) -> None:
        """Update a section of array.

        :param array: Array or VArray
        :param bounds: bounds of a section to be updated
        :param data: new data of the shape of the bounds, or a number that fills them; validated by ``_process_data``
        """
        pass

    @abstractmethod
    def clear(self, array: ArrayT, bounds: Slice) -> None:
        """Set a section of array to the fill value.

        :param array: Array or VArray
        :param bounds: bounds of a section to be cleared
        """
        pass

    @abstractmethod
    def is_deleted(self, array: ArrayT) -> bool:
        """Check if array was deleted.

        :param array: Array to check
        :returns: ``True`` if the array no longer exists in the storage
        """
        pass

    def filter(
        self,
        filters: dict,
        schema: "BaseArraysSchema",
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> list[ArrayT]:
        """Filter arrays by an id or by all the primary attributes.

        :param filters: ``{"id": "<uuid>"}`` or the values of all the primary attributes of the schema
        :param schema: Array or Varray schema
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        :returns: the matching array in a list, an empty list if there is none
        :raises DekerValidationError: if the id is not a UUID string or a datetime attribute is of a wrong type
        :raises DekerFilterError: if the filters are neither an id nor all the primary attributes of the schema
        """
        # If we got "id" in filters, just return the array with the given ID
        # (Till AND/OR logic not implemented)
        if "id" in filters:
            if not is_valid_uuid(filters["id"]):
                raise DekerValidationError(f"Invalid array id {filters['id']!r}: a UUID string is expected")
            array = self.get_by_id(filters["id"], collection, array_adapter, varray_adapter)
            return [array] if array else []

        primary_attribute_filters = {}
        error_message = "Filtering is possible by the id or by all the primary attributes of the schema only"
        # If there are no primary attributes in the schema
        if not schema.primary_attributes:
            raise DekerFilterError(error_message)

        for primary_attr in schema.primary_attributes:
            value = filters.get(primary_attr.name)
            # If any of key attrs wasn't passed, use simple filter
            if value is None:
                raise DekerFilterError(f"{error_message}; {primary_attr.name} is missing")
            if primary_attr.dtype == datetime.datetime:
                if not isinstance(value, (str, datetime.datetime)):
                    raise DekerValidationError(
                        f"Invalid type passed for {primary_attr.name} filtering: {type(value)}; "
                        f"only datetime.datetime or datetime iso-string are allowed"
                    )
                value = get_utc(value)
            primary_attribute_filters[primary_attr.name] = value

        # If extra args were passed
        if len(filters.keys()) > len(primary_attribute_filters.keys()):
            raise DekerFilterError("Some arguments don't exist in schema")

        # Filter by primary attributes

        array = self.get_by_primary_attributes(
            primary_attribute_filters, schema, collection, array_adapter, varray_adapter
        )
        if array:
            return [array]
        return []

    @abstractmethod
    def get_by_id(
        self,
        id_: str,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> ArrayT | None:
        """Find Array or VArray  by id.

        :param id_: Array id
        :param collection: Collection
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        :returns: the array, or ``None`` if there is no array with this id
        """
        pass

    @abstractmethod
    def get_by_primary_attributes(
        self,
        primary_attributes: dict,
        schema: "BaseArraysSchema",
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> ArrayT | None:
        """Find Array or VArray by given primary attributes.

        :param primary_attributes: key attributes
        :param schema: Array or VArray schema
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        :returns: the array, or ``None`` if nothing matches the attributes
        """
        pass

    @abstractmethod
    def __iter__(self) -> Generator["ArrayMeta", None, None]:
        """Iterate over the metadata of all the arrays of the collection.

        :yields: the metadata of every array
        """
        pass


class BaseArrayAdapter(IArrayAdapter[PathT, "Array"], ABC):
    """Interface for Arrays' adapters."""

    @abstractmethod
    def delete_all_by_vid(self, vid: str, collection: "Collection") -> None:
        """Delete all the Arrays a VArray is made of.

        :param vid: VArray id
        :param collection: Collection instance
        """
        pass


class BaseVArrayAdapter(IArrayAdapter[PathT, "VArray"], ABC):
    """Interface for VArrays' adapters."""

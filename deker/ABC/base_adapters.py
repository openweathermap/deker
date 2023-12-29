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
import logging

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generator, List, Optional, Tuple, Type, Union, get_args

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
from deker.types.private.typings import Data, EllipsisType, Numeric, Slice


if TYPE_CHECKING:
    from deker.ABC.base_array import BaseArray
    from deker.ABC.base_collection import BaseCollectionOptions
    from deker.ABC.base_factory import BaseAdaptersFactory
    from deker.ABC.base_schemas import BaseArraysSchema
    from deker.arrays import Array, VArray
    from deker.collection import Collection
    from deker.uri import Uri


class BaseStorageAdapter(ABC):
    """Interface for adapters working with files with data (e.g hdf5, tiff, etc).

    Doesn't accept any params for instantiation.
    """

    file_ext: str
    storage_options: Type["BaseCollectionOptions"]

    @classmethod
    @abstractmethod
    def create(
        cls, path: Path, array_shape: Tuple[int, ...], metadata: Union[str, bytes, dict]
    ) -> None:
        """Create a new hdf5 file with metadata.

        :param path: path to hdf5 file
        :param array_shape: shape of the array
        :param metadata: array metadata
        """
        pass

    @classmethod
    @abstractmethod
    def read_data(
        cls,
        path: Path,
        array_shape: Tuple[int, EllipsisType],  # type: ignore
        bounds: Slice,
        fill_value: Numeric,
        dtype: Type[Numeric],
    ) -> ndarray:
        """Read array data from hdf5 file.

        :param path: path to hdf5 file
        :param array_shape: shape of the array
        :param bounds: array slice
        :param fill_value: value to fill the empty array
        :param dtype: array dtype
        """
        pass

    @classmethod
    @abstractmethod
    def read_meta(cls, path: Path) -> ArrayMeta:
        """Read array metadata from hdf5 file.

        :param path: path to hdf5 file
        """
        pass

    @classmethod
    @abstractmethod
    def update_data(
        cls,
        path: Path,
        bounds: Slice,
        data: Any,
        dtype: Type[Numeric],
        shape: tuple,
        fill_value: Numeric,
        collection_options: Optional["BaseCollectionOptions"],
    ) -> None:
        """Update array data in hdf5 file.

        :param path: path to hdf5 file
        :param bounds: array slice
        :param data: new data for array slice
        :param dtype: array dtype
        :param shape: array shape
        :param fill_value: array fill_value
        :param collection_options: chunking and compression options
        """
        pass

    @classmethod
    @abstractmethod
    def update_meta_custom_attributes(cls, path: Path, attributes: dict) -> dict:
        """Update metadata in the hdf5 file.

        :param path: path to hdf5 file
        :param attributes: new custom attributes
        """
        pass

    @classmethod
    @abstractmethod
    def clear_data(
        cls, path: Path, array_shape: Tuple[int, ...], bounds: Slice, fill_value: Numeric
    ) -> bool:
        """Clear array data in hdf5 file.

        :param path: path to hdf5 file
        :param array_shape: array shape
        :param bounds: array bounds to update
        :param fill_value: array fill_value
        """
        pass


class BaseCollectionAdapter(SelfLoggerMixin, ABC):
    """Base interface for collection adapters."""

    file_ext: str
    metadata_version: str

    def __init__(self, ctx: CTX):
        self.ctx = ctx
        self.uri = self.ctx.uri

    def __del__(self) -> None:
        """Delete object."""
        self.close()

    @property
    @abstractmethod
    def collections_resource(self) -> Path:
        """Return a path to collections' resource."""
        pass

    def close(self) -> None:
        """Close adapter."""
        self.uri = ""

    @abstractmethod
    def create(self, collection: "Collection") -> None:
        """Add collection to storage and creates file structure.

        :param collection: Collection to be created
        """
        pass

    @abstractmethod
    def read(self, name: str) -> dict:
        """Read collection metadata.

        :param name: Collection name
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
        """Clear collection directory.

        :param collection: Collection to be cleared
        """
        pass

    @check_ctx_state
    def create_collection_from_meta(
        self, collection_data: dict, factory: "BaseAdaptersFactory"
    ) -> "Collection":
        """Create a Collection instance from meta information.

        :param collection_data: dict with information about Collection (schemas, vgrid, etc)
        :param factory: adapters factory instance
        """
        from deker.collection import Collection

        name: str = collection_data["name"]
        data: dict = collection_data["schema"]

        # Check schema type
        schema_class = SchemaTypeEnum[collection_data.get("type")].value

        try:
            dtype = DTypeEnum[data["dtype"].split("numpy.")[-1]].value
            fill_value = (
                dtype(data["fill_value"]) if data["fill_value"] is not None else data["fill_value"]
            )
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
            coll_params = storage_adapter.storage_options._process_options(
                collection_data.get("options")
            )

            collection_options = (
                storage_adapter.storage_options(**coll_params) if coll_params else None
            )

            collection = Collection(
                name=name,
                schema=schema,
                adapter=self,
                factory=factory,
                collection_options=collection_options,
                storage_adapter=storage_adapter,
            )

            logging.debug(f"Collection {collection.name} read")
            return collection
        except (KeyError, ValueError, TypeError, AttributeError) as e:
            logging.exception(e)  # noqa[TRY401]
            raise DekerMetaDataError(f'Collection "{name}" metadata is invalid/corrupted: {e}')

    @staticmethod
    def get_storage_adapter(storage_adapter: Optional[str] = None) -> Type[BaseStorageAdapter]:
        """Get storage adapter type.

        :param storage_adapter: Storage adapter name
        """
        from deker_local_adapters import storage_adapter_factory

        return storage_adapter_factory(storage_adapter)

    @abstractmethod
    def __iter__(self) -> "Collection":
        """Iterate over all Collections in the storage."""
        pass


class IArrayAdapter(ABC):
    """Interface for Array  or VArray adapters."""

    def __init__(
        self,
        collection_path: Union["Uri", Path],
        ctx: CTX,
        executor: ThreadPoolExecutor,
        storage_adapter: Type[BaseStorageAdapter],
    ) -> None:
        self.collection_path = collection_path
        self.ctx = ctx
        self.executor = executor
        self.uri = self.ctx.uri
        self.storage_adapter = storage_adapter()

    @staticmethod
    def _process_data(
        array_dtype: Type[Numeric],
        array_shape: tuple,
        data: Data,
        bounds: Optional[Slice] = None,
    ) -> Union[ndarray, Numeric]:
        """Validate data over array dtype and bounds.

        :param array_dtype: array dtype
        :param array_shape: array shape
        :param data: data to be validated
        :param bounds: array bounds
        """
        if not isinstance(data, (list, tuple, np.ndarray, get_args(Numeric))):  # type: ignore[arg-type]
            raise DekerArrayTypeError(f"Invalid data type: {type(data)}; {array_dtype} expected")

        if isinstance(data, get_args(Numeric)):  # type: ignore[arg-type]
            data_type = type(data)
            if data_type != array_dtype:
                cdata = array_dtype(data)  # type: ignore[arg-type]
                if data_type(cdata) != data:  # type: ignore[arg-type,call-overload]
                    raise DekerArrayTypeError(
                        f"Invalid data type: {type(data)}; {array_dtype} expected"
                    )
                data = cdata
        else:
            sliced_bounds = (
                np.index_exp[:]
                if bounds is None or bounds == slice(None, None, None)
                else np.index_exp[bounds]  # type: ignore[type-var]
            )
            shape = create_shape_from_slice(array_shape, sliced_bounds)  # type: ignore[arg-type]

            if isinstance(data, (list, tuple)):
                try:
                    data = np.asarray(data).astype(dtype=array_dtype, casting="no")
                except TypeError as e:
                    raise DekerArrayTypeError(f"Invalid data type: {e}; {array_dtype} expected")

            if isinstance(data, ndarray):
                if data.dtype != array_dtype:
                    raise DekerArrayTypeError(
                        f"Invalid data dtype: {data.dtype}; {array_dtype} expected"
                    )
                if shape != data.shape:
                    raise IndexError(f"Invalid data shape: {data.shape}; {shape} expected")
        return data  # type: ignore[return-value]

    @abstractmethod
    def create(self, array: Union[dict, "BaseArray"]) -> Union["Array", "VArray"]:
        """Create Array or VArray in the database.

        :param array: instance of Array or VArray
        """
        pass

    @abstractmethod
    def read_meta(self, array: Union["BaseArray", Path]) -> ArrayMeta:
        """Read array metadata.

        :param array: Array or VArray instance or Path to meta file
        """
        pass

    @abstractmethod
    def update_meta_custom_attributes(self, array: "BaseArray", attributes: dict) -> None:
        """Update custom attributes in meta.

        :param array: Array or VArray
        :param attributes: attributes to be updated
        """
        pass

    @abstractmethod
    def delete(
        self,
        array: "BaseArray",
    ) -> None:
        """Delete a whole array.

        :param array: Array or VArray
        """
        pass

    @abstractmethod
    def read_data(self, array: "BaseArray", bounds: Slice) -> Union[Numeric, ndarray]:
        """Read data from a section of array.

        :param array: Array or VArray
        :param bounds: bounds of a section to be read
        """
        pass

    @abstractmethod
    def update(self, array: "BaseArray", bounds: Slice, data: Any) -> None:
        """Update a section of array.

        :param array: Array or VArray
        :param bounds: bounds of a section to be updated
        :param data: new data to be inserted into Array
        """
        pass

    @abstractmethod
    def clear(self, array: "BaseArray", bounds: Slice) -> None:
        """Clear a section of array.

        :param array: Array or VArray
        :param bounds: bounds of a section to be cleared
        """
        pass

    @abstractmethod
    def is_deleted(self, array: "BaseArray") -> bool:
        """Check if array was deleted.

        :param array: Array to check
        """
        pass

    def filter(
        self,
        filters: dict,
        schema: "BaseArraysSchema",
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
    ) -> List[Union["Array", "VArray"]]:
        """Filter arrays.

        :param filters:  dict with filtering criteria
        :param schema: Array or Varray schema
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        """
        # If we got "id" in filters, just return the array with the given ID
        # (Till AND/OR logic not implemented)
        if "id" in filters:
            array = self.get_by_id(filters["id"], collection, array_adapter, varray_adapter)
            return [array] if array else []

        primary_attribute_filters = {}
        error_message = "Filtering by non primary attributes is not implemented yet"
        # If there are no primary attributes in the schema
        if not schema.primary_attributes:
            raise NotImplementedError(error_message)

        for primary_attr in schema.primary_attributes:
            value = filters.get(primary_attr.name)
            # If any of key attrs wasn't passed, use simple filter
            if value is None:
                raise NotImplementedError(error_message)
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
    ) -> Optional[Union["Array", "VArray"]]:
        """Find Array or VArray  by id.

        :param id_: Array id
        :param collection: Collection
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
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
    ) -> Optional[Union["Array", "VArray"]]:
        """Find Array or VArray by given primary attributes.

        :param primary_attributes: key attributes
        :param schema: Array or VArray schema
        :param collection: Collection instance
        :param array_adapter: Arrays' adapter
        :param varray_adapter: VArrays' adapter
        """
        pass

    @abstractmethod
    def __iter__(self) -> Generator["ArrayMeta", None, None]:
        """All adapters should provide iterator interface."""
        pass


class BaseArrayAdapter(IArrayAdapter, ABC):
    """Interface for Arrays' adapters."""

    @abstractmethod
    def delete_all_by_vid(self, vid: str, collection: "Collection") -> None:
        """Delete all Arrays in a VArray.

        :param vid: VArray id
        :param collection: Collection instance
        """
        pass


class BaseVArrayAdapter(IArrayAdapter, ABC):
    """Interface for VArrays' adapters."""

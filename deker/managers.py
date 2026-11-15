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

from collections.abc import Generator
from typing import TYPE_CHECKING, Optional, cast

from deker.ABC.base_managers import BaseAbstractManager, BaseManager
from deker.arrays import Array, VArray
from deker.errors import DekerInvalidManagerCallError
from deker.log import SelfLoggerMixin
from deker.schemas import VArraySchema
from deker.types.private.typings import ArrayT

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_schemas import BaseArraysSchema
    from deker.collection import Collection
    from deker.schemas import ArraySchema


class FilteredManager(SelfLoggerMixin, BaseAbstractManager[ArrayT]):
    """Manager for ``Collection`` contents filtering."""

    __slots__ = (
        "__filters",
        "__collection",
        "__array_adapter",
        "__varray_adapter",
    )

    def __init__(
        self,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"],
        schema: "BaseArraysSchema",
        filters: dict,
    ):
        super().__init__(collection, schema, array_adapter, varray_adapter)
        self.__collection = collection
        self.__schema = schema
        self.__array_adapter = array_adapter
        self.__varray_adapter = varray_adapter
        if isinstance(self.__schema, VArraySchema) and not self.__varray_adapter:
            raise AttributeError("FilterManager is missing varray adapter for varray schema")
        self.__filters = filters

    def __get_item_in_list_arrays(self, elem_index: int) -> ArrayT | None:
        """Filter arrays and get element by index.

        :param elem_index: Which element we return
        :returns: the array at the index, or ``None`` if nothing was found
        """
        arrays = self._adapter.filter(
            self.__filters,
            self.__schema,
            self.__collection,
            self.__array_adapter,
            self.__varray_adapter,
        )
        if any(arrays):
            return arrays[elem_index]

    def first(self) -> ArrayT | None:
        """Return first array in the filter."""
        return self.__get_item_in_list_arrays(0)

    def last(self) -> ArrayT | None:
        """Return last array in the filter."""
        return self.__get_item_in_list_arrays(-1)


class DataManager(BaseManager[ArrayT]):
    """Common data manager.

    Its behavior depends on the type of Collection (Array/Varrary)
    """

    __slots__ = (
        "__collection",
        "__array_adapter",
        "__varray_adapter",
    )

    def __init__(
        self,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"] = None,
    ):
        schema = collection.varray_schema or collection.array_schema
        super().__init__(collection, schema, array_adapter, varray_adapter)
        self.__collection = collection
        self.__array_adapter = array_adapter
        self.__varray_adapter = varray_adapter

    def _create_array(
        self,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        id_: str | None = None,
    ) -> Array:
        """Create Array.

        :param primary_attributes: array primary attribute
        :param custom_attributes: array custom attributes
        :param id_: Array uuid string
        :returns: the created ``Array``
        """
        array = Array(
            collection=self.__collection,
            adapter=self.__array_adapter,
            id_=id_,
            primary_attributes=primary_attributes,
            custom_attributes=custom_attributes,
        )
        return self.__array_adapter.create(array)

    def _create_varray(
        self,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        id_: str | None = None,
    ) -> VArray:
        """Create VArray.

        :param primary_attributes: varray primary attribute
        :param custom_attributes: varray custom attributes
        :param id_: VArray uuid string
        :returns: the created ``VArray``
        """
        if self.__varray_adapter is None:
            raise DekerInvalidManagerCallError("VArray adapter is not set")
        varray = VArray(
            collection=self.__collection,
            adapter=self.__varray_adapter,
            array_adapter=self.__array_adapter,
            id_=id_,
            primary_attributes=primary_attributes,
            custom_attributes=custom_attributes,
        )
        return self.__varray_adapter.create(varray)

    def _create(
        self,
        schema: "BaseArraysSchema",
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        id_: str | None = None,
    ) -> Array | VArray:
        """Create Array or VArray.

        :param primary_attributes: array primary attribute
        :param custom_attributes: array custom attributes
        :param schema: schema decides which array will be created
        :param id_: (V)Array uuid string
        :returns: the created ``Array`` or ``VArray``
        """
        if isinstance(schema, VArraySchema):
            return self._create_varray(primary_attributes, custom_attributes, id_)
        return self._create_array(primary_attributes, custom_attributes, id_)

    def create(
        self,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        id_: str | None = None,
    ) -> ArrayT:
        """Create array or varray.

        :param primary_attributes: primary attributes
        :param custom_attributes: custom attributes
        :param id_: unique UUID string
        :returns: the created ``Array`` or ``VArray``
        """
        schema = self.__collection.varray_schema or self.__collection.array_schema
        # the schema decides the kind of arrays, the same way it decides the manager's type parameter
        return cast("ArrayT", self._create(schema, primary_attributes, custom_attributes, id_))


class VArrayManager(SelfLoggerMixin, DataManager[VArray]):
    """Manager for VArrays."""

    __slots__ = (
        "__collection",
        "__array_adapter",
        "__varray_adapter",
    )

    def __init__(
        self,
        collection: "Collection",
        array_adapter: "BaseArrayAdapter",
        varray_adapter: "BaseVArrayAdapter",
    ) -> None:
        super().__init__(collection, array_adapter, varray_adapter)
        self.__collection = collection
        self.__array_adapter: "BaseArrayAdapter" = array_adapter
        self.__varray_adapter: "BaseVArrayAdapter" = varray_adapter

    @property
    def _adapter(self) -> "BaseVArrayAdapter":
        return self.__varray_adapter

    def _get_schema(self) -> "VArraySchema":
        schema = self.__collection.varray_schema
        if schema is None:
            raise DekerInvalidManagerCallError("VArrays cannot be created in a collection without VArraySchema")
        return schema

    def create(
        self,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        id_: str | None = None,
    ) -> VArray:
        """Create varray in collection.

        :param primary_attributes: VArray primary attributes
        :param custom_attributes: VArray custom attributes
        :param id_: VArray unique UUID string
        :returns: the created ``VArray``
        """
        self._get_schema()
        return self._create_varray(primary_attributes, custom_attributes, id_)

    def __iter__(self) -> Generator[VArray, None, None]:
        """Yield VArrays from adapter."""
        self.logger.debug("iterating over VArrays")
        for meta in self._adapter:
            yield VArray._create_from_meta(self.__collection, meta, self.__array_adapter, self.__varray_adapter)


class ArrayManager(SelfLoggerMixin, DataManager[Array]):
    """Manager for Arrays."""

    __slots__ = (
        "__collection",
        "__array_adapter",
        "__varray_adapter",
    )

    def __init__(self, collection: "Collection", array_adapter: "BaseArrayAdapter"):
        super().__init__(collection, array_adapter, None)
        self.__collection = collection
        self.__array_adapter = array_adapter
        self.__varray_adapter = None

    @property
    def _adapter(self) -> "BaseArrayAdapter":
        return self.__array_adapter

    def _get_schema(self) -> "ArraySchema":
        """Override method which returns only array schema."""
        return self.__collection.array_schema

    def create(
        self,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        id_: str | None = None,
    ) -> Array:
        """Create array in collection.

        :param primary_attributes: Array primary attributes
        :param custom_attributes: Array custom attributes
        :param id_: Array unique UUID string
        :returns: the created ``Array``
        """
        return self._create_array(primary_attributes, custom_attributes, id_)

    def __iter__(self) -> Generator[Array, None, None]:
        """Yield Arrays from adapter."""
        self.logger.debug("iterating over Arrays")
        for meta in self._adapter:
            yield Array._create_from_meta(self.__collection, meta, self.__array_adapter)

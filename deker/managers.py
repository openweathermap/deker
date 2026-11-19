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
    """Manager for ``Collection`` contents filtering.

    The filters are either the ``id`` of an array, ``{"id": "<uuid>"}``, or the values of all its primary
    attributes, ``{"name": value, ...}``; the storage is searched when ``first()`` or ``last()`` is called.
    """

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
        """FilteredManager constructor.

        :param collection: the collection to search in
        :param array_adapter: adapter of its ``Arrays``
        :param varray_adapter: adapter of its ``VArrays``, for a collection of ``VArrays``
        :param schema: the schema that decides the kind of arrays
        :param filters: the filters: an id or all the primary attributes
        :raises DekerInvalidManagerCallError: if the schema is a ``VArraySchema`` and no ``VArray`` adapter is given
        """
        super().__init__(collection, schema, array_adapter, varray_adapter)
        self.__collection = collection
        self.__schema = schema
        self.__array_adapter = array_adapter
        self.__varray_adapter = varray_adapter
        if isinstance(self.__schema, VArraySchema) and not self.__varray_adapter:
            raise DekerInvalidManagerCallError("FilterManager is missing varray adapter for varray schema")
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
        """Return the first array matching the filters; ``None`` if there is none.

        :returns: the array or ``None``
        :raises DekerValidationError: if the id is not a UUID string or a datetime attribute is of a wrong type
        :raises DekerFilterError: if the filters are neither an id nor all the primary attributes of the schema
        """
        return self.__get_item_in_list_arrays(0)

    def last(self) -> ArrayT | None:
        """Return the last array matching the filters; ``None`` if there is none.

        :returns: the array or ``None``
        :raises DekerValidationError: if the id is not a UUID string or a datetime attribute is of a wrong type
        :raises DekerFilterError: if the filters are neither an id nor all the primary attributes of the schema
        """
        return self.__get_item_in_list_arrays(-1)


class DataManager(BaseManager[ArrayT]):
    """Common data manager.

    Its behavior depends on the type of Collection: it creates ``VArrays`` in a collection with a ``VArraySchema``,
    ``Arrays`` otherwise.
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
        """DataManager constructor.

        :param collection: the collection managed
        :param array_adapter: adapter of its ``Arrays``
        :param varray_adapter: adapter of its ``VArrays``, for a collection of ``VArrays``
        """
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
        :raises DekerInvalidManagerCallError: if the manager has no ``VArray`` adapter
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
        """Create an array of the kind the schema of the collection decides.

        :param primary_attributes: primary attributes: all of the schema, no other
        :param custom_attributes: custom attributes
        :param id_: unique UUID string; generated if not passed
        :returns: the created ``Array`` or ``VArray``
        :raises DekerValidationError: if the attributes do not match the schema or an array with these primary
          attributes exists
        """
        schema = self.__collection.varray_schema or self.__collection.array_schema
        # the schema decides the kind of arrays, the same way it decides the manager's type parameter
        return cast("ArrayT", self._create(schema, primary_attributes, custom_attributes, id_))


class VArrayManager(SelfLoggerMixin, DataManager[VArray]):
    """Manager for the ``VArrays`` of a collection: ``collection.varrays``."""

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
        """VArrayManager constructor.

        :param collection: the collection managed
        :param array_adapter: adapter of the ``Arrays`` the ``VArrays`` are made of
        :param varray_adapter: adapter of the ``VArrays``
        """
        super().__init__(collection, array_adapter, varray_adapter)
        self.__collection = collection
        self.__array_adapter: "BaseArrayAdapter" = array_adapter
        self.__varray_adapter: "BaseVArrayAdapter" = varray_adapter

    @property
    def _adapter(self) -> "BaseVArrayAdapter":
        return self.__varray_adapter

    def _get_schema(self) -> "VArraySchema":
        """Get the ``VArraySchema`` of the collection.

        :returns: the schema
        :raises DekerInvalidManagerCallError: if the collection has no ``VArraySchema``
        """
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
        :param id_: VArray unique UUID string; generated if not passed
        :returns: the created ``VArray``
        :raises DekerInvalidManagerCallError: if the collection has no ``VArraySchema``
        :raises DekerValidationError: if the attributes do not match the schema or a ``VArray`` with these primary
          attributes exists
        """
        self._get_schema()
        return self._create_varray(primary_attributes, custom_attributes, id_)

    def __iter__(self) -> Generator[VArray, None, None]:
        """Yield every ``VArray`` of the collection.

        :yields: the ``VArrays``, built from their stored metadata
        """
        self.logger.debug("iterating over VArrays")
        for meta in self._adapter:
            yield VArray._create_from_meta(self.__collection, meta, self.__array_adapter, self.__varray_adapter)


class ArrayManager(SelfLoggerMixin, DataManager[Array]):
    """Manager for the ``Arrays`` of a collection: ``collection.arrays``.

    In a collection of ``VArrays`` these are the ``Arrays`` the ``VArrays`` are made of: their primary attributes
    are ``vid`` and ``v_position``.
    """

    __slots__ = (
        "__collection",
        "__array_adapter",
        "__varray_adapter",
    )

    def __init__(self, collection: "Collection", array_adapter: "BaseArrayAdapter"):
        """ArrayManager constructor.

        :param collection: the collection managed
        :param array_adapter: adapter of its ``Arrays``
        """
        super().__init__(collection, array_adapter, None)
        self.__collection = collection
        self.__array_adapter = array_adapter
        self.__varray_adapter = None

    @property
    def _adapter(self) -> "BaseArrayAdapter":
        return self.__array_adapter

    def _get_schema(self) -> "ArraySchema":
        """Get the ``ArraySchema`` of the collection.

        :returns: the schema
        """
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
        :param id_: Array unique UUID string; generated if not passed
        :returns: the created ``Array``
        :raises DekerValidationError: if the attributes do not match the schema or an ``Array`` with these primary
          attributes exists
        """
        return self._create_array(primary_attributes, custom_attributes, id_)

    def __iter__(self) -> Generator[Array, None, None]:
        """Yield every ``Array`` of the collection.

        :yields: the ``Arrays``, built from their stored metadata
        """
        self.logger.debug("iterating over Arrays")
        for meta in self._adapter:
            yield Array._create_from_meta(self.__collection, meta, self.__array_adapter)

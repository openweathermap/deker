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

"""Abstract interfaces for managers."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Optional, cast

from deker.ABC.base_schemas import BaseArraysSchema
from deker.errors import DekerInvalidManagerCallError
from deker.schemas import VArraySchema
from deker.types.private.typings import ArrayT

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter, IArrayAdapter
    from deker.collection import Collection
    from deker.managers import FilteredManager


class BaseAbstractManager(ABC, Generic[ArrayT]):  # noqa: B024
    """Data manager interface.

    ``ArrayT`` is the kind of arrays the manager works with: ``Array`` or ``VArray``, decided by the schema.
    """

    __slots__ = ("__collection", "__array_adapter", "__varray_adapter", "_schema")

    def __init__(
        self,
        collection: "Collection",
        schema: BaseArraysSchema | None,
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"] = None,
    ) -> None:
        """BaseAbstractManager constructor.

        :param collection: the collection managed
        :param schema: the schema that decides the kind of arrays
        :param array_adapter: adapter of the ``Arrays``
        :param varray_adapter: adapter of the ``VArrays``, for a collection of ``VArrays``
        """
        super().__init__()
        self.__collection = collection
        self.__array_adapter = array_adapter
        self.__varray_adapter = varray_adapter
        self._schema = schema

    @property
    def _adapter(self) -> "IArrayAdapter[Any, ArrayT]":
        """Get the adapter matching the schema: VArrays' adapter for a VArray schema, Arrays' adapter otherwise.

        :raises DekerInvalidManagerCallError: if the schema is a ``VArraySchema`` and there is no ``VArray`` adapter
        """
        adapter: BaseArrayAdapter | BaseVArrayAdapter
        if isinstance(self._schema, VArraySchema):
            if self.__varray_adapter is None:
                raise DekerInvalidManagerCallError("VArray adapter is not set for a VArray schema")
            adapter = self.__varray_adapter
        else:
            adapter = self.__array_adapter
        # the schema decides both the kind of arrays and the adapter, so they match by construction
        return cast("IArrayAdapter[Any, ArrayT]", adapter)

    def _get_schema(self) -> BaseArraysSchema:
        """Decide which schema to use.

        :returns: the ``VArraySchema`` of the collection if it has one, the ``ArraySchema`` otherwise
        """
        schema = self.__collection.varray_schema if self.__collection.varray_schema else self.__collection.array_schema
        return schema

    def filter(self, filters: dict) -> "FilteredManager[ArrayT]":
        """Filter Arrays or VArrays by provided condition.

        :param filters: the ``id`` of an array, ``{"id": "<uuid>"}``, or the values of all its primary attributes
        :returns: ``FilteredManager`` with the applied filter; the storage is searched by its ``first()`` or ``last()``
        """
        from deker.managers import FilteredManager

        return FilteredManager(
            self.__collection,
            self.__array_adapter,
            self.__varray_adapter,
            schema=self._get_schema(),
            filters=filters,
        )


class BaseManager(BaseAbstractManager[ArrayT]):
    """Parent class for ArrayManager and VArrayManager."""

    @abstractmethod
    def create(
        self,
        primary_attributes: dict | None = None,
        custom_attributes: dict | None = None,
        id_: str | None = None,
    ) -> ArrayT:
        """Create an array.

        :param primary_attributes: (V)array primary attributes
        :param custom_attributes: (V)array custom attributes
        :param id_: unique UUID string; generated if not passed
        :returns: the created array
        """
        pass

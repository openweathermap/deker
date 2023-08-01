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
from typing import TYPE_CHECKING, Optional, Union

from deker.ABC.base_schemas import BaseArraysSchema
from deker.schemas import VArraySchema


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_array import BaseArray
    from deker.collection import Collection
    from deker.managers import FilteredManager


class BaseAbstractManager(ABC):
    """Data manager interface."""

    __slots__ = ("__collection", "__array_adapter", "__varray_adapter", "_schema")

    def __init__(
        self,
        collection: "Collection",
        schema: Optional[BaseArraysSchema],
        array_adapter: "BaseArrayAdapter",
        varray_adapter: Optional["BaseVArrayAdapter"] = None,
    ) -> None:
        super().__init__()
        self.__collection = collection
        self.__array_adapter = array_adapter
        self.__varray_adapter = varray_adapter
        self._schema = schema

    @property
    def _adapter(self) -> Union["BaseArrayAdapter", "BaseVArrayAdapter"]:
        """Get array adapter from factory."""
        if isinstance(self._schema, VArraySchema):
            return self.__varray_adapter  # type: ignore[return-value]
        return self.__array_adapter

    def _get_schema(self) -> BaseArraysSchema:
        """Decide which schema to use (Array or VArray)."""
        schema = (
            self.__collection.varray_schema
            if self.__collection.varray_schema
            else self.__collection.array_schema
        )
        return schema  # type: ignore[return-value]

    def filter(self, filters: dict) -> "FilteredManager":
        """Filter Arrays or VArrays by provided condition.

        :param filters: filtering parameters
        """
        from deker.managers import FilteredManager

        return FilteredManager(
            self.__collection,
            self.__array_adapter,
            self.__varray_adapter,  # type: ignore[arg-type]
            schema=self._get_schema(),
            filters=filters,
        )


class BaseManager(BaseAbstractManager):
    """Parent class for ArrayManager and VArrayManager."""

    @abstractmethod
    def create(
        self, primary_attributes: Optional[dict] = None, custom_attributes: Optional[dict] = None
    ) -> "BaseArray":
        """Create an array.

        :param primary_attributes: (V)array primary attributes
        :param custom_attributes: (V)array custom attributes
        """
        pass

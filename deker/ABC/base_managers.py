from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Union

from deker.ABC.base_schemas import BaseArraysSchema
from deker.schemas import VArraySchema


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.ABC.base_array import BaseArray
    from deker.collection import Collection
    from deker.managers import FilteredManager


class BaseAbstractManager(ABC):  # noqa: B024
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
        if self.__collection.varray_schema:
            schema = self.__collection.varray_schema
        else:
            schema = self.__collection.array_schema
        return schema

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

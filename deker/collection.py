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

import datetime

from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional, Type, Union

from deker.ABC.base_adapters import BaseCollectionAdapter, BaseStorageAdapter
from deker.ABC.base_collection import BaseCollectionOptions
from deker.arrays import Array, VArray
from deker.errors import (
    DekerInstanceNotExistsError,
    DekerInvalidManagerCallError,
    DekerValidationError,
)
from deker.log import SelfLoggerMixin
from deker.managers import ArrayManager, DataManager, FilteredManager, VArrayManager
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    SchemaTypeEnum,
    TimeDimensionSchema,
    VArraySchema,
)
from deker.tools import not_deleted
from deker.types import Serializer


if TYPE_CHECKING:
    from deker.ABC.base_factory import BaseAdaptersFactory


class Collection(SelfLoggerMixin, Serializer):
    """Collection of ``Arrays`` or ``VArrays``.

    ``Collection`` is a high-level object for managing contents of a set of ``Arrays`` or ``VArrays``
    united into one group under collection name and certain schema.

    Properties
    -----------
    - ``name``: returns ``Collection`` name
    - ``array_schema``: returns schema of embedded ``Arrays``
    - ``varray_schema``: returns schema of ``VArrays`` if applicable, else None
    - ``path``: returns storage path to the ``Collection``
    - ``options``: returns chunking and compression options
    - ``as_dict``: serializes main information about ``Collection`` into dictionary, prepared for JSON

    API methods
    -----------
    - ``create``: according to main schema creates new ``Array`` or ``VArray`` of storage and returns its object
    - ``clear``: according to main schema removes all ``VArrays`` and/or ``Arrays`` from the storage
    - ``delete``: removes ``Collection`` and all its ``VArrays`` and/or ``Arrays`` from the storage
    - ``filter``: filters ``Arrays`` or ``VArrays`` according to main schema and provided conditions
    - ``__iter__``: according to the collection's main schema iterates over all ``Arrays`` or ``VArrays`` in
      ``Collection``, yields their objects
    - ``__str__``: ordinary behaviour
    - ``__repr__``: ordinary behaviour
    """

    __slots__ = (
        "__name",
        "__adapter",
        "__factory",
        "__array_schema",
        "__varray_schema",
        "__manager",
        "__path",
    )

    @staticmethod
    def __create_array_schema_from_varray_schema(schema: VArraySchema) -> ArraySchema:
        """Create ArraySchema from VArraySchema.

        :param schema: VArraySchema instance
        """
        attributes = [
            AttributeSchema(name="vid", dtype=str, primary=True),  # type: ignore[call-arg]
            AttributeSchema(name="v_position", dtype=tuple, primary=True),  # type: ignore[call-arg]
        ]

        dimensions = []
        for n, dim in enumerate(schema.dimensions):
            d = {"name": dim.name, "size": dim.size // schema.vgrid[n]}
            if isinstance(dim, TimeDimensionSchema):
                obj = TimeDimensionSchema
                d["step"] = dim.step
                d["start_value"] = dim.start_value

                if isinstance(dim.start_value, str) and dim.start_value.startswith("$"):
                    d["start_value"] = f"$parent.{dim.start_value[1:]}"
                    attributes.append(
                        AttributeSchema(  # type: ignore[call-arg]
                            name=d["start_value"][1:],
                            dtype=datetime.datetime,
                            primary=False,
                        )
                    )
            else:
                obj = DimensionSchema
                d["labels"] = None
                d["scale"] = None

            dimensions.append(obj(**d))  # type: ignore[arg-type]
        return ArraySchema(  # type: ignore[call-arg]
            dtype=schema.dtype,
            fill_value=schema.fill_value,
            dimensions=dimensions,  # type: ignore[arg-type]
            attributes=attributes,
        )

    def _validate(
        self,
        name: str,
        schema: Union[ArraySchema, VArraySchema],
    ) -> None:
        """Validate name and schema.

        :param name: Collection name
        :param schema: Collection main schema
        """
        if not name or not isinstance(name, str) or name.isspace():
            raise DekerValidationError("Collection name shall be a non-empty string")

        if schema is None or not isinstance(schema, (ArraySchema, VArraySchema)):
            raise DekerValidationError(
                f"Invalid schema type {type(schema)}; ArraySchema or VArraySchema expected"
            )

    def __init__(
        self,
        name: str,
        schema: Union[ArraySchema, VArraySchema],
        adapter: "BaseCollectionAdapter",
        factory: "BaseAdaptersFactory",
        storage_adapter: Type["BaseStorageAdapter"],
        collection_options: Optional[BaseCollectionOptions] = None,
    ) -> None:
        """Collection constructor.

        :param name: ``Collection`` unique name
        :param schema: ``ArraySchema`` or ``VArraySchema`` instance
        :param adapter: ``CollectionAdapter`` instance
        :param factory: ``AdaptersFactory`` instance
        :param storage_adapter: Storage adapter type
        :param collection_options: ``CollectionOptions`` instance or None
        """
        self._validate(name, schema)
        self.__name: str = name
        self.__adapter: BaseCollectionAdapter = adapter
        self.__factory = factory
        self.__path = self.__adapter.collections_resource / name  # type: ignore[attr-defined]
        # Init collection chunking and compression options
        self.__collection_options = collection_options
        self.__storage_adapter = storage_adapter
        array_adapter = self.__factory.get_array_adapter(
            self.__path, self.__storage_adapter, self.__collection_options
        )

        if isinstance(schema, VArraySchema):
            self.__array_schema: ArraySchema = self.__create_array_schema_from_varray_schema(schema)
            self.__varray_schema: VArraySchema = schema
            varray_adapter = self.__factory.get_varray_adapter(
                self.__path, storage_adapter=self.__storage_adapter
            )
            self.__varrays = VArrayManager(self, array_adapter, varray_adapter)
        else:
            self.__array_schema: ArraySchema = schema
            self.__varray_schema: None = None
            varray_adapter = None
            self.__varrays = None

        self.__arrays = ArrayManager(
            self,
            array_adapter,
        )

        self.__manager: DataManager = DataManager(self, array_adapter, varray_adapter)
        self.logger.debug(f"Collection {self.__name} initialized")

    def __del__(self) -> None:
        """Delete object."""
        del self.__adapter
        del self.__manager
        del self.__arrays
        del self.__varrays

    @property
    def _storage_adapter(self) -> Type[BaseStorageAdapter]:
        """Return type of storage adapter."""
        return self.__storage_adapter

    @property
    def name(self) -> str:
        """Get collection name."""
        return self.__name

    @property
    def array_schema(self) -> ArraySchema:
        """Get collection ``ArraySchema``."""
        return self.__array_schema

    @property
    def varray_schema(self) -> Optional[VArraySchema]:
        """Get collection ``VArraySchema``."""
        return self.__varray_schema

    @property
    def path(self) -> Path:
        """Get ``Collection`` fs-path."""
        return self.__path

    @property
    def options(self) -> Optional[BaseCollectionOptions]:
        """Get collection options: chunking, compression, etc."""
        return self.__collection_options

    @property
    def as_dict(self) -> dict:
        """Serialize ``Collection`` to dictionary."""
        dic: dict = {
            "name": self.name,
            "type": SchemaTypeEnum.varray.name
            if self.__varray_schema
            else SchemaTypeEnum.array.name,
            "schema": self.varray_schema.as_dict
            if self.__varray_schema
            else self.array_schema.as_dict,
            "options": self.options.as_dict if self.options else self.options,
            "storage_adapter": self.__storage_adapter.__name__,
            "metadata_version": self.__adapter.metadata_version,
        }

        return dic

    @property
    def varrays(self) -> Optional[VArrayManager]:
        """Return manager for ``VArrays``."""
        if not self.__varrays:
            raise DekerInvalidManagerCallError(
                "``.varrays`` in `Arrays` collection is unavailable; use ``.arrays`` instead"
            )
        return self.__varrays

    @property
    def arrays(self) -> ArrayManager:
        """Return manager for ``Arrays``."""
        return self.__arrays

    @not_deleted
    def create(
        self,
        primary_attributes: Optional[dict] = None,
        custom_attributes: Optional[dict] = None,
        id_: Optional[str] = None,
    ) -> Union[Array, VArray]:
        """Create ``Array`` or ``VArray`` according to collection main schema.

        If ``VArraySchema`` is passed to ``Collection``, all data management will go through ``VArrays``
        as this method will create just ``VArrays`` (``Arrays`` will be created automatically by ``VArray``).
        Otherwise, only ``Arrays`` will be created.

        :param primary_attributes: ``Array`` or ``VArray`` primary attribute
        :param custom_attributes: ``Array`` or ``VArray`` custom attributes
        :param id_: ``Array`` or ``VArray`` unique UUID string
        """
        array = self.__manager.create(primary_attributes, custom_attributes, id_)
        self.logger.debug(
            f"{array.__class__.__name__} id={array.id} {primary_attributes=}, {custom_attributes=} created"
        )
        return array

    def delete(self) -> None:
        """Remove ``Collection`` and all its contents from the database."""
        self.logger.debug(f"Removing collection {self.__name} from database")
        self.__adapter.delete(self)
        self.logger.debug(f"Collection {self.__name} deleted OK")

    @not_deleted
    def clear(self) -> None:
        """Clear all ``Arrays`` and/or ``VArrays``  inside ``Collection``."""
        self.logger.debug(f"Clearing data from collection {self.__name}")
        self.__adapter.clear(self)
        self.logger.debug(f"Collection {self.__name} cleared OK")

    def filter(self, filters: dict) -> FilteredManager:
        """Filter ``Arrays`` or ``VArrays`` by provided conditions.

        :param filters: query conditions for filtering

        .. note::
           Conditions for filtering are either Array or VArray ``id`` value::

             {"id": "some_array_UUID_string"}

           or full scope of primary attributes' values::

             {"primary_attr1_name": its value, "primary_attr2_name": its value, ...}
        """
        return self.__manager.filter(filters)

    def _is_deleted(self) -> bool:
        """Check if collection was deleted."""
        return self.__adapter.is_deleted(self)  # type: ignore[attr-defined]

    def __next__(self) -> None:
        pass

    def __iter__(self) -> Generator[Union[Array, VArray], None, None]:
        """Yield ``VArrays`` or ``VArrays`` from the ``Collection``."""
        if self._is_deleted():
            raise DekerInstanceNotExistsError(
                f"{self} doesn't exist, create new or get an instance again to be able to iterate"
            )
        array_adapter = self.__factory.get_array_adapter(
            self.path, self.__storage_adapter, self.options
        )
        varray_adapter = (
            self.__factory.get_varray_adapter(self.path, storage_adapter=self.__storage_adapter)
            if self.varray_schema
            else None
        )

        adapter = varray_adapter if varray_adapter else array_adapter

        self.logger.debug(f"Iterating over collection {self.name}")
        for meta in adapter:
            if varray_adapter:
                array: VArray = VArray._create_from_meta(
                    self, meta, array_adapter, varray_adapter=varray_adapter
                )
            else:
                array: Array = Array._create_from_meta(self, meta, array_adapter)
            yield array

    def __repr__(self) -> str:
        s = f"{self.__class__.__name__}(name={self.name}, array_schema={self.array_schema})"
        if self.varray_schema:
            s = s[:-1] + f", varray_schema={self.varray_schema})"
        return s

    def __str__(self) -> str:
        return f"{self.name}"

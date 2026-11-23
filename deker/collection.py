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

import datetime

from collections.abc import Generator
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Union, cast

from deker_tools.cancel import check, write_token
from deker_tools.typings import Sentinel, SentinelType

from deker.ABC.base_adapters import BaseCollectionAdapter, BaseStorageAdapter, BaseVArrayAdapter
from deker.ABC.base_collection import BaseCollectionOptions
from deker.ABC.base_schemas import BaseDimensionSchema
from deker.arrays import Array, VArray
from deker.errors import (
    DekerInstanceNotExistsError,
    DekerInvalidManagerCallError,
    DekerInvalidSchemaError,
    DekerValidationError,
    TokenErrorConverter,
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
from deker.types.private.typings import ArrayT
from deker.validators import validate_timeout

if TYPE_CHECKING:
    from deker.ABC.base_factory import BaseAdaptersFactory
    from deker.uri import Uri


class Collection(SelfLoggerMixin, Serializer, Generic[ArrayT]):
    """Collection of ``Arrays`` or ``VArrays``.

    ``Collection`` is a high-level object for managing contents of a set of ``Arrays`` or ``VArrays``
    united into one group under collection name and certain schema.

    The schema decides the kind of arrays in the collection, and ``ArrayT`` tells it to type checkers:
    a collection created with an ``ArraySchema`` is a ``Collection[Array]``, one created with
    a ``VArraySchema`` is a ``Collection[VArray]``; ``create``, ``filter`` and iteration follow that type.

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
        :returns: ``ArraySchema`` for the arrays of the virtual array
        """
        attributes = [
            AttributeSchema(name="vid", dtype=str, primary=True),
            AttributeSchema(name="v_position", dtype=tuple, primary=True),
        ]

        vgrid = schema.vgrid
        if vgrid is None:  # pragma: no cover
            raise DekerInvalidSchemaError("VArraySchema vgrid is not set")
        dimensions: list[BaseDimensionSchema] = []
        for n, dim in enumerate(schema.dimensions):
            d: dict[str, Any] = {"name": dim.name, "size": dim.size // vgrid[n]}
            obj: type[BaseDimensionSchema]
            if isinstance(dim, TimeDimensionSchema):
                obj = TimeDimensionSchema
                d["step"] = dim.step
                d["start_value"] = dim.start_value

                if isinstance(dim.start_value, str) and dim.start_value.startswith("$"):
                    d["start_value"] = f"$parent.{dim.start_value[1:]}"
                    attributes.append(
                        AttributeSchema(
                            name=d["start_value"][1:],
                            dtype=datetime.datetime,
                            primary=False,
                        )
                    )
            else:
                obj = DimensionSchema
                d["labels"] = None
                d["scale"] = None

            dimensions.append(obj(**d))
        return ArraySchema(
            dtype=schema.dtype,
            fill_value=schema.fill_value,
            dimensions=dimensions,
            attributes=attributes,
        )

    def _validate(
        self,
        name: str,
        schema: ArraySchema | VArraySchema,
    ) -> None:
        """Validate name and schema.

        :param name: Collection name
        :param schema: Collection main schema
        :raises DekerValidationError: if the name is not a non-empty string or the schema is neither
          ``ArraySchema`` nor ``VArraySchema``
        """
        if not name or not isinstance(name, str) or name.isspace():
            raise DekerValidationError("Collection name shall be a non-empty string")

        if schema is None or not isinstance(schema, (ArraySchema, VArraySchema)):
            raise DekerValidationError(f"Invalid schema type {type(schema)}; ArraySchema or VArraySchema expected")

    def __init__(
        self,
        name: str,
        schema: ArraySchema | VArraySchema,
        adapter: "BaseCollectionAdapter",
        factory: "BaseAdaptersFactory",
        storage_adapter: type["BaseStorageAdapter"],
        collection_options: BaseCollectionOptions | None = None,
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
        self.__path = self.__adapter.collections_resource / name
        # Init collection chunking and compression options
        self.__collection_options = collection_options
        self.__storage_adapter = storage_adapter
        array_adapter = self.__factory.get_array_adapter(self.__path, self.__storage_adapter, self.__collection_options)

        self.__array_schema: ArraySchema
        self.__varray_schema: VArraySchema | None
        self.__varrays: VArrayManager | None
        varray_adapter: BaseVArrayAdapter | None = None
        if isinstance(schema, VArraySchema):
            self.__array_schema = self.__create_array_schema_from_varray_schema(schema)
            self.__varray_schema = schema
            varray_adapter = self.__factory.get_varray_adapter(self.__path, storage_adapter=self.__storage_adapter)
            self.__varrays = VArrayManager(self, array_adapter, varray_adapter)
        else:
            self.__array_schema = schema
            self.__varray_schema = None
            self.__varrays = None

        self.__arrays = ArrayManager(
            self,
            array_adapter,
        )

        self.__manager: DataManager[ArrayT] = DataManager(self, array_adapter, varray_adapter)
        self.logger.debug("Collection %s initialized", self.__name)

    def __del__(self) -> None:
        """Drop the references to the adapter and the managers."""
        for attr in ("_Collection__adapter", "_Collection__manager", "_Collection__arrays", "_Collection__varrays"):
            if getattr(self, attr, None) is not None:
                delattr(self, attr)

    @property
    def _storage_adapter(self) -> type[BaseStorageAdapter]:
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
    def varray_schema(self) -> VArraySchema | None:
        """Get collection ``VArraySchema``; ``None`` for a collection of ``Arrays``."""
        return self.__varray_schema

    @property
    def path(self) -> Union[Path, "Uri"]:
        """Get the ``Collection`` location in the storage: a ``Path`` for local storages, a ``Uri`` for remote ones."""
        return self.__path

    @property
    def options(self) -> BaseCollectionOptions | None:
        """Get collection options: chunking, compression, etc."""
        return self.__collection_options

    @property
    def as_dict(self) -> dict:
        """Serialize ``Collection`` to dictionary: name, type, schema, options, storage adapter and metadata version."""
        dic: dict = {
            "name": self.name,
            "type": (SchemaTypeEnum.varray.name if self.__varray_schema else SchemaTypeEnum.array.name),
            "schema": (self.__varray_schema.as_dict if self.__varray_schema else self.array_schema.as_dict),
            "options": self.options.as_dict if self.options else self.options,
            "storage_adapter": self.__storage_adapter.__name__,
            "metadata_version": self.__adapter.metadata_version,
        }

        return dic

    @property
    def varrays(self) -> VArrayManager:
        """Return manager for ``VArrays``.

        :raises DekerInvalidManagerCallError: in a collection of ``Arrays``
        """
        if not self.__varrays:
            raise DekerInvalidManagerCallError(
                "``.varrays`` in `Arrays` collection is unavailable; use ``.arrays`` instead"
            )
        return self.__varrays

    @property
    def arrays(self) -> ArrayManager:
        """Return manager for ``Arrays``.

        In a collection of ``VArrays`` these are the ``Arrays`` the ``VArrays`` are made of.
        """
        return self.__arrays

    @not_deleted
    def create(
        self,
        primary_attributes: dict | SentinelType = Sentinel,
        custom_attributes: dict | SentinelType = Sentinel,
        id_: str | SentinelType = Sentinel,
        *,
        timeout: int | SentinelType = Sentinel,
    ) -> ArrayT:
        """Create ``Array`` or ``VArray`` according to collection main schema.

        If ``VArraySchema`` is passed to ``Collection``, all data management will go through ``VArrays``
        as this method will create just ``VArrays`` (``Arrays`` will be created automatically by ``VArray``).
        Otherwise, only ``Arrays`` will be created.

        :param primary_attributes: ``Array`` or ``VArray`` primary attributes; all of the schema, no other
        :param custom_attributes: ``Array`` or ``VArray`` custom attributes
        :param id_: ``Array`` or ``VArray`` unique UUID string; generated if not passed
        :param timeout: seconds to wait for the lock of the symlink directory of the primary attributes; not given -
          ``write_lock_timeout`` of the client
        :returns: the created ``Array`` or ``VArray``
        :raises DekerValidationError: if the attributes do not match the schema, an array with these primary
          attributes already exists, or the timeout is not a positive integer
        :raises DekerLockError: if the symlink directory is not released within the timeout
        :raises DekerCancelledError: if the creation is cancelled before it started
        :raises DekerInstanceNotExistsError: if the collection was deleted
        """
        array = self.__manager.create(primary_attributes, custom_attributes, id_, timeout=timeout)
        self.logger.debug(
            "%s id=%s primary_attributes=%r, custom_attributes=%r created",
            array.__class__.__name__,
            array.id,
            primary_attributes,
            custom_attributes,
        )
        return array

    def delete(self, *, timeout: int | SentinelType = Sentinel) -> None:
        """Remove ``Collection`` and all its contents from the database.

        The deletion takes no lock, so there is nothing to wait for: the timeout is accepted for the uniformity of
        the operations. Once started, the deletion goes to its end.

        :param timeout: seconds to wait for the locks; not used
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerCancelledError: if the deletion is cancelled before it started
        """
        validate_timeout(timeout=timeout)
        self.logger.debug("Removing collection %s from database", self.__name)
        with TokenErrorConverter():
            token = write_token()
            check(token)
            self.__adapter.delete(self, timeout=timeout, token=token)
        self.logger.debug("Collection %s deleted OK", self.__name)

    @not_deleted
    def clear(self, *, timeout: int | SentinelType = Sentinel) -> None:
        """Remove all ``Arrays`` and/or ``VArrays`` from the ``Collection``; the collection itself stays.

        :param timeout: seconds to wait for the lock of the collection; not given - ``write_lock_timeout`` of the
          client
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerLockError: if the lock of the collection is not released within the timeout
        :raises DekerCancelledError: if the clearing is cancelled before it started
        :raises DekerInstanceNotExistsError: if the collection was deleted
        """
        validate_timeout(timeout=timeout)
        self.logger.debug("Clearing data from collection %s", self.__name)
        with TokenErrorConverter():
            token = write_token()
            check(token)
            self.__adapter.clear(self, timeout=timeout, token=token)
        self.logger.debug("Collection %s cleared OK", self.__name)

    def filter(self, filters: dict) -> FilteredManager[ArrayT]:
        """Filter ``Arrays`` or ``VArrays`` by provided conditions.

        :param filters: query conditions for filtering
        :returns: ``FilteredManager`` with the applied filters

        .. note::
           Conditions for filtering are either Array or VArray ``id`` value::

             {"id": "some_array_UUID_string"}

           or full scope of primary attributes' values::

             {"primary_attr1_name": its value, "primary_attr2_name": its value, ...}
        """
        return self.__manager.filter(filters)

    def _is_deleted(self) -> bool:
        """Check if collection was deleted from the storage.

        :returns: ``True`` if the collection does not exist in the storage anymore
        """
        return self.__adapter.is_deleted(self)

    def __iter__(self) -> Generator[ArrayT, None, None]:
        """Yield the ``Arrays`` or the ``VArrays`` of the ``Collection``, as its schema decides.

        :yields: every ``Array`` or ``VArray`` of the collection
        :raises DekerInstanceNotExistsError: if the collection was deleted
        """
        if self._is_deleted():
            raise DekerInstanceNotExistsError(
                f"{self} doesn't exist, create new or get an instance again to be able to iterate"
            )
        array_adapter = self.__factory.get_array_adapter(self.path, self.__storage_adapter, self.options)
        varray_adapter = (
            self.__factory.get_varray_adapter(self.path, storage_adapter=self.__storage_adapter)
            if self.varray_schema
            else None
        )

        adapter = varray_adapter if varray_adapter else array_adapter

        self.logger.debug("Iterating over collection %s", self.name)
        # the schema decides the kind of arrays, the same way it decides the collection's type parameter
        for meta in adapter:
            if varray_adapter:
                yield cast("ArrayT", VArray._create_from_meta(self, meta, array_adapter, varray_adapter=varray_adapter))
            else:
                yield cast("ArrayT", Array._create_from_meta(self, meta, array_adapter))

    def __repr__(self) -> str:
        s = f"{self.__class__.__name__}(name={self.name}, array_schema={self.array_schema})"
        if self.varray_schema:
            s = s[:-1] + f", varray_schema={self.varray_schema})"
        return s

    def __str__(self) -> str:
        return f"{self.name}"

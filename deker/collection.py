import datetime

from pathlib import Path
from typing import TYPE_CHECKING, Generator, Optional, Type, Union

from deker.ABC.base_adapters import BaseCollectionAdapter, BaseStorageAdapter
from deker.ABC.base_collection import BaseCollectionOptions
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
    TimeDimensionSchema,
    VArraySchema,
)
from deker.tools import check_memory, create_array_from_meta, not_deleted
from deker.types import Serializer
from deker.types.enums import SchemaType


if TYPE_CHECKING:
    from deker.ABC.base_factory import BaseAdaptersFactory
    from deker.arrays import Array, VArray


class Collection(SelfLoggerMixin, Serializer):
    """Collection of Arrays or VArrays.

    Collection is a high-level object for managing a (V)Arrays set
    united into one group under collection name and certain schema.

    Getter-properties:
        - name: returns collection name
        - array_schema: returns schema of embedded Arrays
        - varray_schema: returns schema of VArrays if applicable, else None
        - path: returns storage path to the collection
        - options: returns chunking and compression options
        - as_dict: serializes main information about collection into dictionary, prepared for JSON

    API methods:
        - create: according to main schema creates new Array or VArray of storage and returns its object
        - clear: according to main schema removes all VArrays and/or Arrays from storage
        - delete: removes collection with all VArrays and/or Arrays from storage
        - filter: filters Arrays or VArrays according to main schema and provided conditions
        - __iter__: according to the Collection's main schema iterates over all Arrays or VArrays in collection,
         yields their objects
        - __str__: ordinary behaviour
        - __repr__: ordinary behaviour
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
        :param schema: Collection (V)Array schema
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
        """Collection initialization.

        :param name: Collection unique name
        :param schema: ArraySchema or VArraySchema instance
        :param adapter: CollectionAdapter instance
        :param factory: AdaptersFactory
        :param storage_adapter: StorageAdapter instance
        :param collection_options: CollectionOptions instance or None
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
        """Get collection array schema."""
        return self.__array_schema

    @property
    def varray_schema(self) -> Optional[VArraySchema]:
        """Get collection varray schema."""
        return self.__varray_schema

    @property
    def path(self) -> Path:
        """Get collection file storage path."""
        return self.__path

    @property
    def options(self) -> Optional[BaseCollectionOptions]:
        """Get collection options: chunking, compression, etc."""
        return self.__collection_options

    @property
    def as_dict(self) -> dict:
        """Serialize collection to dictionary."""
        dic: dict = {
            "name": self.name,
            "type": SchemaType.varray.value if self.__varray_schema else SchemaType.array.value,
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
        """Property wrapper for checking access to varray methods in arrays collection."""
        if not self.__varrays:
            raise DekerInvalidManagerCallError("You cannot use .varrays in arrays collection.")
        return self.__varrays

    @property
    def arrays(self) -> ArrayManager:
        """Return manager for arrays."""
        return self.__arrays

    @not_deleted
    def create(
        self, primary_attributes: Optional[dict] = None, custom_attributes: Optional[dict] = None
    ) -> Union["Array", "VArray"]:
        """Create Array or VArray according to Collection schemas.

        If VArraySchema is passed to Collection, all data management will go through VArrays
        as this method will create just VArrays (Arrays will be created automatically by VArray).
        Otherwise, only Arrays will be created.

        :param primary_attributes: array primary attribute
        :param custom_attributes: array custom attributes
        """
        schema = self.array_schema
        shape = schema.arrays_shape if hasattr(schema, "arrays_shape") else schema.shape
        check_memory(shape, schema.dtype, self.__adapter.ctx.config.memory_limit)
        array = self.__manager.create(primary_attributes, custom_attributes)
        self.logger.debug(
            f"{array.__class__.__name__} id={array.id} {primary_attributes=}, {custom_attributes=} created"
        )
        return array

    def delete(self) -> None:
        """Remove collection and all its contents from the database."""
        self.logger.debug(f"Removing collection {self.__name} from database")
        self.__adapter.delete(self)
        self.logger.debug(f"Collection {self.__name} deleted OK")

    @not_deleted
    def clear(self) -> None:
        """Clear everything inside collection."""
        self.logger.debug(f"Clearing data from collection {self.__name}")
        self.__adapter.clear(self)
        self.logger.debug(f"Collection {self.__name} cleared OK")

    def filter(self, filters: dict) -> FilteredManager:
        """Filter Arrays or VArrays by provided conditions.

        :param filters: query conditions for filtering
        """
        return self.__manager.filter(filters)

    def _is_deleted(self) -> bool:
        """Check if collection was deleted."""
        return self.__adapter.is_deleted(self)  # type: ignore[attr-defined]

    def __next__(self) -> None:
        pass

    def __iter__(self) -> Generator[Union["Array", "VArray"], None, None]:
        """Yield (V)Arrays within the collection."""
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
            array = create_array_from_meta(self, meta, array_adapter, varray_adapter=varray_adapter)
            yield array

    def __repr__(self) -> str:
        s = f"{self.__class__.__name__}(name={self.name}, array_schema={self.array_schema})"
        if self.varray_schema:
            s = s[:-1] + f", varray_schema={self.varray_schema})"
        return s

    def __str__(self) -> str:
        return f"{self.name}"

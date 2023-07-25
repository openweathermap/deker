"""Deker Client."""
from __future__ import annotations

import importlib
import os
import pkgutil
import warnings

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple, Type, Union

from deker_tools.data import convert_size_to_human
from deker_tools.path import is_path_valid
from psutil import swap_memory, virtual_memory
from tqdm import tqdm

from deker.ABC.base_collection import BaseCollectionOptions
from deker.collection import Collection
from deker.config import DekerConfig
from deker.ctx import CTX
from deker.errors import (
    DekerClientError,
    DekerCollectionNotExistsError,
    DekerMetaDataError,
    DekerValidationError,
    DekerWarning,
)
from deker.integrity import IntegrityChecker
from deker.locks import META_DIVIDER
from deker.log import SelfLoggerMixin, set_logging_level
from deker.schemas import ArraySchema, VArraySchema
from deker.types import LocksExtensions
from deker.types.classes import ArrayLockMeta, CollectionLockMeta, StorageSize
from deker.types.enums import LocksTypes
from deker.uri import Uri


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseCollectionAdapter
    from deker.ABC.base_factory import BaseAdaptersFactory


class Client(SelfLoggerMixin):
    """Deker Client - is the first object a user starts with.

    It is being used for creating and getting collections and provides connection/path to Deker collections'
    storage by `uri`. Local collection uri shall contain `file://` schema and path to the collections storage
    on local machine. Connection to the storage is being provided by a client-based context, which remains
    open while the Client is open, and vice-versa: while the context is open - the Client is open too.

    Client has a context manager which opens and closes context itself:
        ```
        with Client("file://...") as client:
            ~some important job here~
        ```

    Anyway you may use Client directly:
        ```
        client = Client("file://...")
        ~some important job here~
        client.close()
        ```

    As long as Client has a context manager, its instance is reusable:
        ```
        client = Client("file://...")
        ~some important job here~
        client.close()
        with client:
            ~some important job here~
        with client:
            ~some important job here~
        ```

    Getter-properties:
     - is_closed
     - is_open
     - meta-version
     - root_path

    API methods:
        - create_collection: creates a new collection on the storage and returns its object instance to work with.
            Requires:
                * collection unique name: one name - one collection
                * an instance of ArraySchema or VArraySchema
                * (optional) chunking and compression options
        - get_collection: returns an object of Collection by a given name if such exists, otherwise - None
        - check_integrity: checks the integrity of embedded storage database on different levels;
            Either performs all checks and prints found errors or exit on the first error.
            The final report may be saved to file.
        - calculate_storage_size: calculates size of the whole storage or of a defined collection;
        - close: closes Client and its context
        - clear_locks: clears all current locks within the storage or a defined collection
        - __enter__: opens client context manager
        - __exit__: automatically closes client context manager on its exit
        - __iter__: iterates over all the collection within the provided uri-path, yields Collection instances.
    """

    __slots__ = (
        "__adapter",
        "__ctx",
        "__executor",
        "__storage_adapter",
        "__is_closed",
        "__config",
        "__uri",
        "__workers",
        "__factory",
    )

    __plugins: Dict[str, Type[BaseAdaptersFactory]] = {}

    def _open(self) -> None:
        """Open client.

        This method is being automatically invoked on Client initiation and in context manager.
        Normally, you don't need to use it yourself.
        """
        if not self.__plugins:
            raise DekerClientError(
                "No installed adapters are found: run `pip install deker_local_adapters`"
            )

        if self.__uri.scheme not in self.__plugins:
            raise DekerClientError(
                f"Invalid uri: {self.__uri.scheme} is not supported; {self.__uri}"
            )

        if self.is_closed:
            self.__is_closed = False

            try:
                factory = self.__plugins[self.__uri.scheme]
            except AttributeError:
                raise DekerClientError(
                    f"Invalid source: installed package does not provide AdaptersFactory "
                    f"for managing uri scheme {self.__uri.scheme}"
                )

            self.__ctx = CTX(
                uri=self.__uri,
                config=self.__config,
                executor=self.__executor,
                is_closed=self.__is_closed,
            )
            self.__factory = factory(self.__ctx, self.__uri)
            self.__adapter = self.__factory.get_collection_adapter()
            self.logger.info("Client is open")

    def __get_plugins(self) -> None:
        """Get deker adapters plugins."""
        for package in pkgutil.iter_modules():
            if package.ispkg and (
                package.name.startswith("deker_") and package.name.endswith("_adapters")
            ):
                module = importlib.import_module(package.name)
                factory = module.AdaptersFactory
                for scheme in factory.uri_schemes:
                    if scheme not in self.__plugins:
                        self.__plugins.update({scheme: factory})

    def __init__(
        self,
        uri: str = "",
        *,
        executor: Optional[ThreadPoolExecutor] = None,
        workers: Optional[int] = None,
        write_lock_timeout: int = 60,
        write_lock_check_interval: int = 1,
        loglevel: str = "ERROR",
        memory_limit: int = 0,
    ) -> None:
        """Deker client constructor.
        Gets plugins (adapters), sets DekerConfig, creates uri and opens client.
        Raises DekerClientError in case of errors during initialization.

        :param uri: uri for Deker
        :param executor: external ThreadPoolExecutor instance (optional)
        :param workers: number of threads for Deker
        :param write_lock_timeout: Timeout for write locks
        :param write_lock_check_interval: Interval for sleep while waiting for unlock on write
        :param loglevel: Level for logger
        :param memory_limit: Limit of memory allocation per one array/subset in bytes;
            by default - 0, if <= 0 - total RAM + total swap is used,
            This parameter is used for early runtime break in case of potential memory overflow.
        """
        try:
            self.__get_plugins()
            self.__config = DekerConfig(  # type: ignore[call-arg]
                uri=uri,
                workers=workers if workers is not None else cpu_count() + 4,
                write_lock_timeout=write_lock_timeout,
                write_lock_check_interval=write_lock_check_interval,
                loglevel=loglevel,
                memory_limit=(
                    virtual_memory().total + swap_memory().total
                    if memory_limit <= 0
                    else memory_limit
                ),
            )
            self.__uri: Uri = Uri.create(self.__config.uri)
            self.__is_closed: bool = True
            self.__adapter: Optional["BaseCollectionAdapter"] = None
            self.__factory: Optional["BaseAdaptersFactory"] = None
            self.__ctx: Optional[CTX] = None
            set_logging_level(self.__config.loglevel)
            self.__executor: Optional[ThreadPoolExecutor] = executor
            self._open()
        except Exception as e:
            raise DekerClientError(e)

    def __del__(self) -> None:
        """Delete client."""
        self.close()

    def _validate_collection_metadata_version(self, collection_metadata: dict) -> None:
        """Validate collection metadata.

        :param collection_metadata: Dictionary with collection metadata
        """
        if (
            "metadata_version" not in collection_metadata
            or collection_metadata["metadata_version"] > self.meta_version
        ):
            raise DekerMetaDataError(
                f"Collection metadata version is not supported by this version: adapter metadata version: "
                f"{self.meta_version}, collection metadata version: {collection_metadata.get('metadata_version')}"
            )
        else:
            for n, dim in enumerate(collection_metadata["schema"]["dimensions"]):
                if dim["type"] == "generic" and "scale" not in dim:
                    collection_metadata["schema"]["dimensions"][n]["scale"] = None
            collection_metadata["metadata_version"] = self.meta_version

    @property
    def meta_version(self) -> str:
        """Get version of actual collection metadata."""
        return self.__adapter.metadata_version

    @property
    def root_path(self) -> Path:
        """Check client status."""
        return Path(self.__adapter.uri.path) / self.__config.collections_directory

    @property
    def is_closed(self) -> bool:
        """Check client status."""
        return self.__is_closed

    @property
    def is_open(self) -> bool:
        """Check client status."""
        return not self.__is_closed

    def calculate_storage_size(self, collection_name: str = "") -> StorageSize:
        """Get the size of the storage or of a certain collection in bytes or converted to human representation.

        WARNING: Size calculation may take a long time. Maybe you'd like to have some coffee while it's working.
        :param collection_name: Name of a collection. If not passed, the whole storage will be counted.
        """
        paths: List[Tuple[Path, str]] = []
        if collection_name:
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(
                    f"Collection {collection_name} does not exist at this storage"
                )
            paths.append(
                (
                    collection.path / self.__config.array_symlinks_directory,
                    collection._storage_adapter.file_ext,
                )
            )
        else:
            paths.extend(
                [
                    (
                        collection.path / self.__config.array_symlinks_directory,
                        collection._storage_adapter.file_ext,
                    )
                    for collection in self
                ]
            )

        warning = "Size calculation may take a long time. May be, you'd like to have some coffee while it's working."
        self.logger.warning(warning)
        warnings.warn(warning, category=DekerWarning, stacklevel=0)

        size = 0

        with tqdm(paths, ncols=80, desc=str(size)) as pbar:
            for collection, ext in pbar:
                for root, _, files in os.walk(collection):  # type: ignore[type-var]
                    for file in files:  # type: str
                        if file.endswith(ext):  # noqa
                            size += os.path.getsize(Path(root) / file)  # type: ignore[arg-type]
                            pbar.set_description(str(size))
                pbar.set_description(str(size))

        human = convert_size_to_human(size)
        return StorageSize(size, human)

    def close(self) -> None:
        """Close client."""
        if self.__adapter:
            self.__adapter.close()
            self.__adapter = None
        if self.__factory:
            self.__factory.close()
            self.__factory = None
        self.__is_closed = True
        self.__ctx.is_closed = True
        self.logger.info("Client is closed")

    def create_collection(
        self,
        name: str,
        schema: Union[ArraySchema, VArraySchema],  # type: ignore[arg-type]
        collection_options: Optional[BaseCollectionOptions] = None,
        storage_adapter_type: Optional[str] = None,
    ) -> Collection:
        """Create a new collection in the database.

        :param storage_adapter_type: Adapter, which works with files. Default is HDF5StorageAdapter
        :param name: Name of collection
        :param schema: Array or VArray schema
        :param collection_options: Options for compression and chunks
        """
        if not isinstance(name, str) or not name or name.isspace():
            raise DekerValidationError(f"Collection invalid name: {name}")

        if not isinstance(schema, (ArraySchema, VArraySchema)):
            raise DekerValidationError(
                f"Invalid schema type: {type(schema)}; ArraySchema or VArraySchema expected"
            )
        # Pick correct adapter
        collection = Collection(
            name=name,
            schema=schema,
            adapter=self.__adapter,
            factory=self.__factory,
            collection_options=collection_options,
            storage_adapter=self.__adapter.get_storage_adapter(storage_adapter_type),
        )

        self.__adapter.create(collection)
        self.logger.debug(f"Collection {collection.name} created")
        return collection

    def get_collection(
        self,
        name: str,
    ) -> Optional[Collection]:
        """Get collection from database by its name.

        :param name: name of collection
        """
        try:
            collection_data: dict = self.__adapter.read(name)
            self._validate_collection_metadata_version(collection_data)
            collection = self.__adapter.create_collection_from_meta(  # type: ignore[return-value]
                collection_data, self.__factory
            )
            self.logger.debug(f"Collection {name} read from meta")
            return collection  # type: ignore[return-value]
        except DekerCollectionNotExistsError:
            self.logger.info(f"Collection {name} not found")
            return None

    def collection_from_dict(self, collection_data: dict) -> Collection:
        """Create a new collection in the database from collection metadata dictionary.

        :param collection_data: Dictionary with collection metadata
        """
        self._validate_collection_metadata_version(collection_data)
        # TODO: add tests for update logic, edge cases for ifs
        default_fields: dict = {
            "schema": {
                "fill_value": None,
                "attributes": [],
                "dimensions": {"labels": None, "scale": None},
            },
            "options": None,
        }

        for key in default_fields:
            if key not in collection_data:
                collection_data[key] = default_fields[key]
            else:
                if isinstance(default_fields[key], dict):
                    for k in default_fields[key]:
                        if k == "dimensions":
                            for n, dim in enumerate(collection_data[key][k]):
                                if dim["type"] != "time":
                                    if "labels" not in dim:
                                        collection_data[key][k][n]["labels"] = None
                                    if "scale" not in dim:
                                        collection_data[key][k][n]["scale"] = None
                        else:
                            if k not in collection_data[key]:
                                collection_data[key][k] = default_fields[key][k]
        collection = self.__adapter.create_collection_from_meta(  # type: ignore[return-value]
            collection_data, self.__factory
        )
        self.__adapter.create(collection)
        self.logger.debug(f"Collection {collection.name} created from dict")
        return collection  # type: ignore[return-value]

    @staticmethod
    def _iter_collection_locks(
        path: Path, collection_name: str, lock_type: Union[LocksTypes, None] = None
    ) -> list[ArrayLockMeta]:
        """Return Collection arrays' lockfiles.

        :param path: Collection path
        :param collection_name: collection name
            If passed - gets locks only from passed collection, else checks every collection in client
        :param lock_type: LocksTypes enum attribute, leave empty to get every lockfile
        """
        locks: list[ArrayLockMeta] = []
        for file in Path.rglob(path, "*lock"):
            if not lock_type or file.name.endswith(LocksExtensions[lock_type.name].value):
                meta = file.name.split(META_DIVIDER)
                if len(meta) != 4:
                    # discuss other locks print format
                    continue
                if not lock_type:
                    lock_type_name = LocksExtensions(f".{file.name.split('.')[-1]}").name
                    lock_type = LocksTypes[lock_type_name]
                lock: ArrayLockMeta = {
                    "Lockfile": file.name,
                    "Collection": collection_name,
                    "Array": meta[0],
                    "ID": meta[1],
                    "PID": meta[2],
                    "TID": meta[3].split(".")[0],
                    "Type": lock_type.value,
                    "Creation": datetime.fromtimestamp(file.stat().st_ctime).isoformat(),
                }
                print(lock)
                locks.append(lock)
        return locks

    # TODO decide on private/public method
    def _get_locks(
        self,
        collection_name: Union[str, None] = None,
        lock_type: Union[LocksTypes, None] = None,
    ) -> list[Union[CollectionLockMeta, ArrayLockMeta]]:
        """Return lockfiles of collections and arrays.

        :param collection_name: Collection name
            If passed - gets locks only from passed collection, else checks every collection in client
        :param lock_type: LocksTypes enum attribute, leave empty to get every lockfile
        """
        locks: list[Union[CollectionLockMeta, ArrayLockMeta]] = []
        if collection_name:
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(
                    f"Collection {collection_name} does not exist at this storage"
                )
            locks = self._iter_collection_locks(
                self.root_path / collection_name, collection_name, lock_type
            )
        else:
            for directory in Path(self.root_path).iterdir():
                if not lock_type or lock_type == LocksTypes.collection_lock:
                    if directory.is_file() and directory.name.endswith(
                        LocksExtensions.collection_lock.value
                    ):
                        lock: CollectionLockMeta = {
                            "Lockfile": directory.name,
                            "Collection": directory.name[
                                : -len(LocksExtensions.collection_lock.value)
                            ],
                            "Type": LocksTypes.collection_lock.value,
                            "Creation": datetime.fromtimestamp(
                                directory.stat().st_ctime
                            ).isoformat(),
                        }
                        print(lock)
                        locks.append(lock)
                locks.extend(self._iter_collection_locks(directory, directory.name, lock_type))
        return locks

    def clear_locks(self, collection_name: Union[str, None] = None) -> None:
        """Clear (V)Arrays' readlocks.

        :param collection_name: collection name
            If passed - clears lock only in passed collection, else clears locks in every collection in client
        """
        root_path = self.root_path
        files_count = 0

        if collection_name:
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(
                    f"Collection {collection_name} does not exist at this storage"
                )
            root_path = root_path / collection_name

        for file in Path.rglob(root_path, "*lock"):
            if file.name.endswith(
                (LocksExtensions.array_read_lock.value, LocksExtensions.varray_lock.value)
            ):
                file.unlink()
                files_count += 1

        print(f"{files_count} files removed")

    def check_integrity(
        self,
        level: int = 1,
        stop_on_error: bool = True,
        to_file: Union[bool, Path, str] = False,
        collection: Union[str, None] = None,
    ) -> None:
        """Run integrity check.

        There are 4 levels:
            1. Checks collections integrity, initialises every collection if no collection passed in params
            2: Checks arrays/varrays initialization and lockfiles
            3: Checks if arrays/varrays paths are valid, including symlinks
            4: Checks if stored data is consistent with file-by-file single value reading

        :param collection: collection name
            If passed - checks only passed collection, else checks every collection in client
        :param level: check level
        :param stop_on_error: flag to stop on first path or data error
        :param to_file: log errors in file; accepts True/False or a path to file
            If True - logs errors into a default filename in the current directory
            If a path to file is passed - logs error to file
        """
        errors: str = IntegrityChecker(self, self.root_path, stop_on_error, level).check(collection)
        if not errors:
            print("Integrity check found no errors")
            return
        if to_file:
            if isinstance(to_file, Path):
                filename = to_file
            elif isinstance(to_file, str):
                filename = Path(to_file)
            else:
                filename = (
                    Path(".")
                    / f"deker_integrity_report_{datetime.now().isoformat(timespec='seconds')}.txt"
                )
            is_path_valid(filename.parent.absolute())
            with open(filename, "w") as f:
                f.write(errors)
            errors += f"\n\nIntegrity check logged errors in {to_file.absolute()}"
        print(errors)

    def __iter__(self) -> Generator[Collection, None, None]:
        """Iterate over all collections on the storage."""
        for meta in self.__adapter:
            collection = self.__adapter.create_collection_from_meta(
                meta, self.__factory  # type: ignore[arg-type]
            )
            yield collection

    def __enter__(self) -> Client:
        self._open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

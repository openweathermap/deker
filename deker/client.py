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


import importlib
import os
import pkgutil
import warnings

from collections.abc import Callable, Generator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import wraps
from multiprocessing import cpu_count
from pathlib import Path
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar, overload

from deker_tools.data import convert_size_to_human
from deker_tools.log import set_logger
from deker_tools.path import is_path_valid
from psutil import swap_memory, virtual_memory
from tqdm import tqdm

from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.config import DekerConfig
from deker.ctx import CTX
from deker.errors import (
    DekerAdapterClosedError,
    DekerClientError,
    DekerCollectionNotExistsError,
    DekerMetaDataError,
    DekerValidationError,
)
from deker.integrity import IntegrityChecker
from deker.locks import META_DIVIDER
from deker.log import SelfLoggerMixin, format_string, set_logging_level
from deker.schemas import ArraySchema, VArraySchema
from deker.tools import convert_human_memory_to_bytes
from deker.types import ArrayLockMeta, CollectionLockMeta, LocksExtensions, LocksTypes, StorageSize
from deker.uri import Uri
from deker.warnings import DekerWarning

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseCollectionAdapter
    from deker.ABC.base_collection import BaseCollectionOptions
    from deker.ABC.base_factory import BaseAdaptersFactory


P = ParamSpec("P")
R = TypeVar("R")


def _open_client_only(method: Callable[Concatenate["Client", P], R]) -> Callable[Concatenate["Client", P], R]:
    """Reject calls of a closed client with ``DekerClientError``.

    A closed client keeps its adapters, and they answer with ``DekerAdapterClosedError``; the client
    checks its own state first and translates the adapters' error into its own one, without a chain of tracebacks.

    :param method: ``Client`` method
    :returns: the decorated method
    """

    @wraps(method)
    def open_client_method(self: "Client", *args: P.args, **kwargs: P.kwargs) -> R:
        """Call the method of an open client.

        :param self: Client instance
        :param *args: method arguments
        :param **kwargs: method keyword arguments
        :returns: result of the method
        """
        if self.is_closed:
            raise DekerClientError("Client is closed")
        try:
            return method(self, *args, **kwargs)
        except DekerAdapterClosedError:
            raise DekerClientError("Client is closed") from None

    return open_client_method


class Client(SelfLoggerMixin):
    """Deker ``Client`` - is the first object user starts with.

    It is used for creating and getting ``Collections`` and provides connection/path to Deker collections'
    storage by `uri`. Local collection uri shall contain `file://` schema and path to the collections storage
    on local machine. Connection to the storage is provided by a client-based context, which remains
    open while the Client is open, and vice-versa: while the context is open - the Client is open too.

    ``Client`` has a context manager which opens and closes context itself::

      with Client("file://...") as client:
          ~some important job here~

    Anyway you may use ``Client`` directly::

      client = Client("file://...")
      ~some important job here~
      client.close()

    As long as ``Client`` has a context manager, its instance is reusable::

      client = Client("file://...")
      ~some important job here~
      client.close()
      with client:
          ~some important job here~
      with client:
          ~some important job here~

    Properties
    ----------
    - ``is_closed``
    - ``is_open``
    - ``meta-version``
    - ``root_path``

    API methods
    -----------
    - ``create_collection``: creates a new ``Collection`` on the storage and returns its object instance to work with.
      Requires:
        * collection **unique** name
        * an instance of ArraySchema or VArraySchema
        * chunking and compression options (optional); default is ``None``
        * type of a storage adapter (optional); default is ``HDF5StorageAdapter``

    - ``get_collection``: returns an object of ``Collection`` by a given name if such exists, otherwise - ``None``
    - ``check_integrity``: checks the integrity of embedded storage database at different levels;

      Either performs all checks and prints found errors or exit on the first error.
      The final report may be saved to file.

    - ``calculate_storage_size``: calculates size of the whole storage or of a defined ``Collection``;
    - ``close``: closes ``Client`` and its context
    - ``clear_locks``: clears all current locks within the storage or a defined ``Collection``
    - ``__enter__``: opens ``Client`` context manager
    - ``__exit__``: automatically closes ``Client`` context manager on its exit
    - ``__iter__``: iterates over all the collections within the provided uri-path, yields ``Collection`` instances.
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

    __plugins: dict[str, type["BaseAdaptersFactory"]] = {}

    # the state of a client: set in __init__, (re)filled by _open and cleared by close
    __uri: Uri
    __is_closed: bool
    __adapter: "BaseCollectionAdapter | None"
    __factory: "BaseAdaptersFactory | None"
    __ctx: CTX | None
    __executor: ThreadPoolExecutor | None
    kwargs: dict[str, Any]

    def _open(self) -> None:
        """Open client.

        This method is automatically invoked on ``Client`` initiation and in context manager.
        Normally, you don't need to use it yourself.
        """
        if not self.__plugins:
            raise DekerClientError("No installed adapters are found: run `pip install deker_local_adapters`")

        if self.__uri.scheme not in self.__plugins:
            raise DekerClientError(f"Invalid uri: {self.__uri.scheme} is not supported; {self.__uri}")

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
                extra=self.kwargs,
            )
            self.__factory = factory(self.__ctx, self.__uri)
            self.__adapter = self.__factory.get_collection_adapter()
            self.logger.info("Client is open")

    def __get_plugins(self) -> None:
        """Get Deker adapters plugins."""
        for package in pkgutil.iter_modules():
            if package.ispkg and (package.name.startswith("deker_") and package.name.endswith("_adapters")):
                module = importlib.import_module(package.name)
                factory = module.AdaptersFactory
                for scheme in factory.uri_schemes:
                    if scheme not in self.__plugins:
                        self.__plugins.update({scheme: factory})

    def __init__(
        self,
        uri: str = "",
        *,
        executor: ThreadPoolExecutor | None = None,
        workers: int | None = None,
        write_lock_timeout: int = 60,
        write_lock_check_interval: int = 1,
        loglevel: str = "ERROR",
        memory_limit: int | str = 0,
        skip_collection_create_memory_check: bool = False,
        **kwargs: Any,
    ) -> None:
        """Deker client constructor.

        :param uri: uri to Deker storage
        :param executor: external ThreadPoolExecutor instance (optional)
        :param workers: number of threads for Deker
        :param write_lock_timeout: An amount of seconds during which a parallel writing process waits for release
          of the locked file
        :param write_lock_check_interval: An amount of time (in seconds) during which a parallel writing process sleeps
          between checks for locks
        :param loglevel: Level of Deker loggers
        :param memory_limit: Limit of memory allocation per one array/subset in bytes or in human representation of
          kilobytes, megabytes or gigabytes, e.g. ``"100K"``, ``"512M"``, ``"4G"``. Human representations will be
          converted into bytes. If result is ``<= 0`` - total RAM + total swap is used

          .. note:: This parameter is used for early runtime break in case of potential memory overflow
        :param skip_collection_create_memory_check: If we don't want to check size during collection creation
        :param **kwargs: a wildcard, reserved for any extra parameters
        """
        try:
            set_logger(format_string)
            set_logging_level(loglevel.upper())
            self.__get_plugins()
            total_available_mem = virtual_memory().total + swap_memory().total
            memory_limit = convert_human_memory_to_bytes(memory_limit)
            if memory_limit >= total_available_mem or memory_limit <= 0:
                mem_limit = total_available_mem
            else:
                mem_limit = memory_limit

            self.__config = DekerConfig(
                uri=uri,
                workers=workers if workers is not None else cpu_count() + 4,
                write_lock_timeout=write_lock_timeout,
                write_lock_check_interval=write_lock_check_interval,
                loglevel=loglevel.upper(),
                memory_limit=mem_limit,
                skip_collection_create_memory_check=skip_collection_create_memory_check,
            )
            self.__uri = Uri.create(self.__config.uri)
            self.__is_closed = True
            self.__adapter = None
            self.__factory = None
            self.__ctx = None
            self.__executor = executor
            self.kwargs = kwargs
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
        if "metadata_version" not in collection_metadata or collection_metadata["metadata_version"] > self.meta_version:
            raise DekerMetaDataError(
                f"Collection metadata version is not supported by this version: adapter metadata version: "
                f"{self.meta_version}, collection metadata version: {collection_metadata.get('metadata_version')}"
            )

        for n, dim in enumerate(collection_metadata["schema"]["dimensions"]):
            if dim["type"] == "generic" and "scale" not in dim:
                collection_metadata["schema"]["dimensions"][n]["scale"] = None
        collection_metadata["metadata_version"] = self.meta_version

    @property
    def _adapter(self) -> "BaseCollectionAdapter":
        """Get the collections adapter of the open client."""
        if self.__adapter is None:
            raise DekerClientError("Client is closed")
        return self.__adapter

    @property
    @_open_client_only
    def meta_version(self) -> str:
        """Get actual metadata version, provided by local adapters."""
        return self._adapter.metadata_version

    @property
    @_open_client_only
    def root_path(self) -> Path:
        """Get root path to the current storage."""
        return Path(self._adapter.uri.path) / self.__config.collections_directory

    @property
    def _factory(self) -> "BaseAdaptersFactory":
        """Get the adapters factory of the open client."""
        if self.__factory is None:
            raise DekerClientError("Client is closed")
        return self.__factory

    @property
    def _ctx(self) -> CTX:
        """Get the context of the open client."""
        if self.__ctx is None:  # pragma: no cover
            raise DekerClientError("Client is not initialized")
        return self.__ctx

    @property
    def is_closed(self) -> bool:
        """Check client status."""
        return self.__is_closed

    @property
    def is_open(self) -> bool:
        """Check client status."""
        return not self.__is_closed

    @_open_client_only
    def calculate_storage_size(self, collection_name: str = "") -> StorageSize:
        """Get the size of the storage or of a certain collection in bytes or converted to human representation.

        .. warning:: Size calculation may take a long time. Maybe you'd like to have some coffee while it's working.

        :param collection_name: Name of a ``Collection``. If not passed, the whole storage will be counted.
        :returns: storage size in bytes and in human representation
        """
        paths: list[tuple[Path, str]] = []
        if collection_name:
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(f"Collection {collection_name} does not exist at this storage")
            paths.append(
                (
                    self.root_path / collection.name / self.__config.array_symlinks_directory,
                    collection._storage_adapter.file_ext,
                )
            )
        else:
            paths.extend(
                [
                    (
                        self.root_path / collection.name / self.__config.array_symlinks_directory,
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
                for root, _, files in os.walk(collection):
                    for file in files:  # type: str
                        if file.endswith(ext):
                            size += os.path.getsize(Path(root) / file)
                            pbar.set_description(str(size))
                pbar.set_description(str(size))

        human = convert_size_to_human(size)
        return StorageSize(size, human)

    def close(self) -> None:
        """Close client."""
        try:
            if self.__adapter:
                self.__adapter.close()
            if self.__factory:
                self.__factory.close()
            if self.__ctx:
                self.__ctx.is_closed = True
            self.__is_closed = True
        except AttributeError as e:
            self.logger.debug(f"Exception in Client.close(): {e}")
        finally:
            self.logger.info("Client is closed")

    # the overloads carry the decorator too: mypy matches them against the decorated implementation
    @overload
    @_open_client_only
    def create_collection(
        self,
        name: str,
        schema: ArraySchema,
        collection_options: "BaseCollectionOptions | None" = None,
        storage_adapter_type: str | None = None,
    ) -> Collection[Array]: ...

    @overload
    @_open_client_only
    def create_collection(
        self,
        name: str,
        schema: VArraySchema,
        collection_options: "BaseCollectionOptions | None" = None,
        storage_adapter_type: str | None = None,
    ) -> Collection[VArray]: ...

    @_open_client_only
    def create_collection(
        self,
        name: str,
        schema: ArraySchema | VArraySchema,
        collection_options: "BaseCollectionOptions | None" = None,
        storage_adapter_type: str | None = None,
    ) -> Collection[Array] | Collection[VArray]:
        """Create a new ``Collection`` in the database.

        The schema decides the kind of arrays: an ``ArraySchema`` gives a ``Collection[Array]``,
        a ``VArraySchema`` gives a ``Collection[VArray]``.

        :param name: Name of new ``Collection``
        :param schema: ``Array`` or ``VArray`` schema
        :param collection_options: Options for compression and chunks (if applicable)
        :param storage_adapter_type: Type of an adapter, which works with files; default is ``HDF5StorageAdapter``
        :returns: the created ``Collection``
        """
        if not isinstance(name, str) or not name or name.isspace():
            raise DekerValidationError(f"Collection invalid name: {name}")

        if not isinstance(schema, (ArraySchema, VArraySchema)):
            raise DekerValidationError(f"Invalid schema type: {type(schema)}; ArraySchema or VArraySchema expected")
        # Pick correct adapter
        collection = Collection(
            name=name,
            schema=schema,
            adapter=self._adapter,
            factory=self._factory,
            collection_options=collection_options,
            storage_adapter=self._adapter.get_storage_adapter(storage_adapter_type),
        )

        self._adapter.create(collection)
        self.logger.debug(f"Collection {collection.name} created")
        return collection

    @_open_client_only
    def get_collection(
        self,
        name: str,
    ) -> Collection[Array] | Collection[VArray] | None:
        """Get ``Collection`` from database by its name.

        The kind of arrays is read from the stored schema, so the result is a ``Collection[Array]``
        or a ``Collection[VArray]``; check ``varray_schema`` to tell which.

        :param name: Name of a ``Collection``
        :returns: the ``Collection``, or ``None`` if it does not exist
        """
        try:
            collection_data: dict = self._adapter.read(name)
            self._validate_collection_metadata_version(collection_data)
            collection = self._adapter.create_collection_from_meta(collection_data, self._factory)
            self.logger.debug(f"Collection {name} read from meta")
            return collection
        except DekerCollectionNotExistsError:
            self.logger.info(f"Collection {name} not found")
            return None

    def _validate_collection(self, collection_data: dict) -> Collection:
        """Validate ``Collection`` object and return it without creation.

        Not recommended to use except for validation.

        :param collection_data: Dictionary with collection metadata
        :returns: validated ``Collection`` instance
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

            elif isinstance(default_fields[key], dict):
                for k in default_fields[key]:
                    if k == "dimensions":
                        for n, dim in enumerate(collection_data[key][k]):
                            if dim["type"] != "time":
                                if "labels" not in dim:
                                    collection_data[key][k][n]["labels"] = None
                                if "scale" not in dim:
                                    collection_data[key][k][n]["scale"] = None

                    elif k not in collection_data[key]:
                        collection_data[key][k] = default_fields[key][k]
        return self._adapter.create_collection_from_meta(collection_data, self._factory)

    @_open_client_only
    def collection_from_dict(self, collection_data: dict) -> Collection:
        """Create a new ``Collection`` in the database from collection metadata dictionary.

        :param collection_data: Dictionary with collection metadata
        :returns: the created ``Collection``
        """
        collection = self._validate_collection(collection_data)
        self._adapter.create(collection)
        self.logger.debug(f"Collection {collection.name} created from dict")
        return collection

    @staticmethod
    def _iter_collection_locks(
        path: Path, collection_name: str, lock_type: LocksTypes | None = None
    ) -> list[ArrayLockMeta]:
        """Return the lock files of the ``Arrays`` of a collection.

        The only lock files of arrays are the markers of VArray writes, named
        ``<array id><ext>:<pid>:<token>.varraylock``; the read locks live in the kernel and leave no files.

        :param path: ``Collection`` path
        :param collection_name: name of a ``Collection``
        :param lock_type: ``LocksTypes`` enum attribute, leave empty to get every lockfile
        :returns: lock files of the collection arrays
        """
        if lock_type is not None and lock_type != LocksTypes.varray_lock:
            return []
        locks: list[ArrayLockMeta] = []
        for file in Path.rglob(path, f"*{LocksExtensions.varray_lock.value}"):
            meta = file.name[: -len(LocksExtensions.varray_lock.value)].split(META_DIVIDER)
            if len(meta) != 3:  # noqa: PLR2004
                continue  # a marker of another format is not described
            lock: ArrayLockMeta = {
                "Lockfile": file.name,
                "Collection": collection_name,
                "Array": meta[0].split(".")[0],
                "PID": meta[1],
                "Token": meta[2],
                "Type": LocksTypes.varray_lock.value,
                "Creation": datetime.fromtimestamp(file.stat().st_ctime).isoformat(),
            }
            locks.append(lock)
        return locks

    # TODO: decide on private/public method
    def _get_locks(
        self,
        collection_name: str | None = None,
        lock_type: LocksTypes | None = None,
    ) -> list[CollectionLockMeta | ArrayLockMeta]:
        """Return lockfiles of ``Collections`` and ``Arrays``.

        :param collection_name: Name of a ``Collection``
          If passed - gets locks only from passed ``Collection``, else checks every collection in the storage
        :param lock_type: ``LocksTypes`` enum attribute, leave empty to get every lockfile
        :returns: lock files of collections and arrays
        """
        locks: list[CollectionLockMeta | ArrayLockMeta] = []
        if collection_name:
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(f"Collection {collection_name} does not exist at this storage")
            locks.extend(self._iter_collection_locks(self.root_path / collection_name, collection_name, lock_type))
        else:
            for directory in Path(self.root_path).iterdir():
                if (
                    not lock_type
                    or lock_type == LocksTypes.collection_lock
                    and directory.is_file()
                    and directory.name.endswith(LocksExtensions.collection_lock.value)
                ):
                    lock: CollectionLockMeta = {
                        "Lockfile": directory.name,
                        "Collection": directory.name[: -len(LocksExtensions.collection_lock.value)],
                        "Type": LocksTypes.collection_lock.value,
                        "Creation": datetime.fromtimestamp(directory.stat().st_ctime).isoformat(),
                    }
                    locks.append(lock)
                locks.extend(self._iter_collection_locks(directory, directory.name, lock_type))
        return locks

    @_open_client_only
    def clear_locks(self, collection_name: str | None = None) -> None:
        """Remove the markers of VArray writes left by crashed processes.

        The read locks live in the kernel and die with their processes: there is nothing to clear for them.

        :param collection_name: Name of a ``Collection``.
          If passed - clears locks only in the provided collection, else clears locks in every collection in the storage
        """
        root_path = self.root_path
        files_count = 0

        if collection_name:
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(f"Collection {collection_name} does not exist at this storage")
            root_path = root_path / collection_name

        for file in Path.rglob(root_path, f"*{LocksExtensions.varray_lock.value}"):
            file.unlink()
            files_count += 1

        self.logger.info(f"{files_count} lock files removed")

    @_open_client_only
    def check_integrity(
        self,
        level: int = 1,
        stop_on_error: bool = True,
        to_file: bool | Path | str = False,
        collection: str | None = None,
    ) -> None:
        """Run storage integrity check at one of 4 levels.

        1. checks ``Collections`` integrity.  If no collection name was passed, iterates over all the
           ``Collections`` and initialises them one by one
        2. checks ``Arrays``/``VArrays`` initialization and lockfiles
        3. checks if ``Arrays``/``VArrays`` paths are valid, including symlinks
        4. checks if stored data is consistent with file-by-file one point reading

        :param collection: Name of a ``Collection``. If passed - checks only passed collection, else checks
          every collection in the storage
        :param level: Check-level
        :param stop_on_error: Flag to stop on first path or data error
        :param to_file: Dump errors in file; accepts ``True``/``False`` or a path to file. If ``True`` - dump errors
          into a default filename in the current directory; if a ``path`` to file is passed - dump errors to the file
          with a specified name and path.
        """
        errors: str = IntegrityChecker(self, self.root_path, stop_on_error, level).check(collection)
        if not errors:
            self.logger.info("Integrity check found no errors")
            return
        if to_file:
            if isinstance(to_file, Path):
                filename = to_file
            elif isinstance(to_file, str):
                filename = Path(to_file)
            else:
                filename = Path(".") / f"deker_integrity_report_{datetime.now().isoformat(timespec='seconds')}.txt"
            is_path_valid(filename.parent.absolute())
            with filename.open("w") as f:
                f.write(errors)
            errors += f"\n\nIntegrity check logged errors in {filename.absolute()}"
        self.logger.error(errors)

    def __iter__(self) -> Generator[Collection]:
        """Iterate over all collections in the storage."""
        if self.is_closed:
            raise DekerClientError("Client is closed")
        try:
            for meta in self._adapter:
                yield self._adapter.create_collection_from_meta(meta, self._factory)
        except DekerAdapterClosedError:
            raise DekerClientError("Client is closed") from None

    def __enter__(self) -> "Client":
        self._open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()

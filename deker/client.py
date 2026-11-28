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


import importlib
import os
import pkgutil
import warnings

from collections.abc import Callable, Generator, Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from functools import wraps
from multiprocessing import cpu_count
from pathlib import Path
from typing import TYPE_CHECKING, Any, Concatenate, ParamSpec, TypeVar, overload

from deker_tools.cancel import check, write_token
from deker_tools.data import convert_size_to_human
from deker_tools.log import set_logger
from deker_tools.path import is_path_valid
from deker_tools.typings import Sentinel, SentinelType, is_sentinel
from psutil import swap_memory, virtual_memory
from tqdm import tqdm

from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.config import DEFAULT_READ_TIMEOUT, DEFAULT_WRITE_LOCK_TIMEOUT, DekerConfig
from deker.ctx import CTX
from deker.errors import (
    DekerAdapterClosedError,
    DekerClientError,
    DekerCollectionNotExistsError,
    DekerMetaDataError,
    DekerValidationError,
    TokenErrorConverter,
)
from deker.integrity import IntegrityChecker
from deker.log import _ROOT_DEKER_LOGGER_NAME, SelfLoggerMixin, format_string, set_logging_level
from deker.schemas import ArraySchema, VArraySchema
from deker.tools import convert_human_memory_to_bytes
from deker.tools.path import check_storage_directory
from deker.types import (
    DEFAULT_INTEGRITY_CHECKS,
    ArrayLockMeta,
    IntegrityCheck,
    IntegrityReport,
    LocksExtensions,
    LocksTypes,
    StorageSize,
)
from deker.uri import Uri
from deker.validators import validate_collection_name, validate_timeout
from deker.warnings import DekerWarning

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseCollectionAdapter
    from deker.ABC.base_collection import BaseCollectionOptions
    from deker.ABC.base_factory import BaseAdaptersFactory


P = ParamSpec("P")
T = TypeVar("T")


def _open_client_only(method: Callable[Concatenate["Client", P], T]) -> Callable[Concatenate["Client", P], T]:
    """Reject calls of a closed client with ``DekerClientError``.

    A closed client keeps its adapters, and they answer with ``DekerAdapterClosedError``; the client
    checks its own state first and translates the adapters' error into its own one, without a chain of tracebacks.

    :param method: ``Client`` method
    :returns: the decorated method
    """

    @wraps(method)
    def open_client_method(self: "Client", *args: P.args, **kwargs: P.kwargs) -> T:
        """Call the method of an open client.

        :param self: Client instance
        :param args: method arguments
        :param kwargs: method keyword arguments
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
    storage by `uri`. The scheme of the uri picks the installed adapters: ``file://`` with a path to the storage
    on the local machine is served by ``deker-local-adapters``, ``http://`` and ``https://`` by
    ``deker-server-adapters``. Connection to the storage is provided by a client-based context, which remains
    open while the Client is open, and vice-versa: while the context is open - the Client is open too.
    Every method of a closed client raises ``DekerClientError``.

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
    - ``meta_version``
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
    - ``check_integrity``: runs the named integrity checks of the storage and returns the report;

      Collects every finding or stops at the first one, repairs what can be repaired if asked.
      The final report may be saved to file.

    - ``calculate_storage_size``: calculates size of the whole storage or of a defined ``Collection``;
    - ``close``: closes ``Client`` and its context
    - ``empty_trash``: removes everything from the trash of the storage, where the deleted files wait for their removal
    - ``clear_locks``: removes the lock files left by Deker < 2 (deprecated, removed in 3.0.0)
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

        :raises DekerClientError: if no adapters are installed or none of them serves the scheme of the uri
        """
        if not self.__plugins:
            raise DekerClientError("No installed adapters are found: run `pip install deker_local_adapters`")

        if self.__uri.scheme not in self.__plugins:
            raise DekerClientError(f"Invalid uri: {self.__uri.scheme} is not supported; {self.__uri}")

        if self.is_closed:
            self.__is_closed = False

            factory = self.__plugins[self.__uri.scheme]  # the scheme is checked above
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
        """Find the installed adapters: every package named ``deker_*_adapters`` with an ``AdaptersFactory``.

        The first package found for a uri scheme serves it.
        """
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
        executor: ThreadPoolExecutor | SentinelType = Sentinel,
        workers: int | SentinelType = Sentinel,
        read_timeout: int = DEFAULT_READ_TIMEOUT,
        write_lock_timeout: int = DEFAULT_WRITE_LOCK_TIMEOUT,
        write_lock_check_interval: int = 1,
        loglevel: str = "ERROR",
        memory_limit: int | str = 0,
        skip_collection_create_memory_check: bool = False,
        **kwargs: Any,
    ) -> None:
        """Deker client constructor.

        :param uri: uri to Deker storage: ``file:///path`` for a local storage, ``http(s)://host`` for a server
        :param executor: external ThreadPoolExecutor instance; not given - the client makes its own
        :param workers: number of threads for Deker; not given - the number of CPUs plus 4
        :param read_timeout: seconds a read may take, unless the read is given its own ``timeout``
        :param write_lock_timeout: seconds a write waits for the lock of each of its files, unless the write is given
          its own ``timeout``
        :param write_lock_check_interval: An amount of time (in seconds) during which a parallel writing process sleeps
          between checks for locks
        :param loglevel: Level of Deker loggers: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR`` or ``CRITICAL``
        :param memory_limit: Limit of memory allocation per one array/subset in bytes or in human representation of
          kilobytes, megabytes or gigabytes, e.g. ``"100K"``, ``"512M"``, ``"4G"``. Human representations will be
          converted into bytes. If result is ``<= 0`` - total RAM + total swap is used

          .. note:: This parameter is used for early runtime break in case of potential memory overflow
        :param skip_collection_create_memory_check: If we don't want to check size during collection creation
        :param kwargs: extra parameters passed to the adapters as they are, such as ``httpx_conf`` of the server
          adapters
        :raises DekerValidationError: if a timeout is not a positive integer, or the executor is not a
          ``ThreadPoolExecutor``
        :raises DekerClientError: if the client cannot be configured or opened: an invalid uri, a memory limit that
          cannot be parsed, no adapters for the scheme of the uri
        """
        validate_timeout(timeout=read_timeout)
        validate_timeout(timeout=write_lock_timeout)
        if not is_sentinel(executor) and not isinstance(executor, ThreadPoolExecutor):
            raise DekerValidationError(f"executor shall be a ThreadPoolExecutor, got {executor!r}")
        try:
            set_logger(format_string, _ROOT_DEKER_LOGGER_NAME)  # the loggers of the application keep their formats
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
                workers=cpu_count() + 4 if is_sentinel(workers) else workers,
                read_timeout=read_timeout,
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
            self.__executor = None if is_sentinel(executor) else executor  # the factory makes its own then
            self.kwargs = kwargs
            self._open()
        except DekerClientError:
            raise
        except Exception as e:
            raise DekerClientError(str(e)) from e

    def _release(self) -> None:
        """Close the adapter, the factory and the context; a client that failed to construct has nothing to close."""
        try:
            if self.__adapter:
                self.__adapter.close()
            if self.__factory:
                self.__factory.close()
            if self.__ctx:
                self.__ctx.is_closed = True
            self.__is_closed = True
        except AttributeError:  # pragma: no cover  # guard: the constructor raised before the fields were set
            pass

    def __del__(self) -> None:
        """Release the resources of a client dropped without ``close()``.

        Nothing is logged: a finalizer may run at the interpreter shutdown, when the log streams are closed already.
        """
        self._release()

    def _validate_collection_metadata_version(self, collection_metadata: dict) -> None:
        """Check the metadata version of a stored collection and bring the metadata to the current version.

        A collection written by a newer Deker cannot be read; an older one is read: the fields the older version
        did not store (the scale of a generic dimension) get their defaults and the version is set to the current one
        in the dictionary. The file on the disk is not rewritten.

        :param collection_metadata: Dictionary with collection metadata
        :raises DekerMetaDataError: if the metadata has no version or a newer one than the adapters support
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
        if self.__adapter is None:  # pragma: no cover  # guard: set by _open, which every constructor call runs
            raise DekerClientError("Client is closed")
        return self.__adapter

    @property
    @_open_client_only
    def meta_version(self) -> str:
        """Get the metadata version the collections adapter writes."""
        return self._adapter.metadata_version

    @property
    @_open_client_only
    def root_path(self) -> Path:
        """Get the path of the collections directory of the storage."""
        return Path(self._adapter.uri.path) / self.__config.collections_directory

    @property
    def _factory(self) -> "BaseAdaptersFactory":
        """Get the adapters factory of the open client."""
        if self.__factory is None:  # pragma: no cover  # guard: set by _open, which every constructor call runs
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
        """``True`` if the client is closed."""
        return self.__is_closed

    @property
    def is_open(self) -> bool:
        """``True`` if the client is open."""
        return not self.__is_closed

    @_open_client_only
    def calculate_storage_size(self, collection_name: str = "") -> StorageSize:
        """Get the size of the storage or of a certain collection in bytes or converted to human representation.

        The size is the sum of the sizes of the data files of the arrays; the metadata files and the symlinks
        are not counted.

        .. warning:: Size calculation may take a long time. Maybe you'd like to have some coffee while it's working.

        :param collection_name: Name of a ``Collection``. If not passed, the whole storage will be counted.
        :returns: storage size in bytes and in human representation
        :raises DekerCollectionNotExistsError: if the collection does not exist
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
        """Close the client and its context; the client can be reopened with the context manager."""
        self._release()
        self.logger.info("Client is closed")

    # the overloads carry the decorator too: mypy matches them against the decorated implementation
    @overload
    @_open_client_only
    def create_collection(
        self,
        name: str,
        schema: ArraySchema,
        collection_options: "BaseCollectionOptions | None" = None,
        storage_adapter_type: str | SentinelType = Sentinel,
        *,
        timeout: int | SentinelType = Sentinel,
    ) -> Collection[Array]: ...

    @overload
    @_open_client_only
    def create_collection(
        self,
        name: str,
        schema: VArraySchema,
        collection_options: "BaseCollectionOptions | None" = None,
        storage_adapter_type: str | SentinelType = Sentinel,
        *,
        timeout: int | SentinelType = Sentinel,
    ) -> Collection[VArray]: ...

    @_open_client_only
    def create_collection(
        self,
        name: str,
        schema: ArraySchema | VArraySchema,
        collection_options: "BaseCollectionOptions | None" = None,
        storage_adapter_type: str | SentinelType = Sentinel,
        *,
        timeout: int | SentinelType = Sentinel,
    ) -> Collection[Array] | Collection[VArray]:
        """Create a new ``Collection`` in the database.

        The schema decides the kind of arrays: an ``ArraySchema`` gives a ``Collection[Array]``,
        a ``VArraySchema`` gives a ``Collection[VArray]``.

        :param name: Name of new ``Collection``
        :param schema: ``Array`` or ``VArray`` schema
        :param collection_options: Options for compression and chunks (if applicable)
        :param storage_adapter_type: Type of an adapter, which works with files; not given - the default of the
          adapters, ``HDF5StorageAdapter`` for the local storage
        :param timeout: seconds to wait for the lock of the collection; not given - ``write_lock_timeout`` of the
          client
        :returns: the created ``Collection``
        :raises DekerValidationError: if the name is empty, the schema is neither ``ArraySchema`` nor
          ``VArraySchema``, or the timeout is not a positive integer
        :raises DekerLockError: if the lock of the collection is not released within the timeout
        :raises DekerCancelledError: if the creation is cancelled before it started
        """
        validate_timeout(timeout=timeout)
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

        with TokenErrorConverter():
            token = write_token()
            check(token)
            self._adapter.create(collection, timeout=timeout, token=token)
        self.logger.debug("Collection %s created", collection.name)
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
        :raises DekerMetaDataError: if the collection was written by a newer Deker
        """
        try:
            collection_data: dict = self._adapter.read(name)
            self._validate_collection_metadata_version(collection_data)
            collection = self._adapter.create_collection_from_meta(collection_data, self._factory)
            self.logger.debug("Collection %s read from meta", name)
            return collection
        except DekerCollectionNotExistsError:
            self.logger.info("Collection %s not found", name)
            return None

    @_open_client_only
    def validate_collection(self, collection_data: dict) -> Collection:
        """Check a collection metadata dictionary and build the ``Collection`` from it, without storing it.

        Nothing is created in the storage: ``collection_from_dict`` creates the collection from the same dictionary.
        The dictionary has the shape of ``Collection.as_dict`` and is completed in place: the fields it leaves out get
        their defaults (the fill value, the attributes, the labels and the scale of the dimensions, the options) and
        the metadata version is set to the current one.

        :param collection_data: Dictionary with collection metadata
        :returns: the ``Collection``, not stored
        :raises DekerClientError: if the client is closed
        :raises DekerMetaDataError: if the metadata has no version or a newer one than the adapters support, or the
          dtype, the fill value or another field of the schema is invalid
        :raises DekerInvalidSchemaError: if an attribute schema is invalid
        :raises DekerValidationError: if the name, the dimensions, the attributes or the vgrid break the rules of
          the schema, e.g. two attributes or two dimensions have the same name
        :raises DekerStorageError: (of the local adapters) if there is no storage adapter of the given name
        :raises KeyError: if the name, the type, the schema or its dimensions are missing, or the type is unknown
        """
        self._validate_collection_metadata_version(collection_data)  # fills the scale of the generic dimensions
        default_fields: dict = {
            "schema": {
                "fill_value": None,
                "attributes": [],
                "dimensions": {"labels": None},
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
                            if dim["type"] != "time" and "labels" not in dim:
                                collection_data[key][k][n]["labels"] = None

                    elif k not in collection_data[key]:
                        collection_data[key][k] = default_fields[key][k]
        return self._adapter.create_collection_from_meta(collection_data, self._factory)

    @_open_client_only
    def collection_from_dict(self, collection_data: dict, *, timeout: int | SentinelType = Sentinel) -> Collection:
        """Create a new ``Collection`` in the database from collection metadata dictionary.

        The dictionary has the shape of ``Collection.as_dict``; the fields it leaves out get their defaults.

        :param collection_data: Dictionary with collection metadata
        :param timeout: seconds to wait for the lock of the collection; not given - ``write_lock_timeout`` of the
          client
        :returns: the created ``Collection``
        :raises DekerMetaDataError: if the metadata has no version or a newer one than the adapters support
        :raises DekerValidationError: if the timeout is not a positive integer
        :raises DekerLockError: if the lock of the collection is not released within the timeout
        :raises DekerCancelledError: if the creation is cancelled before it started
        """
        validate_timeout(timeout=timeout)
        collection = self.validate_collection(collection_data)
        with TokenErrorConverter():
            token = write_token()
            check(token)
            self._adapter.create(collection, timeout=timeout, token=token)
        self.logger.debug("Collection %s created from dict", collection.name)
        return collection

    @_open_client_only
    def empty_trash(self, *, background: bool = False) -> None:
        """Remove everything from the trash of the storage, where the deleted files wait for their removal.

        The data in the trash is no longer available; the removal frees its disk space. A cancelled operation does not
        stop it.

        :param background: return at once and remove in a daemon thread, which nobody waits for; its error goes to the
          log of Deker
        :raises DekerTrashError: if something in the trash is not removed; not with ``background``
        :raises DekerCriticalError: if the storage has no trash or no collections directory when the call comes; a trash
          lost while it is emptied in the ``background`` is logged only
        """
        self._adapter.empty_trash(background=background)

    # TODO: remove in 3.0.0 with clear_locks: the markers are not made since 2.0.0
    @staticmethod
    def _iter_collection_locks(path: Path, collection_name: str) -> list[ArrayLockMeta]:
        """Return the lock files of the ``Arrays`` of a collection.

        The only lock files of arrays are the markers of VArray writes left by Deker < 2, named
        ``<array id><ext>:<pid>:<token>.varraylock``; since 2.0.0 the locks live in the kernel and leave no files.

        :param path: ``Collection`` path
        :param collection_name: name of a ``Collection``
        :returns: lock files of the collection arrays
        """
        locks: list[ArrayLockMeta] = []
        for file in Path.rglob(path, f"*{LocksExtensions.varray_lock.value}"):
            meta = file.name[: -len(LocksExtensions.varray_lock.value)].split(":")
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

    def _get_locks(
        self,
        collection_name: str | SentinelType = Sentinel,
    ) -> list[ArrayLockMeta]:
        """Return lockfiles of ``Collections`` and ``Arrays``.

        The arrays have none since 2.0.0, only the markers left by Deker < 2 may be found; the lock of a collection is
        its json file, and the ``<collection>.lock`` files of the older versions are not listed.

        :param collection_name: Name of a ``Collection``
          If passed - gets locks only from passed ``Collection``, else checks every collection in the storage
        :returns: lock files of collections and arrays
        :raises DekerCollectionNotExistsError: if the collection does not exist
        :raises DekerCriticalError: if the collections directory of the storage does not exist
        """
        locks: list[ArrayLockMeta] = []
        if not is_sentinel(collection_name):
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(f"Collection {collection_name} does not exist at this storage")
            locks.extend(self._iter_collection_locks(self.root_path / collection_name, collection_name))
        else:
            try:
                entries = list(Path(self.root_path).iterdir())
            except FileNotFoundError:
                check_storage_directory(self.root_path)
                raise
            for entry in entries:
                if entry.is_file():
                    continue  # not a collection: a lock file of an older version, for instance
                locks.extend(self._iter_collection_locks(entry, entry.name))
        return locks

    # TODO: remove in 3.0.0
    @_open_client_only
    def clear_locks(self, collection_name: str | SentinelType = Sentinel) -> None:
        """Remove the markers of VArray writes left by Deker < 2.

        .. deprecated:: 2.0.0
           The locks live in the kernel since 2.0.0 and leave no files: there is nothing to clear.
           Call it once after the upgrade to remove the markers of the older versions; removed in 3.0.0.

        :param collection_name: Name of a ``Collection``.
          If passed - clears locks only in the provided collection, else clears locks in every collection in the storage
        :raises DekerValidationError: if the name of the collection is not a string
        :raises DekerCollectionNotExistsError: if the collection does not exist
        """
        validate_collection_name(name=collection_name)
        warnings.warn(
            "clear_locks is deprecated and will be removed in 3.0.0: Deker leaves no lock files since 2.0.0",
            DeprecationWarning,
            stacklevel=2,
        )
        root_path = self.root_path
        files_count = 0

        if not is_sentinel(collection_name):
            collection = self.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(f"Collection {collection_name} does not exist at this storage")
            root_path = root_path / collection_name

        for file in Path.rglob(root_path, f"*{LocksExtensions.varray_lock.value}"):
            file.unlink()
            files_count += 1

        self.logger.info("%s lock files removed", files_count)

    @_open_client_only
    def check_integrity(
        self,
        collection: str | SentinelType = Sentinel,
        checks: Iterable[IntegrityCheck] = DEFAULT_INTEGRITY_CHECKS,
        stop_on_error: bool = False,
        repair: bool = False,
        workers: int = 1,
        to_file: bool | Path | str = False,
    ) -> IntegrityReport:
        """Check the integrity of the storage and, if asked, repair what can be repaired.

        The checks, from the cheapest to the most expensive:

        - ``collections``: every collection initialises; a collection file of an older metadata version is reported,
          it is never rewritten
        - ``markers``: no markers of VArray writes are left by Deker < 2
        - ``arrays``: every main file initialises an ``Array`` or a ``VArray``; an ``Array`` of a VArray
          collection refers to an existing ``VArray``, has its ``v_position`` inside the ``vgrid`` and carries
          the time attributes of its ``v_position``
        - ``links``: main files and symlinks point at each other, one array per primary attributes
        - ``data``: one cell of every ``Array`` reads back with the dtype of the collection
        - ``counters``: the counter of empty cells of every ``Array`` equals the fill values in its data

        The default set is the first four: metadata and symlinks, no data is read. The report is logged and
        returned; its text goes to a file if ``to_file`` is set. Repairs and several workers are safe only
        when nobody else works with the storage.

        :param collection: name of the only ``Collection`` to check; not given - every collection
        :param checks: the checks to run
        :param stop_on_error: raise ``DekerIntegrityError`` at the first finding instead of collecting them all
        :param repair: repair the repairable findings: collection directories without their files, missing or
          broken symlinks, markers,
          time attributes of Arrays of VArrays, counters of empty cells
        :param workers: threads to check the arrays of a collection with
        :param to_file: write the report to a file: ``True`` for a default-named file in the current directory,
          or the path of the file
        :returns: the report of the findings
        :raises DekerValidationError: if the name of the collection is not a string
        :raises DekerIntegrityError: at the first finding if ``stop_on_error`` is set
        """
        validate_collection_name(name=collection)
        report = IntegrityChecker(self, checks, stop_on_error, repair, workers).check(collection)
        if report.ok:
            self.logger.info("Integrity check found no errors")
            return report
        text = str(report)
        if to_file:
            if isinstance(to_file, Path):
                filename = to_file
            elif isinstance(to_file, str):
                filename = Path(to_file)
            else:
                filename = Path(".") / f"deker_integrity_report_{datetime.now().isoformat(timespec='seconds')}.txt"
            is_path_valid(filename.parent.absolute())
            with filename.open("w") as f:
                f.write(text)
            text += f"\n\nIntegrity check logged errors in {filename.absolute()}"
        self.logger.error(text)
        return report

    def __iter__(self) -> Generator[Collection]:
        """Iterate over all collections in the storage.

        :yields: every ``Collection`` of the storage, built from its stored metadata
        :raises DekerClientError: if the client is closed
        """
        if self.is_closed:
            raise DekerClientError("Client is closed")
        try:
            for meta in self._adapter:
                yield self._adapter.create_collection_from_meta(meta, self._factory)
        except DekerAdapterClosedError:
            raise DekerClientError("Client is closed") from None

    def __enter__(self) -> "Client":
        """Open the client, if it was closed, and return it.

        :returns: the open client
        """
        self._open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Close the client.

        :param exc_type: type of the exception raised in the block, if any
        :param exc_val: the exception
        :param exc_tb: its traceback
        """
        self.close()

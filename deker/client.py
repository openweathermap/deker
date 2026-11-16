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

from collections.abc import Callable, Generator, Iterable
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
from deker.log import SelfLoggerMixin, format_string, set_logging_level
from deker.schemas import ArraySchema, VArraySchema
from deker.tools import convert_human_memory_to_bytes
from deker.types import (
    DEFAULT_INTEGRITY_CHECKS,
    ArrayLockMeta,
    CollectionLockMeta,
    IntegrityCheck,
    IntegrityReport,
    LocksExtensions,
    LocksTypes,
    StorageSize,
)
from deker.uri import Uri
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
    - ``check_integrity``: runs the named integrity checks of the storage and returns the report;

      Collects every finding or stops at the first one, repairs what can be repaired if asked.
      The final report may be saved to file.

    - ``calculate_storage_size``: calculates size of the whole storage or of a defined ``Collection``;
    - ``close``: closes ``Client`` and its context
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
        if self.__adapter is None:  # pragma: no cover  # guard: set by _open, which every constructor call runs
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
    def collection_from_dict(self, collection_data: dict) -> Collection:
        """Create a new ``Collection`` in the database from collection metadata dictionary.

        :param collection_data: Dictionary with collection metadata
        :returns: the created ``Collection``
        """
        collection = self._validate_collection(collection_data)
        self._adapter.create(collection)
        self.logger.debug(f"Collection {collection.name} created from dict")
        return collection

    # TODO: remove in 3.0.0 with clear_locks: the markers are not made since 2.0.0
    @staticmethod
    def _iter_collection_locks(
        path: Path, collection_name: str, lock_type: LocksTypes | None = None
    ) -> list[ArrayLockMeta]:
        """Return the lock files of the ``Arrays`` of a collection.

        The only lock files of arrays are the markers of VArray writes left by Deker < 2, named
        ``<array id><ext>:<pid>:<token>.varraylock``; since 2.0.0 the locks live in the kernel and leave no files.

        :param path: ``Collection`` path
        :param collection_name: name of a ``Collection``
        :param lock_type: ``LocksTypes`` enum attribute, leave empty to get every lockfile
        :returns: lock files of the collection arrays
        """
        if lock_type is not None and lock_type != LocksTypes.varray_lock:
            return []
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
        collection_name: str | None = None,
        lock_type: LocksTypes | None = None,
    ) -> list[CollectionLockMeta | ArrayLockMeta]:
        """Return lockfiles of ``Collections`` and ``Arrays``.

        A collection has a lock file only while it is created or cleared; the arrays have none since 2.0.0,
        only the markers left by Deker < 2 may be found.

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
            for entry in Path(self.root_path).iterdir():
                if entry.is_file():
                    if lock_type in (None, LocksTypes.collection_lock) and entry.name.endswith(
                        LocksExtensions.collection_lock.value
                    ):
                        lock: CollectionLockMeta = {
                            "Lockfile": entry.name,
                            "Collection": entry.name[: -len(LocksExtensions.collection_lock.value)],
                            "Type": LocksTypes.collection_lock.value,
                            "Creation": datetime.fromtimestamp(entry.stat().st_ctime).isoformat(),
                        }
                        locks.append(lock)
                    continue
                locks.extend(self._iter_collection_locks(entry, entry.name, lock_type))
        return locks

    # TODO: remove in 3.0.0
    @_open_client_only
    def clear_locks(self, collection_name: str | None = None) -> None:
        """Remove the markers of VArray writes left by Deker < 2.

        .. deprecated:: 2.0.0
           The locks live in the kernel since 2.0.0 and leave no files: there is nothing to clear.
           Call it once after the upgrade to remove the markers of the older versions; removed in 3.0.0.

        :param collection_name: Name of a ``Collection``.
          If passed - clears locks only in the provided collection, else clears locks in every collection in the storage
        """
        warnings.warn(
            "clear_locks is deprecated and will be removed in 3.0.0: Deker leaves no lock files since 2.0.0",
            DeprecationWarning,
            stacklevel=2,
        )
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
        collection: str | None = None,
        checks: Iterable[IntegrityCheck] = DEFAULT_INTEGRITY_CHECKS,
        stop_on_error: bool = False,
        repair: bool = False,
        workers: int = 1,
        to_file: bool | Path | str = False,
    ) -> IntegrityReport:
        """Check the integrity of the storage and, if asked, repair what can be repaired.

        The checks, from the cheapest to the most expensive:

        - ``collections``: every collection initialises and has its lock file, every lock file has its collection;
          a collection file of an older metadata version is reported, it is never rewritten
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

        :param collection: name of the only ``Collection`` to check, every collection if ``None``
        :param checks: the checks to run
        :param stop_on_error: raise ``DekerIntegrityError`` at the first finding instead of collecting them all
        :param repair: repair the repairable findings: missing or broken symlinks, lock files, markers,
          time attributes of Arrays of VArrays, counters of empty cells
        :param workers: threads to check the arrays of a collection with
        :param to_file: write the report to a file: ``True`` for a default-named file in the current directory,
          or the path of the file
        :returns: the report of the findings
        :raises DekerIntegrityError: at the first finding if ``stop_on_error`` is set
        """
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

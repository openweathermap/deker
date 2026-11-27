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


from collections.abc import Sequence
from pathlib import Path
from threading import Lock
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
    Union,
)

from cantok import AbstractToken
from deker_tools.cancel import resolve_timeout
from deker_tools.typings import SentinelType

from deker.ABC.base_locks import BaseLock, wait_for_unlock
from deker.errors import DekerInstanceNotExistsError, DekerLockError
from deker.flock import Flock
from deker.tools.path import get_main_path

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseVArrayAdapter
    from deker.ABC.base_array import BaseArray
    from deker.arrays import Array, VArray
    from deker.collection import Collection

ArrayFromArgs = Union[Path, Union["Array", "VArray"]]
T = TypeVar("T", bound=Union[Path, "BaseArray"])


class LockWithArrayMixin(Generic[T]):
    """Mixin of the locks of arrays: finds the array among the arguments of the decorated adapter method."""

    args: Sequence[Any]
    kwargs: dict[str, Any]
    instance: Any

    @property
    def array_id(self) -> str:
        """Get the id of the array: from the ``Array`` object, or from the name of the path to its file."""
        # Get instance of the array
        if isinstance(self.array, Path):
            path = self.array
            id_ = path.name.split(".")[0]
        else:
            id_ = self.array.id
        return id_

    @property
    def array(self) -> T:
        """Get the array of the decorated call: the ``array`` keyword argument or the first positional one."""
        array = self.kwargs.get("array") or self.args[1]  # zero arg is 'self'
        return array

    @property
    def dir_path(self) -> Path:
        """Get the directory of the main file of the array."""
        return get_main_path(self.array_id, self.instance.collection_path / self.instance.data_dir)


class ReadArrayLock(LockWithArrayMixin[ArrayFromArgs], BaseLock):
    """Read lock for Array: a shared flock on the main file, held from acquire to release.

    The lock lives in the kernel: a reader that dies holds nothing, and no file is left behind.
    Several readers share the file; a writer, which takes it exclusively, waits for them to leave
    and keeps new readers out while it writes.
    """

    ALLOWED_TYPES = ["LocalArrayAdapter"]

    def get_path(self) -> Path:
        """Get path to the main file of the array, which is the file the shared lock is taken on."""
        path = self.dir_path / (self.array_id + self.instance.file_ext)
        self.logger.debug("Got path for array.id %s lock file: %s", self.array_id, path)
        return path

    def acquire(self, path: Path | None) -> None:
        """Take the shared flock on the main file.

        :param path: path to the main file
        :raises DekerLockError: if the file is held exclusively by a writer
        :raises DekerInstanceNotExistsError: if the file does not exist
        """
        if path is None:  # pragma: no cover
            raise DekerLockError(f"{self.__class__.__name__} has no path to lock")
        lock = Flock(path, shared=True)
        try:
            lock.acquire()
        except DekerLockError:
            raise DekerLockError(f"Array {self.array_id} is locked for update operation, cannot be read.")
        except FileNotFoundError:
            raise DekerInstanceNotExistsError(f"{path} doesn't exist") from None
        self.lock = lock
        self.logger.debug("Acquired shared lock for %s", path)


class WriteArrayLock(LockWithArrayMixin["Array"], BaseLock):
    """Write lock for Array: an exclusive flock on the main file, held from acquire to release.

    The writer waits for the readers of the file to leave and keeps everybody out while it writes.
    A VArray writes its Arrays through this lock, one Array at a time, so two VArray writers of different
    Arrays never wait for each other and a writer that dies holds nothing and leaves no file behind.
    """

    ALLOWED_TYPES = ["LocalArrayAdapter"]

    def get_path(self) -> Path:
        """Get path to the main file of the array, which is the file the exclusive lock is taken on."""
        path = self.dir_path / (self.array_id + self.instance.file_ext)
        self.logger.debug("Got path for array.id %s lock file: %s", self.array.id, path)
        return path

    def check_existing_lock(self, func_args: Sequence, func_kwargs: dict) -> None:  # noqa: ARG002
        """Take the exclusive flock, waiting for the readers and the writers to leave.

        :param func_args: arguments of method call
        :param func_kwargs: keyword arguments of method call: ``timeout`` and ``token`` of the operation
        :raises DekerLockError: if the file is not released within the timeout of the operation
        :raises DekerInstanceNotExistsError: if the file does not exist; nothing is waited for
        """
        self._wait_and_acquire(func_kwargs, f"{self.array} is locked by its readers or a writer")


class ArrayCreationLock:
    """Lock held while a missing ``Array`` of a ``VArray`` is created.

    Two writers of a fresh VArray would otherwise both find the Array missing and create two files for one
    position. The lock is an exclusive flock on the directory of the main file of the VArray (the file itself
    is replaced by every update of the custom attributes, the directory stays), so it works across processes
    and containers on one storage and dies with its holder; the threads of one process queue on a thread lock
    first, so only one of them holds the flock at a time. Creation is short, so the flock is polled often.

    :param adapter: VArray adapter; the lock is a no-op for the server adapters, the server keeps its own order
    :param varray: the VArray whose Array is created
    :param collection: collection of the VArray
    :param timeout: seconds to wait for the lock; ``Sentinel`` - ``write_lock_timeout`` of the client
    :param token: token of the write operation
    """

    POLL_INTERVAL = 0.05
    _thread_locks: dict[Path, Lock] = {}
    _thread_locks_guard = Lock()

    def __init__(
        self,
        adapter: "BaseVArrayAdapter",
        varray: "VArray",
        collection: "Collection",
        *,
        timeout: int | SentinelType,
        token: AbstractToken,
    ) -> None:
        # Circular import otherwise
        from deker_local_adapters.varray_adapter import LocalVArrayAdapter

        self.path: Path | None = None
        if isinstance(adapter, LocalVArrayAdapter):
            self.path = get_main_path(varray.id, adapter.collection_path / adapter.data_dir)
        self.timeout = resolve_timeout(timeout, adapter.ctx.config.write_lock_timeout)
        self.token = token
        self.flock: Flock | None = None
        self.thread_lock: Lock | None = None

    def _try_acquire(self, path: Path) -> bool:
        """Try to take the flock once.

        :param path: directory of the main file of the VArray
        :returns: ``True`` if the flock is held now
        :raises DekerInstanceNotExistsError: if the directory does not exist
        """
        flock = Flock(path)
        try:
            flock.acquire()
        except DekerLockError:
            return False
        except FileNotFoundError:
            raise DekerInstanceNotExistsError(f"{path} doesn't exist") from None
        self.flock = flock
        return True

    def __enter__(self) -> "ArrayCreationLock":
        """Take the thread lock, then the flock.

        :returns: the held lock
        :raises DekerLockError: if the flock is not taken within the timeout
        :raises TokenCancelledError: if the write is cancelled while it waits
        :raises DekerInstanceNotExistsError: if the directory of the VArray does not exist
        """
        if self.path is None:  # the server keeps its own order of creation
            return self
        with self._thread_locks_guard:
            self.thread_lock = self._thread_locks.setdefault(self.path, Lock())
        self.thread_lock.acquire()
        try:
            acquired = wait_for_unlock(
                self._try_acquire, (self.path,), timeout=self.timeout, interval=self.POLL_INTERVAL, token=self.token
            )
        except BaseException:
            self.thread_lock.release()
            self.thread_lock = None
            raise
        if not acquired:
            self.thread_lock.release()
            self.thread_lock = None
            raise DekerLockError(f"VArray {self.path} is locked for the creation of its Arrays")
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """Release the flock and the thread lock.

        :param exc_info: the exception raised in the block, if any
        """
        if self.flock is not None:
            self.flock.release()
            self.flock = None
        if self.thread_lock is not None:
            self.thread_lock.release()
            self.thread_lock = None


class UpdateMetaAttributeLock(WriteArrayLock):
    """Lock for updating the custom attributes of a ``VArray``, waited for like a write lock.

    The lock is an exclusive flock on the directory of the main file of the VArray, not on the json file: the
    update replaces the file, and a lock on it would stay on the old file, letting the next writer in while the
    holder is still at work. The same directory is held while an Array of the VArray is created.
    """

    ALLOWED_TYPES = ["LocalVArrayAdapter"]

    def get_path(self) -> Path:
        """Get the directory of the main file of the VArray, which is the directory the exclusive lock is taken on."""
        path = self.dir_path
        self.logger.debug("Got path for array.id %s lock directory: %s", self.array_id, path)
        return path


class CollectionLock(BaseLock):
    """Lock for collection creation and clearing: an flock on the ``<collection>.lock`` file next to the collection.

    The file stays after the creation as the marker of the collection, which ``check_integrity`` relies on;
    it is removed only when an operation fails and leaves no collection directory behind.
    """

    ALLOWED_TYPES = ["LocalCollectionAdapter"]

    def check_existing_lock(self, func_args: Sequence, func_kwargs: dict) -> None:  # noqa: ARG002
        """Take the flock of the collection, waiting for its holder to leave.

        :param func_args: arguments of method call
        :param func_kwargs: keyword arguments of method call: ``timeout`` and ``token`` of the operation
        :raises DekerLockError: if the collection is not released within the timeout of the operation
        """
        self._wait_and_acquire(func_kwargs, f"Collection {self.args[1].name} is locked by another creation or clearing")

    def get_path(self) -> Path:  # noqa: ARG002
        """Get path to the ``<collection>.lock`` file next to the directory of the collection."""
        collection = self.args[1]
        path = self.instance.collections_resource / (collection.name + ".lock")
        self.logger.debug("Got path for collection %s lock file: %s", collection.name, path)
        return path

    def release(self, e: Exception | None = None) -> None:
        """Release the lock; after a failure the lock file is removed when no collection directory is left.

        A creation may fail between the lock and the directory of the collection (cancelled, over the memory
        limit, an error of the file system): its lock file would mark a collection that does not exist.

        :param e: Exception that might have been raised
        """
        if self.lock:
            self.lock.release()
            if e is not None and not (self.instance.collections_resource / self.args[1].name).is_dir():
                self.lock.file.unlink(missing_ok=True)
            self.lock = None

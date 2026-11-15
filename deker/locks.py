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


import time

from collections.abc import Callable, Sequence
from pathlib import Path
from threading import Lock
from time import sleep
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
    Union,
)

from deker.ABC.base_locks import BaseLock
from deker.errors import DekerLockError, DekerMemoryError
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
    """Base class with getter of array."""

    args: Sequence[Any]
    kwargs: dict[str, Any]
    instance: Any

    @property
    def array_id(self) -> str:
        """Get if from Array, or Path to the array."""
        # Get instance of the array
        if isinstance(self.array, Path):
            path = self.array
            id_ = path.name.split(".")[0]
        else:
            id_ = self.array.id
        return id_

    @property
    def array(self) -> T:
        """Parse array from args and save ref to it."""
        array = self.kwargs.get("array") or self.args[1]  # zero arg is 'self'
        return array

    @property
    def dir_path(self) -> Path:
        """Path to directory with main file."""
        return get_main_path(self.array_id, self.instance.collection_path / self.instance.data_dir)


def wait_for_unlock(check_func: Callable, check_func_args: tuple, timeout: float, interval: float) -> bool:
    """Waiting while there is no locks.

    :param check_func: Func that check if lock has been releases
    :param check_func_args: Args for func
    :param timeout: For how long we should wait lock release
    :param interval: How often we check locks
    :return:
    """
    start_time = time.monotonic()
    while (time.monotonic() - start_time) <= timeout:
        if check_func(*check_func_args):
            return True
        sleep(interval)
    return False


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
        self.logger.debug(f"Got path for array.id {self.array_id} lock file: {path}")
        return path

    def acquire(self, path: Path | None) -> None:
        """Take the shared flock on the main file.

        :param path: path to the main file
        :raises DekerLockError: if the file is held exclusively by a writer
        """
        if path is None:  # pragma: no cover
            raise DekerLockError(f"{self.__class__.__name__} has no path to lock")
        lock = Flock(path, shared=True)
        try:
            lock.acquire()
        except DekerLockError:
            raise DekerLockError(f"Array {self.array_id} is locked for update operation, cannot be read.")
        self.lock = lock
        self.logger.debug(f"Acquired shared lock for {path}")


class WriteArrayLock(LockWithArrayMixin["Array"], BaseLock):
    """Write lock for Array: an exclusive flock on the main file, held from acquire to release.

    The writer waits for the readers of the file to leave and keeps everybody out while it writes.
    A VArray writes its tiles through this lock, one tile at a time, so two VArray writers of different
    tiles never wait for each other and a writer that dies holds nothing and leaves no file behind.
    """

    ALLOWED_TYPES = ["LocalArrayAdapter"]

    def get_path(self) -> Path:
        """Get path to the file for locking."""
        path = self.dir_path / (self.array_id + self.instance.file_ext)
        self.logger.debug(f"Got path for array.id {self.array.id} lock file: {path}")
        return path

    def _try_acquire(self, path: Path) -> bool:
        """Try to take the exclusive flock once.

        :param path: path to the main file
        :returns: ``True`` if the lock is held now, ``False`` if readers or a writer hold the file
        """
        try:
            self.acquire(path)
        except DekerLockError:
            return False
        return True

    def check_existing_lock(self, func_args: Sequence, func_kwargs: dict) -> None:  # noqa: ARG002
        """Take the exclusive flock, waiting for the readers to leave.

        :param func_args: arguments of method call
        :param func_kwargs: keyword arguments of method call
        """
        path = self.get_path()
        if self._try_acquire(path):
            return
        if wait_for_unlock(
            self._try_acquire,
            (path,),
            self.instance.ctx.config.write_lock_timeout,
            self.instance.ctx.config.write_lock_check_interval,
        ):
            return

        # If we hit the timeout, release whatever is held and raise DekerLockError
        self.release()
        raise DekerLockError(f"Array {self.array} is locked with read locks")


class TileCreationLock:
    """Lock held while a missing tile (inner ``Array``) of a VArray is created.

    Two writers of a fresh VArray would otherwise both find the tile missing and create two files for one
    position. The lock is an exclusive flock on the main file of the VArray, so it works across processes and
    containers on one storage and dies with its holder; the threads of one process queue on a thread lock
    first, so only one of them holds the flock at a time. Creation is short, so the flock is polled often.

    :param adapter: VArray adapter; the lock is a no-op for the server adapters, the server keeps its own order
    :param varray: the VArray whose tile is created
    :param collection: collection of the VArray
    """

    POLL_INTERVAL = 0.05
    _thread_locks: dict[Path, Lock] = {}
    _thread_locks_guard = Lock()

    def __init__(self, adapter: "BaseVArrayAdapter", varray: "VArray", collection: "Collection") -> None:
        # Circular import otherwise
        from deker_local_adapters.varray_adapter import LocalVArrayAdapter

        self.path: Path | None = None
        if isinstance(adapter, LocalVArrayAdapter):
            self.path = get_main_path(varray.id, adapter.collection_path / adapter.data_dir) / (
                varray.id + adapter.file_ext
            )
        self.timeout = adapter.ctx.config.write_lock_timeout
        self.flock: Flock | None = None
        self.thread_lock: Lock | None = None

    def _try_acquire(self, path: Path) -> bool:
        """Try to take the flock once.

        :param path: main file of the VArray
        :returns: ``True`` if the flock is held now
        """
        flock = Flock(path)
        try:
            flock.acquire()
        except DekerLockError:
            return False
        self.flock = flock
        return True

    def __enter__(self) -> "TileCreationLock":
        if self.path is None:  # the server keeps its own order of creation
            return self
        with self._thread_locks_guard:
            self.thread_lock = self._thread_locks.setdefault(self.path, Lock())
        self.thread_lock.acquire()
        if not wait_for_unlock(self._try_acquire, (self.path,), self.timeout, self.POLL_INTERVAL):
            self.thread_lock.release()
            self.thread_lock = None
            raise DekerLockError(f"VArray {self.path} is locked for the creation of its tiles")
        return self

    def __exit__(self, *exc_info: Any) -> None:
        if self.flock is not None:
            self.flock.release()
            self.flock = None
        if self.thread_lock is not None:
            self.thread_lock.release()
            self.thread_lock = None


class UpdateMetaAttributeLock(LockWithArrayMixin[Union["Array", "VArray"]], BaseLock):
    """Lock for updating meta."""

    ALLOWED_TYPES = ["LocalArrayAdapter", "LocalVArrayAdapter"]

    def get_path(self) -> Path:
        """Return path to the file that should be locked."""
        # Get directory where file locates
        dir_path = get_main_path(self.array.id, self.instance.collection_path / self.instance.data_dir)
        path = dir_path / f"{self.array.id}{self.instance.file_ext}"
        self.logger.debug(f"Got path for array.id {self.array.id} lock file: {path}")
        return path


class CollectionLock(BaseLock):
    """Lock for collection creation and clearing: an flock on the ``<collection>.lock`` file next to the collection.

    The file stays after the creation as the marker of the collection, which ``check_integrity`` relies on;
    it is removed only when the creation fails and leaves no collection behind.
    """

    ALLOWED_TYPES = ["LocalCollectionAdapter"]

    def get_path(self) -> Path:  # noqa: ARG002
        """Return path to collection lock file."""
        collection = self.args[1]
        path = self.instance.collections_resource / (collection.name + ".lock")
        self.logger.debug(f"Got path for collection {collection.name} lock file: {path}")
        return path

    def release(self, e: Exception | None = None) -> None:
        """Release Collection Lock.

        :param e: Exception that might have been raised
        """
        if self.lock:
            self.lock.release()
            if isinstance(e, DekerMemoryError):
                self.lock.file.unlink()
            self.lock = None

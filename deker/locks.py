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


import os
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
from uuid import uuid4

from deker.ABC.base_locks import BaseLock
from deker.errors import DekerLockError, DekerMemoryError
from deker.flock import Flock
from deker.tools.path import get_main_path
from deker.types.private.enums import LocksExtensions

if TYPE_CHECKING:
    from deker_local_adapters import LocalArrayAdapter

    from deker.ABC.base_adapters import BaseVArrayAdapter
    from deker.ABC.base_array import BaseArray
    from deker.arrays import Array, VArray
    from deker.collection import Collection

META_DIVIDER = ":"
ArrayFromArgs = Union[Path, Union["Array", "VArray"]]
T = TypeVar("T", bound=Union[Path, "BaseArray"])


_process_token: tuple[int, str] | None = None  # (pid, token) of the current process


def process_token() -> str:
    """Get the token of the current process: a random id made once per process.

    Containers on one storage have their own PID namespaces, so a PID does not identify a process there;
    the token does. A forked child gets its own token, as its PID differs from the one the token was made for.

    :returns: 32 hexadecimal characters
    """
    global _process_token  # noqa: PLW0603
    pid = os.getpid()
    if _process_token is None or _process_token[0] != pid:
        _process_token = (pid, uuid4().hex)
    return _process_token[1]


def _varray_lock_marker(filename: Path) -> Path:
    """Get the path of the marker that tells the array is locked by a VArray of the current process.

    :param filename: main file of the array
    :returns: path of the marker file
    """
    return Path(
        f"{filename}{META_DIVIDER}{os.getpid()}{META_DIVIDER}{process_token()}{LocksExtensions.varray_lock.value}"
    )


def _is_own_varray_lock_marker(name: str) -> bool:
    """Check that the name is a VArray lock marker of the current process.

    The PID and the token are compared together with their dividers: the token tells the processes of
    different containers apart, which may share a PID.

    :param name: name of a file in the directory of the array
    :returns: ``True`` if the marker belongs to the current process
    """
    return name.endswith(
        f"{META_DIVIDER}{os.getpid()}{META_DIVIDER}{process_token()}{LocksExtensions.varray_lock.value}"
    )


def _check_write_locks(dir_path: Path, id_: str) -> bool:
    """Checks write locks from VArrays that differs from current.

    :param dir_path: Dir where locks are stored (the one with hdf file)
    :param id_: Id of array
    :returns: ``True`` if the array is locked by a VArray of the current process, ``False`` if it is
        not locked
    """
    for file in dir_path.iterdir():
        # Skip lock from current process.
        # Used when you have to read meta inside .update operation of varray
        if _is_own_varray_lock_marker(file.name):
            return True
        # If we've found another varray lock, that not from current process.
        if file.name.endswith(LocksExtensions.varray_lock.value):
            raise DekerLockError(f"Array {id_} is locked with {file.name}")
    return False


class LockWithArrayMixin(Generic[T]):
    """Base class with getter of array."""

    args: Sequence[Any]
    kwargs: dict[str, Any]
    instance: Any
    is_locked_with_varray: bool = False

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

    def check_existing_lock(self, func_args: Sequence, func_kwargs: dict) -> None:  # noqa: ARG002
        """Find out whether the array is held by a VArray write of the current process.

        Such a read (of the meta inside the update of a VArray) does not lock the file: the VArray holds it.

        :param func_args: arguments for called function.
        :param func_kwargs: keyword arguments for called function.
        """
        self.is_locked_with_varray = _check_write_locks(self.dir_path, self.array_id)

    def acquire(self, path: Path | None) -> None:
        """Take the shared flock on the main file.

        :param path: path to the main file
        :raises DekerLockError: if the file is held exclusively by a writer
        """
        if self.is_locked_with_varray:
            return
        if path is None:  # pragma: no cover
            raise DekerLockError(f"{self.__class__.__name__} has no path to lock")
        lock = Flock(path, shared=True)
        try:
            lock.acquire()
        except DekerLockError:
            raise DekerLockError(f"Array {self.array_id} is locked for update operation, cannot be read.")
        self.lock = lock
        self.logger.debug(f"Acquired shared lock for {path}")

    def release(self, e: Exception | None = None) -> None:
        """Release the shared flock.

        :param e: Exception that may have been raised.
        """
        if self.is_locked_with_varray:
            return
        super().release(e)


class WriteArrayLock(LockWithArrayMixin["Array"], BaseLock):
    """Write lock for arrays."""

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
        # If array belongs to varray, we should check if varray is also locked
        self.is_locked_with_varray = _check_write_locks(self.dir_path, self.array_id)

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

    def release(self, e: Exception | None = None) -> None:
        """Release Flock.

        If array is locked from Varary from current Process, do nothing
        :param e: exception that might have been raised
        """
        if self.is_locked_with_varray:
            return

        super().release(e)

    def acquire(self, path: Path | None) -> None:
        """Make lock using flock.

        If array is locked from Varary from current Process, do nothing
        :param path: Path to file that should be flocked
        """
        if self.is_locked_with_varray:
            return
        super().acquire(path)


class WriteVarrayLock(BaseLock):
    """Write lock for VArrays.

    VArray shall not be locked itself when writing data.
    Only inner Arrays shall be locked.
    If updating subsets do not intersect - it's OK, otherwise the first,
    which managed to obtain all Array locks, will survive.
    """

    ALLOWED_TYPES = ["VSubset"]

    # Locks that have been acquired by varray
    locks: list[tuple[Flock, Path]] = []
    skip_lock: bool = False  # shows that we must skip this lock (e.g server adapters for subset)

    def check_type(self) -> None:
        """Check if the instance type (class) is allowed for locking."""
        # Circular import otherwise
        from deker_local_adapters.varray_adapter import LocalVArrayAdapter

        super().check_type()
        adapter = self.instance._VSubset__adapter
        is_running_on_local = isinstance(adapter, LocalVArrayAdapter)
        if not is_running_on_local:
            self.skip_lock = True

    def get_path(self) -> Path | None:  # noqa: ARG002
        """Path of json Varray file."""
        array = self.instance._VSubset__array
        adapter = self.instance._VSubset__adapter
        path = get_main_path(array.id, self.instance._VSubset__collection.path / adapter.data_dir) / (
            array.id + adapter.file_ext
        )
        self.logger.debug(f"Got path for array.id {array.id} lock file: {path}")
        return path

    def check_locks_for_array_and_set_flock(self, filename: Path) -> Flock:
        """Take the exclusive flock on a tile and mark it as held by this process.

        :param filename: Path to file that should be flocked
        :returns: ``Flock`` set on the file
        """
        # The readers hold the file with shared flocks: the exclusive one is refused while they read
        lock = Flock(filename)
        lock.acquire()

        # Add flag that this array is locked by varray
        _varray_lock_marker(filename).touch()
        return lock

    def _tiles_paths(self, adapter: "LocalArrayAdapter", varray: "VArray") -> list[Path]:
        """Get the main files of the tiles (inner Arrays) covered by the subset.

        :param adapter: Array adapter instance
        :param varray: VArray
        :returns: paths of the existing tiles' main files, resolved from their symlinks
        """
        collection = self.instance._VSubset__collection
        paths = []
        for array_position in self.instance._VSubset__arrays:
            filename = adapter._get_symlink_filename(
                varray.id,
                array_position.vposition,
                collection.array_schema.primary_attributes,
            )
            if filename:
                # Path to the main file (not symlink)
                paths.append(filename.resolve())
        return paths

    def _release_tiles(self) -> None:
        """Release the tiles acquired so far: flocks and own markers."""
        for lock, filename in self.locks:
            lock.release()
            _varray_lock_marker(filename).unlink(missing_ok=True)
        self.locks = []

    def _acquire_tiles(self, paths: Sequence[Path]) -> bool:
        """Acquire all the tiles or none of them.

        The first busy tile releases everything acquired before it, so a waiting writer
        holds nothing and does not block the writers of other tiles.

        :param paths: main files of the tiles
        :returns: ``True`` if every tile is acquired, ``False`` if some tile is busy and nothing is held
        """
        for filename in paths:
            try:
                lock = self.check_locks_for_array_and_set_flock(filename)
            except DekerLockError:
                self._release_tiles()
                self.logger.debug(f"Tile {filename} is busy, released the tiles acquired so far")
                return False
            self.locks.append((lock, filename))
        return True

    def check_existing_lock(self, func_args: Sequence, func_kwargs: dict) -> None:  # noqa: ARG002
        """Acquire every tile of the subset, waiting for busy tiles without holding the others.

        :param func_args: arguments of the function that has been called.
        :param func_kwargs: keyword arguments of the function that has been called.
        """
        adapter = self.instance._VSubset__array_adapter
        varray = self.instance._VSubset__array

        # Clear links to locks
        self.locks = []
        paths = self._tiles_paths(adapter, varray)

        if self._acquire_tiles(paths):
            return

        # Some tiles are busy: nothing is held now, retry the whole acquisition until the timeout
        if wait_for_unlock(
            check_func=self._acquire_tiles,
            check_func_args=(paths,),
            timeout=adapter.ctx.config.write_lock_timeout,
            interval=adapter.ctx.config.write_lock_check_interval,
        ):
            return
        # Nothing is held after a failed acquisition; release keeps the contract of the base lock
        self.release()
        raise DekerLockError(f"VArray {varray} is locked")

    def release(self, e: Exception | None = None) -> None:  # noqa: ARG002
        """Release all locks.

        :param e: Exception that may have been raised.
        """
        self._release_tiles()
        super().release()

    def acquire(self, path: Path | None) -> None:
        """VArray shall not lock itself.

        :param path: path to the file to be locked
        """
        pass

    @staticmethod
    def _inner_method_logic(lock: "BaseLock", args: Sequence, kwargs: dict, func: Callable) -> Any:
        """Logic of acquiring lock and getting result.

        When writing in VArray

        :param lock: The lock that will be acquired
        :param func: decorated function
        :param args: arguments of decorated function
        :param kwargs: keyword arguments of decorated function
        :returns: result of the decorated method
        """
        if not isinstance(lock, WriteVarrayLock):  # pragma: no cover
            raise TypeError(f"{type(lock).__name__} cannot be processed as a VArray write lock")
        # If we want to skip logic of lock (e.g. when we use server adapters)
        if lock.skip_lock:
            return lock.get_result(func, args, kwargs)
        return BaseLock._inner_method_logic(lock, args, kwargs, func)


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
            self.path = get_main_path(varray.id, collection.path / adapter.data_dir) / (varray.id + adapter.file_ext)
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
    """Lock for collection creation."""

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

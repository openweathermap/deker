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

from __future__ import annotations

import fcntl
import os
import time

from pathlib import Path
from threading import get_native_id
from time import sleep
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Sequence,
    Tuple,
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

    from deker.arrays import Array, VArray

META_DIVIDER = ":"
ArrayFromArgs = Union[Path, Union["Array", "VArray"]]
T = TypeVar("T")


def _get_lock_filename(id_: str, lock_ext: LocksExtensions) -> str:
    """Get filename for lockfile.

    :param id_: ID of array
    :param lock_ext: Extension of lock
    :return:
    """
    name = META_DIVIDER.join([f"{id_}", f"{uuid4()}", f"{os.getpid()}", f"{get_native_id()}"])
    return str(name + lock_ext.value)


def _check_write_locks(dir_path: Path, id_: str) -> bool:
    """Checks write locks from VArrays that differs from current.

    :param dir_path: Dir where locks are stored (the one with hdf file)
    :param id_: Id of array
    """
    for file in dir_path.iterdir():
        # Skip lock from current process.
        # Used when you have to read meta inside .update operation of varray
        if file.name.endswith(f"{os.getpid()}{LocksExtensions.varray_lock.value}"):
            return True
        # If we've found another varray lock, that not from current process.
        if file.name.endswith(LocksExtensions.varray_lock.value):  # type: ignore
            raise DekerLockError(f"Array {id_} is locked with {file.name}")
    return False


class LockWithArrayMixin(Generic[T]):
    """Base class with getter of array."""

    args: Optional[List[Any]]
    kwargs: Optional[Dict]
    instance: Optional[Any]
    is_locked_with_varray: bool = False

    @property
    def array_id(self) -> str:
        """Get if from Array, or Path to the array."""
        # Get instance of the array
        if isinstance(self.array, Path):
            path = self.array
            id_ = path.name.split(".")[0]
        else:
            id_ = self.array.id  # type: ignore[attr-defined]
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


def wait_for_unlock(
    check_func: Callable, check_func_args: tuple, timeout: int, interval: float
) -> bool:
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
    """Read lock for Array."""

    ALLOWED_TYPES = ["LocalArrayAdapter"]

    def get_path(self) -> Path:
        """Get path to read-lock file.

        It's only the case for arrays, varrays don't have read locks.
        """
        # Get file directory
        filename = _get_lock_filename(self.array_id, LocksExtensions.array_read_lock)

        # Create read lock file path
        path = self.dir_path / f"{filename}"

        self.logger.debug(f"Got path for array.id {self.array_id} lock file: {path}")
        return path

    def check_existing_lock(self, func_args: Sequence, func_kwargs: Dict) -> None:
        """Read operations are not performed in case there are any WRITE locks.

        Array file would be Flocked for writing / updating, so we just need to try flock it once more
        :param func_args: arguments for called function.
        :param func_kwargs: keyword arguments for called function.
        """
        # Check write locks
        if _check_write_locks(self.dir_path, self.array_id):
            self.is_locked_with_varray = True
            # File was locked with VArray from current process.
            return
        # No write locks found
        path = self.dir_path / (self.array_id + self.instance.file_ext)
        try:
            with open(path, "r") as f:
                fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
                self.logger.debug(f"Set shared flock for {path}")

        except BlockingIOError:
            raise DekerLockError(
                f"Array {self.array_id} is locked for update operation, cannot be read."
            )

    def acquire(self, path: Union[str, Path]) -> Any:
        """Read files will not be flocked - only created.

        :param path: Path to file which should be locked
        """
        # Create lock file
        self.lock = path
        open(path, "a").close()
        self.logger.debug(f"Acquired read lock for {self.lock}")

    def release(self, e: Optional[Exception] = None) -> None:  # noqa[ARG002]
        """Release lock by deleting file.

        :param e: Exception that may have been raised.
        """
        if self.lock and self.lock.exists():
            self.lock.unlink()
            self.logger.debug(f"Releasing read lock for {self.lock}")
            self.lock = None


class WriteArrayLock(LockWithArrayMixin["Array"], BaseLock):
    """Write lock for arrays."""

    ALLOWED_TYPES = ["LocalArrayAdapter"]

    def get_path(self) -> Path:
        """Get path to the file for locking."""
        path = self.dir_path / (self.array_id + self.instance.file_ext)
        self.logger.debug(f"Got path for array.id {self.array.id} lock file: {path}")
        return path

    def check_existing_lock(self, func_args: Sequence, func_kwargs: Dict) -> None:
        """Check read locks before write execution.

        :param func_args: arguments of method call
        :param func_kwargs: keyword arguments of method call
        """
        # If array belongs to varray, we should check if varray is also locked
        self.is_locked_with_varray = _check_write_locks(self.dir_path, self.array_id)

        # Increment write lock, to prevent more read locks coming.
        self.acquire(self.get_path())

        # Pattern that has to find any read locks
        glob_pattern = f"{self.array.id}:*{LocksExtensions.array_read_lock.value}"

        # Wait till there are no more read locks
        if wait_for_unlock(
            lambda path, pattern: not list(path.rglob(pattern)),
            (self.dir_path, glob_pattern),
            self.instance.ctx.config.write_lock_timeout,
            self.instance.ctx.config.write_lock_check_interval,
        ):
            # If all locks are released, go further
            return

        # If we hit the timeout, release write lock and raise DekerLockError
        self.release()
        raise DekerLockError(f"Array {self.array} is locked with read locks")

    def release(self, e: Optional[Exception] = None) -> None:
        """Release Flock.

        If array is locked from Varary from current Process, do nothing
        :param e: exception that might have been raised
        """
        if self.is_locked_with_varray:
            return

        super().release(e)

    def acquire(self, path: Optional[Path]) -> None:
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
    locks: List[Tuple[Flock, Path]] = []
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

    def get_path(self) -> Optional[Path]:  # noqa[ARG002]
        """Path of json Varray file."""
        array = self.instance._VSubset__array
        adapter = self.instance._VSubset__adapter
        path = get_main_path(
            array.id, self.instance._VSubset__collection.path / adapter.data_dir
        ) / (array.id + adapter.file_ext)
        self.logger.debug(f"Got path for array.id {array.id} lock file: {path}")
        return path

    def check_locks_for_array_and_set_flock(self, filename: Path) -> Flock:
        """Check if there is no read lock.

        :param filename: Path to file that should be flocked
        """
        # Check read lock first
        array_id = filename.name.split(".")[0]
        glob_pattern = f"{array_id}:*"
        for _ in filename.parent.rglob(glob_pattern):
            raise DekerLockError(f"Array {array_id} is locked")

        # Check write lock and set it
        lock = Flock(filename)
        lock.acquire()

        # Add flag that this array is locked by varray
        open(f"{filename}:{os.getpid()}{LocksExtensions.varray_lock.value}", "w").close()
        return lock

    def check_arrays_locks(
        self,
        adapter: LocalArrayAdapter,
        varray: VArray,
    ) -> List[Path]:
        """Check all Arrays that are in current VArray.

        :param adapter: Array Adapter instance
        :param varray: VArray
        """
        currently_locked = []

        collection = varray._VArray__collection  # type: ignore[attr-defined]
        for array_position in self.instance._VSubset__arrays:
            filename = adapter._get_symlink_filename(
                varray.id,
                array_position.vposition,
                collection.array_schema.primary_attributes,  # type: ignore[arg-type]
            )
            if not filename:
                continue

            # Path to the main file (not symlink)
            filename = filename.resolve()
            try:
                lock = self.check_locks_for_array_and_set_flock(filename)
                self.locks.append((lock, filename))
            except DekerLockError:
                currently_locked.append(filename)

        return currently_locked

    def check_existing_lock(self, func_args: Sequence, func_kwargs: Dict) -> None:  # noqa[ARG002]
        """If there are any array write/read lock, we shouldn't update varray.

        :param func_args: arguments of the function that has been called.
        :param func_kwargs: keyword arguments of the function that has been called.
        """
        adapter = self.instance._VSubset__array_adapter
        varray = self.instance._VSubset__array

        # Clear links to locks
        self.locks = []
        # Locks that have been acquired by third party process

        # Iterate over Arrays in VArray and try to lock them. If locking fails - wait.
        # If it fails again - release all locks.
        currently_locked = self.check_arrays_locks(adapter, varray)
        if not currently_locked:
            # Release array locks
            return

        # Wait till there are no more read locks
        if wait_for_unlock(
            check_func=lambda: not self.check_arrays_locks(adapter, varray),
            check_func_args=tuple(),
            timeout=adapter.ctx.config.write_lock_timeout,
            interval=adapter.ctx.config.write_lock_check_interval,
        ):
            return
        # Release all locks
        self.release()
        raise DekerLockError(f"VArray {varray} is locked")

    def release(self, e: Optional[Exception] = None) -> None:  # noqa[ARG002]
        """Release all locks.

        :param e: Exception that may have been raised.
        """
        # Release array locks
        for lock, filename in self.locks:
            lock.release()
            Path(f"{filename}:{os.getpid()}{LocksExtensions.varray_lock.value}").unlink(
                missing_ok=True
            )
        super().release()

    def acquire(self, path: Optional[Path]) -> None:
        """VArray shall not lock itself.

        :param path: path to the file to be locked
        """
        pass

    @staticmethod
    def _inner_method_logic(
        lock: "WriteVarrayLock", args: Sequence, kwargs: Dict, func: Callable
    ) -> Any:
        """Logic of acquiring lock and getting result.

        When writing in VArray

        :param lock: The lock that will be acquired
        :param func: decorated function
        :param args: arguments of decorated function
        :param kwargs: keyword arguments of decorated function
        """
        # If we want to skip logic of lock (e.g. when we use server adapters)
        if lock.skip_lock:
            return lock.get_result(func, args, kwargs)
        return super()._inner_method_logic(lock, args, kwargs, func)


class UpdateMetaAttributeLock(LockWithArrayMixin[Union["Array", "VArray"]], BaseLock):
    """Lock for updating meta."""

    ALLOWED_TYPES = ["LocalArrayAdapter", "LocalVArrayAdapter"]

    def get_path(self) -> Path:
        """Return path to the file that should be locked."""
        # Get directory where file locates
        dir_path = get_main_path(
            self.array.id, self.instance.collection_path / self.instance.data_dir
        )
        path = dir_path / f"{self.array.id}{self.instance.file_ext}"
        self.logger.debug(f"Got path for array.id {self.array.id} lock file: {path}")
        return path


class CollectionLock(BaseLock):
    """Lock for collection creation."""

    ALLOWED_TYPES = ["LocalCollectionAdapter"]

    def get_path(self) -> Path:  # noqa[ARG002]
        """Return path to collection lock file."""
        collection = self.args[1]
        path = self.instance.collections_resource / (collection.name + ".lock")
        self.logger.debug(f"Got path for collection {collection.name} lock file: {path}")
        return path

    def release(self, e: Optional[Exception] = None) -> None:
        """Release Collection Lock.

        :param e: Exception that might have been raised
        """
        if self.lock:
            self.lock.release()
            if isinstance(e, DekerMemoryError):
                self.lock.file.unlink()
            self.lock = None

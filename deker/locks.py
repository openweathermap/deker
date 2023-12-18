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
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Sequence, Union, Tuple
from uuid import uuid4

from deker.ABC.base_locks import BaseLock
from deker.errors import DekerLockError, DekerMemoryError
from deker.flock import Flock
from deker.tools.path import get_main_path
from deker.types.private.enums import LocksExtensions


if TYPE_CHECKING:
    from deker_local_adapters import LocalArrayAdapter

    from deker.arrays import Array, VArray
    from deker.types.private.classes import ArrayPositionedData

META_DIVIDER = ":"


class ReadArrayLock(BaseLock):
    """Read lock for Array."""

    ALLOWED_TYPES = ["LocalArrayAdapter"]

    def get_path(self, func_args: Sequence, func_kwargs: Dict) -> Path:
        """Get path to read-lock file.

        It's only the case for arrays, varrays don't have read locks.
        :param func_args: arguments of method call
        :param func_kwargs: keyword arguments of method call
        """
        # Get instance of the array
        array: Union[Path, Union[Array, VArray]] = (
            func_kwargs.get("array") or func_args[1]
        )  # zero arg is 'self'
        if isinstance(array, Path):
            path = array
            id_ = path.name.split(".")[0]
        else:
            id_ = array.id

        # Get file directory
        dir_path = get_main_path(id_, self.instance.collection_path / self.instance.data_dir)
        filename = (
            META_DIVIDER.join([f"{id_}", f"{uuid4()}", f"{os.getpid()}", f"{get_native_id()}"])
            + LocksExtensions.array_read_lock.value
        )
        # Create read lock file path
        path = dir_path / f"{filename}"

        self.logger.debug(f"Got path for array.id {id_} lock file: {path}")
        return path

    def check_existing_lock(self, func_args: Sequence, func_kwargs: Dict) -> None:
        """Read operations are not performed in case there are any WRITE locks.

        Array file would be Flocked for writing / updating, so we just need to try flock it once more
        :param func_args: arguments for called function.
        :param func_kwargs: keyword arguments for called function.
        """
        array: Union[Path, Union[Array, VArray]] = (
            func_kwargs.get("array") or func_args[1]
        )  # zero arg is 'self'
        if isinstance(array, Path):
            path = array
            id_ = path.name.split(".")[0]
        else:
            id_ = array.id

        dir_path = get_main_path(id_, self.instance.collection_path / self.instance.data_dir)
        path = dir_path / (id_ + self.instance.file_ext)
        for file in dir_path.iterdir():
            # Skip lock from current process.
            if file.name.endswith(f"{os.getpid()}{LocksExtensions.varray_lock.value}"):
                self.is_locked_with_varray = True
                return
            # If we've found another varray lock, that not from current process.
            if file.name.endswith(LocksExtensions.varray_lock.value):  # type: ignore
                raise DekerLockError(f"Array {array} is locked with {file.name}")
        try:
            with open(path, "r") as f:
                fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
                self.logger.debug(f"Set shared flock for {path}")

        except BlockingIOError:
            raise DekerLockError(f"Array {id_} is locked for update operation, cannot be read.")

    def acquire(self, path: Union[str, Path]) -> Any:
        """Read files will not be flocked - only created.

        :param path: Path to file which should be locked
        """
        # Create lock file
        self.lock = path
        open(path, "a").close()
        self.logger.debug(f"Acquired lock for {self.lock}")

    def release(self, e: Optional[Exception] = None) -> None:  # noqa[ARG002]
        """Release lock by deleting file.

        :param e: Exception that may have been raised.
        """
        if self.lock and self.lock.exists():
            self.lock.unlink()
            self.lock = None
            self.logger.debug(f"Released lock for {self.lock}")


class WriteArrayLock(BaseLock):
    """Write lock for arrays."""

    ALLOWED_TYPES = ["LocalArrayAdapter"]

    is_locked_with_varray: bool = False

    def get_path(self, func_args: Sequence, func_kwargs: Dict) -> Path:
        """Get path to the file for locking.

        :param func_args: arguments of method call
        :param func_kwargs: keyword arguments of method call
        """
        array = func_kwargs.get("array") or func_args[1]  # zero arg is 'self'
        dir_path = get_main_path(array.id, self.instance.collection_path / self.instance.data_dir)
        path = dir_path / (array.id + self.instance.file_ext)
        self.logger.debug(f"Got path for array.id {array.id} lock file: {path}")
        return path

    def check_existing_lock(self, func_args: Sequence, func_kwargs: Dict) -> None:
        """Check read locks before write execution.

        :param func_args: arguments of method call
        :param func_kwargs: keyword arguments of method call
        """
        # Check current read locks
        array = func_kwargs.get("array") or func_args[1]  # zero arg is 'self'
        dir_path = get_main_path(array.id, self.instance.collection_path / self.instance.data_dir)

        # If array belongs to varray, we should check if varray is also locked
        if array._vid:
            for file in dir_path.iterdir():
                # Skip lock from current process.
                if file.name.endswith(f"{os.getpid()}{LocksExtensions.varray_lock.value}"):
                    self.is_locked_with_varray = True
                    return
                # If we've found another varray lock, that not from current process.
                if file.name.endswith(LocksExtensions.varray_lock.value):  # type: ignore
                    raise DekerLockError(f"Array {array} is locked with {file.name}")

        # Increment write lock, to prevent more read locks coming.
        self.acquire(self.get_path(func_args, func_kwargs))

        # Pattern that has to find any read locks
        glob_pattern = f"{array.id}:*{LocksExtensions.array_read_lock.value}"

        # Wait till there are no more read locks
        start_time = time.monotonic()
        while (time.monotonic() - start_time) <= self.instance.ctx.config.write_lock_timeout:
            if not list(dir_path.rglob(glob_pattern)):
                return

            sleep(self.instance.ctx.config.write_lock_check_interval)

        # If we hit the timeout, release write lock and raise DekerLockError
        self.release()
        raise DekerLockError(f"Array {array} is locked with read locks")

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

    def get_path(self, func_args: Sequence, func_kwargs: Dict) -> Optional[Path]:  # noqa[ARG002]
        """Path of json Varray file.

        :param func_args: arguments of the function that has been called.
        :param func_kwargs: keyword arguments of the function that has been called.
        """
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
        :return:
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
        arrays_positions: List[ArrayPositionedData],
        adapter: LocalArrayAdapter,
        varray: VArray,
    ) -> List[Path]:
        """Check all Arrays that are in current VArray.

        :param arrays_positions: Arrays' positions in VArray
        :param adapter: Array Adapter instance
        :param varray: VArray
        """
        currently_locked = []

        collection = varray._VArray__collection  # type: ignore[attr-defined]
        for array_position in arrays_positions:
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

        arrays_positions: List[ArrayPositionedData] = self.instance._VSubset__arrays

        # Clear links to locks
        self.locks = []
        # Locks that have been acquired by third party process

        # Iterate over Arrays in VArray and try to lock them. If locking fails - wait.
        # If it fails again - release all locks.
        currently_locked = self.check_arrays_locks(arrays_positions, adapter, varray)
        if not currently_locked and (len(self.locks) == len(arrays_positions)):
            # Release array locks
            return

        # Wait till there are no more read locks
        start_time = time.monotonic()
        while (time.monotonic() - start_time) <= adapter.ctx.config.write_lock_timeout:
            if not self.check_arrays_locks(arrays_positions, adapter, varray):
                return
            sleep(adapter.ctx.config.write_lock_check_interval)
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


class CreateArrayLock(BaseLock):
    """Lock that we set when we want to create an array."""

    ALLOWED_TYPES = ["LocalArrayAdapter", "LocalVArrayAdapter"]

    path: Optional[Path] = None

    def get_path(self, func_args: Sequence, func_kwargs: Dict) -> Path:
        """Return path to the file that should be locked.

        :param func_args: arguments for called method
        :param func_kwargs: keyword arguments for called method
        :return:
        """
        array = func_kwargs.get("array") or func_args[1]  # zero arg is 'self'

        # Get file directory path
        dir_path = self.instance.collection_path
        filename = META_DIVIDER.join(
            [
                f"{array.id}",
                f"{uuid4()}",
                f"{os.getpid()}",
                f"{get_native_id()}",
            ]
        )
        # Create lock file
        path = dir_path / f"{filename}{LocksExtensions.array_lock.value}"
        if not path.exists():
            path.open("w").close()

        self.path = path
        self.logger.debug(f"got path for array.id {array.id} lock file: {path}")
        return path

    def check_existing_lock(self, func_args: Sequence, func_kwargs: Dict) -> None:
        """Check if there is currently lock for array creating.

        :param func_args: arguments for called method
        :param func_kwargs: keyword arguments for called method
        """
        from deker.arrays import Array, VArray

        # Check current read locks
        array = func_kwargs.get("array") or func_args[1]  # zero arg is 'self'
        if isinstance(array, dict):
            adapter = array["adapter"].__class__.__name__
            if adapter not in self.ALLOWED_TYPES:
                raise DekerLockError(f"Adapter {adapter} is not allowed to create locks for arrays")

            # TODO: figure out a way to avoid constructing Array object here
            array_type = Array if adapter == self.ALLOWED_TYPES[0] else VArray
            array = array_type(**array)

        dir_path = self.instance.collection_path

        # Pattern that has to find any create locks
        glob_pattern = f"{array.id}:*{LocksExtensions.array_lock.value}"
        for _ in dir_path.rglob(glob_pattern):
            raise DekerLockError(f"Array {array} is locked for creating")

        func_kwargs["array"] = array

    def get_result(self, func: Callable, args: Any, kwargs: Any) -> Any:
        """Call func, and get its result.

        :param func: decorated function
        :param args: arguments of decorated function
        :param kwargs: keyword arguments of decorated function
        """
        if kw_array := kwargs.pop("array"):
            args = list(args)
            # First elem is self, so for functions with array, set array as next arg.
            args[1] = kw_array
            result = func(*tuple(args), **kwargs)
        else:
            result = func(*args, **kwargs)
        return result

    def release(self, e: Optional[Exception] = None) -> None:
        """Release Flock.

        :param e: exception that might have been raised
        """
        self.path.unlink(missing_ok=True)
        super().release(e)


class UpdateMetaAttributeLock(BaseLock):
    """Lock for updating meta."""

    ALLOWED_TYPES = ["LocalArrayAdapter", "LocalVArrayAdapter"]

    def get_path(self, func_args: Sequence, func_kwargs: Dict) -> Path:
        """Return path to the file that should be locked.

        :param func_args: arguments for called method
        :param func_kwargs: keyword arguments for called method
        :return:
        """
        array = func_kwargs.get("array") or func_args[1]  # zero arg is 'self'

        # Get directory where file locates
        dir_path = get_main_path(array.id, self.instance.collection_path / self.instance.data_dir)
        path = dir_path / f"{array.id}{self.instance.file_ext}"
        self.logger.debug(f"Got path for array.id {array.id} lock file: {path}")
        return path


class CollectionLock(BaseLock):
    """Lock for collection creation."""

    ALLOWED_TYPES = ["LocalCollectionAdapter"]

    def get_path(self, func_args: Sequence, func_kwargs: Dict) -> Path:  # noqa[ARG002]
        """Return path to collection lock file.

        :param func_args: arguments for called method
        :param func_kwargs: keyword arguments for called method
        """
        collection = func_args[1]
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

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

"""Abstract interfaces for locks."""
from abc import ABC, abstractmethod
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from deker.flock import Flock
from deker.log import SelfLoggerMixin


class BaseLock(SelfLoggerMixin, ABC):
    """Base class for read/write locks."""

    ALLOWED_TYPES: List[str] = []
    lock: Optional[Union[Flock, Path]] = None
    instance: Optional[Any] = None
    args: Optional[List[Any]] = None
    kwargs: Optional[Dict] = None

    @abstractmethod
    def get_path(self) -> Optional[Path]:
        """Get path to the lock file.

        For Arrays it shall be .arrlock (array read lock) or path to the file (array write lock)
        For VArrays there are no specific locks for reading, for writing - lock on .json
        """
        pass

    def check_existing_lock(self, func_args: Sequence, func_kwargs: Dict) -> None:
        """Check if there is lock for given method.

        :param func_args: Arguments for function that has been called
        :param func_kwargs: Key word arguments for function that has been called
        """
        pass

    def check_type(self) -> None:
        """Check if the instance type (class) is allowed for locking."""
        # Workaround, because isinstance returns BaseClass
        cls: str = self.instance.__class__.__name__
        if cls not in self.ALLOWED_TYPES:
            raise TypeError(f"{cls} lock is not supported")

    def acquire(self, path: Optional[Path]) -> None:
        """Make lock using flock.

        :param path: Path to file that should be flocked
        """
        if not self.lock and path:
            self.lock = Flock(path)
            self.lock.acquire()
            self.logger.debug(f"Set flock for {path}")

    def release(self, e: Optional[Exception] = None) -> None:  # noqa[ARG002]
        """Release Flock.

        :param e: exception that might have been raised
        """
        if self.lock:
            self.lock.release()
            self.lock = None
            self.logger.debug(f"Released lock for {self.lock}")

    def get_result(self, func: Callable, args: Any, kwargs: Any) -> Any:
        """Call func, and get its result.

        :param func: decorated function
        :param args: Arguments of decorated function
        :param kwargs: Keyword arguments of decorated function
        """
        return func(*args, **kwargs)

    @staticmethod
    def _inner_method_logic(lock: "BaseLock", args: Sequence, kwargs: Dict, func: Callable) -> Any:
        """Logic of acquiring lock and getting a result.

        :param lock: lock to be acquired
        :param func: decorated function
        :param args: Arguments of decorated function
        :param kwargs: Keyword arguments of decorated function
        """
        lock.check_existing_lock(args, kwargs)
        path = lock.get_path()
        lock.acquire(path)
        try:
            # Logic is in the separate method, so we can override its behavior
            # E.g., CreateArray lock has specific instructions for array attribute
            result = lock.get_result(func, args, kwargs)
        except Exception as e:
            lock.release(e)
            raise
        lock.release()

        return result

    def __call__(self, func: Callable) -> Any:
        """Create/get file to lock with Flock and lock it.

        If there is an existing lock, raise DekerLockError
        :param func: Func that would be decorated
        """

        @wraps(func)
        def inner(*args: Sequence, **kwargs: Dict[str, Any]) -> Any:
            """Inner function of lock decorator.

            :param args: Arguments of decorated function
            :param kwargs: Keyword arguments of decorated function
            """
            # To keep different references for locks
            lock = self.__class__()
            # as we wrap methods, we should have access to 'self' objects
            lock.instance = kwargs.get("self") or args[0]
            lock.args = args
            lock.kwargs = kwargs
            # Check that we don't have lock on anything that besides methods that require lock
            lock.check_type()

            # Logic is in the separate method, so we can override its behavior
            # E.g., in WriteVArrayLock, where we should skip the whole lock logic
            return lock._inner_method_logic(lock, args, kwargs, func)

        return inner

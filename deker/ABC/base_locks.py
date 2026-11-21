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

"""Abstract interfaces for locks."""

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from functools import wraps
from pathlib import Path
from time import sleep
from typing import Any, ParamSpec, TypeVar

from cantok import AbstractToken, TimeoutToken
from deker_tools.cancel import check, resolve_timeout

from deker.errors import DekerLockError
from deker.flock import Flock
from deker.log import SelfLoggerMixin

P = ParamSpec("P")
R = TypeVar("R")


def wait_for_unlock(
    check_func: Callable[..., bool], check_func_args: tuple, *, timeout: int, interval: float, token: AbstractToken
) -> bool:
    """Call the check until it succeeds, sleeping between the attempts, for the timeout at most.

    The token of the operation is checked between the attempts: a cancelled operation stops waiting at once.

    :param check_func: function that returns ``True`` when the lock is taken (or the wait is over)
    :param check_func_args: arguments of the function
    :param timeout: seconds to keep trying, counted from the start of the wait
    :param interval: seconds between the attempts
    :param token: token of the operation
    :returns: ``True`` if the check succeeded, ``False`` if the timeout is over
    :raises TokenCancelledError: if the operation is cancelled while it waits
    """
    deadline = TimeoutToken(timeout)
    while not deadline.cancelled:
        if check_func(*check_func_args):
            return True
        check(token)
        sleep(interval)
    return False


class BaseLock(SelfLoggerMixin, ABC):
    """Base class of the lock decorators of the adapter methods.

    A lock decorates a method of an adapter; every call gets a fresh lock object bound to the adapter and the
    arguments of the call. The lock takes an ``flock`` on the path ``get_path`` names before the method runs
    and releases it after, whatever the outcome; ``check_existing_lock`` runs before the ``flock`` is taken, for
    the locks that wait.
    """

    ALLOWED_TYPES: list[str] = []
    lock: Flock | None = None
    # bound by __call__ for every decorated call: the decorated method's object and its arguments
    instance: Any
    args: Sequence[Any]
    kwargs: dict[str, Any]

    @abstractmethod
    def get_path(self) -> Path | None:
        """Get the path the ``flock`` is taken on.

        The main file of an ``Array`` or a ``VArray`` for the locks of the arrays, the ``<collection>.lock`` file
        for the lock of a collection.

        :returns: the path, ``None`` for a lock that holds nothing
        """
        pass

    def check_existing_lock(self, func_args: Sequence, func_kwargs: dict) -> None:
        """Run before the ``flock`` is taken; a lock that waits for the file takes the ``flock`` here.

        Does nothing by default.

        :param func_args: Arguments for function that has been called
        :param func_kwargs: Key word arguments for function that has been called
        """
        pass

    def check_type(self) -> None:
        """Check if the instance type (class) is allowed for locking.

        :raises TypeError: if the decorated method belongs to a class the lock does not support
        """
        # Workaround, because isinstance returns BaseClass
        cls: str = self.instance.__class__.__name__
        if cls not in self.ALLOWED_TYPES:
            raise TypeError(f"{cls} lock is not supported")

    def acquire(self, path: Path | None) -> None:
        """Take an exclusive ``flock`` on the path, unless the lock is held already or there is no path.

        :param path: Path to file that should be flocked
        :raises DekerLockError: if the file is locked by another holder
        """
        if not self.lock and path:
            lock = Flock(path)
            lock.acquire()  # a refused lock is not kept: the next attempt starts afresh
            self.lock = lock
            self.logger.debug("Set flock for %s", path)

    def _try_acquire(self, path: Path) -> bool:
        """Try to take the ``flock`` once.

        :param path: path to the file or the directory to lock
        :returns: ``True`` if the lock is held now, ``False`` if someone else holds the file
        """
        try:
            self.acquire(path)
        except DekerLockError:
            return False
        return True

    def _wait_and_acquire(self, func_kwargs: dict, locked_message: str) -> None:
        """Take the ``flock``, waiting for its holders up to the timeout of the decorated call.

        The decorated call carries ``timeout`` (``Sentinel`` when not given: ``write_lock_timeout`` of the client)
        and ``token`` of its operation.

        :param func_kwargs: keyword arguments of the decorated call
        :param locked_message: text of the error when the holders do not leave in time
        :raises DekerLockError: if the file is not released within the timeout
        :raises TokenCancelledError: if the operation is cancelled while it waits
        """
        path = self.get_path()
        if path is None or self._try_acquire(path):  # pragma: no cover  # a waiting lock always has a path
            return
        config = self.instance.ctx.config
        timeout = resolve_timeout(func_kwargs["timeout"], config.write_lock_timeout)
        if wait_for_unlock(
            self._try_acquire,
            (path,),
            timeout=timeout,
            interval=config.write_lock_check_interval,
            token=func_kwargs["token"],
        ):
            return
        # If we hit the timeout, release whatever is held and raise DekerLockError
        self.release()
        raise DekerLockError(locked_message)

    def release(self, e: Exception | None = None) -> None:  # noqa: ARG002
        """Release Flock.

        :param e: exception that might have been raised
        """
        if self.lock:
            self.lock.release()
            self.logger.debug("Released lock for %s", self.lock.file)
            self.lock = None

    def get_result(self, func: Callable, args: Any, kwargs: Any) -> Any:
        """Call func, and get its result.

        :param func: decorated function
        :param args: Arguments of decorated function
        :param kwargs: Keyword arguments of decorated function
        :returns: result of ``func``
        """
        return func(*args, **kwargs)

    @staticmethod
    def _inner_method_logic(lock: "BaseLock", args: Sequence, kwargs: dict, func: Callable) -> Any:
        """Logic of acquiring lock and getting a result: the lock is released whatever the method does.

        :param lock: lock to be acquired
        :param func: decorated function
        :param args: Arguments of decorated function
        :param kwargs: Keyword arguments of decorated function
        :returns: result of the decorated method
        """
        lock.check_existing_lock(args, kwargs)
        path = lock.get_path()
        lock.acquire(path)
        try:
            # Logic is in the separate method, so a lock can override it
            result = lock.get_result(func, args, kwargs)
        except Exception as e:
            lock.release(e)
            raise
        lock.release()

        return result

    def __call__(self, func: Callable[P, R]) -> Callable[P, R]:
        """Decorate a method of an adapter with the lock.

        Every call of the decorated method runs under a fresh lock of this class; if the file is held by someone
        else, the call raises ``DekerLockError``. The decorated method keeps its signature for type checkers.

        :param func: Func that would be decorated
        :returns: the decorated method
        """

        @wraps(func)
        def inner(*args: P.args, **kwargs: P.kwargs) -> R:
            r"""Inner function of lock decorator.

            :param \*args: Arguments of decorated function
            :param \**kwargs: Keyword arguments of decorated function
            :returns: result of the decorated method
            """
            # To keep different references for locks
            lock = self.__class__()
            # as we wrap methods, we should have access to 'self' objects
            lock.instance = kwargs.get("self") or args[0]
            lock.args = args
            lock.kwargs = kwargs
            # Check that we don't have lock on anything that besides methods that require lock
            lock.check_type()

            # Logic is in the separate method, so a lock can override it
            return lock._inner_method_logic(lock, args, kwargs, func)

        return inner

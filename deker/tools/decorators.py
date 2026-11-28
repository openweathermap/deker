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

from collections.abc import Callable
from functools import wraps
from typing import Any, Concatenate, ParamSpec, TypeVar

from deker.errors import DekerAdapterClosedError, DekerInstanceNotExistsError

P = ParamSpec("P")
R = TypeVar("R")
# text of the error of an object that does not exist anymore, with the name of the method it was asked for
_NOT_EXISTING = "{instance} doesn't exist, create new or get an instance again to be able to call {method}"


def check_ctx_state(method: Callable[Concatenate[Any, P], R]) -> Callable[Concatenate[Any, P], R]:
    """Reject the calls of an adapter method after the client context was closed.

    The decorated method keeps its signature for type checkers.

    :param method: adapter method
    :returns: the decorated method, raising ``DekerAdapterClosedError`` on a closed context
    """

    @wraps(method)
    def ctx_checker(self: Any, *args: P.args, **kwargs: P.kwargs) -> R:
        """Inner executor methods wrapper.

        :param self: Adapter instance
        :param args: any
        :param kwargs: any
        :returns: result of the method
        """
        if self.ctx.is_closed:
            raise DekerAdapterClosedError(f"{self.__class__.__name__} is closed")
        return method(self, *args, **kwargs)

    return ctx_checker


def not_deleted(method: Callable[Concatenate[Any, P], R]) -> Callable[Concatenate[Any, P], R]:
    """Set rules of invocation for objects that could be deleted.

    If the object was deleted, do not call func. The decorated method keeps its signature for type checkers.

    :param method: method to decorate
    :returns: the decorated method
    """

    @wraps(method)
    def _inner(self: Any, *args: P.args, **kwargs: P.kwargs) -> R:
        """Check instance for flag.

        :param self: class instance
        :param args: any
        :param kwargs: any
        :returns: result of the method
        """
        if self._is_deleted():
            raise DekerInstanceNotExistsError(_NOT_EXISTING.format(instance=self, method=method.__qualname__))
        return method(self, *args, **kwargs)

    return _inner

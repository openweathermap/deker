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

from functools import wraps
from typing import Any, Callable

from deker.errors import DekerClientError, DekerInstanceNotExistsError


def check_ctx_state(method: Callable) -> Callable:
    """Context checker deco.

    :param method: callable class method
    """

    @wraps(method)
    def ctx_checker(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Inner executor methods wrapper.

        :param self: Adapter instance
        :param args: any
        :param kwargs: any
        """
        if self.ctx.is_closed:
            raise DekerClientError("Client is closed")
        return method(self, *args, **kwargs)

    return ctx_checker


def not_deleted(method: Callable) -> Callable:
    """Set rules of invocation for objects that could be deleted.

    If the object was deleted, do not call func.
    :param method: method to decorate
    """

    @wraps(method)
    def _inner(self: Any, *args: Any, **kwargs: Any) -> Any:
        """Check instance for flag.

        :param self: class instance
        :param args: any
        :param kwargs: any
        """
        if self._is_deleted():
            raise DekerInstanceNotExistsError(
                f"{self} doesn't exist, create new or get an instance again to be able to call {method}"
            )
        return method(self, *args, **kwargs)

    return _inner

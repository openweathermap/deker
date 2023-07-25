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

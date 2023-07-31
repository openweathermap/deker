from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Optional, Type


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseStorageAdapter
    from deker.config import DekerConfig
    from deker.uri import Uri


class CTX:
    """Deker client context."""

    def __init__(
        self,
        uri: "Uri",
        config: "DekerConfig",
        storage_adapter: Optional[Type["BaseStorageAdapter"]] = None,
        executor: Optional[ThreadPoolExecutor] = None,
        is_closed: bool = False,
        extra: Optional[Any] = None,
    ):
        self.uri = uri
        self.storage_adapter = storage_adapter
        self.executor = executor
        self.is_closed = is_closed
        self.config = config
        self.extra: dict = extra if extra else dict()

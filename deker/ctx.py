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

from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from deker_tools.typings import Sentinel, SentinelType, is_sentinel

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseStorageAdapter
    from deker.config import DekerConfig
    from deker.uri import Uri


class CTX:
    """Deker client context: the state the adapters share.

    :param uri: uri of the storage
    :param config: configuration of the client
    :param storage_adapter: type of the adapter of the files, if set
    :param executor: thread pool of the client, created by the adapters factory if not given
    :param is_closed: ``True`` once the client is closed; the adapters refuse to work then
    :param extra: extra keyword arguments of the client, passed to the adapters as they are; not given - none
    """

    def __init__(
        self,
        uri: "Uri",
        config: "DekerConfig",
        storage_adapter: type["BaseStorageAdapter"] | None = None,
        executor: ThreadPoolExecutor | None = None,
        is_closed: bool = False,
        extra: dict[str, Any] | SentinelType = Sentinel,
    ):
        self.uri = uri
        self.storage_adapter = storage_adapter
        self.executor = executor
        self.is_closed = is_closed
        self.config = config
        self.extra: dict[str, Any] = {} if is_sentinel(extra) else extra

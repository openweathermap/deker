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

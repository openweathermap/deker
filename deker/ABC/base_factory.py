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

"""Abstract interfaces for factories."""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from deker.log import SelfLoggerMixin

if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseCollectionAdapter, BaseVArrayAdapter
    from deker.ctx import CTX
    from deker.uri import Uri


class BaseAdaptersFactory(SelfLoggerMixin, ABC):
    """Base interface of Collections, Arrays and VArrays adapters' factory."""

    uri_schemes: tuple[str, ...]

    def __init__(self, ctx: "CTX", uri: "Uri") -> None:
        """BaseAdaptersFactory constructor: takes the executor of the context or creates an own one.

        :param ctx: context of the client
        :param uri: uri of the storage
        """
        self.ctx = ctx
        self.uri = uri
        self.executor: ThreadPoolExecutor
        self._own_executor: bool
        if self.ctx.executor is None:
            self.executor = ThreadPoolExecutor(self.ctx.config.workers)
            self.ctx.executor = self.executor
            self._own_executor = True
        else:
            self.executor = self.ctx.executor
            self._own_executor = False
        self.logger.debug("%s instantiated", self.__class__.__name__)

    def __del__(self) -> None:
        """Close the factory when it is dropped."""
        self.close()

    @abstractmethod
    def close(self) -> None:
        """Close the own executor; an executor given by the user is left to the user."""
        try:
            if self.executor and self._own_executor:
                self.executor.shutdown(wait=True, cancel_futures=False)
        except AttributeError:
            pass

    @abstractmethod
    def get_array_adapter(self, *args: Any, **kwargs: Any) -> "BaseArrayAdapter":
        r"""Create ArrayAdapter instance.

        :param \*args: any arguments
        :param \**kwargs: any keyword arguments
        :returns: ``ArrayAdapter`` instance
        """
        pass

    @abstractmethod
    def get_varray_adapter(self, *args: Any, **kwargs: Any) -> "BaseVArrayAdapter":
        r"""Create VArrayAdapter instance.

        :param \*args: any arguments
        :param \**kwargs: any keyword arguments
        :returns: ``VArrayAdapter`` instance
        """
        pass

    @abstractmethod
    def get_collection_adapter(self, *args: Any, **kwargs: Any) -> "BaseCollectionAdapter":
        r"""Create collection adapter.

        :param \*args: any arguments
        :param \**kwargs: any keyword arguments
        :returns: ``CollectionAdapter`` instance
        """
        pass

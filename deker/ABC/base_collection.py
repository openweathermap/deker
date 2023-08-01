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

"""Abstract interfaces for collection."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from deker.types.private.classes import Serializer


class BaseCollectionOptions(Serializer, ABC):
    """Base interface for collection options.

    Options, such as chunks, compression -
    or whatever that may somehow influence storage files' size or structure -
    depend on provided storage adapter
    """

    chunks: Any
    compression_opts: Any

    @classmethod
    @abstractmethod
    def _process_options(cls, storage_options: Optional[dict]) -> dict:
        """Validate and convert collection storage options.

        :param storage_options: options for storing data by a certain storage adapter,
        like chunks, compression, etc. Such options are passed to a collection to be used for every
        Array in it.
        """
        pass

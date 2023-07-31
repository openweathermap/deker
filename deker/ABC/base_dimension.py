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

"""Abstract wrappers for array dimension and dimension index labels."""

from abc import ABC
from typing import Union

from deker.errors import DekerValidationError
from deker.types.private.classes import Serializer


class BaseDimension(Serializer, ABC):
    """Dimension abstract object providing all its inheritors with common actions and methods."""

    __slots__ = ("__name", "__size", "__step")

    def _validate(self, name: str, size: int, **kwargs: dict) -> None:  # noqa[ARG002]
        if not isinstance(name, str):
            raise DekerValidationError("Name shall be str")
        if not name or name.isspace():
            raise DekerValidationError("Name cannot be empty")
        if size is None or isinstance(size, bool) or not isinstance(size, int) or size <= 0:
            raise DekerValidationError("Size shall be a positive int")

    def __init__(self, name: str, size: int, **kwargs: dict) -> None:
        super().__init__()
        self._validate(name, size, **kwargs)  # pragma: no cover
        self.__name = name  # pragma: no cover
        self.__size = size  # pragma: no cover
        self.__step = 1  # pragma: no cover

    @property
    def name(self) -> str:
        """Dimension name."""
        return self.__name  # pragma: no cover

    @property
    def size(self) -> int:
        """Dimension size."""
        return self.__size  # pragma: no cover

    @property
    def step(self) -> Union[int, float]:
        """Dimension values step."""
        return self.__step  # pragma: no cover

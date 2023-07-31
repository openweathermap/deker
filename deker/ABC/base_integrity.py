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

"""Abstract interfaces for integrity checker."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any, Type


if TYPE_CHECKING:
    from deker.client import Client


class BaseChecker(ABC):
    """Checker abstract object. Implements chain of responsibility pattern for integrity check."""

    def __init__(
        self,
        stop_on_error: bool,
        paths: dict,
        errors: dict,
        level: int,
        client: "Client",
        root_path: Path,
    ):
        super().__init__()
        self.stop_on_error = stop_on_error
        self.paths = paths
        self.errors = errors
        self.level = level
        self.client = client
        self.root_path = root_path
        self.ctx = client._Client__ctx  # type: ignore
        self.next_checker = None

    def _parse_errors(self) -> str:
        """Parse self.errors and return string."""
        res = ""
        if not self.errors:
            return res
        for key in self.errors:
            res += key + "\n\t- " + "\n\t- ".join(self.errors[key]) + "\n"
        return res

    def add_checker(self, checker: Type["BaseChecker"]) -> None:
        """Add next checker, uses self to init.

        :param checker: next_checker type
        """
        current = self
        while current.next_checker:
            current = current.next_checker
        current.next_checker = checker(
            self.stop_on_error, self.paths, self.errors, self.level, self.client, self.root_path
        )

    @abstractmethod
    def check(self, *args: Any, **kwargs: Any) -> None:
        """Check integrity. Shall be implemented in every Checker.

        :param args: args
        :param kwargs: kwargs
        """
        pass

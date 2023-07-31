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

import logging
import os

from logging import Logger


_ROOT_DEKER_LOGGER_NAME = "Deker"
_level = os.getenv("DEKER_LOGLEVEL", "WARNING")
_format = "%(name)s %(levelname)-4s [%(asctime)s] %(message)s"
_logger = logging.getLogger(_ROOT_DEKER_LOGGER_NAME)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter(fmt=_format))
_logger.addHandler(_handler)


class SelfLoggerMixin(object):
    """Mixin with a logger object with a possibility to log its actions."""

    __logger: Logger = None

    @property
    def logger(self) -> Logger:
        """Lazy deker logger property."""
        if not self.__logger:
            self.__logger = _logger.getChild(self.__class__.__name__)
        return self.__logger


def set_logging_level(level: str) -> None:
    """Set level of all deker loggers.

    :param level: level of logging
    """
    _logger = logging.getLogger(_ROOT_DEKER_LOGGER_NAME)
    items = list(_logger.manager.loggerDict.items())  # for Python 2 and 3
    items.sort()
    for name, logger in items:
        if _logger.name in name:
            logger.setLevel(level)

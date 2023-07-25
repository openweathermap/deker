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

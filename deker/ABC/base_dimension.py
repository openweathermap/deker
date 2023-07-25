"""Abstract wrappers for array dimension and dimension index labels."""

from abc import ABC
from typing import Union

from deker.errors import DekerValidationError
from deker.types.classes import Serializer


class BaseDimension(Serializer, ABC):  # noqa: B024
    """Dimension abstract object providing all its inheritors with common actions and methods."""

    __slots__ = ("__name", "__size", "__step")

    def _validate(self, name: str, size: int, **kwargs: dict) -> None:
        if not isinstance(name, str):
            raise DekerValidationError("Name shall be str")
        if not name or name.isspace():
            raise DekerValidationError("Name can not be empty")
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

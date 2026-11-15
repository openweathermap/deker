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

import datetime
import math

from typing import Any

from deker.ABC.base_dimension import BaseDimension
from deker.errors import DekerValidationError
from deker.log import SelfLoggerMixin
from deker.types import IndexLabels, Labels, Scale


def validate_labels(name: str, size: int, labels: Any) -> None:
    """Check the labels of a dimension: the rules are the same for the schema and for the dimension.

    Labels are a non-empty list or tuple of unique non-blank strings, or of unique finite floats, one label
    per cell of the dimension. Integers and booleans are not labels: an integer index is a position, and a
    label that is an integer could never be told from one.

    :param name: name of the dimension, for the message
    :param size: size of the dimension
    :param labels: labels to check
    :raises DekerValidationError: if the labels break one of the rules
    """
    prefix = f"Invalid labels of dimension {name}: "
    if not isinstance(labels, (tuple, list)):
        raise DekerValidationError(prefix + f"a list or a tuple is expected, got {type(labels).__name__}")
    if not labels:
        raise DekerValidationError(prefix + "labels cannot be empty, use None instead")
    if len(labels) != size:
        raise DekerValidationError(prefix + f"{len(labels)} labels for {size} cells")
    if all(isinstance(label, str) for label in labels):
        if any(not label or label.isspace() for label in labels):
            raise DekerValidationError(prefix + "a label cannot be an empty or a blank string")
    elif all(isinstance(label, float) and not isinstance(label, bool) for label in labels):
        if any(not math.isfinite(label) for label in labels):
            raise DekerValidationError(prefix + "a label cannot be nan or infinite")
    else:
        raise DekerValidationError(prefix + "labels shall be all strings or all floats")
    if len(set(labels)) < len(labels):
        raise DekerValidationError(prefix + "labels shall be unique")


class Dimension(SelfLoggerMixin, BaseDimension):
    """``Dimension`` of a grid axes or any other series except time (for time series use ``TimeDimension``).

    May be used for defining the majority of parameters which may be stored in an array.
    You can use ``labels`` or ``scale`` parameter to create a mapping of some names or values to the dimension indexes.
    The constructor validates the parameters, sets the defaults and converts labels and scale
    to their types.

    :param name: dimension unique name (e.g. "length")
    :param size: dimension cells quantity
    :param labels: ordered sequence of unique dimension values names (strings or floats), mapped to their
      indexes; default None

      ::

        ["name1", "name2", ..., "nameN"]
        (0.25, 0.5, ..., 1.0)

    :param scale: a description of dimension regular scale; default None
    :param **kwargs: any keyword arguments
    """

    __slots__ = ("__name", "__size", "__step", "__labels", "__scale")

    def _validate(  # noqa: C901,RUF100
        self,
        name: str,
        size: int,
        labels: Labels | None = None,
        scale: dict | Scale | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        super()._validate(name, size)

        if all(param is None for param in (labels, scale)):
            return

        if all(param is not None for param in (labels, scale)):
            raise DekerValidationError(
                f"Invalid Dimension {name} arguments: either `labels` or `scale` or none of them should be passed, "
                f"not both"
            )

        if scale is not None:
            if not isinstance(scale, (dict, Scale)):
                raise DekerValidationError(
                    f"Scale parameter value shall be an instance of deker.types.classes.Scale, not {type(scale)}"
                )

            if isinstance(scale, dict):
                try:
                    scale_ = Scale(**scale)
                except TypeError as e:
                    raise DekerValidationError(e)
            else:
                scale_ = scale

            for attr in scale_._fields:
                value = getattr(scale_, attr)
                if attr == "name":
                    if value is not None and (not isinstance(value, str) or not value or value.isspace()):
                        raise DekerValidationError(f"Scale attribute '{attr}' value shall be non-empty string")

                elif not isinstance(value, float):
                    raise DekerValidationError(f"Scale attribute '{attr}' value shall be float")

            scale_end_non_inclusive = scale_.start_value
            scale_end_non_inclusive += scale_.step * size
            scale_size = int(round(abs((scale_end_non_inclusive - scale_.start_value) / scale_.step)))
            if scale_size != size:  # pragma: no cover  # guard: the scale end is computed from the size itself
                raise DekerValidationError(f"Dimension {name} wrong scale: does not match dimension size {size}")

        if labels is not None:
            validate_labels(name, size, labels)

    def __init__(
        self,
        name: str,
        size: int,
        labels: Labels | None = None,
        scale: dict | Scale | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(name, size, **kwargs)
        self._validate(name, size, labels, scale)
        self.__name: str = name
        self.__size: int = size
        self.__step: int = 1  # step attribute kept for further usage unification
        self.__labels: IndexLabels | None = IndexLabels(labels) if labels else None
        self.__scale: Scale | None = Scale(**scale) if isinstance(scale, dict) else scale
        self.logger.debug(f"Dimension {name} instantiated")

    @property
    def step(self) -> int:
        """Get ``Dimension`` step."""
        return self.__step

    @property
    def name(self) -> str:
        """Get ``Dimension`` name."""
        return self.__name

    @property
    def size(self) -> int:
        """Get ``Dimension`` size."""
        return self.__size

    @property
    def labels(self) -> IndexLabels | None:
        """Get ``Dimension`` labels."""
        return self.__labels

    @property
    def scale(self) -> Scale | None:
        """Get ``Dimension`` regular scale."""
        return self.__scale

    @property
    def as_dict(self) -> dict:
        """Serialize ``Dimension`` into dict."""
        dic = {"name": self.name, "size": self.size, "step": self.step}
        if self.labels:
            dic["labels"] = self.labels
        if self.scale:
            dic["scale"] = self.scale._asdict() if self.scale else self.scale
        return dic

    def __len__(self) -> int:
        """Get ``Dimension`` size."""
        return self.size

    def __repr__(self) -> str:
        s = f"{self.__class__.__name__}(name={self.name!r}, size={self.size!r}, step={self.step!r})"
        if self.labels:
            s = s[:-1] + f", labels={self.labels})"
        if self.scale:
            s = s[:-1] + f", scale={self.scale})"
        return s

    def __str__(self) -> str:
        return str(self.size)


class TimeDimension(SelfLoggerMixin, BaseDimension):
    """Special dimension for time series.

    :param name: dimension name (e.g. "time")
    :param size: dimension cells quantity

    :param start_value: time mark of the first cell (index 0) in dimension
    :param step: a ``datetime.timedelta`` step of ``TimeDimension`` series within the grid

    .. note::

      ``start_value`` shall be:
        - either ``datetime.datetime`` object with explicit ``timezone``
        - or ``datetime.isoformat()`` with explicit ``timezone``
        - or a reference to an array ``attribute`` name which shall start with ``$`` *(attribute name shall be defined
          without prefix "$")*::

            custom_attributes(
                {"my_start_value": datetime.datetime(2023, 1, 1, 0, tzinfo=timezone.utc),
                }

            TimeDimension(start_value: "$my_start_value", ...)
    """

    __slots__ = ("__name", "__size", "__start_value", "__step_label", "__step")

    def __init__(
        self,
        name: str,
        size: int,
        start_value: datetime.datetime | str,
        step: datetime.timedelta,
    ) -> None:
        super().__init__(name, size, start_value=start_value, step=step)
        self.__name: str = name
        self.__size: int = size
        self.__step: datetime.timedelta = step
        self.__start_value: datetime.datetime | str = start_value
        self.logger.debug(f"dimension {name} instantiated")

    def _validate(
        self,
        name: str,
        size: int,
        start_value: datetime.datetime | str | None = None,
        step: datetime.timedelta | None = None,
        **kwargs: Any,  # noqa: ARG002
    ) -> None:
        super()._validate(name, size)
        if not isinstance(step, datetime.timedelta) or not step:
            raise DekerValidationError('TimeDimension "step" shall be a none-zero datetime.timedelta')

        if isinstance(start_value, str):
            if not start_value.startswith("$"):
                incorrect_time_dimension_error = (
                    'TimeDimension "start_value" of <str> type shall be '
                    "an iso-format datetime string with explicit tz info"
                    "or a reference to an attribute name starting with `$`"
                )
                try:
                    dt = datetime.datetime.fromisoformat(start_value)
                    if dt.tzinfo is None:
                        raise DekerValidationError(incorrect_time_dimension_error)
                except ValueError:
                    raise DekerValidationError(incorrect_time_dimension_error)

        elif isinstance(start_value, datetime.datetime):
            if start_value.tzinfo is None:
                raise DekerValidationError(
                    'TimeDimension "start_value" of <datetime> type shall be '
                    "a datetime.datetime instance with explicit tzinfo"
                )

        else:
            raise DekerValidationError(
                'TimeDimension "start_value" shall be a datetime.datetime instance with explicit tzinfo'
                "or an iso-format datetime string with explicit tz info "
                "or a reference to an attribute name starting with `$`"
            )

    @property
    def name(self) -> str:
        """Get ``TimeDimension`` name."""
        return self.__name

    @property
    def size(self) -> int:
        """Get ``TimeDimension`` size."""
        return self.__size

    @property
    def start_value(self) -> datetime.datetime | str:
        """Get ``TimeDimension`` start value: a ``datetime`` or an attribute reference ``"$name"``."""
        return self.__start_value

    @property
    def step(self) -> datetime.timedelta:
        """Get ``TimeDimension`` step."""
        return self.__step

    @property
    def as_dict(self) -> dict:
        """Serialize ``TimeDimension`` into dict."""
        return {
            "name": self.name,
            "size": self.size,
            "step": self.step,
            "start_value": (
                self.start_value.isoformat() if isinstance(self.start_value, datetime.datetime) else self.start_value
            ),
        }

    def __len__(self) -> int:
        """Get ``TimeDimension`` size."""
        return self.size

    def __repr__(self) -> str:
        s = (
            f"{self.__class__.__name__}("
            f"name={self.name!r}, "
            f"size={self.size!r}, "
            f"step={self.step!r}, "
            f"start_value={self.start_value!r})"
        )
        return s

    def __str__(self) -> str:
        return str(self.size)

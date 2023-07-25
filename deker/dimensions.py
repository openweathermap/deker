from __future__ import annotations

import datetime

from typing import Any, Optional, Tuple, Union

from deker.ABC.base_dimension import BaseDimension
from deker.errors import DekerValidationError
from deker.log import SelfLoggerMixin
from deker.types import Labels, Scale


class IndexLabels(tuple, SelfLoggerMixin):
    """A tuple providing a mapping of values labels to their indexes.

    Provides direct and reversed ordered mappings of dimension's values names (labels)
    to their position in the axis flat array (indexes).

    Labels shall be unique values within the full scope of the passed object.
    Valid Labels are list or tuple of unique strings, integers or floats:
            ["name1", "name2", ..., "nameN"] | (0.2, 0.1, 0.4, 0.3)
    """

    @classmethod
    def __validate(cls, labels: Optional[Labels]) -> Tuple[Union[str, int, float], ...]:
        error = DekerValidationError(
            "Labels shall be a list or tuple of unique values of the same type (strings, integers or floats), "
            "not None or empty list or tuple"
        )
        if (
            not labels
            or not isinstance(labels, (list, tuple))
            or all(
                (
                    any(not isinstance(val, str) for val in labels),
                    any(not isinstance(val, int) for val in labels),
                    any(not isinstance(val, float) for val in labels),
                )
            )
        ):
            raise error
        return labels if isinstance(labels, tuple) else tuple(labels)

    def __new__(cls, labels: Optional[Labels]) -> IndexLabels:
        """Override tuple new method. Make validation.

        :param labels: labels
        """
        return super(IndexLabels, cls).__new__(cls, cls.__validate(labels))  # type: ignore[arg-type]

    def __init__(self, labels: Optional[Labels]) -> None:
        """IndexLabels constructor.
        Logs initialization start.

        :param labels: list or tuple of unique strings, integers or floats
        """
        self.logger.debug("labels initialized")

    @property
    def first(self) -> Union[str, int, float]:
        """Get first label."""
        return self[0]

    @property
    def last(self) -> Union[str, int, float]:
        """Get last label."""
        return self[-1]

    def name_to_index(self, name: Union[str, int, float]) -> Optional[int]:
        """Get label index by its name.

        :param name: label name
        """
        try:
            return self.index(name)
        except ValueError as e:
            mes = str(e) + f": no name {name}"
            self.logger.debug(mes)
            return None

    def index_to_name(self, idx: int) -> Optional[Union[str, int, float]]:
        """Get label name by its index.

        :param idx: label index
        """
        try:
            return self[idx]
        except IndexError as e:
            mes = str(e) + f": no index {idx}"
            self.logger.debug(mes)
            return None

    def __str__(self) -> str:
        return str(tuple(self))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"


class Dimension(SelfLoggerMixin, BaseDimension):
    """Dimension of grid axes or any other series except time (for time series use TimeDimension).

    May be used for defining the majority of parameters which may be stored in an array.
    You can use labels parameter to create a mapping of some names to the dimension indexes.
    """

    __slots__ = ("__name", "__size", "__step", "__labels", "__scale")

    def _validate(  # noqa: C901
        self,
        name: str,
        size: int,
        labels: Optional[Labels] = None,
        scale: Optional[Union[dict, Scale]] = None,
        **kwargs: Any,
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
                    f"Scale parameter value shall be an instance of deker.types.classes.Scale, not {type(self.scale)}"
                )

            if isinstance(scale, dict):
                try:
                    scale: Scale = Scale(**scale)
                except TypeError as e:
                    raise DekerValidationError(e)

            for attr in scale.__annotations__:
                value = getattr(scale, attr)
                if attr == "name":
                    if value is not None and (
                        not isinstance(value, str) or not value or value.isspace()
                    ):
                        raise DekerValidationError(
                            f"Scale attribute '{attr}' value shall be non-empty string"
                        )
                else:
                    if not isinstance(value, float):
                        raise DekerValidationError(f"Scale attribute '{attr}' value shall be float")

            scale_end_non_inclusive = scale.start_value
            scale_end_non_inclusive += scale.step * size
            scale_size = int(round(abs((scale_end_non_inclusive - scale.start_value) / scale.step)))
            if scale_size != size:
                raise DekerValidationError(
                    f"Dimension {name} wrong scale: does not match dimension size {size}"
                )

        if labels is not None:
            common_labels_exc = (
                "Labels shall be an ordered sequence or mapping of unique elements to their "
                "indexes in the dimension axis"
            )

            if not isinstance(labels, (tuple, list, dict)):
                raise DekerValidationError(common_labels_exc)

            if isinstance(labels, (list, tuple, dict)) and not labels:
                raise DekerValidationError("Label can not be empty, use None instead")

            if len(labels) != size:
                raise DekerValidationError("Labels quantity do not match dimension size")

            if isinstance(labels, dict):
                for key, val in labels.items():
                    if not isinstance(key, str) or not isinstance(val, int):
                        raise DekerValidationError(common_labels_exc)

            if isinstance(labels, (list, tuple)):
                if all(isinstance(el, (list, tuple)) for el in labels):
                    keys = {el[0] for el in labels}
                    vals = {el[1] for el in labels}
                    if len(keys) < len(labels) or len(vals) < len(labels):
                        raise DekerValidationError(common_labels_exc)
                    if not all(isinstance(key, str) for key in keys) or not all(
                        isinstance(val, int) for val in vals
                    ):
                        raise DekerValidationError(common_labels_exc)
                else:
                    if len(set(labels)) < len(labels) or all(
                        (
                            all(not isinstance(el, str) for el in labels),
                            all(not isinstance(el, int) for el in labels),
                            all(not isinstance(el, float) for el in labels),
                        )
                    ):
                        raise DekerValidationError(common_labels_exc)

    def __init__(
        self,
        name: str,
        size: int,
        labels: Optional[Labels] = None,
        scale: Optional[dict] = None,
        **kwargs: Any,
    ) -> None:
        """Dimension constructor.
        Validates parameters, sets them to default and converts labels and scale to a corresponding type.

        :param name: dimension name (e.g. "time")
        :param size: dimension cells quantity
        :param labels: ordered mapping of dimension values names to their indexes; default None
            ["name1", "name2", ..., "nameN"] |
            ("name1", "name2", ..., "nameN") |
            [("name1", 0), ("name2", 1), ..., ("nameN", N)] |
            (("name1", 0), ("name2", 1), ..., ("nameN", N)) |
            {"name1": 0, "name2": 1, ..., "nameN": N}
        :param scale: a description of dimension regular scale; default None
        :param kwargs: any keyword arguments
        """
        super().__init__(name, size, **kwargs)
        self._validate(name, size, labels, scale)
        self.__name: str = name
        self.__size: int = size
        self.__step: int = 1  # step attribute kept for further usage unification
        self.__labels: Optional[IndexLabels] = labels if not labels else IndexLabels(labels)
        self.__scale: Optional[Scale] = scale if not scale else Scale(**scale)
        self.logger.debug(f"Dimension {name} instantiated")

    @property
    def step(self) -> int:
        """Name getter."""
        return self.__step

    @property
    def name(self) -> str:
        """Name getter."""
        return self.__name

    @property
    def size(self) -> int:
        """Dimension size getter."""
        return self.__size

    @property
    def labels(self) -> Optional[IndexLabels]:
        """Dimension values labels getter."""
        return self.__labels

    @property
    def scale(self) -> Optional[Scale]:
        """Dimension regular scale getter."""
        return self.__scale

    @property
    def as_dict(self) -> dict:
        """Serialize self attributes into dict."""
        dic = {"name": self.name, "size": self.size, "step": self.step}
        if self.labels:
            dic["labels"] = self.labels
        if self.scale:
            dic["scale"] = self.scale._asdict() if self.scale else self.scale
        return dic

    def __len__(self) -> int:
        return self.size

    def __repr__(self) -> str:
        s = (
            f"{self.__class__.__name__}("
            f"name={self.name!r}, "
            f"size={self.size!r}, "
            f"step={self.step!r}"
            f")"
        )
        if self.labels:
            s = s[:-1] + f", labels={self.labels})"
        if self.scale:
            s = s[:-1] + f", scale={self.scale})"
        return s

    def __str__(self) -> str:
        return str(self.size)


class TimeDimension(SelfLoggerMixin, BaseDimension):
    """Dimension for time series."""

    __slots__ = ("__name", "__size", "__start_value", "__step_label", "__step")

    def __init__(
        self,
        name: str,
        size: int,
        start_value: datetime.datetime,
        step: datetime.timedelta,
    ) -> None:
        """TimeDimension initialization.

        :param name: dimension name (e.g. "time")
        :param size: dimension cells quantity
        :param start_value: time mark of the first cell (index 0) in a dimension
            (may be used for ranges and series building);
         Shall be:
          - either datetime.datetime object with explicit timezone
          - or datetime.isoformat() with explicit timezone
          - or a reference to an array attribute name
                Reference shall start with `$`, attribute name shall be without prefix:
                TimeDimension(start_value: "$my_start_value", ...)
                custom_attributes({"my_start_value": datetime.datetime(now, tzinfo=timezone.utc)}
        :param step: step of dimension series within the values grid; default 1
            (e.g. time_dim[0] = 0 and time_dim[1] = 3 and time_dim[2] = 6 --> step = 3)
        """
        super().__init__(name, size, **{"start_value": start_value, "step": step})  # type: ignore[arg-type]
        self.__name: str = name
        self.__size: int = size
        self.__step: datetime.timedelta = step
        self.__start_value: datetime.datetime = start_value
        self.logger.debug(f"dimension {name} instantiated")

    def _validate(
        self,
        name: str,
        size: int,
        start_value: Union[datetime.datetime, str],
        step: datetime.timedelta,
        **kwargs: Any,
    ) -> None:
        super()._validate(name, size)
        if not isinstance(step, datetime.timedelta) or not step:
            raise DekerValidationError(
                'TimeDimension "step" shall be a none-zero datetime.timedelta'
            )

        if isinstance(start_value, str):
            if not start_value.startswith("$"):
                incorrect_time_dimension_error = (
                    'TimeDimension "start_value" of <str> type shall be '
                    "an iso-format datetime string with explicit tz info"
                    "or a reference to an attribute name starting with `$`"
                )
                try:
                    dt = datetime.datetime.fromisoformat(start_value)  # type: ignore[arg-type]
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
        """Name getter."""
        return self.__name

    @property
    def size(self) -> int:
        """Dimension size getter."""
        return self.__size

    @property
    def start_value(self) -> Union[datetime.datetime, str]:
        """Dimension start value getter."""
        return self.__start_value

    @property
    def step(self) -> datetime.timedelta:
        """Dimension step getter."""
        return self.__step

    @property
    def as_dict(self) -> dict:
        """Serialize self attributes into dict."""
        return {
            "name": self.name,
            "size": self.size,
            "step": self.step,
            "start_value": (
                self.start_value.isoformat()
                if isinstance(self.start_value, datetime.datetime)
                else self.start_value
            ),
        }

    def __len__(self) -> int:
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

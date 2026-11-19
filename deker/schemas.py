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

import datetime

from collections.abc import Sequence
from enum import Enum
from typing import Optional, Union

import numpy as np

from attr import dataclass
from deker_tools.time import get_utc

from deker.ABC.base_schemas import BaseArraysSchema, BaseAttributeSchema, BaseDimensionSchema
from deker.dimensions import validate_labels
from deker.errors import DekerInvalidSchemaError, DekerValidationError
from deker.log import SelfLoggerMixin
from deker.types import DimensionType, DTypeEnum, Labels, Numeric, Scale


@dataclass(repr=True)
class AttributeSchema(SelfLoggerMixin, BaseAttributeSchema):
    """Schema of an attribute.

    Describes requirements for the primary or custom attribute of Array or VArray. A primary attribute is part of
    the identity of an array: it is set on creation, never changes and the arrays are filtered by it. A custom
    attribute keeps extra information and may be updated.

    :param name: attribute name
    :param dtype: attribute data type: ``int``, ``float``, ``complex``, a NumPy number type, ``str``, ``tuple``
      or ``datetime.datetime``
    :param primary: boolean flag for setting attribute as a key (``True``) or custom (``False``)
    :raises DekerValidationError: if the name is not a non-empty string, the dtype is not one of the listed or
      ``primary`` is not a boolean
    """

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        super().__attrs_post_init__()
        try:
            self.dtype = DTypeEnum(self.dtype).value
        except (ValueError, KeyError):
            raise DekerValidationError(f"Invalid dtype value {self.dtype}")

        if not isinstance(self.primary, bool):
            raise DekerValidationError(f"Invalid primary value {self.primary}; boolean expected")
        self.logger.debug("%s instantiated", self.name)

    @property
    def as_dict(self) -> dict:
        """Serialize Attribute schema as dict."""
        d = {"name": self.name, "primary": self.primary}
        if self.dtype == str:
            d["dtype"] = "string"
        elif self.dtype == tuple:
            d["dtype"] = "tuple"
        elif self.dtype == datetime.datetime:
            d["dtype"] = "datetime"
        else:
            try:
                d["dtype"] = DTypeEnum.get_name(DTypeEnum(self.dtype))
            except (KeyError, ValueError) as e:
                raise DekerInvalidSchemaError(f'Schema "{self.__class__.__name__}" is invalid/corrupted: {e}')

        return d


@dataclass(repr=True)
class DimensionSchema(SelfLoggerMixin, BaseDimensionSchema):
    """Schema of a ``Dimension`` for the majority of series except time.

    For time series use ``TimeDimensionSchema``.

    :param name: dimension unique name
    :param size: dimension cells quantity
    :param labels: an ordered list or tuple of unique cells names: all strings (non-blank) or all finite floats,
      one per cell, so its length equals the dimension size. Integers and booleans are not labels.

      May be useful if some data from different sources is grouped in one array, e.g.::

        DimensionSchema(
            name="weather_data",
            size=3,
            labels=["temperature", "pressure", "humidity"]
        )

      what means, that `pressure` data can be always found in such dimension at index ``1`` (not ``0``, nor ``-1``).

    :param scale: optional parameter; represents a regular scale description for dimension axis.
      For example, we describe dimensions for the Earth's latitude and longitude grades::

         dims = [
                DimensionSchema(name="y", size=721),
                DimensionSchema(name="x", size=1440),
             ]

      Such description may exist, but it's not quite sufficient. We can provide information for the grid::

        dims = [
                DimensionSchema(
                    name="y",
                    size=721,
                    scale=Scale(start_value=90, step=-0.25, name="lat")
                ),
                DimensionSchema(
                    name="x",
                    size=1440,
                    scale=Scale(start_value=-180, step=0.25, name="lon")
                ),
            ]

      This extra information permits us provide fancy indexing by lat/lon coordinates in degrees::

         EarthGridArray[1, 1] == EarthGridArray[89.75, -179.75]

    .. note::
       Parameters ``scale`` and ``labels`` provide a possibility of fancy indexing.
       If you are bored with calculating index positions, you may slice by labels instead them.

    .. attention:: Either ``scale`` or ``labels`` parameter or none of them shall be passed to the constructor.
       Not both of them.

    :raises DekerValidationError: if the name or the size is invalid, both labels and scale are passed, or the labels
      or the scale break their rules
    """

    labels: Optional["Labels"] = None
    scale: Union["Scale", dict] | None = None

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        super().__attrs_post_init__()
        if self.labels and self.scale:
            raise DekerValidationError(
                f"Invalid DimensionSchema {self.name} arguments: either `labels` or `scale` or none of them should "
                f"be passed, not both"
            )
        if self.labels is not None:
            validate_labels(self.name, self.size, self.labels)
        if self.scale is not None:
            if not isinstance(self.scale, (Scale, dict)):
                raise DekerValidationError(
                    f"Scale parameter value shall be an instance of deker.types.classes.Scale "
                    f"or its dict mapping, not {type(self.scale)}"
                )
            if isinstance(self.scale, dict):
                try:
                    self.scale = Scale(**self.scale)
                except TypeError as e:
                    raise DekerValidationError(e)
            for attr in self.scale._fields:
                value = getattr(self.scale, attr)
                if attr == "name":
                    if value is not None and (not isinstance(value, str) or not value or value.isspace()):
                        raise DekerValidationError(f"`Scale attribute '{attr}' value shall be non-empty string")
                else:
                    if not isinstance(value, float):
                        raise DekerValidationError(f"Scale attribute '{attr}' value shall be float")

        self.logger.debug("%s instantiated", self.name)

    @property
    def as_dict(self) -> dict:
        """Serialize DimensionSchema into dictionary."""
        d = super().as_dict
        d["labels"] = self.labels
        d["scale"] = self.scale._asdict() if isinstance(self.scale, Scale) else self.scale
        d["type"] = DimensionType.generic.value
        return d


@dataclass(repr=True)
class TimeDimensionSchema(SelfLoggerMixin, BaseDimensionSchema):
    """Dimension schema for time series.

    Describes data distribution within some time: the value of the cell ``i`` is ``start_value + step * i``.

    :param name: dimension name
    :param size: dimension cells quantity
    :param start_value: time of the dimension's zero-point: a ``datetime.datetime`` with a timezone, such an ISO
      string (both are converted to UTC) or a reference to an attribute
    :param step: a non-zero ``datetime.timedelta``, the common time step for all the ``Arrays`` or ``VArrays``
      of the collection
    :raises DekerValidationError: if the name or the size is invalid, the step is not a non-zero ``timedelta``,
      or the start value is neither a timezone-aware datetime, nor such an ISO string, nor an attribute reference

    .. note::
       For setting a **common** start date and time for all the arrays use ``datetime.datetime`` with explicit timezone
       (``tzinfo``) or a string of ``datetime.isoformat()``.

       For setting an **individual** start date and time for each array pass a name of the attribute in the attributes
       list. Such reference shall start with ``$``, e.g. ``start_value="$my_attr_name"``.

       In this case the schema of such attribute (typed ``datetime.datetime``) must be provided by AttributeSchema
       and you shall pass ``datetime.datetime`` with explicit ``timezone`` on a ``Array`` or ``VArray`` creation to
       the correspondent attribute.
    """

    start_value: datetime.datetime | str
    step: datetime.timedelta

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        super().__attrs_post_init__()
        if not isinstance(self.step, datetime.timedelta) or not self.step:
            raise DekerValidationError('TimeDimension schema "step" shall be a datetime.timedelta instance')

        if isinstance(self.start_value, str):
            if not self.start_value or self.start_value.isspace():
                raise DekerValidationError(
                    'TimeDimension schema "start_value" shall be a non-empty string or a datetime.datetime instance'
                )

            if not self.start_value.startswith("$"):
                try:
                    self.start_value = get_utc(self.start_value)
                except ValueError:
                    raise DekerValidationError(
                        'TimeDimension schema "start_value" shall have a datetime.datetime type '
                        "with an explicit timezone or a string reference to a key or custom attribute name"
                    )
        elif isinstance(self.start_value, datetime.datetime):
            self.start_value = get_utc(self.start_value)
        else:
            raise DekerValidationError(
                'TimeDimension schema "start_value" shall be a datetime.datetime instance '
                "or an iso-format datetime.datetime string "
                "or a reference to an attribute name starting with `$`"
            )
        self.logger.debug("%s instantiated", self.name)

    @property
    def as_dict(self) -> dict:
        """Serialize TimeDimensionSchema into dictionary."""
        d = super().as_dict
        d["type"] = DimensionType.time.value
        d["start_value"] = (
            self.start_value.isoformat() if isinstance(self.start_value, datetime.datetime) else self.start_value
        )
        d["step"] = {
            "days": self.step.days,
            "seconds": self.step.seconds,
            "microseconds": self.step.microseconds,
        }
        return d


def __common_arrays_attributes_post_init__(self: BaseArraysSchema) -> None:  # noqa: N807
    """Validate attributes for ArraySchema or VArraySchema.

    :param self: ArraySchema or VArraySchema instance
    :raises DekerValidationError: if the attributes are not ``AttributeSchemas`` with unique names, or a time
      dimension refers to an attribute that is missing or is not a ``datetime.datetime``
    """
    if self.attributes is None:
        self.attributes = tuple()

    if not isinstance(self.attributes, (tuple, list)):
        raise DekerValidationError("Attributes shall be a list or tuple of AttributeSchema")
    if any(not isinstance(a, AttributeSchema) for a in self.attributes):
        raise DekerValidationError("Attributes shall be a list or tuple of AttributeSchema")

    if len({attr.name for attr in self.attributes}) != len(self.attributes):
        raise DekerValidationError("Attribute name shall be unique")

    time_attribute_name = None
    for dim in self.dimensions:
        if isinstance(dim, TimeDimensionSchema) and isinstance(dim.start_value, str):
            time_attribute_name = dim.start_value[1:]

        if time_attribute_name and not any(
            attr.name == time_attribute_name and attr.dtype == datetime.datetime for attr in self.attributes
        ):
            raise DekerValidationError(f"No {time_attribute_name} attribute with dtype `datetime.datetime` is provided")

    if self.attributes and isinstance(self.attributes, list):
        self.attributes = tuple(self.attributes)


@dataclass(repr=True, kw_only=True)
class VArraySchema(SelfLoggerMixin, BaseArraysSchema):
    """VArray schema - a common schema for all VArrays in Collection.

    Virtual array is an "array of arrays": it is split by its virtual grid into ordinary ``Arrays``, each keeping
    one part of the data in a file of its own.

    This schema describes the structure of the collection virtual arrays and how they are split into arrays.
    The ``ArraySchema`` of the ``Arrays`` is constructed from the ``VArraySchema`` automatically: the dimensions are
    divided by the ``vgrid`` and the ``Arrays`` get the primary attributes ``vid`` and ``v_position``.

    :param dtype: an object, representing final data type of every array, e.g. ``int`` or ``numpy.float32``;
      ``int``, ``float`` and ``complex`` stand for ``numpy.int64``, ``numpy.float64`` and ``numpy.complex128``
    :param dimensions: an ordered sequence of DimensionSchemas and/or TimeDimensionSchemas with unique names
    :param attributes: an optional sequence of AttributeSchema. If there is a TimeDimensionSchema, which ``start_value``
      parameter refers to some attribute name, attributes must contain at least such attribute schema, e.g.::

        AttributeSchema(
            name="forecast_dt",
            dtype=datetime.datetime,
            primary=False  # or True
          )

    :param vgrid: an ordered sequence of positive integers; used for splitting ``VArray`` into ordinary ``Arrays``.

      Each VArray dimension "size" shall be divided by the correspondent integer without remainders, thus an
      ``Array's`` shape is created. If there is no need to split any dimension, its vgrid positional integer
      shall be ``1``.

    :param arrays_shape: an ordered sequence of positive integers; used for setting the shape of ordinary ``Arrays``
      laying under a ``VArray``.

      Each integer in the sequence represents the total quantity of cells in the correspondent dimension
      of each ordinary ``Array``. Each ``VArray`` dimension "size" shall be divided by the correspondent integer
      without remainders, thus a ``VArray's`` vgrid is created.

    :param fill_value: an optional value for filling in empty cells; if ``None``, the minimal value of an integer
      dtype or ``nan`` of a floating or complex one is used. Numpy ``nan`` can be used only for floating numpy dtypes.
    :raises DekerValidationError: if the dimensions, the dtype, the fill value or the attributes are invalid, or
      neither or both of ``vgrid`` and ``arrays_shape`` are passed, or they do not divide the dimensions
    """

    vgrid: list[int] | tuple[int, ...] | None = None
    arrays_shape: list[int] | tuple[int, ...] | None = None
    fill_value: Numeric | None = None
    attributes: Sequence[AttributeSchema] | None = None

    def __attrs_post_init__(self) -> None:
        """Validate schema, convert `vgrid` or `arrays_shape` to tuple and calculate the other grid splitter."""
        super().__attrs_post_init__()
        __common_arrays_attributes_post_init__(self)

        # get all grid splitters, passed by user
        splitters = {attr: getattr(self, attr) for attr in ("vgrid", "arrays_shape") if getattr(self, attr)}

        # validate found splitters; should be just one parameter
        if len(splitters) < 1:
            raise DekerValidationError("Either `vgrid` or `arrays_shape` shall be passed")
        if len(splitters) > 1:
            raise DekerValidationError("Either `vgrid` or `arrays_shape` shall be passed, not both")

        # extract grid splitter and its value
        splitter, value = tuple(splitters.items())[0]

        # validate splitter value: splitters holds the truthy one only
        if (
            not isinstance(value, (list, tuple))
            or not value
            or len(value) != len(self.dimensions)
            or not all(isinstance(item, int) for item in value)
            or any(item < 1 for item in value)
        ):
            raise DekerValidationError(
                f"{splitter.capitalize()} shall be a list or tuple of positive integers, with total elements "
                f"quantity equal to dimensions quantity ({len(self.dimensions)})"
            )
        for n, dim in enumerate(self.dimensions):
            remainder = dim.size % value[n]
            if remainder != 0:
                raise DekerValidationError(
                    f"Dimensions shall be split by {splitter} into equal parts without remainder: "
                    f"{dim.name} size % {splitter} element = "
                    f"{dim.size} % {value[n]} = {remainder}"
                )

        # convert splitter value to tuple
        if isinstance(value, list):
            setattr(self, splitter, tuple(value))

        # calculate second splitter name and value; set its value as tuple
        if splitter == "vgrid":
            other_splitter = "arrays_shape"
        else:
            other_splitter = "vgrid"
        setattr(
            self,
            other_splitter,
            tuple(int(i) for i in np.asarray(self.shape) // np.asarray(value)),
        )
        self.logger.debug("instantiated")

    @property
    def as_dict(self) -> dict:
        """Serialize as dict."""
        d = super().as_dict
        d["vgrid"] = self.vgrid
        return d


@dataclass(repr=True, kw_only=True)
class ArraySchema(SelfLoggerMixin, BaseArraysSchema):
    """Array schema - a common schema for all the ``Arrays`` in ``Collection``.

    It describes the structure of the collection arrays.

    :param dimensions: an ordered sequence of ``DimensionSchemas`` and/or ``TimeDimensionSchemas``
    :param dtype: an object, representing final data type of every array, e.g. ``int`` or ``numpy.float32``;
      ``int``, ``float`` and ``complex`` stand for ``numpy.int64``, ``numpy.float64`` and ``numpy.complex128``
    :param attributes: an optional sequence of AttributeSchema. If there is a TimeDimensionSchema, which ``start_value``
      parameter refers to some attribute name, attributes must contain at least such attribute schema, e.g.::

        AttributeSchema(
            name="forecast_dt",
            dtype=datetime.datetime,
            primary=False  # or True
          )

    :param fill_value: an optional value for filling in empty cells; if ``None``, the minimal value of an integer
      dtype or ``nan`` of a floating or complex one is used. Numpy ``nan`` can be used only for floating numpy dtypes.
    :raises DekerValidationError: if the dimensions, the dtype, the fill value or the attributes are invalid
    """

    fill_value: Numeric | None = None
    attributes: Sequence[AttributeSchema] | None = None

    def __attrs_post_init__(self) -> None:
        """Validate schema."""
        super().__attrs_post_init__()
        __common_arrays_attributes_post_init__(self)
        self.logger.debug("instantiated")


# moved here from deker.types.private.enums because of circular import
class SchemaTypeEnum(Enum):
    """The two kinds of a collection schema: ``array`` for ``ArraySchema``, ``varray`` for ``VArraySchema``."""

    varray = VArraySchema
    array = ArraySchema

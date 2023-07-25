import datetime

from typing import List, Optional, Tuple, Union

import numpy as np

from attr import dataclass

from deker.ABC.base_schemas import BaseArraysSchema, BaseAttributeSchema, BaseDimensionSchema
from deker.errors import DekerInvalidSchemaError, DekerValidationError
from deker.log import SelfLoggerMixin
from deker.tools.time import convert_to_utc
from deker.types import DTypeEnum, Labels, Numeric
from deker.types.classes import Scale
from deker.types.enums import DimensionType


@dataclass(repr=True)
class AttributeSchema(SelfLoggerMixin, BaseAttributeSchema):
    """Schema of an attribute.

    Describes requirements to Arrays' or VArrays' key or custom attribute.
    :param name: attribute name
    :param dtype: attribute data type
    :param primary: boolean flag for setting attribute as a key (True) or custom (False)
    """

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        super().__attrs_post_init__()
        try:
            self.dtype = DTypeEnum(self.dtype).value  # noqa
        except (ValueError, KeyError):
            raise DekerValidationError(f"Invalid dtype value {self.dtype}")

        if not isinstance(self.primary, bool):
            raise DekerValidationError(f"Invalid primary value {self.primary}; boolean expected")
        self.logger.debug(f"{self.name} instantiated")

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
                raise DekerInvalidSchemaError(
                    f'Schema "{self.__class__.__name__}" is invalid/corrupted: {e}'
                )

        return d


@dataclass(repr=True)
class DimensionSchema(SelfLoggerMixin, BaseDimensionSchema):
    """Dimension schema for the majority of series except time.

    For time series use TimeDimensionSchema.
    :param name: dimension name
    :param size: dimension cells quantity

    Parameters "scale" and "labels" provide a possibility of fancy indexing.
    If you are bored with calculating index positions, you may slice by labels instead them.
    Either "scale" or "labels" arguments shall be passed or none of them.

    :param labels: optional parameter; represents an ordered sequence of unique cells names
        or a mapping of unique cells names to their position (index) in dimension row.
        Size of such sequence or mapping shall be equal to the dimension size.
        May be useful if some data from different sources is grouped in one array, e.g.
        `DimensionSchema(name="weather_data", size=3, labels=["temperature", "pressure", "humidity"])`
        What means, that `pressure` data can be always found in such dimension at index 1 (nor 0, nor -1).
    :param scale: optional parameter; represents a regular scale description for dimension axis.
        E.g. we describe a dimensions schema for the Earth's latitude and longitude grades:
            dims = [
                    DimensionSchema(name="y", size=721),
                    DimensionSchema(name="x", size=1440),
                ]
        Such description may exist, but it's not quite sufficient. We can provide information for the grid:
            dims = [
                        DimensionSchema(name="y", size=721, scale=Scale(start_value=90, step=-0.5),
                        DimensionSchema(name="x", size=1440, scale=Scale(start_value=-180, step=0.5)),
                    ]
        This extra information permits us provide fancy indexing by lat/lon coordinates in degrees:
        EarthGridArray[1, 1] == EarthGridArray[89.5, -179.5]
    """

    labels: Optional["Labels"] = None
    scale: Optional[Union["Scale", dict]] = None

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        super().__attrs_post_init__()
        if self.labels and self.scale:
            raise DekerValidationError(
                f"Invalid DimensionSchema {self.name} arguments: either `labels` or `scale` or none of them should "
                f"be passed, not both"
            )
        if self.labels is not None:
            message = "Labels shall be a sequence of unique strings or a mapping of unique strings to unique ints"
            if not self.labels:
                raise DekerValidationError(message)
            if isinstance(self.labels, Scale) or not isinstance(self.labels, (dict, list, tuple)):
                raise DekerValidationError(message)
        if self.scale is not None:
            if not isinstance(self.scale, (Scale, dict)):
                raise DekerValidationError(
                    f"Scale parameter value shall be an instance of deker.types.classes.Scale "
                    f"or its dict mapping, not {type(self.scale)}"
                )
            if isinstance(self.scale, dict):
                try:
                    self.scale = Scale(**self.scale)
                except AttributeError as e:
                    raise DekerValidationError(e)
            for attr in self.scale.__annotations__:
                value = getattr(self.scale, attr)
                if attr == "name":
                    if value is not None and (
                        not isinstance(value, str) or not value or value.isspace()
                    ):
                        raise DekerValidationError(
                            f"`Scale attribute '{attr}' value shall be non-empty string"
                        )
                else:
                    if not isinstance(value, float):
                        raise DekerValidationError(f"Scale attribute '{attr}' value shall be float")

        self.logger.debug(f"{self.name} instantiated")

    @property
    def as_dict(self) -> dict:
        """Serialize DimensionSchema into dictionary."""
        d = super().as_dict
        d["labels"] = self.labels
        d["scale"] = self.scale._asdict() if self.scale else self.scale
        d["type"] = DimensionType.generic.value
        return d


@dataclass(repr=True)
class TimeDimensionSchema(SelfLoggerMixin, BaseDimensionSchema):
    """Dimension schema for time series.

    Used to describe data representation within some time.
    :param name: dimension name
    :param size: dimension cells quantity
    :param start_value: time of the dimension's zero-point;
        - For setting a common start date and time for all the arrays:
            use `datetime.datetime` with explicit timezone (`tzinfo`) or a datetime isoformatted-string
        - For setting individual start time for each array:
            pass a name of the attribute in the attributes list. Such reference shall start with `$`,
            e.g. `start_value="$my_attr_name"`. In this case the schema of such attribute
            (typed `datetime.datetime`) must be provided by AttributeSchema and you shall pass
            datetime.datetime with explicit timezone on an array creation to the correspondent attribute.

    :param step: Use `datetime.timedelta` or its mapping to set a common time step for all the arrays.
    """

    start_value: Union[datetime.datetime, str]
    step: datetime.timedelta

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        super().__attrs_post_init__()
        if not isinstance(self.step, datetime.timedelta) or not self.step:
            raise DekerValidationError(
                'TimeDimension schema "step" shall be a datetime.timedelta instance'
            )

        if isinstance(self.start_value, str):
            if not self.start_value or self.start_value.isspace():
                raise DekerValidationError(
                    'TimeDimension schema "start_value" shall be a non-empty string or a datetime.datetime instance'
                )

            if not self.start_value.startswith("$"):
                try:
                    self.start_value = convert_to_utc(self.start_value)
                except ValueError:
                    raise DekerValidationError(
                        'TimeDimension schema "start_value" shall have a datetime.datetime type '
                        "with an explicit timezone or a string reference to a key or custom attribute name"
                    )
        elif isinstance(self.start_value, datetime.datetime):
            self.start_value = convert_to_utc(self.start_value)
        else:
            raise DekerValidationError(
                'TimeDimension schema "start_value" shall be a datetime.datetime instance '
                "or an iso-format datetime.datetime string "
                "or a reference to an attribute name starting with `$`"
            )
        self.logger.debug(f"{self.name} instantiated")

    @property
    def as_dict(self) -> dict:
        """Serialize TimeDimensionSchema into dictionary."""
        d = super().as_dict
        d["type"] = DimensionType.time.value
        d["start_value"] = (
            self.start_value.isoformat()
            if isinstance(self.start_value, datetime.datetime)
            else self.start_value
        )
        d["step"] = {
            "days": self.step.days,
            "seconds": self.step.seconds,
            "microseconds": self.step.microseconds,
        }
        return d


def __common_arrays_attributes_post_init__(self: BaseArraysSchema) -> None:
    """Validate attributes for (V)ArraySchema.

    :param self: (V)ArraySchema instance
    """
    if not isinstance(self.attributes, (tuple, list)):
        raise DekerValidationError("Attributes shall be a list or tuple of AttributeSchema")
    if any(not isinstance(a, AttributeSchema) for a in self.attributes):
        raise DekerValidationError("Attributes shall be a list or tuple of AttributeSchema")

    if len(set(attr.name for attr in self.attributes)) != len(self.attributes):
        raise DekerValidationError("Attribute name shall be unique")

    time_attribute_name = None
    for dim in self.dimensions:
        if isinstance(dim, TimeDimensionSchema):
            if isinstance(dim.start_value, str):
                time_attribute_name = dim.start_value[1:]

        if time_attribute_name:
            if not any(
                attr.name == time_attribute_name and attr.dtype == datetime.datetime
                for attr in self.attributes
            ):
                raise DekerValidationError(
                    f"No {time_attribute_name} "
                    f"attribute with dtype `datetime.datetime` is provided"
                )

    if self.attributes and isinstance(self.attributes, list):
        self.attributes = tuple(self.attributes)


@dataclass(repr=True, kw_only=True)
class VArraySchema(SelfLoggerMixin, BaseArraysSchema):
    """VArray schema - a common schema for all VArrays in Collection.

    Virtual array is an "array of arrays", or an "image of pixels".
    If we consider VArray as an image - it is being split by virtual grid into tiles. In this case each tile -
    is an ordinary array.
    This schema describes the structure of the collection virtual arrays and how it is being split into arrays.
    ArraySchema is being automatically constructed from VArraySchema.
    :param dtype: an object, representing final data type of every array, e.g. `int` or `numpy.float32`
    :param dimensions: an ordered sequence of DimensionSchemas and/or TimeDimensionSchemas;
        Size of each dimension represents total varray cells (points/pixels) quantity.
        TimeDimensionSchema "start_value" parameter may refer to an attribute field "name", starting with `$`, e.g.
        `TimeDimensionSchema(name="forecasts", size=129, start_value = "$forecast_dt", step = timedelta(hours=3))`
        In this case the schema of such attribute (typed `datetime.datetime`) must be provided by AttributeSchema.
    :param vgrid: an ordered sequence of positive integers; used for splitting virtual array into ordinary arrays.
        Each integer in the sequence represents the total number of arrays in the correspondent dimension.
         Each varray dimension "size" shall be divided by the correspondent integer
        without remainders. If there is no need to split any dimension, its vgrid positional integer shall be 1.
    :param fill_value: an optional value for filling in empty cells;
        If None - default value for each dtype will be used. Numpy `nan` can be used only for floating numpy dtypes.
    :param attributes: an optional sequence of AttributeSchema. If there is a TimeDimensionSchema, which "start_value"
    parameter refers to some attribute name, attributes must contain at least such attribute schema, e.g.
    `AttributeSchema(name="forecast_dt", dtype=datetime.datetime, primary=False (or True))`
    """

    vgrid: Union[List[int], Tuple[int, ...]]
    fill_value: Union[Numeric, type(np.nan), None] = None  # type: ignore[valid-type]
    attributes: Union[List[AttributeSchema], Tuple[AttributeSchema, ...]] = tuple()

    def __attrs_post_init__(self) -> None:
        """Validate schema, convert vgrid to tuple and calculate arrays_shape."""
        super().__attrs_post_init__()
        __common_arrays_attributes_post_init__(self)
        if (
            not isinstance(self.vgrid, (list, tuple))
            or not self.vgrid
            or len(self.vgrid) != len(self.dimensions)
            or not all(isinstance(g, int) for g in self.vgrid)
            or any(g < 1 for g in self.vgrid)
        ):
            raise DekerValidationError(
                f"Vgrid shall be a list or tuple of positive integers, with total elements quantity "
                f"equal to dimensions quantity ({len(self.dimensions)}) "
            )
        for n, dim in enumerate(self.dimensions):
            remainder = dim.size % self.vgrid[n]  # type: ignore[valid-type]
            if remainder != 0:
                raise DekerValidationError(
                    "Dimensions shall be split by vgrid into equal parts without remainder: "
                    f"{dim.name} size % vgrid element = "
                    f"{dim.size} % {self.vgrid[n]} = {remainder}"  # type: ignore[valid-type]
                )
        if isinstance(self.vgrid, list):
            self.vgrid = tuple(self.vgrid)
        self.arrays_shape = tuple(int(i) for i in np.asarray(self.shape) // np.asarray(self.vgrid))
        self.logger.debug("instantiated")

    @property
    def as_dict(self) -> dict:
        """Serialize as dict."""
        d = super().as_dict
        d["vgrid"] = self.vgrid
        return d


@dataclass(repr=True, kw_only=True)
class ArraySchema(SelfLoggerMixin, BaseArraysSchema):
    """Array schema - a common schema for all the arrays in collection.

    It describes the structure of the collection arrays.
    :param dtype: an object, representing final data type of every array, e.g. `int` or `numpy.float32`
    :param dimensions: an ordered sequence of DimensionSchemas and/or TimeDimensionSchemas;
        The size of each dimension represents its cells quantity.
        TimeDimensionSchema "start_value" parameter may refer to an attribute field "name", starting with `$`, e.g.
        `TimeDimensionSchema(name="forecasts", size=129, start_value="$forecast_dt", step=timedelta(hours=3))`
        In this case, the schema of this attribute (typed `datetime.datetime`) must be provided by AttributeSchema
        and you shall pass `datetime.datetime` with explicit timezone on an array creation to the correspondent
        attribute.
    :param fill_value: an optional value for filling in empty cells;
        If None - default value for each dtype will be used. Numpy `nan` can be used only for floating numpy dtypes.
    :param attributes: an optional sequence of AttributeSchema. If there is a TimeDimensionSchema, which "start_value"
    parameter refers to some attribute name, attributes must contain at least such attribute schema, e.g.
    `AttributeSchema(name="forecast_dt", dtype=datetime.datetime, primary=False (or True))`
    """

    fill_value: Union[Numeric, type(np.nan), None] = None  # type: ignore[valid-type]
    attributes: Union[List[AttributeSchema], Tuple[AttributeSchema, ...]] = tuple()

    def __attrs_post_init__(self) -> None:
        """Validate schema."""
        super().__attrs_post_init__()
        __common_arrays_attributes_post_init__(self)
        self.logger.debug("instantiated")

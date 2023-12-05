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

"""Abstract interfaces for schemas."""

import datetime

from typing import List, Optional, Tuple, Type, Union

import numpy as np

from attr import dataclass

from deker.errors import DekerInvalidSchemaError, DekerValidationError
from deker.tools.schema import get_default_fill_value
from deker.types.private.enums import DTypeEnum
from deker.types.private.typings import Numeric, NumericDtypes


@dataclass(repr=True)
class BaseSchema:
    """Base schema interface."""

    name: str

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        if (
            isinstance(self.name, bool)
            or not isinstance(self.name, str)
            or not self.name
            or self.name.isspace()
        ):
            raise DekerValidationError('Dimension schema "name" shall be a non-empty string')

    @property
    def as_dict(self) -> dict:
        """Serialize schema as dict."""
        return {"name": self.name}

    def __str__(self) -> str:
        return self.__repr__()


@dataclass(repr=True)
class BaseAttributeSchema(BaseSchema):
    """Base schema of attribute."""

    dtype: Type[Union[int, float, complex, str, tuple, datetime.datetime]]
    primary: bool


@dataclass(repr=True)
class BaseDimensionSchema(BaseSchema):
    """Dimension base schema interface."""

    size: int

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        super().__attrs_post_init__()
        if isinstance(self.size, bool) or not isinstance(self.size, int) or not self.size:
            raise DekerValidationError('Dimension schema "size" shall be a positive integer')

    @property
    def as_dict(self) -> dict:
        """Serialize DimensionSchema as dict."""
        d = super().as_dict
        d["size"] = self.size
        return d


@dataclass(repr=True, kw_only=True)
class BaseArraysSchema:
    """Base class for schema."""

    dtype: Type[Numeric]
    fill_value: Union[Numeric, type(np.nan), None]  # type: ignore[valid-type]
    dimensions: Union[List[BaseDimensionSchema], Tuple[BaseDimensionSchema, ...]]
    attributes: Union[List[BaseAttributeSchema], Tuple[BaseAttributeSchema, ...], None]

    @property
    def primary_attributes(self) -> Optional[Tuple[BaseAttributeSchema, ...]]:
        """Return only primary attributes."""
        attrs = tuple(attr for attr in self.attributes if attr.primary)
        return attrs if attrs else None

    @property
    def custom_attributes(self) -> Optional[Tuple[BaseAttributeSchema, ...]]:
        """Return only custom attributes."""
        attrs = tuple(attr for attr in self.attributes if not attr.primary)
        return attrs if attrs else None

    def __attrs_post_init__(self) -> None:
        """Validate after init."""
        dimensions_type_error = (
            "Dimensions shall be a list or tuple of DimensionSchemas or TimeDimensionSchemas"
        )
        if not isinstance(self.dimensions, (list, tuple)):
            raise DekerValidationError(dimensions_type_error)
        if not self.dimensions or any(
            not isinstance(d, BaseDimensionSchema) for d in self.dimensions
        ):
            raise DekerValidationError(dimensions_type_error)
        if isinstance(self.dimensions, list):
            self.dimensions = tuple(self.dimensions)

        if len({d.name for d in self.dimensions}) < len(self.dimensions):
            raise DekerValidationError("Dimensions shall have unique names")

        if self.dtype not in NumericDtypes:
            raise DekerValidationError(f"Invalid dtype {self.dtype}")

        try:
            if self.dtype == int:
                self.dtype = np.int64
            elif self.dtype == float:
                self.dtype = np.float64
            elif self.dtype == complex:
                self.dtype = np.complex128
            DTypeEnum(self.dtype).value  # noqa[B018]
        except ValueError:
            raise DekerValidationError(f"Invalid dtype value {self.dtype}")

        self.fill_value = (
            get_default_fill_value(self.dtype) if self.fill_value is None else self.fill_value
        )

        try:
            fill_value = self.dtype(self.fill_value)  # type: ignore[arg-type]
            np.array(fill_value, self.dtype)
            self.fill_value = fill_value
        except ValueError as e:
            raise DekerValidationError(
                f"Invalid `fill_value` value for dtype {type(self.dtype)}: {e}"
            )

        self.__shape: Tuple[int, ...] = tuple(dim.size for dim in self.dimensions)
        self.__named_shape: Tuple[Tuple[str, int], ...] = tuple(
            (dim.name, dim.size) for dim in self.dimensions
        )

    @property
    def shape(self) -> Tuple[int, ...]:
        """Get array shape."""
        return self.__shape

    @property
    def named_shape(self) -> Tuple[Tuple[str, int], ...]:
        """Get array shape mapping."""
        return self.__named_shape

    @property
    def as_dict(self) -> dict:
        """Serialize as dict."""
        error = f'Schema "{self.__class__.__name__}" is invalid/corrupted: '

        if self.dtype not in NumericDtypes:
            raise DekerInvalidSchemaError(error + f"wrong dtype {self.dtype}")
        try:
            dtype = DTypeEnum.get_name(DTypeEnum(self.dtype))
            fill_value = None if np.isnan(self.fill_value) else str(self.fill_value)  # type: ignore[arg-type]

            return {
                "dimensions": tuple(dim.as_dict for dim in self.dimensions),
                "dtype": dtype,
                "attributes": tuple(attr.as_dict for attr in self.attributes),
                "fill_value": fill_value,
            }
        except (KeyError, ValueError) as e:
            raise DekerInvalidSchemaError(error + str(e))

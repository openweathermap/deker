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

from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Type, Union

import numpy as np

from deker.dimensions import Dimension, TimeDimension
from deker.errors import DekerInvalidSchemaError
from deker.types import DTypeEnum, Numeric
from deker.validators import process_time_dimension_attrs


if TYPE_CHECKING:
    from deker.ABC.base_schemas import BaseDimensionSchema
    from deker.schemas import AttributeSchema, TimeDimensionSchema


def get_default_fill_value(dtype: Type[Numeric]) -> Any:
    """Get default fill value by dtype.

    :param dtype: schema dtype
    """
    if issubclass(dtype, np.inexact):
        return np.nan
    return np.iinfo(dtype).min  # type: ignore[type-var]


def create_dimensions(
    dimension_schemas: Tuple["BaseDimensionSchema", ...], attributes: Dict[str, Any]
) -> Tuple[Union[Dimension, TimeDimension], ...]:
    """Create Dimension and/or TimeDimension instances from dictionaries.

     Dimensions' and primary/custom attributes' schemas,
     stored as dictionaries in collection metadata, are converted into objects:
     Dimensions and/or TimeDimensions.

    :param dimension_schemas: a tuple of dimensions' schemas
    :param attributes: primary and custom attributes' joint dictionary
    """

    def create_time_dimension(tdim_schema: "TimeDimensionSchema") -> TimeDimension:
        """Create TimeDimension from schema.

        :param tdim_schema: TimeDimensionSchema
        """
        d = tdim_schema.as_dict
        if isinstance(d["start_value"], str):
            if d["start_value"].startswith("$"):
                d["start_value"] = process_time_dimension_attrs(attributes, d["start_value"][1:])
            else:
                d["start_value"] = datetime.datetime.fromisoformat(d["start_value"])

        if isinstance(d["step"], dict):
            d["step"] = datetime.timedelta(**d["step"])
        d.pop("type")
        return TimeDimension(**d)

    dimensions: List[Union[Dimension, TimeDimension]] = []
    for schema in dimension_schemas:
        if hasattr(schema, "start_value"):
            dimensions.append(create_time_dimension(schema))  # type: ignore[arg-type]
        else:
            dimensions.append(Dimension(**schema.as_dict))
    return tuple(dimensions)


def create_attributes_schema(attributes_schemas: List[dict]) -> Tuple["AttributeSchema", ...]:
    """Create AttributeSchema instances from a list of dictionaries.

    Primary and/or custom attributes' schemas, stored as dictionaries in collection metadata,
    are converted into AttributeSchema instances.

    :param attributes_schemas: a list of attributes' schemas dictionaries
    """
    from deker.schemas import AttributeSchema

    attributes = []
    try:
        for params in attributes_schemas:
            dtype = DTypeEnum[params["dtype"].split("numpy.")[-1]].value
            attr_schema = AttributeSchema(**{**params, "dtype": dtype})
            attributes.append(attr_schema)
        return tuple(attributes)
    except (KeyError, ValueError, AttributeError) as e:
        raise DekerInvalidSchemaError(f'Schema "AttributeSchema" is invalid/corrupted : {e}')


def create_dimensions_schema(dimension_schemas: List[dict]) -> Tuple["BaseDimensionSchema", ...]:
    """Create DimensionSchema and/or TimeDimensionSchema instances from a list of dictionaries.

     Dimensions' schemas, stored as dictionaries in collection metadata, are converted into objects:
     DimensionSchema and/or TimeDimensionSchema.

    :param dimension_schemas: a list of dimensions' schemas dictionaries
    """
    from deker.schemas import DimensionSchema, TimeDimensionSchema

    schemas = []
    for dim in dimension_schemas:
        if "start_value" not in dim.keys():
            converted_dim = DimensionSchema(  # type: ignore[call-arg]
                name=dim["name"], size=dim["size"], labels=dim["labels"], scale=dim.get("scale")
            )
        else:
            try:
                start_value = datetime.datetime.fromisoformat(dim["start_value"])
            except ValueError:
                start_value = dim["start_value"]

            converted_dim = TimeDimensionSchema(  # type: ignore[call-arg]
                name=dim["name"],
                size=dim["size"],
                start_value=start_value,
                step=datetime.timedelta(**dim["step"]),
            )
        schemas.append(converted_dim)
    return tuple(schemas)

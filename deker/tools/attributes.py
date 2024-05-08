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
import re

from collections import OrderedDict
from datetime import datetime
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type, Union

import numpy as np

from deker_tools.time import get_utc


if TYPE_CHECKING:
    from deker import AttributeSchema


def serialize_attribute_value(
    val: Any,
) -> Union[Tuple[str, int, float, tuple], str, int, float, tuple]:
    """Serialize attribute value.

    :param val: complex number
    """
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, np.ndarray):
        return val.tolist()  # type: ignore[attr-defined]
    if isinstance(val, complex):
        return str(val)
    if isinstance(val, np.integer):
        return int(val)
    if isinstance(val, np.floating):
        return float(val)
    if isinstance(val, np.complexfloating):
        return str(complex(val))
    if isinstance(val, (list, tuple)):
        return serialize_attribute_nested_tuples(val)  # type: ignore[arg-type]

    return val


def serialize_attribute_nested_tuples(value: Union[tuple, list]) -> Tuple[Any, ...]:
    """Serialize attribute nested tuples and their elements.

    :param value: tuple instance
    """
    serialized = []
    for el in value:
        if isinstance(el, (list, tuple)):
            val = serialize_attribute_nested_tuples(el)
        else:
            val = serialize_attribute_value(el)
        serialized.append(val)  # type: ignore[arg-type]
    return tuple(serialized)


def deserialize_attribute_value(val: Any, dtype: Type, from_tuple: bool) -> Any:
    """Deserialize attribute value.

    :param val: attribute value
    :param dtype: attribute dtype from schema or type of tuple element
    :param from_tuple: flag for tuple inner elements
    """
    if dtype == datetime:
        val = get_utc(val)
    else:
        val = dtype(val)

    if isinstance(val, (list, tuple)) and dtype == tuple:
        return deserialize_attribute_nested_tuples(val)  # type: ignore[arg-type]

    if dtype == str:
        # if the value comes from a tuple as one of its elements
        if from_tuple:
            # it may be a serialized string representation of a complex number
            complex_number_regex = re.compile(
                r"^(\()([+-]?)\d+(?:\.\d+)?(e?)([+-]?)(\d+)?"
                r"([+-]?)\d+(?:\.\d+)?(e?)([+-]?)(\d+)?j(\))$"
            )
            # as far as we don't exactly know what it is
            # we try to catch it by a regular expression
            if re.findall(complex_number_regex, val):  # type: ignore[arg-type]
                try:
                    # and to convert it to a complex number if there's a match
                    return complex(val)  # type: ignore[arg-type]
                except ValueError:
                    # if conversion fails we return string
                    return val

    return val


def deserialize_attribute_nested_tuples(value: Tuple[Any, ...]) -> Tuple[Any, ...]:
    """Deserialize attribute nested tuples and their elements.

    :param value: attribute tuple value
    """
    deserialized = []
    for el in value:
        if isinstance(el, (tuple, list)):
            value = deserialize_attribute_nested_tuples(el)  # type: ignore[arg-type]
        else:
            value = deserialize_attribute_value(el, type(el), True)
        deserialized.append(value)
    return tuple(deserialized)


def make_ordered_dict(
    primary_attributes: Optional[dict],
    custom_attributes: Optional[dict],
    attrs_schema: Union[List["AttributeSchema"], Tuple["AttributeSchema", ...]],
) -> Tuple[OrderedDict, OrderedDict]:
    """Ensure that attributes in dict are located in correct order (Based on schema).

    :param primary_attributes:  Primary attributes dict
    :param custom_attributes: Custom attributes dict
    :param attrs_schema: Schema of attributes to get order
    """
    # To ensure the order of attributes
    ordered_primary_attributes: OrderedDict = OrderedDict()
    ordered_custom_attributes: OrderedDict = OrderedDict()

    # Iterate over every attribute in schema:
    for attr_schema in attrs_schema:
        if attr_schema.primary:
            attributes_from_meta = primary_attributes
            result_attributes = ordered_primary_attributes
        else:
            attributes_from_meta = custom_attributes
            result_attributes = ordered_custom_attributes

        value = attributes_from_meta[attr_schema.name]
        if value is None and not attr_schema.primary:
            result_attributes[attr_schema.name] = value
            continue

        result_attributes[attr_schema.name] = deserialize_attribute_value(
            value, attr_schema.dtype, False
        )

    return ordered_primary_attributes, ordered_custom_attributes

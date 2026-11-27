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
import uuid

from collections.abc import Sequence
from typing import TYPE_CHECKING, Union

from deker_tools.time import get_utc
from deker_tools.typings import SentinelType, is_sentinel

from deker.errors import DekerValidationError
from deker.types import DTypeEnum

if TYPE_CHECKING:
    from deker.ABC.base_schemas import BaseDimensionSchema
    from deker.schemas import ArraySchema, AttributeSchema, VArraySchema


def process_time_dimension_attrs(attributes: dict, attr_name: str) -> datetime.datetime:
    """Get the attribute a time dimension starts at, as a UTC datetime.

    :param attributes: attributes of the array
    :param attr_name: name of the attribute
    :returns: the time attribute as a datetime
    :raises DekerValidationError: if the attribute is missing or ``None``
    """
    time_attribute = attributes.get(attr_name)
    if time_attribute is None:
        raise DekerValidationError("No start value provided for time dimension")
    if time_attribute.tzinfo is None or time_attribute.tzinfo != datetime.timezone.utc:
        time_attribute = get_utc(time_attribute)
    return time_attribute


def __dictionaries(primary: object, custom: object) -> tuple[str, ...]:
    """Name the dictionaries of the attributes an error is in, the primary ones first.

    :param primary: true if the error is in the primary attributes
    :param custom: true if the error is in the custom attributes
    :returns: the names, for ``DekerValidationError.parameters``
    """
    return tuple(name for name, guilty in (("primary_attributes", primary), ("custom_attributes", custom)) if guilty)


def __validate_attribute_type(attribute: object, parameters: tuple[str, ...]) -> None:
    """Validate attribute type over allowed types; a tuple is validated element by element.

    :param attribute: attribute object
    :param parameters: the dictionary of the attribute, for the error
    :raises DekerValidationError: if the type of the value is not an attribute dtype
    """
    if isinstance(attribute, tuple):
        for attr in attribute:
            __validate_attribute_type(attr, parameters)

    dtype = type(attribute)
    if attribute is not None:
        try:
            DTypeEnum(dtype).value  # noqa: B018  # validation by side effect, same as in BaseArraysSchema
        except (ValueError, KeyError):
            raise DekerValidationError(f"Invalid dtype value {dtype}", parameters=parameters)


def __process_attributes_types(
    attrs_schema: tuple["AttributeSchema", ...],
    primary_attributes: dict,
    custom_attributes: dict,
    dimensions: Sequence["BaseDimensionSchema"],
) -> None:
    """Validate attributes types over schema and update dicts if needed.

    A custom attribute left out is set to ``None``, unless a time dimension starts at it; datetime attributes
    are converted to UTC in place.

    :param attrs_schema: attributes schema
    :param primary_attributes: primary attributes to validate
    :param custom_attributes: custom attributes to validate
    :param dimensions: dimensions schemas
    :raises DekerValidationError: if a primary attribute is missing, a value has a wrong type, or a custom
      attribute a time dimension starts at is ``None``
    """
    from deker.schemas import TimeDimensionSchema

    attributes = {**primary_attributes, **custom_attributes}
    for attr in attrs_schema:
        if attr.primary:
            # check if primary attribute is not missing and its type
            if attr.name not in attributes:
                raise DekerValidationError(f"Key attribute missing: {attr.name}", parameters=("primary_attributes",))
            if not isinstance(primary_attributes[attr.name], attr.dtype):
                raise DekerValidationError(
                    f'Key attribute "{attr.name}" invalid type: {type(primary_attributes[attr.name])}; '
                    f"expected {attr.dtype}",
                    parameters=("primary_attributes",),
                )

            # validate tuples contents type
            if isinstance(primary_attributes[attr.name], tuple):
                __validate_attribute_type(primary_attributes[attr.name], ("primary_attributes",))

        else:
            # check if custom attribute is not missing and its type
            custom_attribute = custom_attributes.get(attr.name)
            if custom_attribute is not None and not isinstance(custom_attribute, attr.dtype):
                raise DekerValidationError(
                    f'Custom attribute "{attr.name}" invalid type {type(custom_attributes[attr.name])}; '
                    f"expected {attr.dtype}",
                    parameters=("custom_attributes",),
                )

            if custom_attribute is None:
                if attr.dtype == datetime.datetime:
                    for d in dimensions:
                        if (
                            isinstance(d, TimeDimensionSchema)
                            and isinstance(d.start_value, str)
                            and d.start_value.startswith("$" + attr.name)
                        ):
                            raise DekerValidationError(
                                f'Custom attribute "{attr.name}" cannot be None', parameters=("custom_attributes",)
                            )
                custom_attributes[attr.name] = None

            # validate tuples
            if isinstance(custom_attributes[attr.name], tuple):
                __validate_attribute_type(custom_attributes[attr.name], ("custom_attributes",))

        # convert datetime attribute with set datetime value to utc if needed
        if attr.dtype == datetime.datetime and attr.name in attributes and attributes[attr.name] is not None:
            try:
                utc = get_utc(attributes[attr.name])
                if attr.primary:
                    primary_attributes[attr.name] = utc
                else:
                    custom_attributes[attr.name] = utc
            except (ValueError, TypeError) as e:  # pragma: no cover  # guard: the value is a datetime, checked above
                raise DekerValidationError(e, parameters=__dictionaries(attr.primary, not attr.primary))


def process_attributes(
    schema: Union["ArraySchema", "VArraySchema"],
    primary_attributes: dict | SentinelType,
    custom_attributes: dict | SentinelType,
) -> tuple[dict, dict]:
    """Validate attributes over schema and return them.

    :param schema: ArraySchema or VArraySchema instance
    :param primary_attributes: attributes to validate; ``Sentinel`` - none given
    :param custom_attributes: attributes to validate; ``Sentinel`` - none given
    :returns: validated primary and custom attributes: the custom ones completed with ``None`` for the attributes
      left out, the datetimes in UTC
    :raises DekerValidationError: if attributes are given to a schema without them, the primary ones are missing,
      a name is both primary and custom, a name is not in the schema, an attribute is not in the dictionary of its
      kind, or a value has a wrong type
    """
    from deker.schemas import VArraySchema

    array_type = "VArray" if isinstance(schema, VArraySchema) else "Array"

    attrs_schema: tuple["AttributeSchema", ...] = tuple(schema.attributes or ()) if schema else ()

    primary_attributes = {} if is_sentinel(primary_attributes) else primary_attributes
    custom_attributes = {} if is_sentinel(custom_attributes) else custom_attributes

    if any((primary_attributes, custom_attributes)) and not attrs_schema:
        raise DekerValidationError(
            f"{array_type} attributes schema is missing".capitalize(),
            parameters=__dictionaries(primary_attributes, custom_attributes),
        )

    if any(attr.primary for attr in attrs_schema) and not primary_attributes:
        raise DekerValidationError("No primary attributes provided", parameters=__dictionaries(True, False))

    # check if attributes have unique names
    if any((primary_attributes, custom_attributes)):
        key_names = set(primary_attributes.keys())
        custom_names = set(custom_attributes.keys())
        names_intersection = custom_names.intersection(key_names)
        if names_intersection:
            raise DekerValidationError(
                f"Key and custom attributes shall not have same names; invalid names: {names_intersection}",
                parameters=__dictionaries(True, True),
            )

    attributes = {**primary_attributes, **custom_attributes}

    # check extra attributes
    schema_attrs_names = {attr.name for attr in attrs_schema}
    extra_names = set(attributes.keys()).difference(schema_attrs_names)
    if extra_names:
        raise DekerValidationError(
            f"Setting additional attributes not listed in schema is not allowed. "
            f"Invalid attributes: {sorted(extra_names)}",
            parameters=__dictionaries(primary_attributes.keys() & extra_names, custom_attributes.keys() & extra_names),
        )

    # check every attribute is given in its own dictionary
    primary_names = {attr.name for attr in attrs_schema if attr.primary}
    misplaced = []
    if custom_among_primary := sorted(primary_attributes.keys() - primary_names):
        misplaced.append(f"{custom_among_primary} shall be in custom attributes")
    if primary_among_custom := sorted(custom_attributes.keys() & primary_names):
        misplaced.append(f"{primary_among_custom} shall be in primary attributes")
    if misplaced:
        raise DekerValidationError(
            f"Attributes passed in the wrong dictionary: {'; '.join(misplaced)}",
            parameters=__dictionaries(custom_among_primary, primary_among_custom),
        )

    __process_attributes_types(
        attrs_schema,
        primary_attributes,
        custom_attributes,
        schema.dimensions,
    )
    return primary_attributes, custom_attributes


def validate_custom_attributes_update(
    schema: Union["ArraySchema", "VArraySchema"],
    primary_attributes: dict,
    custom_attributes: dict,
    attributes: dict,
) -> dict:
    """Validate custom attributes update over schema.

    The custom attributes left out of the update keep their current values, the start of a $-time
    dimension among them.

    :param schema: ArraySchema or VArraySchema instance
    :param primary_attributes: (V)Array primary attributes
    :param custom_attributes: old custom attributes
    :param attributes: new custom attributes to validate
    :returns: validated attributes: the given ones and the current values of the others
    :raises DekerValidationError: if no attributes are given, a name is primary or not in the schema, or a value has
      a wrong type
    """
    if not attributes:
        raise DekerValidationError("No attributes passed for update", parameters=__dictionaries(False, True))
    for attr in schema.attributes or ():
        if not attr.primary and attr.name not in attributes:
            attributes[attr.name] = custom_attributes[attr.name]

    try:
        process_attributes(schema, primary_attributes, attributes)
    except DekerValidationError as e:
        e.parameters = __dictionaries(False, True)  # the update is given the custom attributes only
        raise
    return attributes


def is_valid_uuid(id_: str) -> bool:
    """Validate if id is in uuid format.

    :param id_: id to validate
    :returns: ``True`` if the string is a UUID
    """
    if not isinstance(id_, str) or len(id_.split("-")) != 5 or len(id_) != 36:
        return False
    try:
        uuid.UUID(id_)
        return True
    except ValueError:
        return False


# TODO (next breaking release): CHECK THE NAMES AND THE VALUES THAT BECOME PATHS OF THE STORAGE.
# The name of a collection and the string values of the primary attributes become parts of the paths of the
# storage as they are: only the type and the emptiness of a collection name are checked, here and in the
# constructor of ``Collection``. Any sign passes: spaces, shell signs, a slash inside, which is unsafe for the code
# that hands the paths to external commands and ambiguous in a URL. A value with ``..`` or an absolute path takes
# the directory of a collection or the symlink of an array OUTSIDE of the storage (reproduced: an Array with the
# string primary attribute ``../../../../x`` gets its symlink outside). Both the restriction of the signs and the
# check that a path stays inside its directory are breaking changes for the collections and the arrays stored
# with such names, for the replication of the collections onto new nodes and for ``collection_from_dict`` as a
# way around ``create_collection``; they need one separate decision, a version of the metadata and a migration.
def validate_collection_name(*, name: str | SentinelType) -> None:
    """Check the name of a collection given to an operation over the storage: a string, or not given.

    :param name: name of the collection, or ``Sentinel`` when not given
    :raises DekerValidationError: if the name is not a string
    """
    if not is_sentinel(name) and not isinstance(name, str):
        raise DekerValidationError(f"collection name shall be a string, got {name!r}")


def validate_timeout(*, timeout: int | SentinelType) -> None:
    """Check the timeout of an operation or a setting of the client: seconds, a positive integer, or not given.

    :param timeout: seconds, or ``Sentinel`` when not given
    :raises DekerValidationError: if the timeout is not a positive integer
    """
    if is_sentinel(timeout):
        return
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise DekerValidationError(f"timeout shall be a positive integer number of seconds, got {timeout!r}")

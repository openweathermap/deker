import datetime

from typing import TYPE_CHECKING, Optional, Tuple, Union

from deker.errors import DekerValidationError
from deker.tools.time import convert_to_utc


if TYPE_CHECKING:
    from deker.schemas import ArraySchema, AttributeSchema, VArraySchema


def process_time_dimension_attrs(attributes: dict, attr_name: str) -> datetime.datetime:
    """Validate time attribute and return its value.

    :param attributes: attributes to validate
    :param attr_name: attribute name to validate
    """
    time_attribute = attributes.get(attr_name)
    if time_attribute is None:
        raise DekerValidationError("No start value provided for time dimension")
    if time_attribute.tzinfo is None or time_attribute.tzinfo != datetime.timezone.utc:
        time_attribute = convert_to_utc(time_attribute)
    return time_attribute


def __process_attrs(
    attrs_schema: Tuple["AttributeSchema", ...],
    attributes: dict,
    primary_attributes: dict,
    custom_attributes: dict,
) -> None:
    for attr in attrs_schema:
        if attr.primary:
            if attr.name not in attributes:
                raise DekerValidationError(f"Key attribute missing: {attr.name}")
            if not isinstance(primary_attributes[attr.name], attr.dtype):
                raise DekerValidationError(
                    f'Key attribute "{attr.name}" invalid type: {type(primary_attributes[attr.name])}; '
                    f"expected {attr.dtype}"
                )

        else:
            custom_attribute = custom_attributes.get(attr.name)
            if custom_attribute is not None:
                if not isinstance(custom_attribute, attr.dtype):
                    raise DekerValidationError(
                        f'Custom attribute "{attr.name}" invalid type {type(custom_attributes[attr.name])}; '
                        f"expected {attr.dtype}"
                    )

            if custom_attribute is None:
                if attr.dtype == datetime.datetime:
                    raise DekerValidationError(f'Custom attribute "{attr.name}" can not be None')
                custom_attributes[attr.name] = None

        if attr.dtype == datetime.datetime and attr.name in attributes:
            try:
                utc = convert_to_utc(attributes[attr.name])
                if attr.primary:
                    primary_attributes[attr.name] = utc
                else:
                    custom_attributes[attr.name] = utc
            except (ValueError, TypeError) as e:
                raise DekerValidationError(e)


def process_attributes(
    schema: Union["ArraySchema", "VArraySchema"],
    primary_attributes: Optional[dict],
    custom_attributes: Optional[dict],
) -> Tuple[dict, dict]:
    """Validate attributes over schema.

    :param schema: ArraySchema or VArraySchema instance
    :param primary_attributes: attributes to validate
    :param custom_attributes: attributes to validate
    """
    from deker.schemas import VArraySchema

    if isinstance(schema, VArraySchema):
        array_type = "VArray"
    else:
        array_type = "Array"

    attrs_schema = schema.attributes if schema else None

    if primary_attributes is None:
        primary_attributes = {}
    if custom_attributes is None:
        custom_attributes = {}

    if any((primary_attributes, custom_attributes)) and not attrs_schema:
        raise DekerValidationError(f"{array_type} attributes schema is missing".capitalize())

    if any(attr.primary for attr in attrs_schema) and not primary_attributes:
        raise DekerValidationError("No primary attributes provided")

    # check if attributes have unique names
    if any((primary_attributes, custom_attributes)):
        key_names = set(primary_attributes.keys())
        custom_names = set(custom_attributes.keys())
        names_intersection = custom_names.intersection(key_names)
        if names_intersection:
            raise DekerValidationError(
                f"Key and custom attributes shall not have same names; invalid names: {names_intersection}"
            )

    attributes = {**primary_attributes, **custom_attributes}

    # check extra attributes
    schema_attrs_names = {attr.name for attr in attrs_schema}
    extra_names = set(attributes.keys()).difference(schema_attrs_names)
    if extra_names:
        raise DekerValidationError(
            f"Setting additional attributes not listed in schema is not allowed. "
            f"Invalid attributes: {sorted(extra_names)}"
        )
    __process_attrs(attrs_schema, attributes, primary_attributes, custom_attributes)  # type: ignore[arg-type]
    return primary_attributes, custom_attributes

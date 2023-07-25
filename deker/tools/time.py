from datetime import datetime, timezone
from typing import Optional, Union


def now() -> datetime:
    """Get UTC now datetime."""
    return datetime.utcnow().replace(tzinfo=timezone.utc)


def convert_to_utc(dt: Union[str, int, float, datetime]) -> datetime:
    """Convert datetime with any timezone or without it to UTC.

    :param dt: datetime.datetime object, timestamp or datetime isostring
    """
    if isinstance(dt, datetime):
        dt = dt.isoformat()  # convert any timezone objects to native format
    elif isinstance(dt, (float, int)):
        dt = datetime.utcfromtimestamp(dt).isoformat()

    dt_object = datetime.fromisoformat(dt)

    if dt_object.tzinfo is None:
        dt_object = dt_object.replace(tzinfo=timezone.utc)
    elif dt_object.tzinfo != timezone.utc:
        tm = dt_object.timestamp()
        dt_object = datetime.utcfromtimestamp(tm).replace(tzinfo=timezone.utc)
    return dt_object


def convert_datetime_attrs_to_iso(attrs: Optional[dict]) -> Optional[dict]:
    """Convert datetime attributes to iso-format if possible.

    :param attrs: attributes dict
    """
    if attrs is not None and not isinstance(attrs, dict):
        raise TypeError(f"Invalid attribute value type: {type(attrs)}; expected dict")

    if attrs:
        return {
            key: (value.isoformat() if isinstance(value, datetime) else value)
            for key, value in attrs.items()
        }
    return attrs


def convert_iso_attrs_to_datetime(attrs: Optional[dict]) -> Optional[dict]:
    """Convert iso-format attributes to datetime if possible.

    :param attrs: attributes dict
    """
    if attrs is not None and not isinstance(attrs, dict):
        raise TypeError(f"Invalid attribute value type: {type(attrs)}; expected dict")

    if attrs:
        new_attrs = {}
        for key, value in attrs.items():
            if isinstance(value, str):
                try:
                    new_attrs[key] = datetime.fromisoformat(value)
                except (TypeError, ValueError):
                    new_attrs[key] = value
            else:
                new_attrs[key] = value
        return new_attrs
    return attrs

from datetime import datetime
from typing import Optional


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

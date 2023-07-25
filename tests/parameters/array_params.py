from datetime import datetime
from typing import List


def attributes_validation_params() -> List[dict]:
    """Generates attributes validation params"."""
    default_attributes = {"primary_attributes": {"primary_attribute": 1}}
    return [
        {
            "custom_attributes": {
                "custom_attribute": "test",
                "time_attr_name": datetime.now(),
            },
            **default_attributes,
        },
        {
            "custom_attributes": {
                "custom_attribute": 1.0,
                "time_attr_name": datetime.now(),
            },
            "primary_attributes": {"primary_attribute": "a"},
        },
        {
            "custom_attributes": {
                "custom_attribute": 1.0,
                "time_attr_name": datetime.now(),
            },
        },
        {"custom_attributes": {"time_attr_name": None}, **default_attributes},
        {"custom_attributes": {"time_attr_name": "word"}, **default_attributes},
    ]

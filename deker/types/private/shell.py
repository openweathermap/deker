from deker.client import Client
from deker.log import set_logging_level
from deker.schemas import (
    ArraySchema,
    AttributeSchema,
    DimensionSchema,
    TimeDimensionSchema,
    VArraySchema,
)
from deker.types import Scale


__all__ = (  # F405
    "Client",
    "DimensionSchema",
    "TimeDimensionSchema",
    "ArraySchema",
    "AttributeSchema",
    "VArraySchema",
    "Scale",
    "set_logging_level",
)

# shell_completions are used in deker-shell to exclude Deker objects
# from autocompletion of parameters during initialization
shell_completions = tuple(deker_obj + "(" for deker_obj in __all__)

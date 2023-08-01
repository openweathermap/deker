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

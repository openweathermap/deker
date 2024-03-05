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

import uuid

from typing import Dict, List, Tuple, Union

import numpy as np

from deker_tools.data import convert_size_to_human
from psutil import swap_memory, virtual_memory

from deker.errors import DekerMemoryError, DekerValidationError


def calculate_total_cells_in_array(seq: Union[Tuple[int, ...], List[int]]) -> int:
    """Get total quantity of cells in the array.

    :param seq: sequence of integers (normally array shape)
    """
    return int(np.prod(np.array(seq)))


def convert_human_memory_to_bytes(memory_limit: Union[int, str]) -> int:
    """Convert a human memory limit to bytes.

    :param memory_limit: memory limit provided by the user
    """
    bytes_ = 1024
    mapping: Dict[str, int] = {"k": bytes_, "m": bytes_**2, "g": bytes_**3, "t": bytes_**4}
    error = (
        f"invalid memory_limit value: {memory_limit}; expected `int` or `str` in format [number][unit] "
        f'where unit is one of ["k", "K", "m", "M", "g", "G", "t", "T"], e.g. "8G" or "512m"'
    )
    if not isinstance(memory_limit, (int, str)):
        raise DekerValidationError(error)

    if isinstance(memory_limit, int):
        return memory_limit

    try:
        return int(memory_limit)
    except ValueError:
        pass

    limit, div = memory_limit[:-1], memory_limit.lower()[-1]
    try:
        int_limit: int = int(limit)
        bytes_result: int = int_limit * mapping[div]
        return bytes_result
    except Exception:
        raise DekerValidationError(error)


def check_memory(shape: tuple, dtype: type, mem_limit_from_settings: int) -> None:
    """Memory allocation checker decorator.

    Checks if it is possible to allocate memory for array/subset.

    :param shape: array or subset shape
    :param dtype: array or subset values dtype
    :param mem_limit_from_settings: deker ram limit in bytes
    """
    array_values = calculate_total_cells_in_array(shape)
    array_size_bytes = np.dtype(dtype).itemsize * array_values
    array_size_human = convert_size_to_human(array_size_bytes)

    total_machine_mem = virtual_memory().total + swap_memory().total
    total_human_mem = convert_size_to_human(total_machine_mem)
    current_limit = virtual_memory().available + swap_memory().free
    limit = min(mem_limit_from_settings, current_limit)
    limit_human = convert_size_to_human(limit)
    config_human_limit = convert_size_to_human(mem_limit_from_settings)

    deker_limit = f"{config_human_limit} ({mem_limit_from_settings} bytes). "
    limit_message = (
        "Deker Client memory usage is limited to the total memory available on this machine: "
        + deker_limit
    )
    advise = ""
    if total_machine_mem > mem_limit_from_settings:
        limit_message = (
            f"Total memory available on this machine is {total_human_mem} ({total_machine_mem} bytes). "
            f"Deker Client memory usage is manually limited to " + deker_limit
        )
        advise = " Also, you may try to increase the value of Deker Client memory limit."

    if array_size_bytes > limit:
        raise DekerMemoryError(
            f"Can not allocate {array_size_human} for array/subset with shape {shape} and dtype {dtype}. "
            f"{limit_message}"
            f"Current available free memory per array/subset is {limit_human} ({limit} bytes). "
            f"Reduce your schema or the `shape` of your subset or revise other processes memory usage.{advise}"
        )


def get_id() -> str:
    """Generate unique id with uuid4."""
    return str(uuid.uuid4())

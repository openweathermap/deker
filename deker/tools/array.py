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

import numpy as np

from deker_tools.data import convert_size_to_human
from psutil import swap_memory, virtual_memory

from deker.errors import DekerMemoryError, DekerValidationError


def calculate_total_cells_in_array(seq: tuple[int, ...] | list[int]) -> int:
    """Get total quantity of cells in the array.

    :param seq: sequence of integers (normally array shape)
    :returns: number of cells
    """
    return int(np.prod(np.array(seq)))


def convert_human_memory_to_bytes(memory_limit: int | str) -> int:
    """Convert a human memory limit to bytes.

    :param memory_limit: memory limit provided by the user: a number of bytes, or a string of a number and a unit
      (``k``, ``m``, ``g``, ``t``, in any case), e.g. ``"8G"`` or ``"512m"``
    :returns: memory limit in bytes
    :raises DekerValidationError: if the value is neither a number nor such a string
    """
    bytes_ = 1024
    mapping: dict[str, int] = {"k": bytes_, "m": bytes_**2, "g": bytes_**3, "t": bytes_**4}
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


# the share of the limit of the client a request may take without a query of the free memory of the machine:
# a query costs a read of the system tables on every read or write, and a small request fits anyway
UNCHECKED_SHARE_OF_LIMIT = 0.01


def check_memory(shape: tuple, dtype: type, mem_limit_from_settings: int) -> None:
    """Check that the data of the shape and dtype fit the memory.

    The limit is the smaller of the limit of the client and the memory free on the machine right now. The
    free memory is queried only for a request above ``UNCHECKED_SHARE_OF_LIMIT`` of the limit of the client:
    a smaller one fits anyway, and the query is not free.

    :param shape: array or subset shape
    :param dtype: array or subset values dtype
    :param mem_limit_from_settings: deker ram limit in bytes
    :raises DekerMemoryError: if the data would not fit
    """
    array_values = calculate_total_cells_in_array(shape)
    array_size_bytes = np.dtype(dtype).itemsize * array_values
    if array_size_bytes <= mem_limit_from_settings * UNCHECKED_SHARE_OF_LIMIT:
        return
    array_size_human = convert_size_to_human(array_size_bytes)
    config_human_limit = convert_size_to_human(mem_limit_from_settings)
    deker_limit = f"{config_human_limit} ({mem_limit_from_settings} bytes). "
    if array_size_bytes > mem_limit_from_settings:
        raise DekerMemoryError(
            f"Can not allocate {array_size_human} for array/subset with shape {shape} and dtype {dtype}. "
            f"Deker Client memory usage is limited to {deker_limit}"
            f"Reduce your schema or the `shape` of your subset or increase the value of Deker Client memory limit."
        )

    virtual, swap = virtual_memory(), swap_memory()
    total_machine_mem = virtual.total + swap.total
    total_human_mem = convert_size_to_human(total_machine_mem)
    current_limit = virtual.available + swap.free
    limit = min(mem_limit_from_settings, current_limit)
    limit_human = convert_size_to_human(limit)

    limit_message = "Deker Client memory usage is limited to the total memory available on this machine: " + deker_limit
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
    """Generate a unique id of an array: a ``uuid4`` string.

    :returns: the id
    """
    return str(uuid.uuid4())

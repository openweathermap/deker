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

from functools import singledispatch
from typing import Any, Dict, List, Tuple, Union

import numpy as np

from deker_tools.data import convert_size_to_human
from deker_tools.time import get_utc
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
    mapping: Dict[str, int] = {"k": bytes_, "m": bytes_**2, "g": bytes_**3}
    error = (
        f"invalid memory_limit value: {memory_limit}; expected `int` or `str` in format [number][unit] "
        f'where unit is one of ["k", "K", "m", "M", "g", "G"], e.g. "8G" or "512m"'
    )
    if not isinstance(memory_limit, (int, str)):
        raise DekerValidationError(error)

    if isinstance(memory_limit, int):
        return memory_limit

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

    current_limit = virtual_memory().available + swap_memory().free
    limit = min(mem_limit_from_settings, current_limit)
    limit_human = convert_size_to_human(limit)

    if array_size_bytes > limit:
        raise DekerMemoryError(
            f"Can not allocate {array_size_human} for array/subset with shape {shape} and dtype {dtype}. "
            f"Current Deker limit per array/subset is {limit_human}. Value in config: {mem_limit_from_settings}"
            f"Reduce shape or dtype of your array/subset or increase Deker RAM limit."
        )


def get_id(array: Any) -> str:
    """Generate unique id by object type and datetime.

    :param array: any object
    """
    from deker.arrays import Array, VArray

    @singledispatch
    def generate_id(arr: Any) -> str:
        """Generate unique id by object type and datetime.

        :param arr: any object
        """
        raise TypeError(f"Invalid object type: {type(arr)}")

    @generate_id.register(Array)
    def array_id(arr: Array) -> str:  # noqa[ARG001]
        """Generate id for Array.

        :param arr: Array type
        """
        return str(uuid.uuid5(uuid.NAMESPACE_X500, "array" + get_utc().isoformat()))

    @generate_id.register(VArray)
    def varray_id(arr: VArray) -> str:  # noqa[ARG001]
        """Generate id for VArray.

        :param arr: VArray type
        """
        return str(uuid.uuid5(uuid.NAMESPACE_OID, "varray" + get_utc().isoformat()))

    return generate_id(array)

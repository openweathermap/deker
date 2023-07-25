import uuid

from datetime import datetime
from functools import singledispatch
from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Union

import numpy as np

from deker_tools.data import convert_size_to_human
from psutil import swap_memory, virtual_memory

from deker.errors import DekerMemoryError, DekerMetaDataError
from deker.tools.time import convert_to_utc, now
from deker.types import ArrayMeta


if TYPE_CHECKING:
    from deker.ABC.base_adapters import BaseArrayAdapter, BaseVArrayAdapter
    from deker.arrays import Array, VArray
    from deker.collection import Collection


def calculate_total_cells_in_array(seq: Union[Tuple[int, ...], List[int]]) -> int:
    """Get total quantity of cells in the array.

    :param seq: sequence of integers (normally array shape)
    """
    return int(np.prod(np.array(seq)))


def check_memory(shape: tuple, dtype: type, mem_limit_from_settings: int) -> None:
    """Memory allocation checker decorator.
    Checks if it is possible to allocate memory for array/subset.

    :param shape: array or subset shape
    :param dtype: array or subset values dtype
    :param mem_limit_from_settings: deker ram limit in bytes
    """
    pow_bytes = 1024
    array_values = calculate_total_cells_in_array(shape)
    array_size_bytes = np.dtype(dtype).itemsize * array_values
    array_size_human = convert_size_to_human(array_size_bytes, pow_bytes=pow_bytes)

    current_limit = virtual_memory().available + swap_memory().free
    limit = min(mem_limit_from_settings, current_limit)
    limit_human = convert_size_to_human(limit, pow_bytes=pow_bytes)

    if array_size_bytes > limit:
        raise DekerMemoryError(
            f"Can not allocate {array_size_human} for array/subset with shape {shape} and dtype {dtype}. "
            f"Current Deker limit per array/subset is {limit_human}. Value in config: {mem_limit_from_settings}"
            f"Reduce shape or dtype of your array/subset or increase Deker RAM limit."
        )


def create_array_from_meta(
    collection: "Collection",
    meta: "ArrayMeta",
    array_adapter: "BaseArrayAdapter",
    varray_adapter: Optional["BaseVArrayAdapter"] = None,
) -> Union["Array", "VArray"]:
    """Create Array or VArray from metadata.

    :param collection: Collection instance
    :param meta: array metadata
    :param array_adapter: Array adapter instance
    :param varray_adapter: VArray adapter instance
    """
    from deker.arrays import Array, VArray

    # instantiates "start_value" in attributes as a `datetime.datetime` from metadata iso-string
    if varray_adapter:
        array_type = VArray
        attrs_schema = collection.varray_schema.attributes
    else:
        array_type = Array
        attrs_schema = collection.array_schema.attributes
    try:
        for attr in attrs_schema:
            if attr.primary:
                attributes = meta["primary_attributes"]
            else:
                attributes = meta["custom_attributes"]

            value = attributes[attr.name]

            if attr.dtype == datetime:
                attributes[attr.name] = convert_to_utc(value)
            if attr.dtype == tuple:
                if (attr.primary or (not attr.primary and value is not None)) and not isinstance(
                    value, list
                ):
                    raise DekerMetaDataError(
                        f"Collection '{collection.name}' metadata is corrupted: "
                        f"attribute '{attr.name}' has invalid type '{type(value)}'; '{attr.dtype}' expected"
                    )

                if attr.primary or (not attr.primary and value is not None):
                    attributes[attr.name] = tuple(value)

        arr_params = {
            "collection": collection,
            "adapter": array_adapter,
            "id_": meta["id"],
            "primary_attributes": meta.get("primary_attributes"),
            "custom_attributes": meta.get("custom_attributes"),
        }
        if varray_adapter:
            arr_params.update({"adapter": varray_adapter, "array_adapter": array_adapter})
        return array_type(**arr_params)  # type: ignore[arg-type]
    except (KeyError, ValueError) as e:
        raise DekerMetaDataError(f"{array_type} metadata invalid/corrupted: {e}")


def get_id(array: Any) -> str:
    """Generate unique id by object type and datetime.

    :param array: any object
    """
    from deker.arrays import Array, VArray  # noqa: F811

    @singledispatch
    def generate_id(arr: Any) -> str:
        """Generate unique id by object type and datetime.

        :param arr: any object
        """
        raise TypeError(f"Invalid object type: {type(arr)}")

    @generate_id.register(Array)
    def array_id(arr: Array) -> str:
        """Generate id for Array.

        :param arr: Array type
        """
        return str(uuid.uuid5(uuid.NAMESPACE_X500, "array" + now().isoformat()))

    @generate_id.register(VArray)
    def varray_id(arr: VArray) -> str:
        """Generate id for VArray.

        :param arr: VArray type
        """
        return str(uuid.uuid5(uuid.NAMESPACE_OID, "varray" + now().isoformat()))

    return generate_id(array)

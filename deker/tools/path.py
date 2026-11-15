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

import hashlib
import os

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, Union, runtime_checkable

from deker.types import Paths
from deker.types.private.enums import LocksExtensions

if TYPE_CHECKING:
    from deker.ABC.base_array import BaseArray
    from deker.ABC.base_schemas import BaseAttributeSchema
    from deker.arrays import Array, VArray


def get_symlink_path(
    path_to_symlink_dir: Path,
    primary_attributes_schema: tuple["BaseAttributeSchema", ...] | None,
    primary_attributes: dict,
) -> Path:
    """Make symlink path from given attributes.

    :param path_to_symlink_dir: path to array or varray symlinks in certain collection
    :param primary_attributes_schema: schemas of primary attributes
    :param primary_attributes: all primary attributes for a path
    :returns: path of the symlink
    """
    symlink_path = path_to_symlink_dir
    if primary_attributes_schema:
        for attr in primary_attributes_schema:
            attribute = primary_attributes[attr.name]
            if attr.name == "v_position":
                value = "-".join(str(el) for el in attribute)
            else:
                value = attribute.isoformat() if isinstance(attribute, datetime) else str(attribute)
            symlink_path = symlink_path / value
    return symlink_path


def get_main_path(array_id: str, data_directory: Path) -> Path:
    """Generate main path for the given array id by its type.

    :param array_id: Array or VArray id
    :param data_directory: Path to a certain data directory
    :returns: path of the directory holding the main file
    """
    main_tree, rest = array_id.split("-", 1)
    main_tree = os.path.sep.join(s for s in main_tree)
    return data_directory / main_tree / rest


@runtime_checkable
class DiskAdapter(Protocol):
    """Adapter that keeps arrays as files on disk, in data and symlinks directories of the collection."""

    data_dir: str
    symlinks_dir: str


def _get_disk_adapter(array: Union["Array", "VArray"]) -> DiskAdapter:
    """Get the adapter of the array, which shall keep its files on disk.

    :param array: Array or VArray instance
    :returns: the array adapter with data and symlinks directories
    """
    adapter = array._adapter
    if not isinstance(adapter, DiskAdapter):  # pragma: no cover
        raise TypeError(f"{type(adapter).__name__} keeps no data and symlinks directories: paths cannot be built")
    return adapter


def _get_array_paths(arr: "Array", base_path: Path) -> Paths:
    """Generate paths for Array.

    :param arr: Array type
    :param base_path: collection base path
    :returns: main and symlink paths of the array
    """
    adapter = _get_disk_adapter(arr)
    # main_path
    main_path = get_main_path(arr.id, base_path / adapter.data_dir)

    # symlink
    primary_attributes: tuple["BaseAttributeSchema", ...] | None = arr.schema.primary_attributes
    symlink_path = get_symlink_path(
        path_to_symlink_dir=base_path / adapter.symlinks_dir,
        primary_attributes_schema=primary_attributes,
        primary_attributes={
            **arr.primary_attributes,
            "vid": arr._vid,
            "v_position": arr._v_position,
        },
    )
    return Paths(main_path, symlink_path)


def _get_varray_paths(arr: "VArray", base_path: Path) -> Paths:
    """Generate paths for VArray.

    :param arr: VArray type
    :param base_path: collection base path
    :returns: main and symlink paths of the virtual array
    """
    adapter = _get_disk_adapter(arr)
    # main_path
    main_path = get_main_path(arr.id, base_path / adapter.data_dir)

    # symlink
    primary_attributes: tuple["BaseAttributeSchema", ...] | None = arr.schema.primary_attributes
    symlink_path = get_symlink_path(
        path_to_symlink_dir=base_path / adapter.symlinks_dir,
        primary_attributes_schema=primary_attributes,
        primary_attributes={**arr.primary_attributes},
    )
    return Paths(main_path, symlink_path)


def get_paths(array: "BaseArray", collection_path: Path | str) -> Paths:
    """Create main and symlink paths for arrays.

    :param array: Array or VArray instance; any other type raises ``TypeError``
    :param collection_path: path to a certain collection defined by user
    :returns: namedtuple with "main" and "symlink" pathlib.Paths
    """
    from deker.arrays import Array, VArray

    coll_path = Path(collection_path) if isinstance(collection_path, str) else collection_path
    if isinstance(array, VArray):
        return _get_varray_paths(array, coll_path)
    if isinstance(array, Array):
        return _get_array_paths(array, coll_path)
    raise TypeError(f"Invalid object type: {type(array)}")


def get_array_lock_path(array: Union["Array", "VArray"], data_directory_path: Path) -> Path:
    """Get array lockfile path.

    :param array: array to lock
    :param data_directory_path: path to data directory
    :returns: path of the array lock file
    """
    file = hashlib.md5(str(array.primary_attributes).encode()).hexdigest() + LocksExtensions.array_lock.value
    return data_directory_path / file

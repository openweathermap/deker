# deker - multidimensional arrays storage engine
# Copyright (C) 2023-2026  Openweather Ltd.
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

import logging
import os
import re
import uuid

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn, Protocol, Union, runtime_checkable

from deker.errors import DekerCriticalError, DekerInstanceNotExistsError
from deker.log import _ROOT_DEKER_LOGGER_NAME
from deker.types import Paths

if TYPE_CHECKING:
    from deker.ABC.base_array import BaseArray
    from deker.ABC.base_schemas import BaseAttributeSchema
    from deker.arrays import Array, VArray


def get_symlink_path(
    path_to_symlink_dir: Path,
    primary_attributes_schema: tuple["BaseAttributeSchema", ...] | None,
    primary_attributes: dict,
) -> Path:
    """Make the directory of the symlink of an array from its primary attributes.

    The directory is a nested path of the values in the order of the schema: a datetime as its ISO string,
    a ``v_position`` as its numbers joined with ``-``, the others as their string form. Without primary
    attributes it is the symlinks directory itself.

    :param path_to_symlink_dir: path to array or varray symlinks in certain collection
    :param primary_attributes_schema: schemas of primary attributes
    :param primary_attributes: all primary attributes for a path
    :returns: directory of the symlink
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


def check_storage_directory(path: Path) -> None:
    """Check a directory of the storage the client creates when it opens: ``collections`` or ``trash``.

    Nobody removes these directories, so Deker cannot work on without one: a missing one is logged as critical and
    raised. Every operation of the local adapters checks the collections directory at its entry, the deletions and the
    clearings check the trash too (``CheckStorage`` of the local adapters); the paths of the errors check it again.

    :param path: the directory
    :raises DekerCriticalError: if the directory does not exist
    """
    if not path.is_dir():
        message = f"Storage directory {path} doesn't exist"
        logging.getLogger(_ROOT_DEKER_LOGGER_NAME).critical(message)
        raise DekerCriticalError(message)


def raise_not_found(path: Path, collections: Path) -> NoReturn:
    """Raise the error of a file or a directory of the storage that is not found.

    The only answer of Deker to a missing file: when the collections directory of the storage is gone, that is
    critical; otherwise the array or the collection the path belongs to does not exist. Called where
    ``FileNotFoundError`` is caught, the error it raises does not chain it.

    :param path: the file or directory not found, named in the error
    :param collections: the collections directory of the storage
    :raises DekerCriticalError: if the collections directory does not exist
    :raises DekerInstanceNotExistsError: if the collections directory is in place
    """
    check_storage_directory(collections)
    raise DekerInstanceNotExistsError(f"{path} doesn't exist") from None


# ``<main name>.<32 hex digits>.tmp``: the temporary file of an operation that replaces a main file
_TEMPORARY_NAME = re.compile(r"(?P<main>.+\.[^.]+)\.[0-9a-f]{32}\.tmp")


def get_temporary_path(main_file: Path) -> Path:
    """Name the temporary file of an operation that replaces a main file: next to it, with a random tail.

    The only scheme of the temporary files of the data directories, ``<main name>.<random hex>.tmp``: the full clearing
    of an Array and the update of the custom attributes of a VArray write their new file so, the check of the integrity
    finds the files left so. The name does not end with the extension of the main file, so no walk of the main files
    takes it for one.

    :param main_file: the main file
    :returns: the path of the temporary file
    """
    return main_file.with_name(f"{main_file.name}.{uuid.uuid4().hex}.tmp")


def is_temporary_path(path: Path) -> bool:
    """Check that a file is named as a temporary file of a main file, by ``get_temporary_path``.

    :param path: the file
    :returns: ``True`` for ``<main name>.<32 hex digits>.tmp``
    """
    return _TEMPORARY_NAME.fullmatch(path.name) is not None


def get_main_of_temporary(temporary: Path) -> Path:
    """Get the main file a temporary file is named after.

    :param temporary: a temporary file, as ``is_temporary_path`` tells it
    :returns: the main file, next to the temporary one
    """
    return temporary.with_name(temporary.name.rsplit(".", 2)[0])


def get_main_path(array_id: str, data_directory: Path) -> Path:
    """Generate the directory of the main file of an array from its id.

    The first group of the uuid becomes a tree of one-character directories, the rest of the uuid is the
    last directory: ``0f1e.../`` -> ``0/f/1/e/.../<rest>``.

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

    :param array: Array or VArray instance
    :param collection_path: path to a certain collection defined by user
    :returns: namedtuple with "main" and "symlink" pathlib.Paths: the directory of the main file and the
      directory of the symlink
    :raises TypeError: for any other object
    """
    from deker.arrays import Array, VArray

    coll_path = Path(collection_path) if isinstance(collection_path, str) else collection_path
    if isinstance(array, VArray):
        return _get_varray_paths(array, coll_path)
    if isinstance(array, Array):
        return _get_array_paths(array, coll_path)
    raise TypeError(f"Invalid object type: {type(array)}")

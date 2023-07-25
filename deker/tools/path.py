import hashlib
import os

from datetime import datetime
from functools import singledispatch
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Tuple, Union

from deker.types import Paths
from deker.types.enums import LocksExtensions


if TYPE_CHECKING:
    from deker.ABC.base_schemas import BaseAttributeSchema
    from deker.arrays import Array, VArray


def get_symlink_path(
    path_to_symlink_dir: Path,
    primary_attributes_schema: Optional[Tuple["BaseAttributeSchema", ...]],
    primary_attributes: dict,
) -> Path:
    """Make symlink path from given attributes.

    :param path_to_symlink_dir: path to array or varray symlinks in certain collection
    :param primary_attributes_schema: schemas of primary attributes
    :param primary_attributes: all primary attributes for a path
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
    """
    main_tree, rest = array_id.split("-", 1)
    main_tree = os.path.sep.join((s for s in main_tree))
    return data_directory / main_tree / rest


def get_paths(array: Union["Array", "VArray"], collection_path: Union[Path, str]) -> Paths:
    """Create main and symlink paths for arrays.

    :param array: (V)Array instance
    :param collection_path: path to a certain collection defined by user
    :returns: namedtuple with "main" and "symlink" pathlib.Paths
    """
    from deker.arrays import Array, VArray  # noqa: F811

    @singledispatch
    def get_path(arr: Union["Array", "VArray"], base_path: Path) -> Paths:
        """Generate paths by object type.

        :param arr: any object
        :param base_path: collection base path
        """
        raise TypeError(f"Invalid object type: {type(arr)}")

    @get_path.register(Array)
    def array_path(arr: Array, base_path: Path) -> Paths:
        """Generate paths for Array.

        :param arr: Array type
        :param base_path: collection base path
        """
        # main_path
        main_path = get_main_path(arr.id, base_path / arr._adapter.data_dir)  # type: ignore[operator, attr-defined]

        # symlink
        primary_attributes: Optional[
            Tuple["BaseAttributeSchema", ...]
        ] = arr.schema.primary_attributes
        symlink_path = get_symlink_path(
            path_to_symlink_dir=base_path / arr._adapter.symlinks_dir,  # type: ignore[operator, attr-defined]
            primary_attributes_schema=primary_attributes,
            primary_attributes={
                **arr.primary_attributes,
                "vid": arr._vid,
                "v_position": arr._v_position,
            },
        )
        return Paths(main_path, symlink_path)

    @get_path.register(VArray)
    def varray_path(arr: VArray, base_path: Path) -> Paths:
        """Generate paths for VArray.

        :param arr: VArray type
        :param base_path: collection base path
        """
        # main_path
        main_path = get_main_path(arr.id, base_path / arr._adapter.data_dir)  # type: ignore[operator, attr-defined]

        # symlink
        primary_attributes: Optional[Tuple["BaseAttributeSchema"]] = arr.schema.primary_attributes
        symlink_path = get_symlink_path(
            path_to_symlink_dir=base_path / arr._adapter.symlinks_dir,  # type: ignore[operator, attr-defined]
            primary_attributes_schema=primary_attributes,
            primary_attributes={**arr.primary_attributes},
        )
        return Paths(main_path, symlink_path)

    coll_path = Path(collection_path) if isinstance(collection_path, str) else collection_path
    return get_path(array, coll_path)


def get_array_lock_path(array: Union["Array", "VArray"], data_directory_path: Path) -> Path:
    """Get array lockfile path.

    :param array: array to lock
    :param data_directory_path: path to data directory
    """
    file = (
        hashlib.md5(str(array.primary_attributes).encode()).hexdigest()
        + LocksExtensions.array_lock.value
    )  # noqa
    return data_directory_path / file

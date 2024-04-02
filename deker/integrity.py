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

from collections import defaultdict
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import tqdm

from deker.ABC.base_integrity import BaseChecker
from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.errors import (
    DekerBaseApplicationError,
    DekerCollectionNotExistsError,
    DekerIntegrityError,
    DekerMetaDataError,
)
from deker.managers import ArrayManager, VArrayManager
from deker.tools import get_main_path, get_symlink_path
from deker.types.private.enums import LocksExtensions


if TYPE_CHECKING:
    from deker.client import Client


class DataChecker(BaseChecker):
    """Checks Array's single number data from subset."""

    CHECKER_LEVEL = 4

    def check(self, array: Array) -> None:
        """Check if array can be read correctly.

        :param array: array to check
        """
        if self.level < self.CHECKER_LEVEL:
            return

        try:
            subset = array[tuple(s - 1 for s in array.shape)]
            data = subset.read()
        except Exception as e:
            raise DekerIntegrityError(f"Array {array.id} data is corrupted: {e!s}")
        if data.dtype != array.dtype:
            raise DekerIntegrityError(f"Array {array.id} data is corrupted: incorrect dtype")


class PathsChecker(BaseChecker):
    """Checks Array or VArray paths."""

    CHECKER_LEVEL = 3

    def _validate_path_array_without_key_attributes(
        self, main_path: Path, symlink_path: Path, files: list
    ) -> None:
        """Validate symlinks in array without key attributes.

        :param main_path: path to the array data
        :param symlink_path: path to symlink directory
        :param files: files in symlink directory
        """
        for file in files:
            if main_path / file == Path.readlink(symlink_path / file):
                self.paths[main_path] = symlink_path
                break
        if not self.paths.get(main_path):
            raise DekerIntegrityError(f"Incorrect symlink {symlink_path}")

    def _validate_paths(self, main_path: Path, symlink_path: Path, collection: Collection) -> None:
        """Validate symlink and main paths.

        :param main_path: Array or VArray main path
        :param symlink_path: Array or VArray symlink
        :param collection: Collection
        """
        if not symlink_path.exists():
            raise DekerIntegrityError(f"Symlink {symlink_path} not found")

        files = [str(f.name) for f in symlink_path.iterdir()]

        if len(files) < 1:
            raise DekerIntegrityError(f"Symlink {symlink_path} not found")

        if collection.array_schema.primary_attributes:
            if len(files) > 1:
                files = ["\t- " + str(file) + "\n" for file in files]
                raise DekerIntegrityError(
                    f"There are unnecessary files in directory:\n{symlink_path}\n {''.join(files)}"
                )

            if main_path / files[0] == Path.readlink(symlink_path / files[0]):
                self.paths[main_path] = symlink_path
            else:
                raise DekerIntegrityError(f"Incorrect symlink {symlink_path}")
        else:
            self._validate_path_array_without_key_attributes(main_path, symlink_path, files)

    def check(self, array: Union[Array, VArray], collection: Collection) -> None:
        """Check symlink and main paths.

        :param array: array to check
        :param collection: Collection
        """
        if self.level < self.CHECKER_LEVEL:
            return
        if isinstance(array, Array):
            main_path_args = (array.id, collection.path / self.ctx.config.array_data_directory)
            symlink_dir = self.ctx.config.array_symlinks_directory
            symlink_path_args = (
                collection.path / symlink_dir,
                array.schema.primary_attributes,
                {
                    **array.primary_attributes,
                    "vid": array._vid,
                    "v_position": array._v_position,
                },
            )
        else:
            main_path_args = (array.id, collection.path / self.ctx.config.varray_data_directory)
            symlink_dir = self.ctx.config.varray_symlinks_directory
            symlink_path_args = (
                collection.path / symlink_dir,
                array.schema.primary_attributes,
                {**array.primary_attributes},
            )
        main_path = get_main_path(*main_path_args)
        symlink_path = get_symlink_path(*symlink_path_args)
        self._validate_paths(main_path, symlink_path, collection)

        if self.next_checker:
            self.next_checker.check(array)


class ArraysChecker(BaseChecker):
    """Checks arrays in collection."""

    CHECKER_LEVEL = 2

    def check_arrays_locks(self, collection: Collection) -> None:
        """Check if Arrays or VArrays have no lockfiles left from create method.

        :param collection: Collection to be checked
        """
        for file in Path.rglob(collection.path, "*lock"):
            if file.name.endswith(LocksExtensions.array_lock.value):
                self.errors[f"Collection {collection.name} array create locks:"].append(file.name)
            elif file.name.endswith(LocksExtensions.array_read_lock.value):
                self.errors[
                    f"Collection {collection.name} array read locks are detected. Use "
                    f"client.clear_locks:"
                ].append(file.name)
            elif file.name.endswith(LocksExtensions.varray_lock.value):
                self.errors[
                    f"Collection {collection.name} varray write locks are detected. Use "
                    f"client.clear_locks:"
                ].append(file.name)
        if self.stop_on_error and self.errors:
            raise DekerIntegrityError(self._parse_errors())

    def _check_varrays_or_arrays(
        self, collection: Collection, data_manager: Union[ArrayManager, Optional[VArrayManager]]
    ) -> None:
        """Check if Arrays or VArrays in Collection are initializing.

        :param collection: Collection to be checked
        :param data_manager: DataManager to get arrays or varrays from collection
        """
        try:
            for array in data_manager:
                try:
                    if self.next_checker:
                        self.next_checker.check(array, collection)
                except DekerBaseApplicationError as e:
                    if self.stop_on_error:
                        raise DekerIntegrityError(str(e))
                    self.errors[f"Collection {collection.name} arrays integrity errors:"].append(
                        str(e)
                    )
        except DekerMetaDataError as e:
            if self.stop_on_error:
                raise e
            self.errors[f"Collection {collection.name} (V)Arrays initialization errors:"].append(
                str(e)
            )

    def check(self, collection: Collection) -> None:
        """Check if Arrays or VArrays and their locks in Collection are valid.

        :param collection: Collection to be checked
        """
        if self.level < self.CHECKER_LEVEL:
            return
        self.check_arrays_locks(collection)

        self._check_varrays_or_arrays(collection, collection.arrays)
        if collection.varray_schema:
            self._check_varrays_or_arrays(collection, collection.varrays)
        return


class CollectionsChecker(BaseChecker):
    """Checks collections initialization & lockfiles."""

    CHECKER_LEVEL = 1

    def check_collections(self) -> list:
        """Check collections integrity."""
        collections: list = []
        locks: list = []
        for directory in Path(self.root_path).iterdir():
            try:
                if directory.is_file() and directory.name.endswith(
                    LocksExtensions.collection_lock.value
                ):
                    locks.append(directory.name[: -len(LocksExtensions.collection_lock.value)])
                collection = self.client.get_collection(directory.name)
                if collection:
                    collections.append(collection)
            except DekerBaseApplicationError as e:
                self.errors["Collections initialization errors:"].append(str(e))
        for collection in collections:
            if collection.name not in locks:
                self.errors["Collections locks errors:"].append(
                    f"BaseLock for {collection.name} not found"
                )
        for lock in locks:
            for collection in collections:
                if collection.name == lock:
                    break
            else:
                self.errors["Collections locks errors:"].append(
                    f"Collection with lock {lock} not found"
                )
        if self.stop_on_error and self.errors:
            raise DekerIntegrityError(self._parse_errors())
        return collections

    def check(self, collection_name: Optional[str] = None) -> None:
        """Check collections and run integrity check for every collection if level > 1.

        :param collection_name: optional collection to be checked
        """
        if collection_name:
            collection: Collection = self.client.get_collection(collection_name)
            if not collection:
                raise DekerCollectionNotExistsError(
                    f"Collection {collection_name} does not exist at this storage"
                )
            if self.level > self.CHECKER_LEVEL:
                if self.next_checker:
                    self.next_checker.check(collection)
        collections = self.check_collections()
        if self.level > self.CHECKER_LEVEL:
            collections_pbar = tqdm.tqdm(collections)
            for collection in collections_pbar:
                collections_pbar.set_description(f'Checking collection "{collection.name}"')
                if self.next_checker:
                    self.next_checker.check(collection)


class IntegrityChecker(BaseChecker):
    """Storage integrity checker."""

    # :
    def __init__(self, client: "Client", root_path: Path, stop_on_error: bool, level: int) -> None:
        super().__init__(stop_on_error, {}, defaultdict(list), level, client, root_path)  # type: ignore[attr-defined]
        self.add_checker(CollectionsChecker)
        self.add_checker(ArraysChecker)
        self.add_checker(PathsChecker)
        self.add_checker(DataChecker)

    def check(self, collection_name: Optional[str] = None) -> str:
        """Run integrity check.

        :param collection_name: optional collection to check
        """
        print("Integrity check is running...")
        if self.next_checker:
            self.next_checker.check(collection_name)
        return self._parse_errors()

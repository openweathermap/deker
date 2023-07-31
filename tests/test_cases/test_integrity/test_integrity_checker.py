import json
import os
import shutil
import sys

from datetime import datetime, timezone
from pathlib import Path
from typing import Type

import h5py
import numpy
import numpy as np
import pytest

from deker_local_adapters import LocalCollectionAdapter

from tests.parameters.collection_params import ClientParams

from deker.ABC import BaseStorageAdapter
from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.ctx import CTX
from deker.errors import DekerCollectionNotExistsError, DekerIntegrityError, DekerMetaDataError
from deker.integrity import (
    ArraysChecker,
    CollectionsChecker,
    DataChecker,
    IntegrityChecker,
    PathsChecker,
)
from deker.schemas import ArraySchema, VArraySchema
from deker.tools import get_array_lock_path, get_main_path, get_symlink_path
from deker.types import LocksExtensions
from deker.uri import Uri


class TestIntegrityChecker:
    def test_check_ok(self, client: Client, root_path: Path, array_schema: ArraySchema, ctx: CTX):
        """Tests if function raises no exception by default."""

        collection = client.create_collection("test_integrity_ok", array_schema)
        collection.create()
        integrity_checker = IntegrityChecker(
            client, root_path / ctx.config.collections_directory, True, 4
        )
        try:
            integrity_checker.check()
        finally:
            collection.delete()

    def test_check_ok_collection(
        self, client: Client, root_path: Path, array_schema: ArraySchema, ctx: CTX
    ):
        """Tests if function raises no exception by default if collection name is provided."""

        collection = client.create_collection("test_integrity_ok", array_schema)
        collection.create()
        integrity_checker = IntegrityChecker(
            client, root_path / ctx.config.collections_directory, True, 4
        )
        try:
            integrity_checker.check("test_integrity_ok")
        finally:
            collection.delete()

    def test_check_collection_does_not_exist(
        self, client: Client, root_path: Path, array_schema: ArraySchema, ctx: CTX
    ):
        """Tests if function raises no exception by default if collection name is provided."""

        integrity_checker = IntegrityChecker(
            client, root_path / ctx.config.collections_directory, True, 4
        )
        with pytest.raises(DekerCollectionNotExistsError):
            integrity_checker.check("collection_does_not_exist")

    def test_check_locks(
        self, client: Client, root_path: Path, array_schema: ArraySchema, ctx: CTX
    ):
        """Tests if function returns error if lock is not found."""

        collection = client.create_collection("test_integrity_locks", array_schema)
        collection.create()
        integrity_checker = IntegrityChecker(
            client, root_path / ctx.config.collections_directory, False, 4
        )

        try:
            filename = collection.path.parent / (collection.name + ".lock")
            os.remove(filename)
            errors = integrity_checker.check()
            assert (
                errors
                == f"Collections locks errors:\n\t- BaseLock for {collection.name} not found\n"
            )
        finally:
            collection.delete()
            shutil.rmtree(
                collection.path,
                onerror=FileNotFoundError,
            )

    def test_check_extra_locks(
        self, client: Client, root_path: Path, array_schema: ArraySchema, ctx: CTX
    ):
        """Tests if function returns error if there are extra locks in a folder."""

        collection = client.create_collection("test_integrity_locks_extra", array_schema)
        collection.create()
        integrity_checker = IntegrityChecker(
            client, root_path / ctx.config.collections_directory, False, 4
        )

        try:
            shutil.rmtree(
                collection.path,
                onerror=FileNotFoundError,
            )
            errors = integrity_checker.check()
            assert (
                errors
                == f"Collections locks errors:\n\t- Collection with lock {collection.name} not found\n"
            )
        finally:
            filename = collection.path.parent / (collection.name + ".lock")
            os.remove(filename)
            collection.delete()

    def test_check_return(
        self,
        array_schema_with_attributes: ArraySchema,
        client: Client,
        root_path: Path,
        ctx: CTX,
        uri: Uri,
        storage_adapter: Type[BaseStorageAdapter],
    ):
        """Tests if function returns errors."""
        integrity_checker = IntegrityChecker(
            client, root_path / ctx.config.collections_directory, False, 4
        )
        collection = client.create_collection("test_return", array_schema_with_attributes)
        array_1 = collection.create(
            primary_attributes={"primary_attribute": 1},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )

        array_2 = collection.create(
            primary_attributes={"primary_attribute": 2},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )

        main_path = get_main_path(array_1.id, collection.path / array_1._adapter.data_dir)
        file = array_1.id + storage_adapter.file_ext
        filename = main_path / file

        with h5py.File(filename, "r+") as f:
            ds = f.create_dataset("data", dtype=int, shape=(2, 2, 2))
            ds.flush()
            f.flush()

        symlink_path = get_symlink_path(
            path_to_symlink_dir=array_2._Array__collection.path / array_2._adapter.symlinks_dir,
            primary_attributes_schema=array_2._Array__collection.array_schema.primary_attributes,
            primary_attributes=array_2.primary_attributes,
        )
        files = os.listdir(symlink_path)
        Path.unlink(symlink_path / files[0])

        try:
            errors = integrity_checker.check()
            error_1 = f"Symlink {symlink_path} not found\n"
            error_2 = f"Array {array_1.id} data is corrupted: Index (9) out of range for (0-1)\n"

            assert error_2 in errors and error_1 in errors
        finally:
            collection.delete()

    def test_check_array_raises_on_init(
        self,
        array_schema_with_attributes: ArraySchema,
        client: Client,
        root_path: Path,
        ctx: CTX,
        uri: Uri,
        storage_adapter: Type[BaseStorageAdapter],
    ):
        """Tests if function raises exception if array file is incorrect."""
        collection = client.create_collection(
            "test_check_array_raises_on_init", array_schema_with_attributes
        )
        integrity_checker = IntegrityChecker(
            client, root_path / ctx.config.collections_directory, True, 4
        )
        array = collection.create(
            primary_attributes={"primary_attribute": 10},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )

        main_path = get_main_path(array.id, collection.path / array._adapter.data_dir)
        file = array.id + storage_adapter.file_ext
        filename = main_path / file

        with h5py.File(filename, "r+") as f:
            ds = f.get("meta")
            meta = json.loads(ds[()])
            del f["meta"]
            f.flush()

            meta["primary_attributes"] = {"incorrect_attribute": 10}
            json_meta = json.dumps(meta, default=str)
            ds = f.create_dataset(
                "meta",
                dtype=f"S{sys.getsizeof(json_meta.encode('utf-8'))}",
                shape=(),
                data=json_meta,
            )
            ds.flush()
            f.flush()
        try:
            with pytest.raises(DekerMetaDataError):
                assert integrity_checker.check()
        finally:
            array.delete()


class TestCollectionsChecker:
    def test_check_ok(
        self,
        inserted_array_with_attributes: Array,
        client: Client,
        collections_checker: CollectionsChecker,
    ):
        """Tests if function raises no exception by default."""
        collections_checker.check()

    def test_check_locks(
        self, client: Client, collections_checker: CollectionsChecker, array_schema: ArraySchema
    ):
        """Tests if function returns error if lock is not found."""

        collection = client.create_collection("test_arrays_integrity_locks", array_schema)
        collection.create()

        try:
            filename = collection.path.parent / (collection.name + ".lock")
            os.remove(filename)
            with pytest.raises(DekerIntegrityError):
                collections_checker.check()
        finally:
            collection.delete()
            shutil.rmtree(
                collection.path,
                onerror=FileNotFoundError,
            )


class TestPathsChecker:
    def test_check_ok(
        self, inserted_array_with_attributes: Array, client: Client, paths_checker: PathsChecker
    ):
        """Tests if function raises no exception by default."""
        paths_checker.check(
            inserted_array_with_attributes, inserted_array_with_attributes._Array__collection  # type: ignore[attr-defined]
        )

    def test_check_symlink_not_found(
        self,
        array_collection_with_attributes: Collection,
        client: Client,
        paths_checker: PathsChecker,
    ):
        """Tests if function raises exception if symlink not found."""
        array = array_collection_with_attributes.create(
            primary_attributes={"primary_attribute": 5},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )

        symlink_path = get_symlink_path(
            path_to_symlink_dir=array._Array__collection.path / array._adapter.symlinks_dir,
            primary_attributes_schema=array._Array__collection.array_schema.primary_attributes,
            primary_attributes=array.primary_attributes,
        )
        files = os.listdir(symlink_path)
        Path.unlink(symlink_path / files[0])
        try:
            with pytest.raises(DekerIntegrityError):
                paths_checker.check(array, array._Array__collection)
        finally:
            array.delete()

    def test_check_extra_files(
        self,
        array_collection_with_attributes: Collection,
        client: Client,
        paths_checker: PathsChecker,
    ):
        """Tests if function raises exception if there are unnecessary files in directory."""
        array = array_collection_with_attributes.create(
            primary_attributes={"primary_attribute": 5},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )

        symlink_path = get_symlink_path(
            path_to_symlink_dir=array._Array__collection.path / array._adapter.symlinks_dir,
            primary_attributes_schema=array._Array__collection.array_schema.primary_attributes,
            primary_attributes=array.primary_attributes,
        )
        open(symlink_path / "test_file", "x")
        try:
            with pytest.raises(DekerIntegrityError):
                paths_checker.check(array, array._Array__collection)
        finally:
            array.delete()

    def test_check_symlink_extra(
        self,
        array_collection_with_attributes: Collection,
        storage_adapter,
        paths_checker: PathsChecker,
    ):
        """Tests if function raises exception if there are extra symlinks in the directory."""
        array_1 = array_collection_with_attributes.create(
            primary_attributes={"primary_attribute": 1},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )
        array_2 = array_collection_with_attributes.create(
            primary_attributes={"primary_attribute": 2},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )

        main_path = get_main_path(
            array_1.id, array_collection_with_attributes.path / array_1._adapter.data_dir
        )
        file = array_1.id + storage_adapter.file_ext
        filename = main_path / file

        symlink_path = get_symlink_path(
            path_to_symlink_dir=array_collection_with_attributes.path
            / array_2._adapter.symlinks_dir,
            primary_attributes_schema=array_collection_with_attributes.array_schema.primary_attributes,
            primary_attributes=array_2.primary_attributes,
        )

        os.symlink(filename, symlink_path / file)
        try:
            paths_checker.check(array_1, array_collection_with_attributes)

            with pytest.raises(DekerIntegrityError):
                paths_checker.check(array_2, array_collection_with_attributes)
        finally:
            array_1.delete()
            array_2.delete()

    def test_check_varray_ok(self, inserted_varray: VArray, paths_checker: PathsChecker):
        """Tests if function raises no exception by default."""
        paths_checker.check(
            inserted_varray,
            inserted_varray._VArray__collection,  # type: ignore[attr-defined]
        )

    def test_check_varray(
        self, inserted_varray: VArray, client: Client, paths_checker: PathsChecker
    ):
        """Tests if check_paths works correctly with for varray."""
        symlink_path = get_symlink_path(
            path_to_symlink_dir=inserted_varray._VArray__collection.path  # type: ignore[attr-defined]
            / inserted_varray._adapter.symlinks_dir,
            primary_attributes_schema=None,
            primary_attributes=inserted_varray.primary_attributes,
        )
        files = os.listdir(symlink_path)
        Path.unlink(symlink_path / files[0])
        with pytest.raises(DekerIntegrityError):
            paths_checker.check(
                inserted_varray, inserted_varray._VArray__collection  # type: ignore[attr-defined]
            )

    def test_check_raises_on_array_symlink(
        self,
        inserted_varray: VArray,
        client: Client,
        arrays_checker: ArraysChecker,
        factory,
    ):
        """Tests checker raises error if some array has no symlink."""
        root_path = (
            inserted_varray._VArray__collection.path  # type: ignore[attr-defined]
            / factory.ctx.config.array_symlinks_directory
            / inserted_varray.id
        )
        for root, _, files in os.walk(root_path):
            for file in files:
                if file.endswith(factory.ctx.storage_adapter.file_ext):
                    symlink = Path(os.path.join(root, file))
                    symlink.unlink()

        with pytest.raises(DekerIntegrityError):
            arrays_checker.check(inserted_varray._VArray__collection)  # type: ignore[attr-defined]


class TestArraysChecker:
    def test_check_varray_collection_raises_on_array_data(
        self,
        varray_schema: VArraySchema,
        client: Client,
        array_data: np.ndarray,
        arrays_checker: ArraysChecker,
        root_path: Path,
        factory,
    ):
        """Tests if checker raises exception in case of incorrect array's data in varray collection."""
        varray_collection = client.create_collection(
            name="test_check_varray_collection_raises_on_array_data", schema=varray_schema
        )
        array = varray_collection.create()
        array[:].update(array_data)
        root_path = varray_collection.path / factory.ctx.config.array_symlinks_directory / array.id
        for root, _, files in os.walk(root_path):
            corrupted = False
            for file in files:
                if file.endswith(factory.ctx.storage_adapter.file_ext):
                    symlink = Path(os.path.join(root, file))
                    with h5py.File(symlink.readlink(), "r+") as f:
                        del f["data"]
                        ds = f.create_dataset("data", dtype=int, shape=(2, 2, 2))
                        ds.flush()
                        f.flush()
                    break
            if corrupted:
                break
        try:
            with pytest.raises(DekerIntegrityError):
                arrays_checker.check(varray_collection)  # type: ignore
        finally:
            varray_collection.delete()

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_check_ok(
        self,
        params: dict,
        arrays_checker: ArraysChecker,
        collection_adapter: LocalCollectionAdapter,
        client: Client,
    ):
        """Tests if checker raises no exception by default.

        :param params: collection params
        :param arrays_checker: checker fixture
        :param collection_adapter: adapter fixture
        :param client: client instance
        """
        collection = client.create_collection(**params)
        filename = collection.path / (collection.name + collection_adapter.file_ext)
        open(filename, "w").close()
        try:
            arrays_checker.check(collection)
        finally:
            collection.delete()

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_check_arrays_locks(self, params: dict, client: Client, arrays_checker: ArraysChecker):
        """Tests if function returns error if there are extra lock files for arrays.

        :param params: collection params
        :param client: client instance
        :param arrays_checker: checker fixture
        """
        collection = client.create_collection(**params)
        array = collection.create()
        lockfile = get_array_lock_path(array, collection.path / array._adapter.data_dir)
        open(lockfile, mode="w")
        try:
            with pytest.raises(DekerIntegrityError):
                arrays_checker.check_arrays_locks(collection)
        finally:
            collection.delete()

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_check_arrays_locks_read(
        self, params: dict, client: Client, arrays_checker: ArraysChecker
    ):
        """Tests if function returns error if there are extra readlock files for arrays.

        :param params: collection params
        :param client: client instance
        :param arrays_checker: checker fixture
        """
        collection = client.create_collection(**params)
        array = collection.create()
        lockfile = collection.path / array._adapter.data_dir / LocksExtensions.array_read_lock.value
        open(lockfile, mode="w")
        try:
            with pytest.raises(DekerIntegrityError):
                arrays_checker.check_arrays_locks(collection)
        finally:
            collection.delete()

    @pytest.mark.parametrize("params", [ClientParams.ArraySchema.OK.no_vgrid_no_attrs()])
    def test_check_arrays_locks_varray(
        self, params: dict, client: Client, arrays_checker: ArraysChecker
    ):
        """Tests if function returns error if there are extra lock files for varrays.

        :param params: collection params
        :param client: client instance
        :param arrays_checker: checker fixture
        """
        collection = client.create_collection(**params)
        array = collection.create()
        lockfile = collection.path / array._adapter.data_dir / LocksExtensions.varray_lock.value
        open(lockfile, mode="w")
        try:
            with pytest.raises(DekerIntegrityError):
                arrays_checker.check_arrays_locks(collection)
        finally:
            collection.delete()

    def test_check_arrays_locks_read_array_in_varray(
        self, varray_collection: Collection, arrays_checker: ArraysChecker
    ):
        """Tests if function returns error if there are extra readlock files for array in varray.

        :param varray_collection: collection instance
        :param arrays_checker: checker fixture
        """
        varray = varray_collection.create()
        lockfile = (
            varray_collection.path
            / varray._adapter.data_dir
            / LocksExtensions.array_read_lock.value
        )
        open(lockfile, mode="w")
        try:
            with pytest.raises(DekerIntegrityError):
                arrays_checker.check_arrays_locks(varray_collection)
        finally:
            varray_collection.delete()


class TestDataChecker:
    @pytest.mark.parametrize(
        ("dtype", "shape"),
        [
            (int, (10, 10, 10)),
            (numpy.float64, (1, 1, 1)),
        ],
    )
    def test_check_raises_on_data(
        self,
        dtype: type,
        shape: np.shape,
        array_collection_with_attributes: Collection,
        client: Client,
        data_checker: DataChecker,
        storage_adapter,
    ):
        """Tests if function raises exception in case of incorrect data dtype or shape.

        :param dtype: ArraySchema dtype
        :param shape: ArraySchema shape
        :param array_collection_with_attributes: collection instance
        :param client: client instance
        :param data_checker: checker fixture
        :param storage_adapter: adapter fixture
        """

        array = array_collection_with_attributes.create(
            primary_attributes={"primary_attribute": 1},
            custom_attributes={"time_attr_name": datetime.now(timezone.utc)},
        )
        main_path = get_main_path(
            array.id, array_collection_with_attributes.path / array._adapter.data_dir
        )
        file = array.id + storage_adapter.file_ext
        filename = main_path / file

        with h5py.File(filename, "r+") as f:
            ds = f.create_dataset("data", dtype=dtype, shape=shape)
            ds.flush()
            f.flush()

        try:
            with pytest.raises(DekerIntegrityError):
                assert data_checker.check(array)
        finally:
            array.delete()

    def test_ok(self, inserted_array: Array, client: Client, data_checker: DataChecker):
        """Tests if function raises no exception by default."""
        data_checker.check(inserted_array)


if __name__ == "__main__":
    pytest.main()

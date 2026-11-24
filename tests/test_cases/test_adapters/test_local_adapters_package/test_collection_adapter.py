import errno
import os
import shutil
import time

from pathlib import Path
from uuid import uuid4

import pytest

from cantok import DefaultToken, SimpleToken
from deker_local_adapters import AdaptersFactory, LocalCollectionAdapter
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from deker_tools.cancel import TokenCancelledError
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.parameters.common import random_string

from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerAdapterClosedError,
    DekerCollectionAlreadyExistsError,
    DekerCollectionNotExistsError,
    DekerLockError,
    DekerMemoryError,
)
from deker.flock import Flock
from deker.schemas import ArraySchema, DimensionSchema


@pytest.mark.asyncio
class TestCollectionAdapter:
    """Class for testing local collection adapter."""

    def test_collection_adapter_create_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        root_path: Path,
        array_schema: ArraySchema,
        factory: AdaptersFactory,
    ):
        """Tests if adapter creates array_collection in DB.

        :param collection_adapter: CollectionAdapter
        :param root_path: Path to collections directory
        :param array_schema: ArraySchema instance
        """
        name = random_string()
        collection_adapter.create(
            Collection(
                name=name,
                schema=array_schema,
                adapter=collection_adapter,
                factory=factory,
                storage_adapter=HDF5StorageAdapter,
            ),
            timeout=Sentinel,
            token=DefaultToken(),
        )
        assert root_path.joinpath(factory.ctx.config.collections_directory).joinpath(name).exists()
        # the lock file of the creation stays as the marker of the collection
        assert root_path.joinpath(factory.ctx.config.collections_directory).joinpath(name + ".lock").exists()

    def test_collection_adapter_create_collection_memory_error(
        self,
        collection_adapter: LocalCollectionAdapter,
        factory: AdaptersFactory,
        mocker: MockerFixture,
    ):
        """Tests if adapter raises memory error on collection creation.

        :param collection_adapter: CollectionAdapter
        :param factory: AdaptersFactory
        """
        mocker.patch.object(collection_adapter.ctx.config, "memory_limit", 100)  # the ctx is shared by the class
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=10000),
                DimensionSchema(name="y", size=10000),
            ],
            dtype=float,
        )
        col_name = "memory_excess_adapter"
        with pytest.raises(DekerMemoryError):
            collection_adapter.create(
                Collection(
                    name=col_name,
                    schema=schema,
                    adapter=collection_adapter,
                    factory=factory,
                    storage_adapter=HDF5StorageAdapter,
                ),
                timeout=Sentinel,
                token=DefaultToken(),
            )

    @pytest.mark.parametrize(
        ("splitter", "expected"),
        [
            ({"vgrid": [1, 3]}, (3, 1)),
            ({"arrays_shape": [1, 3]}, (3, 1)),
            ({"vgrid": [3, 1]}, (1, 3)),
            ({"arrays_shape": [3, 1]}, (1, 3)),
        ],
    )
    def test_create_from_dict_varray_collection(
        self, client: Client, varray_collection: Collection, splitter: dict, expected: tuple
    ):
        """Check if creation from dict is correct."""
        schema = {
            "metadata_version": "0.2",
            "name": "varray_from_dict_test_collection",
            "type": "varray",
            "schema": {
                "dtype": "int",
                "fill_value": -1,
                "attributes": [
                    {"name": "primary_attr_1", "dtype": "string", "primary": True},
                    {"name": "custom_attr_1", "dtype": "datetime", "primary": False},
                ],
                "dimensions": [
                    {
                        "type": "generic",
                        "name": "dimension_1",
                        "size": 3,
                        "labels": ["label_1", "label_2", "label_3"],
                    },
                    {
                        "type": "time",
                        "name": "dimension_2",
                        "size": 3,
                        "start_value": "$custom_attr_1",
                        "step": {"days": 0, "seconds": 14400, "microseconds": 0},
                    },
                ],
            },
            "options": {
                "chunks": {"mode": "manual", "size": [1, 3]},
                "compression": {"compression": "gzip", "options": [9]},
            },
        }
        col_from_dict = None

        try:
            schema["schema"].update(**splitter)
            col_from_dict = client.collection_from_dict(schema)
            assert col_from_dict
            assert col_from_dict.name == schema["name"]
            if list(splitter.keys())[0] == "vgrid":
                other_splitter = "arrays_shape"
            else:
                other_splitter = "vgrid"
            assert getattr(col_from_dict.varray_schema, other_splitter) == expected
        finally:
            if col_from_dict:
                col_from_dict.delete()

    def test_create_from_other_varray_collection_dict(self, client: Client, varray_collection: Collection):
        """Check if creation from dict is correct."""
        col_from_dict = None

        new_col_dict = varray_collection.as_dict
        new_col_dict["name"] = str(uuid4())
        try:
            col_from_dict = client.collection_from_dict(new_col_dict)
            # Compare through dicts
            col_dict = col_from_dict.as_dict

            del col_dict["name"]
            del new_col_dict["name"]

            assert col_dict == new_col_dict
        finally:
            if col_from_dict:
                col_from_dict.delete()

    def test_collection_adapter_deletes_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        array_collection: Collection,
        root_path: Path,
    ):
        """Tests if array_collection adapter deletes array_collection from db properly.

        :param collection_adapter: array_collection adapter
        :param array_collection: Pre created array_collection
        :param root_path: Path to collections directory
        """
        collection_adapter.delete(array_collection, timeout=Sentinel, token=DefaultToken())
        assert not os.path.exists(array_collection.path)
        assert not os.path.exists(array_collection.path.parent / (array_collection.name + ".lock"))

    def test_collection_adapter_delete_tolerates_a_collection_already_gone(
        self, collection_adapter: LocalCollectionAdapter, array_collection: Collection
    ):
        """Deleting a collection whose directory is gone already is not an error: the lock file is removed anyway."""
        shutil.rmtree(array_collection.path)
        lock = array_collection.path.parent / (array_collection.name + ".lock")
        assert lock.exists()
        collection_adapter.delete(array_collection, timeout=Sentinel, token=DefaultToken())
        assert not lock.exists()

    def test_collection_adapter_delete_raises_when_the_directory_cannot_be_removed(
        self, collection_adapter: LocalCollectionAdapter, array_collection: Collection, mocker: MockerFixture
    ):
        """A failure to remove the directory is not swallowed: the collection would stay with its files."""
        # a real OSError carries errno and strerror: rmtree of Python 3.12+ adds the filename to it before raising
        mocker.patch.object(os, "rmdir", side_effect=PermissionError(errno.EACCES, "read-only storage"))
        with pytest.raises(PermissionError, match="read-only storage"):
            collection_adapter.delete(array_collection, timeout=Sentinel, token=DefaultToken())
        assert os.path.exists(array_collection.path)

    def test_collection_adapter_get_collection(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
    ):
        """Tests if array_collection adapter fetch array_collection from db correctly.

        :param array_collection: Pre created array_collection
        :param collection_adapter: array_collection adapter
        """
        schema = collection_adapter.read(name=array_collection.name)
        assert schema

    def test_collection_adapter_clear(
        self,
        array_collection: Collection,
        collection_adapter: LocalCollectionAdapter,
        root_path: Path,
    ):
        """Tests clearing data directory.

        :param array_collection: Pre created collection
        :param collection_adapter: collection adapter
        :param root_path: Path to collections directory
        """
        collection_adapter.clear(array_collection, timeout=Sentinel, token=DefaultToken())

        collection_dir = None
        for child in root_path.joinpath(collection_adapter.ctx.config.collections_directory).iterdir():
            if child.name == array_collection.name:
                collection_dir = child

        assert collection_dir is not None
        collection_dir = os.listdir(collection_dir)
        assert f"{array_collection.name}.json" in collection_dir
        assert os.path.exists(array_collection.path.parent / (array_collection.name + ".lock"))

    def test_collection_adapter_iter(self, client: Client, array_schema: ArraySchema, root_path: Path, ctx):
        for root, dirs, files in os.walk(root_path / ctx.config.collections_directory):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

        names = {random_string() for _ in range(10)}
        collections = [client.create_collection(name, array_schema) for name in names]
        adapter: LocalCollectionAdapter = collections[0]._Collection__adapter
        coll_names = {coll["name"] for coll in adapter}
        assert names.issubset(coll_names)
        assert names - coll_names == set()


class TestCollectionAdapterClosed:
    @pytest.mark.parametrize("method", ["create", "delete", "clear", "is_deleted"])
    def test_closed_adapter_raises(self, ctx, array_collection: Collection, method: str):
        """Tests if a collection adapter of a closed context answers with DekerAdapterClosedError."""
        adapter = LocalCollectionAdapter(ctx)
        ctx.is_closed = True
        try:
            with pytest.raises(DekerAdapterClosedError):
                getattr(adapter, method)(array_collection)
            with pytest.raises(DekerAdapterClosedError):
                adapter.read(array_collection.name)
            with pytest.raises(DekerAdapterClosedError):
                list(adapter)
        finally:
            ctx.is_closed = False

    def test_closed_factory_raises(self, ctx, uri):
        """Tests if a factory of a closed context gives no adapters."""
        factory = AdaptersFactory(ctx, uri)
        ctx.is_closed = True
        try:
            with pytest.raises(DekerAdapterClosedError):
                factory.get_collection_adapter()
        finally:
            ctx.is_closed = False


class TestCollectionAdapterEdges:
    def test_create_path_raises_if_directory_exists(self, collection_adapter: LocalCollectionAdapter):
        name = "already-there"
        path = collection_adapter._LocalCollectionAdapter__create_path(name)
        try:
            with pytest.raises(DekerCollectionAlreadyExistsError):
                collection_adapter._LocalCollectionAdapter__create_path(name)
        finally:
            shutil.rmtree(path)

    def test_create_writes_nothing_into_a_missing_directory(
        self, collection_adapter: LocalCollectionAdapter, root_path: Path
    ):
        collection_adapter._LocalCollectionAdapter__create("ghost", {}, root_path / "ghost")
        assert not list(root_path.rglob("ghost*"))

    def test_read_raises_for_missing_collection(self, collection_adapter: LocalCollectionAdapter):
        with pytest.raises(DekerCollectionNotExistsError):
            collection_adapter.read("no-such-collection")

    def test_clear_removes_foreign_files_and_directories(self, client: Client, array_collection: Collection):
        foreign_file = array_collection.path / "stray.txt"
        foreign_file.write_text("x")
        foreign_dir = array_collection.path / "stray_dir"
        foreign_dir.mkdir()
        (foreign_dir / "inner.txt").write_text("x")
        array_collection.clear()
        assert not foreign_file.exists()
        assert not foreign_dir.exists()
        assert (array_collection.path / (array_collection.name + ".json")).exists()

    def test_create_skips_memory_check_when_configured(self, root_path, array_schema, executor):
        from tests.parameters.uri import embedded_uri

        client = Client(embedded_uri(root_path), executor=executor, skip_collection_create_memory_check=True)
        collection = client.create_collection("no-memory-check", array_schema)
        try:
            assert collection.path.exists()
        finally:
            collection.delete()
            client.close()


if __name__ == "__main__":
    pytest.main()


class TestCollectionAdapterCancellation:
    """The collection adapter stops a cancelled operation before the collection is touched."""

    @pytest.mark.parametrize("operation", ["clear", "delete"])
    def test_cancelled_token_changes_nothing(self, array_collection: Collection, collection_adapter, operation):
        array = array_collection.create()
        token = SimpleToken()
        token.cancel()
        with pytest.raises(TokenCancelledError):
            getattr(collection_adapter, operation)(array_collection, timeout=Sentinel, token=token)
        assert array_collection.path.exists()
        assert [a.id for a in array_collection] == [array.id]

    def test_create_with_a_cancelled_token_creates_nothing(
        self, client: Client, collection_adapter, array_schema, name
    ):
        collection = Collection(
            name=name,
            schema=array_schema,
            adapter=collection_adapter,
            factory=client._factory,
            storage_adapter=HDF5StorageAdapter,
        )
        token = SimpleToken()
        token.cancel()
        with pytest.raises(TokenCancelledError):
            collection_adapter.create(collection, timeout=Sentinel, token=token)
        assert not (collection_adapter.collections_resource / name).exists()
        assert not (collection_adapter.collections_resource / (name + ".lock")).exists(), "no lock without a collection"

    @pytest.mark.parametrize(
        ("operation", "failure", "collection_left"),
        [
            pytest.param("create", "makedirs", False, id="create-fails-before-the-directory"),
            pytest.param("create", "metadata", True, id="create-fails-after-the-directory"),
            pytest.param("create", "exists", True, id="create-of-an-existing-collection"),
            pytest.param("clear", "walk", True, id="clear-fails"),
        ],
    )
    def test_a_failed_operation_leaves_the_lock_only_beside_a_collection(
        self,
        client: Client,
        collection_adapter: LocalCollectionAdapter,
        array_schema: ArraySchema,
        name: str,
        mocker: MockerFixture,
        operation: str,
        failure: str,
        collection_left: bool,
    ):
        """The lock file marks a collection: a failure removes it only when no collection directory is left."""
        collection = Collection(
            name=name,
            schema=array_schema,
            adapter=collection_adapter,
            factory=client._factory,
            storage_adapter=HDF5StorageAdapter,
        )
        if failure in ("exists", "walk"):
            collection_adapter.create(collection, timeout=Sentinel, token=DefaultToken())
        error = OSError(errno.ENOSPC, "No space left on device")
        if failure == "makedirs":
            mocker.patch("deker_local_adapters.collection_adapter.os.makedirs", side_effect=error)
        elif failure == "metadata":
            mocker.patch.object(LocalCollectionAdapter, "_LocalCollectionAdapter__create", side_effect=error)
        elif failure == "walk":
            mocker.patch("deker_local_adapters.collection_adapter.os.walk", side_effect=error)
        root = collection_adapter.collections_resource
        try:
            with pytest.raises((OSError, DekerCollectionAlreadyExistsError)):
                getattr(collection_adapter, operation)(collection, timeout=Sentinel, token=DefaultToken())
            assert (root / name).is_dir() is collection_left
            assert (root / (name + ".lock")).exists() is collection_left
        finally:
            mocker.stopall()
            collection_adapter.delete(collection, timeout=Sentinel, token=DefaultToken())

    @pytest.mark.timeout(10)
    def test_clear_waits_for_the_collection_up_to_the_timeout(self, array_collection: Collection, collection_adapter):
        array = array_collection.create()
        with Flock(collection_adapter.collections_resource / (array_collection.name + ".lock")):
            start = time.monotonic()
            with pytest.raises(DekerLockError):
                collection_adapter.clear(array_collection, timeout=1, token=DefaultToken())
            assert time.monotonic() - start < 2.5
        assert [a.id for a in array_collection] == [array.id]

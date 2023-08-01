import os
import shutil

from pathlib import Path

import pytest

from deker_local_adapters import LocalCollectionAdapter
from pytest_mock import MockerFixture

from tests.parameters.common import random_string

from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerMemoryError
from deker.schemas import ArraySchema, DimensionSchema


@pytest.mark.asyncio()
class TestCollectionAdapter:
    """Class for testing local collection adapter."""

    def test_collection_adapter_create_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        root_path,
        array_schema: ArraySchema,
        factory,
    ):
        """Tests if adapter creates array_collection in DB.

        :param collection_adapter: CollectionAdapter
        :param root_path: Path to collections directory
        :param array_schema: ArraySchema instance
        """
        name = random_string()
        collection_adapter.create(
            Collection(
                name,
                array_schema,
                collection_adapter,
                factory,
                storage_adapter=collection_adapter.get_storage_adapter(),
            )
        )
        assert root_path.joinpath(factory.ctx.config.collections_directory).joinpath(name).exists()

    def test_collection_adapter_create_collection_memory_error(
        self,
        collection_adapter: LocalCollectionAdapter,
        root_path: Path,
        factory,
        mocker: MockerFixture,
    ):
        """Tests if adapter raises memory error on collection creation.

        :param collection_adapter: CollectionAdapter
        :param root_path: Path to collections directory
        """
        mocker.patch.object(collection_adapter.ctx.config, "memory_limit", 100)
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
                    col_name,
                    schema,
                    collection_adapter,
                    factory,
                    collection_adapter.get_storage_adapter(),
                )
            )

    def test_collection_adapter_deletes_collection(
        self,
        collection_adapter: LocalCollectionAdapter,
        array_collection: Collection,
        root_path,
    ):
        """Tests if array_collection adapter deletes array_collection from db properly.

        :param collection_adapter: array_collection adapter
        :param array_collection: Pre created array_collection
        :param root_path: Path to collections directory
        """
        collection_adapter.delete(array_collection)
        assert not os.path.exists(array_collection.path)
        assert not os.path.exists(array_collection.path.parent / (array_collection.name + ".lock"))

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
        root_path,
    ):
        """Tests clearing data directory.

        :param collection: Pre created collection
        :param collection_adapter: collection adapter
        :param root_path: Path to collections directory
        """
        collection_adapter.clear(array_collection)

        collection_dir = None
        for child in root_path.joinpath(
            collection_adapter.ctx.config.collections_directory
        ).iterdir():
            if child.name == array_collection.name:
                collection_dir = child

        assert collection_dir is not None
        collection_dir = os.listdir(collection_dir)
        assert f"{array_collection.name}.json" in collection_dir

    def test_collection_adapter_iter(
        self, client: Client, array_schema: ArraySchema, root_path: Path, ctx
    ):
        for root, dirs, files in os.walk(root_path / ctx.config.collections_directory):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                shutil.rmtree(os.path.join(root, d))

        names = {random_string() for _ in range(10)}
        collections = [client.create_collection(name, array_schema) for name in names]
        adapter: LocalCollectionAdapter = collections[
            0
        ]._Collection__adapter  # type: ignore[attr-defined]
        coll_names = {coll["name"] for coll in adapter}
        assert names.issubset(coll_names)
        assert names - coll_names == set()


if __name__ == "__main__":
    pytest.main()

from typing import List

import pytest

from deker_local_adapters import LocalCollectionAdapter

from tests.parameters.uri import embedded_uri

from deker.client import Client
from deker.collection import Collection
from deker.tools import get_paths


class TestCollectionConcurrency:
    def test_multiple_clients_clear_same_collection(
        self,
        array_collection: Collection,
        root_path,
        collection_adapter: LocalCollectionAdapter,
        storage_adapter,
    ):
        """Tests multiple clients clear collections data even if it has been cleared before.

        :param array_collection: Collection to clear
        :param root_path: collections root path
        """
        path = array_collection.path
        schema = path / (array_collection.name + collection_adapter.file_ext)
        assert path.exists()
        assert schema.exists()
        array = array_collection.create()
        assert array
        array_paths = get_paths(array, path)
        main_path = array_paths.main / (array.id + storage_adapter.file_ext)
        symlink_path = array_paths.symlink / (array.id + storage_adapter.file_ext)
        assert main_path.exists()
        assert symlink_path.exists()

        clients: List[Client] = [Client(str(embedded_uri(root_path))) for _ in range(10)]
        assert len({id(c) for c in clients}) == 10
        colls: List[Collection] = [c.get_collection(array_collection.name) for c in clients]
        assert all(isinstance(c, Collection) for c in colls)
        for c in colls:
            c.clear()

        assert not main_path.exists()
        assert not symlink_path.exists()
        assert path.exists()
        assert schema.exists()

    def test_multiple_clients_delete_same_collection(self, array_collection, root_path):
        """Tests if multiple clients return True on delete even if array_collection has been already deleted.

        :param array_collection: pre created Collection instance
        :param root_path: collections root path
        """
        clients: List[Client] = [Client(str(embedded_uri(root_path))) for _ in range(10)]  # type: ignore
        assert len({id(c) for c in clients}) == 10
        colls: List[Collection] = [c.get_collection(array_collection.name) for c in clients]  # type: ignore
        assert all(isinstance(c, Collection) for c in colls)
        for c in colls:
            c.delete()


if __name__ == "__main__":
    pytest.main()

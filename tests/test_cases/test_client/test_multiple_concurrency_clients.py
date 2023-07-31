import multiprocessing

from multiprocessing import Pool
from multiprocessing.pool import ThreadPool

import pytest

from deker_local_adapters import LocalCollectionAdapter
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter

from tests.parameters.common import random_string
from tests.parameters.uri import embedded_uri

from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerCollectionAlreadyExistsError, DekerLockError
from deker.locks import Flock
from deker.schemas import ArraySchema


cores = multiprocessing.cpu_count() // 2
WORKERS = cores if cores > 2 else 2


def collection_adapter_methods(method: str, path: str, name: str, array_schema: ArraySchema):
    """Calls mocked collection adapter methods and checks for DekerLockError exceptions"""
    client = Client(path, loglevel="ERROR")
    ctx = client._Client__ctx
    ctx.storage_adapter = HDF5StorageAdapter
    adapter = LocalCollectionAdapter(ctx)
    storage_adapter = adapter.get_storage_adapter()
    factory = client._Client__factory

    collection = Collection(name, array_schema, adapter, factory, storage_adapter)
    try:
        if method == "create":
            adapter.create(collection)
            return collection.name
        if method == "delete":
            return adapter.delete(collection)
        if method == "clear":
            return adapter.clear(collection)
    except DekerLockError:
        return DekerLockError


class TestMultipleClients:
    def test_multiple_processes_create_collection(
        self, root_path, array_schema: ArraySchema, client: Client
    ):
        """Tests multiple processes cannot create same collection."""
        name = "new_collection_for_processes"
        collection = client.create_collection(name, array_schema)
        try:
            path = collection.path.parent / (collection.name + ".lock")
            open(path, "w").close()
            flock = Flock(path)
            flock.acquire()
            try:
                with Pool(WORKERS) as pool:
                    res = pool.starmap(
                        collection_adapter_methods,
                        [("create", str(embedded_uri(root_path)), name, array_schema)] * WORKERS,
                    )
                    assert res.count(DekerLockError) == WORKERS
            finally:
                flock.release()
            collection = client.get_collection(name)
            assert isinstance(collection, Collection)
        finally:
            if not collection._is_deleted():
                collection.delete()

    def test_multiple_clients_create_collections(self, root_path, array_schema: ArraySchema):
        """Tests multiple clients can create different collections."""
        coros = 10

        with Pool(WORKERS) as pool:
            args = []
            for _ in range(coros):
                args.append(["create", str(embedded_uri(root_path)), random_string(), array_schema])
            colls = pool.starmap(collection_adapter_methods, args)

        assert colls
        assert len(colls) == coros
        assert all(isinstance(c, str) for c in colls)
        assert set([arg[2] for arg in args]) == set(colls)

    def test_multiple_clients_can_not_create_same_collection(
        self, root_path, array_schema: ArraySchema, client: Client
    ):
        """Tests multiple clients cannot create same collection."""
        coros = 10
        name = "new_col"
        with Pool(WORKERS) as pool:
            args = []
            for _ in range(coros):
                args.append(["create", str(embedded_uri(root_path)), name, array_schema])

            with pytest.raises(DekerCollectionAlreadyExistsError):
                colls = pool.starmap(collection_adapter_methods, args)

            assert client.get_collection(name)

    def test_update_different_arrays_concurrently_with_one_client(
        self, array_collection, root_path, array_data
    ):
        """Tests one client can update different arrays in same collection concurrently."""
        coros = 10
        with Client(str(embedded_uri(root_path))) as client:
            coll: Collection = client.get_collection(array_collection.name)  # type: ignore
            try:
                arrays = []
                with ThreadPool(WORKERS) as pool:
                    for _ in range(coros):
                        arrays.append(pool.apply(coll.create))

                assert len(arrays) == coros
                updates = []
                with ThreadPool(WORKERS) as pool:
                    for array in arrays:
                        updates.append(pool.apply(array[:].update, [array_data]))

                assert len(updates) == coros
            finally:
                if not coll._is_deleted():
                    coll.delete()


if __name__ == "__main__":
    pytest.main()

import pytest

from deker_local_adapters.errors import DekerStorageError
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from pytest_mock import MockerFixture

from deker.client import Client
from deker.schemas import ArraySchema


@pytest.mark.parametrize(
    "storage_adapter_type", [pytest.param("name", id="unknown-name"), pytest.param(None, id="none")]
)
def test_raise_on_wrong_storage(client: Client, name: str, array_schema: ArraySchema, storage_adapter_type):
    storage_adapter_type = name if storage_adapter_type == "name" else storage_adapter_type
    with pytest.raises(DekerStorageError):
        client.create_collection(name=name, storage_adapter_type=storage_adapter_type, schema=array_schema)


@pytest.mark.parametrize(
    ("kwargs", "adapter_type"),
    [
        pytest.param({}, HDF5StorageAdapter, id="not-given"),
        pytest.param({"storage_adapter_type": "HDF5StorageAdapter"}, HDF5StorageAdapter, id="by-name"),
    ],
)
def test_storage_adapter(kwargs: dict, adapter_type: type, client: Client, name: str, array_schema: ArraySchema):
    coll = client.create_collection(name, array_schema, **kwargs)
    assert coll._storage_adapter == adapter_type
    coll.delete()


def test_different_collections_different_adapters(
    client: Client, name: str, array_schema: ArraySchema, mocker: MockerFixture
):
    coll = client.create_collection(name, array_schema)
    array = coll.create()
    assert isinstance(array._adapter.storage_adapter, HDF5StorageAdapter)

    class MockedHDFStorage(HDF5StorageAdapter):
        pass

    mocker.patch("deker_local_adapters.storage_adapter_factory", lambda x: MockedHDFStorage)
    coll2 = client.create_collection("newname", array_schema)
    array2 = coll2.create()
    assert isinstance(array2._adapter.storage_adapter, MockedHDFStorage)
    assert isinstance(array._adapter.storage_adapter, HDF5StorageAdapter) and not isinstance(
        array._adapter.storage_adapter, MockedHDFStorage
    )
    coll2.delete()
    coll.delete()

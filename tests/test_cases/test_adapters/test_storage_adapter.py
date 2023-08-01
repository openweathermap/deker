from typing import Optional, Type

import pytest

from deker_local_adapters.errors import DekerStorageError
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from pytest_mock import MockerFixture

from deker.client import Client
from deker.schemas import ArraySchema


def test_raise_on_wrong_storage(client: Client, name: str, array_schema: ArraySchema):
    with pytest.raises(DekerStorageError):
        client.create_collection(name=name, storage_adapter_type=name, schema=array_schema)


@pytest.mark.parametrize(
    "adapter, adapter_type",
    ((None, HDF5StorageAdapter), ("HDF5StorageAdapter", HDF5StorageAdapter)),
)
def test_storage_adapter(
    adapter: Optional[str], adapter_type: Type, client: Client, name: str, array_schema: ArraySchema
):
    coll = client.create_collection(name, array_schema, None, adapter)
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

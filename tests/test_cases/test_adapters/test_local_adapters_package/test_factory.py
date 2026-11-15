import pytest

from deker_local_adapters import LocalArrayAdapter, LocalCollectionAdapter, LocalVArrayAdapter
from deker_local_adapters.errors import DekerStorageError
from deker_local_adapters.factory import AdaptersFactory, storage_adapter_factory
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter


class TestArraysAdaptersFactory:
    """Tests for arrays' adapters factory."""

    def test_adapters_factory_init(self, ctx, uri):
        """Test factory instantiation.

        :param ctx: client context
        """
        factory = AdaptersFactory(ctx, uri)
        assert factory

    def test_adapters_factory_create_array_adapter(self, ctx, array_collection, uri):
        """Test factory creates ArrayAdapter.

        :param array_collection: Collection instance
        """
        factory = AdaptersFactory(ctx, uri)
        adapter = factory.get_array_adapter(array_collection.path, HDF5StorageAdapter)
        assert adapter
        assert isinstance(adapter, LocalArrayAdapter)

    def test_adapters_factory_create_array_adapters(self, array_collection, ctx, uri):
        """Test factory creates different ArrayAdapters.

        :param array_collection: Collection instance
        """
        factory = AdaptersFactory(ctx, uri)
        adapters = [factory.get_array_adapter(array_collection.path, HDF5StorageAdapter) for _ in range(10)]
        assert adapters
        ids = {id(adapter) for adapter in adapters}
        assert len(ids) == len(adapters)

    def test_adapters_factory_create_varray_adapter(self, varray_collection, ctx, uri):
        """Test factory creates VArrayAdapter.

        :param varray_collection: Collection instance
        """
        factory = AdaptersFactory(ctx, uri)
        adapter = factory.get_varray_adapter(varray_collection.path, HDF5StorageAdapter)
        assert adapter
        assert isinstance(adapter, LocalVArrayAdapter)

    def test_adapters_factory_create_varray_adapters(self, varray_collection, ctx, uri):
        """Test factory creates different VArrayAdapters.

        :param varray_collection: Collection instance
        """
        factory = AdaptersFactory(ctx, uri)
        adapters = [factory.get_varray_adapter(varray_collection.path, HDF5StorageAdapter) for _ in range(10)]
        assert adapters
        ids = {id(adapter) for adapter in adapters}
        assert len(ids) == len(adapters)

    def test_adapters_factory_create_collection(self, ctx, uri):
        factory = AdaptersFactory(ctx, uri)
        adapter = factory.get_collection_adapter()
        assert isinstance(adapter, LocalCollectionAdapter)


class TestStorageAdapterFactory:
    def test_default_is_hdf5(self):
        assert storage_adapter_factory() is HDF5StorageAdapter
        assert storage_adapter_factory("HDF5StorageAdapter") is HDF5StorageAdapter

    def test_unknown_adapter_raises(self):
        with pytest.raises(DekerStorageError):
            storage_adapter_factory("ZarrStorageAdapter")


if __name__ == "__main__":
    pytest.main()

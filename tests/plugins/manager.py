import pytest

from deker_local_adapters import HDF5StorageAdapter

from deker.collection import Collection
from deker.managers import DataManager


@pytest.fixture()
def collection_manager_with_attributes(
    array_collection_with_attributes: Collection, factory
) -> DataManager:
    """Collection manager with attributes fixture.

    :param array_collection_with_attributes: a Collection instance with attribute from fixture
    """
    manager = DataManager(
        array_collection_with_attributes,
        factory.get_array_adapter(
            array_collection_with_attributes.path,
            HDF5StorageAdapter,
            array_collection_with_attributes.options,
        ),
    )
    return manager


@pytest.fixture()
def va_collection_manager_with_attributes(
    varray_collection_with_attributes: Collection, factory
) -> DataManager:
    """Varray Collection manager with attributes fixture.

    :param varray_collection_with_attributes: a Collection instance with attribute from fixture
    """
    manager = DataManager(
        varray_collection_with_attributes,
        factory.get_array_adapter(
            varray_collection_with_attributes.path,
            HDF5StorageAdapter,
            varray_collection_with_attributes.options,
        ),
        factory.get_varray_adapter(
            varray_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter
        ),
    )
    return manager


@pytest.fixture()
def collection_manager(array_collection: Collection, factory) -> DataManager:
    """Collection manager fixture.

    :param array_collection: a Collection instance from fixture
    """
    manager = DataManager(
        array_collection,
        factory.get_array_adapter(
            array_collection.path, HDF5StorageAdapter, array_collection.options
        ),
    )
    return manager


@pytest.fixture()
def va_collection_manager(varray_collection: Collection, factory) -> DataManager:
    """VArray Collection manager fixture.

    :param varray_collection: a Collection instance from fixture
    """
    manager = DataManager(
        varray_collection,
        factory.get_array_adapter(
            varray_collection.path, HDF5StorageAdapter, varray_collection.options
        ),
        factory.get_varray_adapter(varray_collection.path, storage_adapter=HDF5StorageAdapter),
    )
    return manager

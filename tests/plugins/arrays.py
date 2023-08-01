import uuid

from datetime import datetime, timezone

import numpy as np
import pytest

from deker_local_adapters import HDF5StorageAdapter
from deker_local_adapters.factory import AdaptersFactory

from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.errors import DekerLockError
from deker.schemas import ArraySchema


@pytest.fixture()
def array(array_collection: Collection, factory) -> Array:
    """Returns an instance of Array.

    :param array_collection: Collection object
    :param factory: ArraysAdaptersFactory object
    """
    return Array(
        array_collection,
        factory.get_array_adapter(array_collection.path, storage_adapter=HDF5StorageAdapter),
    )


@pytest.fixture()
def array_data(array_schema: ArraySchema) -> np.ndarray:
    """Returns an instance of Array data.

    :param array_schema: Array schema
    """
    return np.asarray(range(10 * 10 * 10), dtype=array_schema.dtype).reshape(array_schema.shape)


@pytest.fixture()
def array_with_attributes(
    array_collection_with_attributes: Collection,
    factory: AdaptersFactory,
) -> Array:
    """Returns an instance of Array.

    :param array_collection_with_attributes: Array collection with primary attributes
    :param factory: ArraysAdaptersFactory instance
    """
    return Array(
        array_collection_with_attributes,
        factory.get_array_adapter(
            array_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter
        ),
        primary_attributes={"primary_attribute": 3},
        custom_attributes={"custom_attribute": 0.5, "time_attr_name": datetime.now(timezone.utc)},
    )


@pytest.fixture()
def inserted_array(array_collection: Collection, array_data: np.ndarray) -> Array:
    """Returns an instance of Array with data initialized in file.

    :param array_collection: Collection object
    :param array_data: array real data
    """
    array = array_collection.create()
    array[:].update(array_data)
    yield array
    try:
        array.delete()
    except (BlockingIOError, DekerLockError):
        pass


@pytest.fixture()
def inserted_array_with_attributes(
    array_collection_with_attributes: Collection, array_data: np.ndarray
) -> VArray:
    """Returns an instance of Array with data initialized in file and attributes.

    :param array_collection_with_attributes: Collection object
    :param array_data: array real data
    """
    array = array_collection_with_attributes.create(
        **{
            "primary_attributes": {"primary_attribute": 3},
            "custom_attributes": {
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        }
    )
    array[:].update(array_data)
    yield array
    array.delete()


@pytest.fixture()
def varray(varray_collection: Collection, factory) -> VArray:
    """Returns an instance of VArray."""
    return VArray(
        collection=varray_collection,
        adapter=factory.get_varray_adapter(
            varray_collection.path, storage_adapter=HDF5StorageAdapter
        ),
        array_adapter=factory.get_array_adapter(
            varray_collection.path, storage_adapter=HDF5StorageAdapter
        ),
        primary_attributes={},
        custom_attributes={},
        id_=str(uuid.uuid4()),
    )


@pytest.fixture()
def varray_with_attributes(
    varray_collection_with_attributes: Collection,
    factory: AdaptersFactory,
) -> VArray:
    """Returns an instance of VArray with attributes.

    :param varray_collection_with_attributes: VArray collection with primary attributes
    :param factory: ArraysAdaptersFactory instance
    """
    return VArray(
        varray_collection_with_attributes,
        factory.get_varray_adapter(
            varray_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter
        ),
        factory.get_array_adapter(
            varray_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter
        ),
        primary_attributes={"primary_attribute": 3},
        custom_attributes={"custom_attribute": 0.5, "time_attr_name": datetime.now(timezone.utc)},
    )


@pytest.fixture()
def inserted_varray(varray_collection: Collection, array_data: np.ndarray) -> Array:
    """Returns an instance of VArray with data initialized in file.

    :param varray_collection: Collection object
    :param array_data: array real data
    """
    array = varray_collection.create()
    array[:].update(array_data)
    yield array
    array.delete()


@pytest.fixture()
def inserted_varray_with_attributes(
    varray_collection_with_attributes: Collection, array_data: np.ndarray
) -> VArray:
    """Returns an instance of VArray with data initialized in file and attributes.

    :param varray_collection_with_attributes: Collection object
    :param array_data: array real data
    """
    array: VArray = varray_collection_with_attributes.create(
        **{
            "primary_attributes": {"primary_attribute": 3},
            "custom_attributes": {
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        }
    )
    array[:].update(array_data)
    yield array
    array.delete()


@pytest.fixture()
def scaled_array(scaled_collection) -> Array:
    """Creates Array with regular scale description and data."""
    data = np.random.random(scaled_collection.array_schema.shape)
    array = scaled_collection.create()
    array[:].update(data)
    yield array
    array.delete()


@pytest.fixture()
def timed_array(timed_collection) -> Array:
    """Creates Array with time dimensions and data."""
    data = np.random.random(timed_collection.array_schema.shape)
    array = timed_collection.create({"time": datetime(2023, 1, 1)})
    array[:].update(data)
    yield array
    array.delete()


@pytest.fixture()
def scaled_varray(scaled_varray_collection) -> Array:
    """Creates VArray with regular scale description and data."""
    data = np.random.random(scaled_varray_collection.varray_schema.shape)
    array = scaled_varray_collection.create()
    array[:].update(data)
    yield array
    array.delete()


@pytest.fixture()
def timed_varray(timed_varray_collection) -> Array:
    """Creates VArray with time dimensions and data."""
    data = np.random.random(timed_varray_collection.varray_schema.shape)
    array = timed_varray_collection.create({"time": datetime(2023, 1, 1)})
    array[:].update(data)
    yield array
    array.delete()

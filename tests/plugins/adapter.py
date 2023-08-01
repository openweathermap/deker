from concurrent.futures import ThreadPoolExecutor
from typing import Type

import pytest

from deker_local_adapters import (
    HDF5StorageAdapter,
    LocalArrayAdapter,
    LocalCollectionAdapter,
    LocalVArrayAdapter,
)

from deker.ABC import BaseStorageAdapter
from deker.collection import Collection
from deker.ctx import CTX


@pytest.fixture()
def collection_adapter(ctx: CTX) -> LocalCollectionAdapter:
    """Creates a PathCollection adapter.

    :param ctx: client context
    """
    adapter = LocalCollectionAdapter(ctx)
    adapter.get_storage_adapter()
    return adapter


@pytest.fixture(scope="class")
def storage_adapter() -> Type[BaseStorageAdapter]:
    return HDF5StorageAdapter


@pytest.fixture()
def local_array_adapter(
    array_collection: Collection, ctx: CTX, executor: ThreadPoolExecutor
) -> LocalArrayAdapter:
    """Instance of LocalArrayAdapter.

    Instance would be different from the one that appears in Array fixture.
    """
    return LocalArrayAdapter(
        collection_path=array_collection.path,
        ctx=ctx,
        executor=executor,
        collection_options=array_collection.options,
        storage_adapter=HDF5StorageAdapter,
    )


@pytest.fixture()
def local_varray_adapter(
    varray_collection: Collection, ctx: CTX, executor: ThreadPoolExecutor
) -> LocalVArrayAdapter:
    """Instance of LocalVArrayAdapter.

    Instance would be different from the one that appears in VArray fixture.
    """
    return LocalVArrayAdapter(
        collection_path=varray_collection.path,
        ctx=ctx,
        executor=executor,
        storage_adapter=HDF5StorageAdapter,
    )

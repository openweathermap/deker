import pytest

from deker_local_adapters import LocalArrayAdapter

from deker.arrays import VArray
from deker.locks import ReadArrayLock, WriteArrayLock, WriteVarrayLock
from deker.subset import VSubset


@pytest.fixture()
def read_array_lock(local_array_adapter: LocalArrayAdapter) -> ReadArrayLock:
    """Create an instance of ReadArrayLock."""
    lock = ReadArrayLock()
    lock.instance = local_array_adapter
    return lock


@pytest.fixture()
def write_array_lock(local_array_adapter: LocalArrayAdapter) -> WriteArrayLock:
    """Create an instance of WriteArrayLock."""
    lock = WriteArrayLock()
    lock.instance = local_array_adapter
    return lock


@pytest.fixture()
def write_varray_lock(inserted_varray: VArray, varray_full_subset: VSubset) -> WriteVarrayLock:
    """Create an instance of WriteVarrayLock."""
    lock = WriteVarrayLock()
    lock.instance = varray_full_subset
    return lock

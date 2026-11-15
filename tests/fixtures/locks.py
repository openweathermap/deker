import pytest

from deker_local_adapters import LocalArrayAdapter

from deker.locks import ReadArrayLock, WriteArrayLock


@pytest.fixture
def read_array_lock(local_array_adapter: LocalArrayAdapter) -> ReadArrayLock:
    """Create an instance of ReadArrayLock."""
    lock = ReadArrayLock()
    lock.instance = local_array_adapter
    return lock


@pytest.fixture
def write_array_lock(local_array_adapter: LocalArrayAdapter) -> WriteArrayLock:
    """Create an instance of WriteArrayLock."""
    lock = WriteArrayLock()
    lock.instance = local_array_adapter
    return lock

import os
import re

from multiprocessing.context import BaseContext
from multiprocessing.synchronize import Event
from pathlib import Path
from threading import Thread, Timer, get_native_id
from time import sleep
from uuid import UUID, uuid4

import pytest

from deker_local_adapters import LocalArrayAdapter
from pytest_mock import MockerFixture

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerLockError, DekerMemoryError
from deker.locks import (
    META_DIVIDER,
    Flock,
    ReadArrayLock,
    WriteArrayLock,
    WriteVarrayLock,
    _check_write_locks,
    _is_own_varray_lock_marker,
    _varray_lock_marker,
)
from deker.schemas import ArraySchema, DimensionSchema
from deker.tools import get_main_path
from deker.types import LocksExtensions


def make_read_lock(filepath: str, array_id: UUID, file_created: Event, error_raised: Event = None):
    """Function to be executed in different process.

    :param filepath: Path to file (hdf, tiff, etc)
    """
    path = Path(filepath)
    lock = path.parent / (
        f"{array_id}:{uuid4()}:{os.getpid()}:{get_native_id()}{LocksExtensions.array_read_lock.value}"
    )
    open(lock, "w").close()
    file_created.set()

    # If error_raised wasn't passed, we just sleep small amount of time
    if not error_raised:
        sleep(1)
    else:
        error_raised.wait()

    lock.unlink()


def call_check_existing_lock(lock: WriteVarrayLock, errors: list[Exception]) -> None:
    """Call the lock's check in a thread, collecting the exception instead of losing it.

    :param lock: VArray write lock bound to a subset
    :param errors: list to put the raised exception into
    """
    try:
        lock.check_existing_lock(func_args=[], func_kwargs={})
    except Exception as e:
        errors.append(e)


class TestVArrayLockMarker:
    """Test the markers that tell an Array is locked by a VArray of the current process."""

    def test_marker_path(self, tmp_path: Path):
        """Tests the marker lies next to the main file and carries the PID with its divider.

        :param tmp_path: temporary directory
        """
        filename = tmp_path / "array.hdf5"
        marker = _varray_lock_marker(filename)
        assert marker.parent == tmp_path
        assert marker.name == f"array.hdf5{META_DIVIDER}{os.getpid()}{LocksExtensions.varray_lock.value}"

    def test_own_marker_is_recognized(self, tmp_path: Path):
        """Tests the marker of the current process is recognized as its own.

        :param tmp_path: temporary directory
        """
        assert _is_own_varray_lock_marker(_varray_lock_marker(tmp_path / "array.hdf5").name)

    @pytest.mark.parametrize(
        "pid",
        [
            lambda: os.getpid() + 1,
            lambda: int(f"1{os.getpid()}"),  # ends with the own PID
            lambda: int(f"{os.getpid()}0"),  # starts with the own PID
        ],
        ids=["other", "own_pid_as_suffix", "own_pid_as_prefix"],
    )
    def test_foreign_marker_is_not_recognized(self, pid):
        """Tests the marker of another process is never taken for the own one.

        :param pid: PID of the other process
        """
        name = f"array.hdf5{META_DIVIDER}{pid()}{LocksExtensions.varray_lock.value}"
        assert not _is_own_varray_lock_marker(name)

    def test_check_write_locks_no_markers(self, tmp_path: Path):
        """Tests a directory without markers is not locked.

        :param tmp_path: temporary directory
        """
        (tmp_path / "array.hdf5").touch()
        assert _check_write_locks(tmp_path, "array") is False

    def test_check_write_locks_own_marker(self, tmp_path: Path):
        """Tests the own marker means the array is locked by a VArray of the current process.

        :param tmp_path: temporary directory
        """
        _varray_lock_marker(tmp_path / "array.hdf5").touch()
        assert _check_write_locks(tmp_path, "array") is True

    @pytest.mark.parametrize(
        "pid",
        [
            lambda: os.getpid() + 1,
            lambda: int(f"1{os.getpid()}"),
            lambda: int(f"{os.getpid()}0"),
        ],
        ids=["other", "own_pid_as_suffix", "own_pid_as_prefix"],
    )
    def test_check_write_locks_foreign_marker(self, tmp_path: Path, pid):
        """Tests the marker of another process means the array is locked by someone else.

        :param tmp_path: temporary directory
        :param pid: PID of the other process
        """
        (tmp_path / f"array.hdf5{META_DIVIDER}{pid()}{LocksExtensions.varray_lock.value}").touch()
        with pytest.raises(DekerLockError):
            _check_write_locks(tmp_path, "array")


class TestReadArrayLock:
    """Test if ReadArraylock creates lock file on reading, and we cannot write into it."""

    def test_read_lock_check_type_ok(self, array: Array, local_array_adapter):
        """Test if check of type is working properly on right cases."""

        lock = ReadArrayLock()
        lock.instance = local_array_adapter
        lock.check_type()

    def test_read_lock_check_type_fail(self):
        """Test if check of type is working properly on wrong cases."""

        class Mock:
            def mock(self, *args, **kwargs):
                pass

        with pytest.raises(TypeError):
            mock = Mock()
            lock = ReadArrayLock(mock())
            lock.instance = mock
            lock.check_type()

    def test_read_array_lock_path(
        self,
        read_array_lock: ReadArrayLock,
        inserted_array: Array,
        local_array_adapter: LocalArrayAdapter,
    ):
        """Check if read_array_lock.get_path method works correctly."""
        dir_path = get_main_path(inserted_array.id, local_array_adapter.collection_path / local_array_adapter.data_dir)

        pattern = re.compile(
            str(
                dir_path
                / (f"{inserted_array.id}:.*:{os.getpid()}:{get_native_id()}{LocksExtensions.array_read_lock.value}")
            )
        )
        read_array_lock.args = []
        read_array_lock.kwargs = {"array": inserted_array}
        assert pattern.match(str(read_array_lock.get_path()))

    def test_read_array_check_existing_lock(
        self,
        read_array_lock: ReadArrayLock,
        inserted_array: Array,
        local_array_adapter: LocalArrayAdapter,
    ):
        """Check correctness of check_existing_lock method"""
        dir_path = get_main_path(inserted_array.id, local_array_adapter.collection_path / local_array_adapter.data_dir)
        hdf_path = dir_path / (inserted_array.id + local_array_adapter.file_ext)
        with Flock(hdf_path):
            with pytest.raises(DekerLockError):
                read_array_lock.kwargs = {"array": inserted_array}
                read_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})

    def test_read_array_locks_create_files(
        self,
        read_array_lock: ReadArrayLock,
        local_array_adapter: LocalArrayAdapter,
        mocker: MockerFixture,
        inserted_array,
    ):
        """Test if read array lock creates lock files."""

        # Mock uuid to make path the same
        uuid = uuid4()
        mocker.patch("deker.locks.uuid4", lambda: uuid)

        # Get path of the file that should be created
        read_array_lock.kwargs = {"array": inserted_array}
        filepath = read_array_lock.get_path()

        # Mocked method to test
        def check_file(self, *args, **kwargs):
            """Check if file was created"""
            assert filepath.exists()

        func = read_array_lock(check_file)

        # Call BaseLock
        func(local_array_adapter, inserted_array)


class TestWriteArrayLock:
    """Test if WriteArrayLock creates lock file on reading, and we cannot write into it."""

    def test_write_array_lock_check_type_ok(self, array: Array, local_array_adapter):
        """Test if check of type is working properly on right cases."""

        lock = WriteArrayLock()
        lock.instance = local_array_adapter
        lock.check_type()

    def test_write_array_lock_check_type_fail(self):
        """Test if check of type is working properly on wrong cases."""

        class Mock:
            def mock(self, *args, **kwargs):
                pass

        with pytest.raises(TypeError):
            mock = Mock()
            lock = WriteArrayLock(mock())
            lock.instance = mock
            lock.check_type()

    def test_write_array_lock_lock_path(
        self,
        write_array_lock: WriteArrayLock,
        inserted_array: Array,
        local_array_adapter: LocalArrayAdapter,
    ):
        """Check correctness of get_path method."""
        dir_path = get_main_path(inserted_array.id, local_array_adapter.collection_path / local_array_adapter.data_dir)
        hdf_path = dir_path / (inserted_array.id + local_array_adapter.file_ext)
        write_array_lock.kwargs = {"array": inserted_array}
        assert hdf_path == write_array_lock.get_path()

    def test_write_array_lock_check_existing_lock_no_read_locks(
        self,
        write_array_lock: WriteArrayLock,
        inserted_array: Array,
        local_array_adapter: LocalArrayAdapter,
        mocker: MockerFixture,
    ):
        """Check correctness of check_existing_lock method."""
        # Mock acquire as we do not want to lock file here.
        acquire = mocker.patch.object(write_array_lock, "acquire", autospec=True)
        release = mocker.patch.object(write_array_lock, "release", autospec=True)
        write_array_lock.kwargs = {"array": inserted_array}
        write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
        acquire.assert_called()
        release.assert_not_called()

    def test_write_array_lock_check_existing_lock_read_locks_success(
        self,
        write_array_lock: WriteArrayLock,
        inserted_array: Array,
        local_array_adapter: LocalArrayAdapter,
        mocker: MockerFixture,
        mp_context: BaseContext,
    ):
        """Check correctness of check_existing_lock method."""
        file_created = mp_context.Event()

        # Mock acquire as we do not want to lock file here.
        mocker.patch.object(write_array_lock, "acquire", lambda *a, **kw: file_created.wait(10), spec=True)
        release = mocker.patch.object(write_array_lock, "release", autospec=True)

        # Make read lock
        write_array_lock.kwargs = {"array": inserted_array}
        filepath = write_array_lock.get_path()
        process = mp_context.Process(target=make_read_lock, args=(filepath, inserted_array.id, file_created))
        process.start()

        write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})

        release.assert_not_called()
        process.kill()

    def test_write_array_lock_check_existing_lock_read_locks_fail(
        self,
        write_array_lock: WriteArrayLock,
        inserted_array: Array,
        local_array_adapter: LocalArrayAdapter,
        mocker: MockerFixture,
        mp_context: BaseContext,
    ):
        """Test if checking existing lock fails with timeout."""
        file_created = mp_context.Event()

        # Set lock timeout to wait minimal amount of time
        mocker.patch.object(write_array_lock.instance.ctx.config, "write_lock_timeout", 1)

        # Mock acquire with signal to start locking properly
        mocker.patch.object(write_array_lock, "acquire", lambda *a, **kw: file_created.wait(10), spec=True)
        release = mocker.patch.object(write_array_lock, "release", autospec=True)

        # Make read lock
        write_array_lock.kwargs = {"array": inserted_array}
        filepath = write_array_lock.get_path()
        process = mp_context.Process(target=make_read_lock, args=(filepath, inserted_array.id, file_created))
        process.start()

        # Call check existing
        with pytest.raises(DekerLockError):
            write_array_lock.kwargs = {"array": inserted_array}
            write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})

        # If error raised, close process.
        release.assert_called()

        process.kill()


class TestWriteVarrayLock:
    def test_write_varray_lock_check_type_ok(self, varray: VArray):
        subset = varray[:]
        lock = WriteVarrayLock()
        lock.instance = subset
        lock.check_type()

    def test_write_varray_skip(self, mocker: MockerFixture, inserted_varray: VArray):
        acquire = mocker.patch.object(WriteVarrayLock, "acquire")
        subset = inserted_varray[:]

        class Mock:
            def mock(self, *args, **kwargs):
                pass

        mocker.patch.object(subset, "_VSubset__adapter", Mock())
        lock = WriteVarrayLock()
        lock.instance = subset
        lock.check_type()
        assert lock.skip_lock

        get_result = mocker.patch.object(lock, "get_result")
        lock._inner_method_logic(lock, (), {}, subset.clear)
        acquire.assert_not_called()
        get_result.assert_called()

    def test_write_array_lock_check_type_fail(self):
        """Test if check of type is working properly on wrong cases."""

        class Mock:
            def mock(self, *args, **kwargs):
                pass

        with pytest.raises(TypeError):
            mock = Mock()
            lock = WriteVarrayLock()
            lock.instance = mock
            lock.check_type()

    def test_check_get_path(
        self,
        inserted_varray: VArray,
        varray_collection: Collection,
        local_varray_adapter,
        write_varray_lock: WriteVarrayLock,
    ):
        """Test getting path of write varray lock."""
        path = get_main_path(inserted_varray.id, varray_collection.path / local_varray_adapter.data_dir) / (
            inserted_varray.id + local_varray_adapter.file_ext
        )
        write_varray_lock.kwargs = {"array": inserted_varray}
        assert path == write_varray_lock.get_path()

    def test_varray_lock_holds_nothing_while_a_tile_is_busy(
        self,
        mocker: MockerFixture,
        write_varray_lock: WriteVarrayLock,
        varray_collection: Collection,
        inserted_varray: VArray,
        mp_context: BaseContext,
    ):
        """A writer waiting for a busy tile shall hold no other tile: no flock and no own marker on them."""
        file_created = mp_context.Event()
        error_raised = mp_context.Event()
        config = write_varray_lock.instance._VSubset__array_adapter.ctx.config
        mocker.patch.object(config, "write_lock_timeout", 3)
        mocker.patch.object(config, "write_lock_check_interval", 0.1)

        arrays = list(varray_collection.arrays)
        busy, *free = arrays
        busy_path = busy._Array__adapter._get_main_path_to_file(busy)
        process = mp_context.Process(target=make_read_lock, args=(busy_path, busy.id, file_created, error_raised))
        process.start()
        file_created.wait()

        errors: list[Exception] = []
        writer = Thread(target=call_check_existing_lock, args=(write_varray_lock, errors))
        writer.start()
        try:
            sleep(1)  # the writer is waiting for the busy tile now
            assert writer.is_alive()
            for array in free:
                path = array._Array__adapter._get_main_path_to_file(array)
                assert not list(path.parent.glob(f"{path.name}:*{LocksExtensions.varray_lock.value}"))
                flock = Flock(path)
                flock.acquire()
                flock.release()
        finally:
            error_raised.set()
            writer.join()
            process.join()
        assert errors == []
        assert len(write_varray_lock.locks) == len(arrays)
        write_varray_lock.release()

    def test_varray_lock_acquires_tiles_once_they_are_free(
        self,
        mocker: MockerFixture,
        write_varray_lock: WriteVarrayLock,
        varray_collection: Collection,
        inserted_varray: VArray,
        mp_context: BaseContext,
    ):
        """A writer shall get all the tiles once the busy one is released, instead of failing by timeout."""
        file_created = mp_context.Event()
        error_raised = mp_context.Event()
        config = write_varray_lock.instance._VSubset__array_adapter.ctx.config
        mocker.patch.object(config, "write_lock_timeout", 5)
        mocker.patch.object(config, "write_lock_check_interval", 0.1)

        arrays = list(varray_collection.arrays)
        busy = arrays[0]
        busy_path = busy._Array__adapter._get_main_path_to_file(busy)
        process = mp_context.Process(target=make_read_lock, args=(busy_path, busy.id, file_created, error_raised))
        process.start()
        file_created.wait()

        releaser = Timer(0.5, error_raised.set)
        releaser.start()
        try:
            write_varray_lock.check_existing_lock(func_args=[], func_kwargs={})
        finally:
            releaser.join()
            process.join()
        assert len(write_varray_lock.locks) == len(arrays)
        write_varray_lock.release()
        assert write_varray_lock.locks == []
        for array in arrays:
            path = array._Array__adapter._get_main_path_to_file(array)
            assert not list(path.parent.glob(f"{path.name}:*{LocksExtensions.varray_lock.value}"))

    def test_check_existing_locks_fail(
        self,
        mocker: MockerFixture,
        write_varray_lock,
        varray_collection: Collection,
        inserted_varray: VArray,
        mp_context: BaseContext,
    ):
        """
        Deker checks for each affected Array if Array read and write lock count == 0
        If Array read or write lock count > 0, Deker sleeps for WRITE_LOCK_CHECK_INTERVAL
        and repeats this check until WRITE_LOCK_TIMEOUT has passed since the first check attempt.
        In case WRITE_LOCK_TIMEOUT passed and Array read or write lock count still > 0,
        Deker decrements write lock count for VArray and write locks that it has incremented
        on affected Arrays and returns lock error
        Array read and write lock count == 0, Deker increments its write lock count

        :return:
        """
        file_created = mp_context.Event()
        error_raised = mp_context.Event()

        release = mocker.patch.object(write_varray_lock, "release", autospec=True)

        # Set lock timeout to wait minimal amount of time
        mocker.patch.object(write_varray_lock.instance._VSubset__array_adapter.ctx.config, "write_lock_timeout", 1)
        processes = []
        # Make read lock
        for array in varray_collection.arrays:
            adapter = array._Array__adapter
            filepath = adapter._get_main_path_to_file(array)
            # We don't use pool here, as we should keep state of locks
            process = mp_context.Process(target=make_read_lock, args=(filepath, array.id, file_created, error_raised))
            process.start()
            processes.append(process)

        file_created.wait()

        # Call check existing
        with pytest.raises(DekerLockError):
            write_varray_lock.check_existing_lock(func_args=[], func_kwargs={})

        # If error raised, close process.
        error_raised.set()
        release.assert_called()

        for process in processes:
            process.kill()

    def test_check_existing_locks_success(
        self,
        mocker: MockerFixture,
        write_varray_lock,
        varray_collection: Collection,
        inserted_varray: VArray,
        mp_context: BaseContext,
    ):
        """
        Deker checks for each affected Array if Array read and write lock count == 0
        If Array read or write lock count > 0, Deker sleeps for WRITE_LOCK_CHECK_INTERVAL
        and repeats this check until WRITE_LOCK_TIMEOUT has passed since the first check attempt.
        In case WRITE_LOCK_TIMEOUT passed and Array read or write lock count still > 0,
        Deker decrements write lock count for VArray and write locks that it has incremented
        on affected Arrays and returns lock error
        Array read and write lock count == 0, Deker increments its write lock count
        """
        file_created = mp_context.Event()
        error_raised = mp_context.Event()

        release = mocker.patch.object(write_varray_lock, "release", autospec=True)

        # Set lock timeout to wait minimal amount of time
        mocker.patch.object(write_varray_lock.instance._VSubset__array_adapter.ctx.config, "write_lock_timeout", 1)
        processes = []
        # Make read lock
        for array in varray_collection.arrays:
            adapter = array._Array__adapter
            filepath = adapter._get_main_path_to_file(array)
            # We don't use pool here, as we should keep state of locks
            process = mp_context.Process(target=make_read_lock, args=(filepath, array.id, file_created, error_raised))
            process.start()
            processes.append(process)

        file_created.wait()

        # Call check existing
        with pytest.raises(DekerLockError):
            write_varray_lock.check_existing_lock(func_args=[], func_kwargs={})

        # If error raised, close process.
        error_raised.set()
        release.assert_called()

        for process in processes:
            process.kill()


class TestCollectionLock:
    def test_lock_deletes_after_memory_error(self, client: Client, collection_adapter):
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=100000),
                DimensionSchema(name="y", size=100000),
            ],
            dtype=float,
        )
        col_name = "memory_excess_dict"
        with pytest.raises(DekerMemoryError):
            client.create_collection(col_name, schema)
        assert not (
            collection_adapter.collections_resource / f"{col_name}{LocksExtensions.collection_lock.value}"
        ).exists()


if __name__ == "__main__":
    pytest.main()

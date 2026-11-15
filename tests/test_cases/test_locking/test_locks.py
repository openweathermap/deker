import fcntl
import os

from multiprocessing import Queue
from multiprocessing.context import BaseContext
from multiprocessing.synchronize import Event
from pathlib import Path
from threading import Thread, Timer
from time import sleep
from types import SimpleNamespace
from uuid import uuid4

import pytest

from deker_local_adapters import LocalArrayAdapter
from pytest_mock import MockerFixture

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerLockError, DekerMemoryError
from deker.locks import (
    META_DIVIDER,
    CollectionLock,
    Flock,
    ReadArrayLock,
    TileCreationLock,
    WriteArrayLock,
    WriteVarrayLock,
    _check_write_locks,
    _is_own_varray_lock_marker,
    _varray_lock_marker,
    process_token,
)
from deker.schemas import ArraySchema, DimensionSchema
from deker.tools import get_main_path
from deker.types import LocksExtensions


def hold_shared_flock(filepath: str, lock_set: Event, release: Event | None = None) -> None:
    """Hold a shared flock on the file in another process, as a reader does.

    :param filepath: path to the main file of the array
    :param lock_set: event set once the flock is held
    :param release: event to wait for before releasing; one second of holding if not passed
    """
    with open(filepath) as fd:
        fcntl.flock(fd, fcntl.LOCK_SH)
        lock_set.set()
        if release is None:
            sleep(1)
        else:
            release.wait()


def hold_shared_flock_forever(filepath: str, lock_set: Event) -> None:
    """Hold a shared flock on the file until the process is killed, as a crashed reader does.

    :param filepath: path to the main file of the array
    :param lock_set: event set once the flock is held
    """
    with open(filepath) as fd:
        fcntl.flock(fd, fcntl.LOCK_SH)
        lock_set.set()
        sleep(3600)


def is_own_marker_in_child(name: str, queue: Queue) -> None:
    """Report from another process whether the marker of the parent process is taken for its own.

    :param name: name of the marker made by the parent process
    :param queue: queue for the answer
    """
    queue.put(_is_own_varray_lock_marker(name))


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
    """The markers that tell an Array is locked by a VArray of the current process.

    The marker carries the PID for the eye and the token of the process for the check: containers on one
    storage have their own PID namespaces, so two processes may share a PID but never a token.
    """

    def test_marker_path(self, tmp_path: Path):
        """Tests the marker lies next to the main file and carries the PID and the token with their dividers."""
        filename = tmp_path / "array.hdf5"
        marker = _varray_lock_marker(filename)
        assert marker.parent == tmp_path
        assert marker.name == (
            f"array.hdf5{META_DIVIDER}{os.getpid()}{META_DIVIDER}{process_token()}{LocksExtensions.varray_lock.value}"
        )

    def test_process_token_is_stable_inside_the_process(self):
        assert process_token() == process_token()
        assert len(process_token()) == 32

    def test_own_marker_is_recognized(self, tmp_path: Path):
        assert _is_own_varray_lock_marker(_varray_lock_marker(tmp_path / "array.hdf5").name)

    @pytest.mark.parametrize(
        ("pid", "token"),
        [
            (lambda: os.getpid() + 1, lambda: process_token()),
            (lambda: os.getpid(), lambda: uuid4().hex),  # the same PID in another container
            (lambda: os.getpid(), lambda: f"1{process_token()}"),
            (lambda: os.getpid(), lambda: f"{process_token()}0"),
            (lambda: int(f"1{os.getpid()}"), lambda: process_token()),
        ],
        ids=["other_pid", "same_pid_other_token", "own_token_as_suffix", "own_token_as_prefix", "own_pid_as_suffix"],
    )
    def test_foreign_marker_is_not_recognized(self, pid, token):
        """Tests the marker of another process is never taken for the own one, whatever its PID."""
        name = f"array.hdf5{META_DIVIDER}{pid()}{META_DIVIDER}{token()}{LocksExtensions.varray_lock.value}"
        assert not _is_own_varray_lock_marker(name)

    def test_old_marker_without_a_token_is_foreign(self):
        """Tests a marker of the previous format (PID only) is never taken for the own one."""
        assert not _is_own_varray_lock_marker(
            f"array.hdf5{META_DIVIDER}{os.getpid()}{LocksExtensions.varray_lock.value}"
        )

    def test_child_process_does_not_own_the_marker_of_the_parent(self, tmp_path: Path, mp_context: BaseContext):
        """Tests a child process, forked or spawned, has its own token and takes the parent marker for a foreign one."""
        name = _varray_lock_marker(tmp_path / "array.hdf5").name
        queue = mp_context.Queue()
        process = mp_context.Process(target=is_own_marker_in_child, args=(name, queue))
        process.start()
        try:
            assert queue.get(timeout=30) is False
        finally:
            process.join(30)

    def test_check_write_locks_no_markers(self, tmp_path: Path):
        (tmp_path / "array.hdf5").touch()
        assert _check_write_locks(tmp_path, "array") is False

    def test_check_write_locks_own_marker(self, tmp_path: Path):
        _varray_lock_marker(tmp_path / "array.hdf5").touch()
        assert _check_write_locks(tmp_path, "array") is True

    @pytest.mark.parametrize(
        ("pid", "token"),
        [
            (lambda: os.getpid() + 1, lambda: process_token()),
            (lambda: os.getpid(), lambda: uuid4().hex),
            (lambda: int(f"1{os.getpid()}"), lambda: process_token()),
        ],
        ids=["other_pid", "same_pid_other_token", "own_pid_as_suffix"],
    )
    def test_check_write_locks_foreign_marker(self, tmp_path: Path, pid, token):
        """Tests the marker of another process means the array is locked by someone else."""
        (
            tmp_path / f"array.hdf5{META_DIVIDER}{pid()}{META_DIVIDER}{token()}{LocksExtensions.varray_lock.value}"
        ).touch()
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

    def test_read_array_lock_path_is_the_main_file(
        self, read_array_lock: ReadArrayLock, inserted_array: Array, local_array_adapter: LocalArrayAdapter
    ):
        """Tests the read lock is taken on the main file of the array itself: no marker file is involved."""
        dir_path = get_main_path(inserted_array.id, local_array_adapter.collection_path / local_array_adapter.data_dir)
        read_array_lock.args = []
        read_array_lock.kwargs = {"array": inserted_array}
        assert read_array_lock.get_path() == dir_path / (inserted_array.id + local_array_adapter.file_ext)

    def test_reader_is_refused_while_a_writer_holds_the_file(
        self, read_array_lock: ReadArrayLock, inserted_array: Array, local_array_adapter: LocalArrayAdapter
    ):
        """Tests a reader cannot start while the file is flocked exclusively."""
        dir_path = get_main_path(inserted_array.id, local_array_adapter.collection_path / local_array_adapter.data_dir)
        hdf_path = dir_path / (inserted_array.id + local_array_adapter.file_ext)
        read_array_lock.kwargs = {"array": inserted_array}
        with Flock(hdf_path):
            with pytest.raises(DekerLockError):
                read_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
                read_array_lock.acquire(read_array_lock.get_path())

    def test_read_lock_is_a_shared_flock_held_until_release(
        self, read_array_lock: ReadArrayLock, inserted_array: Array, local_array_adapter: LocalArrayAdapter
    ):
        """Tests the reader holds a shared flock from acquire to release: a writer cannot take the file meanwhile."""
        read_array_lock.kwargs = {"array": inserted_array}
        path = read_array_lock.get_path()
        read_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
        read_array_lock.acquire(path)
        try:
            with pytest.raises(DekerLockError):
                Flock(path).acquire()  # exclusive
            with open(path) as another_reader:  # shared: another reader is welcome
                fcntl.flock(another_reader, fcntl.LOCK_SH | fcntl.LOCK_NB)
        finally:
            read_array_lock.release()
        writer = Flock(path)
        writer.acquire()  # the file is free once the reader has released
        writer.release()

    def test_read_creates_no_files(
        self, read_array_lock: ReadArrayLock, local_array_adapter: LocalArrayAdapter, inserted_array: Array
    ):
        """Tests a read leaves no marker files: the lock lives in the kernel and dies with the process."""
        read_array_lock.kwargs = {"array": inserted_array}
        dir_path = read_array_lock.get_path().parent
        before = set(dir_path.iterdir())

        def check_files(self, *args, **kwargs):
            assert set(dir_path.iterdir()) == before

        read_array_lock(check_files)(local_array_adapter, inserted_array)
        assert set(dir_path.iterdir()) == before

    def test_reader_inside_the_own_varray_write_does_not_flock(
        self, read_array_lock: ReadArrayLock, inserted_array: Array, local_array_adapter: LocalArrayAdapter
    ):
        """Tests a read of a tile locked by a VArray of the current process skips the flock (the VArray holds it)."""
        read_array_lock.kwargs = {"array": inserted_array}
        path = read_array_lock.get_path()
        marker = _varray_lock_marker(path)
        marker.touch()
        try:
            with Flock(path):  # the VArray writer of this process holds the file
                read_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
                read_array_lock.acquire(path)
                read_array_lock.release()
        finally:
            marker.unlink()


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
        self, write_array_lock: WriteArrayLock, inserted_array: Array, mocker: MockerFixture, mp_context: BaseContext
    ):
        """Tests the writer waits for a reader of another process and holds the file once the reader has left."""
        file_created = mp_context.Event()
        release = mocker.spy(write_array_lock, "release")
        write_array_lock.kwargs = {"array": inserted_array}
        filepath = write_array_lock.get_path()
        process = mp_context.Process(target=hold_shared_flock, args=(str(filepath), file_created))
        process.start()
        file_created.wait(10)
        try:
            write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
            release.assert_not_called()
            assert write_array_lock.lock is not None
        finally:
            write_array_lock.release()
            process.kill()

    def test_write_array_lock_check_existing_lock_read_locks_fail(
        self, write_array_lock: WriteArrayLock, inserted_array: Array, mocker: MockerFixture, mp_context: BaseContext
    ):
        """Tests the writer gives up with DekerLockError when the reader holds the file longer than the timeout."""
        file_created = mp_context.Event()
        reader_may_leave = mp_context.Event()
        mocker.patch.object(write_array_lock.instance.ctx.config, "write_lock_timeout", 1)
        release = mocker.spy(write_array_lock, "release")
        write_array_lock.kwargs = {"array": inserted_array}
        filepath = write_array_lock.get_path()
        process = mp_context.Process(target=hold_shared_flock, args=(str(filepath), file_created, reader_may_leave))
        process.start()
        file_created.wait(10)
        try:
            with pytest.raises(DekerLockError):
                write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
            release.assert_called()
            assert write_array_lock.lock is None
        finally:
            reader_may_leave.set()
            process.join(10)

    def test_writer_holds_the_file_after_the_readers_leave(
        self, write_array_lock: WriteArrayLock, inserted_array: Array, mp_context: BaseContext
    ):
        """Tests the writer ends up holding the exclusive flock once the reader of another process has left."""
        file_created = mp_context.Event()
        write_array_lock.kwargs = {"array": inserted_array}
        filepath = write_array_lock.get_path()
        process = mp_context.Process(target=hold_shared_flock, args=(str(filepath), file_created))
        process.start()
        file_created.wait(10)
        try:
            write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
            try:
                with pytest.raises(DekerLockError):
                    Flock(filepath).acquire()
                with open(filepath) as reader, pytest.raises(BlockingIOError):
                    fcntl.flock(reader, fcntl.LOCK_SH | fcntl.LOCK_NB)
            finally:
                write_array_lock.release()
        finally:
            process.kill()

    def test_writer_proceeds_after_a_dead_reader(
        self, write_array_lock: WriteArrayLock, inserted_array: Array, mocker: MockerFixture, mp_context: BaseContext
    ):
        """Tests a reader killed in the middle of a read holds nothing: the kernel releases its flock."""
        file_created = mp_context.Event()
        mocker.patch.object(write_array_lock.instance.ctx.config, "write_lock_timeout", 1)
        write_array_lock.kwargs = {"array": inserted_array}
        filepath = write_array_lock.get_path()
        process = mp_context.Process(target=hold_shared_flock_forever, args=(str(filepath), file_created))
        process.start()
        file_created.wait(10)
        process.kill()
        process.join(10)

        write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
        write_array_lock.release()


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
        process = mp_context.Process(target=hold_shared_flock, args=(str(busy_path), file_created, error_raised))
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
        process = mp_context.Process(target=hold_shared_flock, args=(str(busy_path), file_created, error_raised))
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
            process = mp_context.Process(target=hold_shared_flock, args=(str(filepath), file_created, error_raised))
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
            process = mp_context.Process(target=hold_shared_flock, args=(str(filepath), file_created, error_raised))
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

    def test_release_without_a_lock_does_nothing(self):
        """A lock that was never taken (acquire refused) has nothing to release."""
        lock = CollectionLock()
        assert lock.lock is None
        assert lock.release() is None
        assert lock.release(DekerMemoryError("too big")) is None
        assert lock.lock is None


class TestTileCreationLock:
    """The exclusive flock on the main file of a VArray while one of its tiles is created."""

    @pytest.fixture
    def main_file(self, inserted_varray: VArray) -> Path:
        adapter = inserted_varray._adapter
        return get_main_path(inserted_varray.id, adapter.collection_path / adapter.data_dir) / (
            inserted_varray.id + adapter.file_ext
        )

    def test_lock_holds_the_main_file_while_entered(self, inserted_varray: VArray, main_file: Path):
        collection = inserted_varray._VArray__collection
        lock = TileCreationLock(inserted_varray._adapter, inserted_varray, collection)
        assert lock.path == main_file
        with lock:
            assert lock.flock is not None
            assert lock.thread_lock is not None and lock.thread_lock.locked()
            with pytest.raises(DekerLockError):
                Flock(main_file).acquire()
        assert lock.flock is None
        assert lock.thread_lock is None
        assert not TileCreationLock._thread_locks[main_file].locked()
        other = Flock(main_file)
        other.acquire()
        other.release()

    def test_lock_times_out_when_the_file_is_held(self, inserted_varray: VArray, main_file: Path):
        """A holder that never lets go: the lock gives up after write_lock_timeout and frees the thread lock."""
        collection = inserted_varray._VArray__collection
        config = inserted_varray._adapter.ctx.config
        holder = Flock(main_file)
        holder.acquire()
        timeout = config.write_lock_timeout
        config.write_lock_timeout = 1
        try:
            lock = TileCreationLock(inserted_varray._adapter, inserted_varray, collection)
            assert lock._try_acquire(main_file) is False
            assert lock.flock is None
            with pytest.raises(DekerLockError, match="locked for the creation of its tiles"):
                with lock:
                    pass
            assert lock.thread_lock is None
            assert not TileCreationLock._thread_locks[main_file].locked()
        finally:
            config.write_lock_timeout = timeout
            holder.release()

    def test_lock_is_a_no_op_for_an_adapter_of_another_storage(self, inserted_varray: VArray, main_file: Path):
        """The server adapters keep their own order of creation: no path, no flock, no thread lock."""
        collection = inserted_varray._VArray__collection
        adapter = SimpleNamespace(ctx=inserted_varray._adapter.ctx)
        lock = TileCreationLock(adapter, inserted_varray, collection)
        assert lock.path is None
        with lock as entered:
            assert entered is lock
            assert lock.flock is None
            assert lock.thread_lock is None
            other = Flock(main_file)
            other.acquire()
            other.release()
        assert lock.flock is None
        assert lock.thread_lock is None


if __name__ == "__main__":
    pytest.main()

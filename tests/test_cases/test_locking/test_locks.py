import fcntl

from multiprocessing.context import BaseContext
from multiprocessing.synchronize import Event
from pathlib import Path
from time import sleep
from types import SimpleNamespace

import pytest

from deker_local_adapters import LocalArrayAdapter
from pytest_mock import MockerFixture

from deker.arrays import Array, VArray
from deker.client import Client
from deker.errors import DekerLockError, DekerMemoryError
from deker.locks import (
    ArrayCreationLock,
    CollectionLock,
    Flock,
    ReadArrayLock,
    UpdateMetaAttributeLock,
    WriteArrayLock,
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


LEGACY_MARKER = f"{{name}}:1:{'0' * 32}{LocksExtensions.varray_lock.value}"  # a VArray write marker of Deker < 2


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

        lock = ReadArrayLock()
        lock.instance = Mock()
        with pytest.raises(TypeError, match="Mock lock is not supported"):
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

    def test_leftover_marker_of_an_older_version_is_ignored(
        self, read_array_lock: ReadArrayLock, inserted_array: Array, local_array_adapter: LocalArrayAdapter
    ):
        """Tests a VArray write marker left by an older Deker neither refuses the reader nor replaces its flock."""
        read_array_lock.kwargs = {"array": inserted_array}
        path = read_array_lock.get_path()
        marker = path.parent / LEGACY_MARKER.format(name=path.name)
        marker.touch()
        try:
            read_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
            read_array_lock.acquire(path)
            assert read_array_lock.lock is not None
            with pytest.raises(DekerLockError):
                Flock(path).acquire()  # the reader holds the file itself
            read_array_lock.release()
            assert marker.exists()  # not ours to remove: client.clear_locks does it
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

        lock = WriteArrayLock()
        lock.instance = Mock()
        with pytest.raises(TypeError, match="Mock lock is not supported"):
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

    def test_leftover_marker_of_an_older_version_is_ignored(
        self, write_array_lock: WriteArrayLock, inserted_array: Array, local_array_adapter: LocalArrayAdapter
    ):
        """Tests a VArray write marker left by an older Deker neither refuses the writer nor replaces its flock."""
        write_array_lock.kwargs = {"array": inserted_array}
        path = write_array_lock.get_path()
        marker = path.parent / LEGACY_MARKER.format(name=path.name)
        marker.touch()
        try:
            write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array})
            assert write_array_lock.lock is not None
            with pytest.raises(DekerLockError):
                Flock(path).acquire()  # the writer holds the file itself
            write_array_lock.release()
            assert marker.exists()
        finally:
            marker.unlink()

    def test_write_creates_no_files(
        self, write_array_lock: WriteArrayLock, local_array_adapter: LocalArrayAdapter, inserted_array: Array
    ):
        """Tests a write leaves no marker files: the lock lives in the kernel and dies with the process."""
        write_array_lock.kwargs = {"array": inserted_array}
        dir_path = write_array_lock.get_path().parent
        before = set(dir_path.iterdir())

        def check_files(self, *args, **kwargs):
            assert set(dir_path.iterdir()) == before

        write_array_lock(check_files)(local_array_adapter, inserted_array)
        assert set(dir_path.iterdir()) == before


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

    def test_lock_file_stays_as_the_marker_of_the_collection(self, client: Client, collection_adapter):
        """Tests the lock file stays after the creation: check_integrity knows the collection by it."""
        schema = ArraySchema(dimensions=[DimensionSchema(name="x", size=2)], dtype=float)
        collection = client.create_collection("created_and_marked", schema)
        try:
            lock_file = (
                collection_adapter.collections_resource / f"{collection.name}{LocksExtensions.collection_lock.value}"
            )
            assert lock_file.exists()
            with Flock(lock_file):
                pass  # not held once the creation is over
            collection.clear()
            assert lock_file.exists()
        finally:
            collection.delete()

    def test_release_without_a_lock_does_nothing(self):
        """A lock that was never taken (acquire refused) has nothing to release."""
        lock = CollectionLock()
        assert lock.lock is None
        assert lock.release() is None
        assert lock.release(DekerMemoryError("too big")) is None
        assert lock.lock is None


class TestArrayCreationLock:
    """The exclusive flock on the directory of the main file of a VArray while one of its Arrays is created.

    The directory, not the json file: the file is replaced on every update of the custom attributes, and a lock
    on it would stay on the old file.
    """

    @pytest.fixture
    def main_dir(self, inserted_varray: VArray) -> Path:
        adapter = inserted_varray._adapter
        return get_main_path(inserted_varray.id, adapter.collection_path / adapter.data_dir)

    def test_lock_holds_the_directory_of_the_main_file_while_entered(self, inserted_varray: VArray, main_dir: Path):
        collection = inserted_varray._VArray__collection
        lock = ArrayCreationLock(inserted_varray._adapter, inserted_varray, collection)
        assert lock.path == main_dir
        with lock:
            assert lock.flock is not None
            assert lock.thread_lock is not None and lock.thread_lock.locked()
            with pytest.raises(DekerLockError):
                Flock(main_dir).acquire()
        assert lock.flock is None
        assert lock.thread_lock is None
        assert not ArrayCreationLock._thread_locks[main_dir].locked()
        other = Flock(main_dir)
        other.acquire()
        other.release()

    def test_lock_survives_the_replacement_of_the_main_file(self, inserted_varray: VArray, main_dir: Path):
        """An update of the custom attributes replaces the json file; the creation lock still holds."""
        collection = inserted_varray._VArray__collection
        lock = ArrayCreationLock(inserted_varray._adapter, inserted_varray, collection)
        with lock:
            main_file = main_dir / (inserted_varray.id + inserted_varray._adapter.file_ext)
            replacement = main_dir / "replacement.json"
            replacement.write_text(main_file.read_text())
            replacement.replace(main_file)
            with pytest.raises(DekerLockError):
                Flock(main_dir).acquire()

    def test_lock_times_out_when_the_directory_is_held(self, inserted_varray: VArray, main_dir: Path):
        """A holder that never lets go: the lock gives up after write_lock_timeout and frees the thread lock."""
        collection = inserted_varray._VArray__collection
        config = inserted_varray._adapter.ctx.config
        holder = Flock(main_dir)
        holder.acquire()
        timeout = config.write_lock_timeout
        config.write_lock_timeout = 1
        try:
            lock = ArrayCreationLock(inserted_varray._adapter, inserted_varray, collection)
            assert lock._try_acquire(main_dir) is False
            assert lock.flock is None
            with pytest.raises(DekerLockError, match="locked for the creation of its Arrays"):
                with lock:
                    pass
            assert lock.thread_lock is None
            assert not ArrayCreationLock._thread_locks[main_dir].locked()
        finally:
            config.write_lock_timeout = timeout
            holder.release()

    def test_lock_is_a_no_op_for_an_adapter_of_another_storage(self, inserted_varray: VArray, main_dir: Path):
        """The server adapters keep their own order of creation: no path, no flock, no thread lock."""
        collection = inserted_varray._VArray__collection
        adapter = SimpleNamespace(ctx=inserted_varray._adapter.ctx)
        lock = ArrayCreationLock(adapter, inserted_varray, collection)
        assert lock.path is None
        with lock as entered:
            assert entered is lock
            assert lock.flock is None
            assert lock.thread_lock is None
            other = Flock(main_dir)
            other.acquire()
            other.release()
        assert lock.flock is None
        assert lock.thread_lock is None


class DirectoryProbe:
    """Stands in for a method of the adapter and notes whether the directory was locked when it was called."""

    def __init__(self, directory: Path, original):
        self.directory = directory
        self.original = original
        self.held: list[bool] = []

    def __call__(self, array: VArray) -> Path:
        flock = Flock(self.directory)
        try:
            flock.acquire()
        except DekerLockError:
            self.held.append(True)
        else:
            self.held.append(False)
            flock.release()
        return self.original(array)


class TestUpdateMetaAttributeLock:
    """The exclusive flock on the directory of the main file of a VArray while its custom attributes are updated.

    The directory, not the json file: the file is replaced by the update, and a lock on it would stay on the old
    file, letting the next writer in while the holder is still at work.
    """

    @pytest.fixture
    def lock(self, inserted_varray: VArray) -> UpdateMetaAttributeLock:
        lock = UpdateMetaAttributeLock()
        lock.instance = inserted_varray._adapter
        lock.args = (inserted_varray._adapter, inserted_varray)
        lock.kwargs = {}
        return lock

    def test_lock_path_is_the_directory_of_the_main_file(self, lock: UpdateMetaAttributeLock, inserted_varray: VArray):
        adapter = inserted_varray._adapter
        assert lock.get_path() == get_main_path(inserted_varray.id, adapter.collection_path / adapter.data_dir)

    def test_lock_allows_the_varray_adapter_only(self, lock: UpdateMetaAttributeLock, local_array_adapter):
        lock.check_type()
        lock.instance = local_array_adapter
        with pytest.raises(TypeError, match="LocalArrayAdapter lock is not supported"):
            lock.check_type()

    def test_writer_waits_for_the_holder_and_gives_up_after_the_timeout(
        self, lock: UpdateMetaAttributeLock, inserted_varray: VArray, mocker: MockerFixture
    ):
        """A second updater waits for the first one; a holder that never lets go makes it fail after the timeout."""
        config = inserted_varray._adapter.ctx.config
        mocker.patch.object(config, "write_lock_timeout", 1)
        mocker.patch.object(config, "write_lock_check_interval", 0.1)
        holder = Flock(lock.get_path())
        holder.acquire()
        try:
            with pytest.raises(DekerLockError, match="is locked"):
                lock.check_existing_lock(func_args=(), func_kwargs={})
            assert lock.lock is None
        finally:
            holder.release()
        lock.check_existing_lock(func_args=(), func_kwargs={})
        assert lock.lock is not None
        lock.release()

    def test_update_of_the_attributes_holds_the_directory(self, inserted_varray_with_attributes: VArray, mocker):
        """The adapter method runs under the lock: the directory is held while it works."""
        varray = inserted_varray_with_attributes
        adapter = varray._adapter
        directory = get_main_path(varray.id, adapter.collection_path / adapter.data_dir)
        probe = DirectoryProbe(directory, adapter._get_main_path_to_file)
        mocker.patch.object(adapter, "_get_main_path_to_file", side_effect=probe)
        varray.update_custom_attributes({"custom_attribute": 0.9})
        assert probe.held == [True]


if __name__ == "__main__":
    pytest.main()

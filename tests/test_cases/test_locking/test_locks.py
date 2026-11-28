import fcntl
import json
import logging
import os
import shutil
import threading
import time

from multiprocessing.context import BaseContext
from multiprocessing.synchronize import Event
from pathlib import Path
from time import sleep
from types import SimpleNamespace
from uuid import uuid4

import pytest

from cantok import DefaultToken, SimpleToken
from deker_local_adapters import HDF5StorageAdapter, LocalArrayAdapter
from deker_tools.cancel import TokenCancelledError
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.parameters.uri import embedded_uri

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerCollectionAlreadyExistsError,
    DekerCollectionNotExistsError,
    DekerCriticalError,
    DekerInstanceNotExistsError,
    DekerLockError,
    DekerMemoryError,
)
from deker.locks import (
    ArrayCreationLock,
    CollectionLock,
    Flock,
    ReadArrayLock,
    UpdateMetaAttributeLock,
    WaitingReadArrayLock,
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


NOT_GIVEN = {"timeout": Sentinel, "token": DefaultToken()}


def open_descriptors() -> int:
    """Count the open descriptors of this process."""
    return len(os.listdir("/proc/self/fd"))


class MoveAfterOpen:
    """Stands in for ``os.open`` of ``deker.flock``: after the first opening of the path, moves a file once.

    The move lands between the opening of the file and its ``flock``: ``source`` to ``target``.
    """

    def __init__(self, path: Path, source: Path, target: Path):
        self.path = path
        self.source = source
        self.target = target
        self.real = os.open
        self.calls = 0

    def __call__(self, file: Path, flags: int, *args) -> int:
        descriptor = self.real(file, flags, *args)
        if Path(file) == self.path:
            self.calls += 1
            if self.calls == 1:
                os.replace(self.source, self.target)
        return descriptor


def add_one(self, array: VArray, *, timeout, token) -> None:
    """Add one to a custom attribute in the json file of the VArray as the update of the attributes writes it.

    :param self: the VArray adapter
    :param array: the VArray
    :param timeout: not used
    :param token: not used
    """
    path = self._get_main_path_to_file(array)
    meta = json.loads(path.read_text())
    meta["custom_attributes"]["custom_attribute"] += 1
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(meta))
    os.replace(temporary, path)


def add_under_the_update_lock(uri: str, collection_name: str, varray_id: str, times: int) -> None:
    """Add one to the custom attribute of the VArray the given number of times, each under the update lock.

    :param uri: uri of the storage
    :param collection_name: name of the collection of the VArray
    :param varray_id: id of the VArray
    :param times: how many times to add one
    """
    locked_add_one = UpdateMetaAttributeLock()(add_one)
    with Client(uri) as client:
        varray = client.get_collection(collection_name).filter({"id": varray_id}).last()
        for _ in range(times):
            locked_add_one(varray._adapter, varray, timeout=Sentinel, token=DefaultToken())


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

    def test_reader_of_a_missing_file_is_refused_as_not_existing(
        self, read_array_lock: ReadArrayLock, inserted_array: Array, tmp_path: Path
    ):
        """Tests the file gone before the reader opens it is an Array that does not exist, not a raw error."""
        read_array_lock.kwargs = {"array": inserted_array}
        path = read_array_lock.get_path()
        path.rename(tmp_path / path.name)
        with pytest.raises(DekerInstanceNotExistsError) as error:
            read_array_lock.acquire(path)
        assert str(error.value) == f"{path} doesn't exist"
        assert read_array_lock.lock is None


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
        write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array, **NOT_GIVEN})
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
            write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array, **NOT_GIVEN})
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
                write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array, **NOT_GIVEN})
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
            write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array, **NOT_GIVEN})
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

        write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array, **NOT_GIVEN})
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
            write_array_lock.check_existing_lock(func_args=[], func_kwargs={"array": inserted_array, **NOT_GIVEN})
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

    def test_writer_of_a_missing_file_is_refused_as_not_existing(
        self, write_array_lock: WriteArrayLock, inserted_array: Array, tmp_path: Path
    ):
        """Tests the file gone before the writer opens it is an Array that does not exist, without waiting."""
        write_array_lock.kwargs = {"array": inserted_array}
        path = write_array_lock.get_path()
        path.rename(tmp_path / path.name)
        with pytest.raises(DekerInstanceNotExistsError) as error:
            write_array_lock.check_existing_lock(func_args=[], func_kwargs=dict(NOT_GIVEN))
        assert str(error.value) == f"{path} doesn't exist"
        assert write_array_lock.lock is None


@pytest.mark.parametrize("lock_name", ["read_array_lock", "write_array_lock"])
class TestArrayLockTakesTheFileAtThePath:
    """After the flock, the locks of the arrays hold the file that lies at the path."""

    def test_file_replaced_between_the_open_and_the_lock_is_locked_again_at_the_path(
        self, request: pytest.FixtureRequest, lock_name: str, inserted_array: Array, mocker: MockerFixture
    ):
        lock = request.getfixturevalue(lock_name)
        lock.kwargs = {"array": inserted_array}
        path = lock.get_path()
        replacement = path.with_name("replacement")
        shutil.copyfile(path, replacement)
        probe = MoveAfterOpen(path, replacement, path)
        mocker.patch("deker.flock.os.open", probe)
        before = open_descriptors()
        lock.acquire(path)
        try:
            assert os.fstat(lock.lock.fd).st_ino == os.stat(path).st_ino
            assert probe.calls == 2
            assert open_descriptors() == before + 1
        finally:
            lock.release()
        assert open_descriptors() == before

    def test_file_gone_between_the_open_and_the_lock_is_refused_as_not_existing(
        self,
        request: pytest.FixtureRequest,
        lock_name: str,
        inserted_array: Array,
        mocker: MockerFixture,
        tmp_path: Path,
    ):
        lock = request.getfixturevalue(lock_name)
        lock.kwargs = {"array": inserted_array}
        path = lock.get_path()
        mocker.patch("deker.flock.os.open", MoveAfterOpen(path, path, tmp_path / path.name))
        before = open_descriptors()
        with pytest.raises(DekerInstanceNotExistsError) as error:
            lock.acquire(path)
        assert str(error.value) == f"{path} doesn't exist"
        assert lock.lock is None
        assert open_descriptors() == before


class TestWaitingReadArrayLock:
    """The shared flock on the main file that waits for the writer up to the timeout of the call."""

    @pytest.fixture
    def lock(self, local_array_adapter: LocalArrayAdapter, inserted_array: Array) -> WaitingReadArrayLock:
        lock = WaitingReadArrayLock()
        lock.instance = local_array_adapter
        lock.args = (local_array_adapter, inserted_array)
        lock.kwargs = {"array": inserted_array}
        return lock

    @pytest.fixture
    def writer(self, lock: WaitingReadArrayLock):
        writer = Flock(lock.get_path())
        writer.acquire()
        yield writer
        writer.release()

    @pytest.fixture
    def interval(self, lock: WaitingReadArrayLock, mocker: MockerFixture) -> None:
        mocker.patch.object(lock.instance.ctx.config, "write_lock_check_interval", 0.1)

    @pytest.mark.timeout(10)
    def test_lock_gives_up_after_the_timeout_while_the_writer_holds_the_file(
        self, lock: WaitingReadArrayLock, inserted_array: Array, writer: Flock, interval
    ):
        before = open_descriptors()
        start = time.monotonic()
        with pytest.raises(DekerLockError) as error:
            lock.check_existing_lock(func_args=(), func_kwargs={"timeout": 1, "token": DefaultToken()})
        assert time.monotonic() - start >= 1
        assert str(error.value) == f"{inserted_array} is locked by a writer"
        assert lock.lock is None
        assert open_descriptors() == before

    @pytest.mark.timeout(10)
    def test_lock_is_taken_once_the_writer_leaves(self, lock: WaitingReadArrayLock, writer: Flock, interval):
        leaving = threading.Timer(1, writer.release)
        start = time.monotonic()
        leaving.start()
        try:
            lock.check_existing_lock(func_args=(), func_kwargs={"timeout": 5, "token": DefaultToken()})
            elapsed = time.monotonic() - start
            assert lock.lock is not None and lock.lock.shared
        finally:
            leaving.join()
            lock.release()
        assert 0.9 <= elapsed < 3

    def test_lock_is_taken_at_once_while_the_readers_hold_the_file(self, lock: WaitingReadArrayLock):
        reader = Flock(lock.get_path(), shared=True)
        reader.acquire()
        try:
            start = time.monotonic()
            lock.check_existing_lock(func_args=(), func_kwargs={"timeout": 5, "token": DefaultToken()})
            assert time.monotonic() - start < 0.5
            assert lock.lock is not None
            lock.release()
        finally:
            reader.release()

    def test_decorated_call_holds_one_shared_flock_and_leaves_no_descriptor(
        self, lock: WaitingReadArrayLock, local_array_adapter: LocalArrayAdapter, inserted_array: Array
    ):
        """The wait takes the flock before the call; the second acquire of the decorator does not take another."""
        path = lock.get_path()
        before = open_descriptors()
        during: list[int] = []

        def read(self, array, *, timeout, token):
            during.append(open_descriptors())
            with pytest.raises(DekerLockError):
                Flock(path).acquire()  # exclusive: refused
            other = Flock(path, shared=True)  # shared: welcome
            other.acquire()
            other.release()

        WaitingReadArrayLock()(read)(local_array_adapter, inserted_array, timeout=1, token=DefaultToken())
        assert during == [before + 1]
        assert open_descriptors() == before

    @pytest.mark.timeout(10)
    def test_cancelled_token_stops_the_wait(self, lock: WaitingReadArrayLock, writer: Flock, interval):
        token = SimpleToken()
        canceller = threading.Timer(0.3, token.cancel)
        before = open_descriptors()
        canceller.start()
        with pytest.raises(TokenCancelledError):
            lock.check_existing_lock(func_args=(), func_kwargs={"timeout": 30, "token": token})
        canceller.join()
        assert lock.lock is None
        assert open_descriptors() == before

    def test_lock_of_a_missing_file_is_refused_as_not_existing(self, lock: WaitingReadArrayLock, tmp_path: Path):
        path = lock.get_path()
        path.rename(tmp_path / path.name)
        with pytest.raises(DekerInstanceNotExistsError) as error:
            lock.check_existing_lock(func_args=(), func_kwargs={"timeout": 5, "token": DefaultToken()})
        assert str(error.value) == f"{path} doesn't exist"
        assert lock.lock is None


class ProbeHeld:
    """Stand for ``json.dumps`` of the collection adapter: before the metadata is made, it looks whether the json file
    of the collection is held, then makes the metadata.

    :param real: the real ``json.dumps``
    :param path: the json file
    """

    def __init__(self, real, path: Path) -> None:
        self.real = real
        self.path = path
        self.held: list[bool] = []

    def __call__(self, *args, **kwargs) -> str:
        try:
            with Flock(self.path):
                self.held.append(False)
        except DekerLockError:
            self.held.append(True)
        return self.real(*args, **kwargs)


class HoldTheWriter:
    """Stand for ``json.dumps`` of the collection adapter: the first creator tells it holds the lock of the collection
    and waits for a go before it makes its metadata.

    :param real: the real ``json.dumps``
    """

    def __init__(self, real) -> None:
        self.real = real
        self.holding = threading.Event()
        self.go = threading.Event()

    def __call__(self, *args, **kwargs) -> str:
        if not self.holding.is_set():
            self.holding.set()
            self.go.wait(30)
        return self.real(*args, **kwargs)


class TellTheWait:
    """Stand for the sleep between the attempts to take a lock: tells that a creator waits, then sleeps.

    :param real: the real sleep
    :param release: called once, at the first wait, when not ``None``
    """

    def __init__(self, real, release=None) -> None:
        self.real = real
        self.release = release
        self.waiting = threading.Event()

    def __call__(self, seconds: float) -> None:
        if not self.waiting.is_set():
            self.waiting.set()
            if self.release is not None:
                self.release()
        self.real(seconds)


class Create:
    """Create a collection in a thread and keep the result: the collection or the error.

    :param client: client of the storage
    :param name: name of the collection
    :param schema: its schema
    """

    def __init__(self, client: Client, name: str, schema: ArraySchema) -> None:
        self.client = client
        self.name = name
        self.schema = schema
        self.result: object = None

    def __call__(self) -> None:
        try:
            self.result = self.client.create_collection(self.name, self.schema, timeout=30)
        except Exception as e:  # noqa: BLE001  # the result of the creation, checked by the test
            self.result = e


class DeleteBeforeTheWrite:
    """Stand for ``json.dumps`` of the collection adapter: the collection is deleted while its creator holds its lock,
    before the metadata is written.

    :param real: the real ``json.dumps``
    :param collection: the collection being created
    """

    def __init__(self, real, collection: Collection) -> None:
        self.real = real
        self.collection = collection

    def __call__(self, *args, **kwargs) -> str:
        self.collection.delete(_empty_trash=False)
        return self.real(*args, **kwargs)


class TakeAwayAfterTheDirectory:
    """Stand for ``os.mkdir``: after the first real call, takes a directory away, as the deletion of the collection or
    the loss of the collections directory of the storage does between the directory of the collection and its file.

    :param real: the real ``os.mkdir``
    :param directory: the directory to take away
    :param target: where it goes
    """

    def __init__(self, real, directory: Path, target: Path) -> None:
        self.real = real
        self.directory = directory
        self.target = target
        self.calls = 0

    def __call__(self, *args, **kwargs) -> None:
        self.real(*args, **kwargs)
        self.calls += 1
        if self.calls == 1:
            self.directory.rename(self.target)


class TakeAwayUnderTheLock:
    """Stand for ``get_result`` of the collection lock: once the lock is taken, takes a directory away before the
    creation reads the json file, as the deletion of the collection or the loss of the collections directory does.

    :param real: the real ``get_result``
    :param directory: the directory to take away
    :param target: where it goes
    """

    def __init__(self, real, directory: Path, target: Path) -> None:
        self.real = real
        self.directory = directory
        self.target = target

    def __call__(self, lock, *args, **kwargs):
        assert lock.lock is not None  # held
        self.directory.rename(self.target)
        return self.real(lock, *args, **kwargs)


class RemadeBeforeTheOpen:
    """Stand for ``Flock.acquire``: the first time, the directory of the file is taken away and made again empty, as a
    deletion and another creator of the collection do, and the file is not found; then the real acquire.

    :param real: the real ``Flock.acquire``
    :param directory: the directory of the collection
    :param target: where it goes
    """

    def __init__(self, real, directory: Path, target: Path) -> None:
        self.real = real
        self.directory = directory
        self.target = target
        self.calls = 0

    def __call__(self, flock: Flock) -> None:
        self.calls += 1
        if self.calls == 1:
            self.directory.rename(self.target)
            self.directory.mkdir()
            raise FileNotFoundError(2, "No such file or directory", str(flock.file))
        self.real(flock)


class TestCollectionLock:
    """The lock of a collection is an exclusive flock on its json file, created empty with the directory of the
    collection; it is taken by the creation of the collection only. Under it, an empty file is written, a written one
    is a collection that exists. No other file is made.
    """

    SCHEMA = ArraySchema(dimensions=[DimensionSchema(name="x", size=2)], dtype=float)

    @pytest.fixture
    def name(self, client: Client) -> str:
        """Name of a collection that does not exist; whatever the test makes of it is removed afterwards.

        :param client: client of the storage
        """
        name = f"locked_{uuid4().hex}"
        yield name
        shutil.rmtree(client._adapter.collections_resource / name, ignore_errors=True)

    @staticmethod
    def json_file(client: Client, name: str) -> Path:
        """Path of the json file of a collection.

        :param client: client of the storage
        :param name: name of the collection
        :returns: the path
        """
        return client._adapter.collections_resource / name / (name + ".json")

    @staticmethod
    def hold(client: Client, name: str) -> Flock:
        """Hold the lock of a collection as its creator does: the directory and the empty json file, flocked.

        :param client: client of the storage
        :param name: name of the collection
        :returns: the held flock
        """
        (client._adapter.collections_resource / name).mkdir()
        holder = Flock(TestCollectionLock.json_file(client, name), create=True)
        holder.acquire()
        return holder

    def test_lock_is_on_the_json_file_of_the_collection(self, client: Client, collection_adapter, name: str):
        lock = CollectionLock()
        lock.instance = collection_adapter
        lock.args = [collection_adapter, SimpleNamespace(name=name)]
        assert lock.get_path() == collection_adapter.collections_resource / name / (name + ".json")

    def test_json_file_is_held_while_it_is_written(self, client: Client, name: str, mocker: MockerFixture):
        probe = ProbeHeld(json.dumps, self.json_file(client, name))
        mocker.patch("deker_local_adapters.collection_adapter.json.dumps", side_effect=probe)
        collection = client.create_collection(name, self.SCHEMA)
        assert probe.held == [True]
        assert json.loads(self.json_file(client, name).read_text()) == json.loads(json.dumps(collection.as_dict))
        with Flock(self.json_file(client, name)):
            pass  # not held once the creation is over

    def test_no_lock_file_is_made(self, client: Client, name: str):
        collections = client._adapter.collections_resource
        before = sorted(collections.glob("*.lock"))
        collection = client.create_collection(name, self.SCHEMA)
        collection.create()
        collection.clear()
        assert sorted(collections.glob("*.lock")) == before
        assert sorted(path.name for path in collection.path.iterdir()) == [name + ".json"]
        collection.delete()
        assert sorted(collections.glob("*.lock")) == before

    def test_memory_error_creates_nothing(self, client: Client, name: str):
        schema = ArraySchema(
            dimensions=[DimensionSchema(name="x", size=100000), DimensionSchema(name="y", size=100000)], dtype=float
        )
        with pytest.raises(DekerMemoryError):
            client.create_collection(name, schema)
        assert not (client._adapter.collections_resource / name).exists()

    def test_existing_collection_is_not_written_again(self, client: Client, name: str):
        """The json file of the collection is written: the collection exists, the second creator learns so."""
        collection = client.create_collection(name, self.SCHEMA)
        other = ArraySchema(dimensions=[DimensionSchema(name="x", size=3)], dtype=float)
        with pytest.raises(DekerCollectionAlreadyExistsError) as error:
            client.create_collection(name, other)
        assert str(error.value) == f"Collection {name} already exists"
        assert json.loads(self.json_file(client, name).read_text()) == json.loads(json.dumps(collection.as_dict))

    def test_empty_json_file_left_by_a_creator_is_written(self, client: Client, name: str):
        """A creator that stopped before it wrote leaves the json file empty: the next creator writes it."""
        self.hold(client, name).release()
        collection = client.create_collection(name, self.SCHEMA)
        assert json.loads(self.json_file(client, name).read_text()) == json.loads(json.dumps(collection.as_dict))

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("setting", [False, True], ids=["argument", "client-setting"])
    def test_held_lock_is_waited_for_up_to_the_timeout(self, client: Client, name: str, mocker: MockerFixture, setting):
        holder = self.hold(client, name)
        kwargs = {"timeout": 1}
        if setting:
            mocker.patch.object(client._Client__ctx.config, "write_lock_timeout", 1)
            kwargs = {}
        try:
            start = time.monotonic()
            with pytest.raises(DekerLockError) as error:
                client.create_collection(name, self.SCHEMA, **kwargs)
            assert 1 <= time.monotonic() - start < 2.5
            assert str(error.value) == f"Collection {name} is locked by another creation"
        finally:
            holder.release()
        assert self.json_file(client, name).read_text() == ""

    @pytest.mark.timeout(10)
    def test_creation_goes_on_once_the_lock_is_released(self, client: Client, name: str, mocker: MockerFixture):
        holder = self.hold(client, name)
        wait = TellTheWait(time.sleep, holder.release)
        mocker.patch("deker.ABC.base_locks.sleep", side_effect=wait)
        collection = client.create_collection(name, self.SCHEMA, timeout=5)
        assert wait.waiting.is_set()
        assert json.loads(self.json_file(client, name).read_text()) == json.loads(json.dumps(collection.as_dict))

    @pytest.mark.timeout(60)
    def test_two_creators_of_one_name_with_different_schemas(self, client: Client, name: str, mocker: MockerFixture):
        """The second creator waits for the first one and learns that the collection exists; the file is the first
        one's.
        """
        writer = HoldTheWriter(json.dumps)
        mocker.patch("deker_local_adapters.collection_adapter.json.dumps", side_effect=writer)
        wait = TellTheWait(time.sleep)
        mocker.patch("deker.ABC.base_locks.sleep", side_effect=wait)
        first = Create(client, name, self.SCHEMA)
        second = Create(client, name, ArraySchema(dimensions=[DimensionSchema(name="x", size=3)], dtype=float))
        threads = [threading.Thread(target=first), threading.Thread(target=second)]
        threads[0].start()
        assert writer.holding.wait(30)
        threads[1].start()
        assert wait.waiting.wait(30)
        writer.go.set()
        for thread in threads:
            thread.join(30)
        assert isinstance(first.result, Collection)
        assert isinstance(second.result, DekerCollectionAlreadyExistsError)
        assert str(second.result) == f"Collection {name} already exists"
        assert json.loads(self.json_file(client, name).read_text()) == json.loads(json.dumps(first.result.as_dict))

    def test_deletion_while_the_creator_holds_the_lock(self, client: Client, name: str, mocker: MockerFixture):
        """The deletion takes no lock: the creator writes the file it holds, which is in the trash already. The
        creation is done and the deletion is after it: the collection does not exist.
        """
        trash = client._adapter.trash_resource
        assert list(trash.iterdir()) == []
        collection = Collection(name, self.SCHEMA, client._adapter, client._factory, HDF5StorageAdapter)
        mocker.patch(
            "deker_local_adapters.collection_adapter.json.dumps",
            side_effect=DeleteBeforeTheWrite(json.dumps, collection),
        )
        assert client._adapter.create(collection, timeout=Sentinel, token=DefaultToken()) is None
        assert not (client._adapter.collections_resource / name).exists()
        assert client.get_collection(name) is None
        (moved,) = trash.iterdir()
        assert json.loads((moved / (name + ".json")).read_text()) == json.loads(json.dumps(collection.as_dict))
        client.empty_trash()
        assert list(trash.iterdir()) == []

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="collection deleted")]
    )
    def test_directory_gone_before_its_file_is_opened(self, tmp_path: Path, mocker: MockerFixture, storage_gone: bool):
        """The creator makes the directory again and goes on; the loss of the collections directory is critical."""
        with Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR") as own:
            collections = own._adapter.collections_resource
            directory = collections if storage_gone else collections / "own"
            taker = TakeAwayAfterTheDirectory(os.mkdir, directory, tmp_path / "away")
            mocker.patch.object(os, "mkdir", side_effect=taker)
            if storage_gone:
                critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
                with pytest.raises(DekerCriticalError) as error:
                    own.create_collection("own", self.SCHEMA)
                assert str(error.value) == f"Storage directory {collections} doesn't exist"
                critical.assert_called_once_with(f"Storage directory {collections} doesn't exist")
                assert taker.calls == 1
                assert not collections.exists()
            else:
                collection = own.create_collection("own", self.SCHEMA)
                assert taker.calls == 2
                assert json.loads((collections / "own" / "own.json").read_text()) == json.loads(
                    json.dumps(collection.as_dict)
                )
                assert list((tmp_path / "away").iterdir()) == []  # the file was never made in the old directory

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="collection deleted")]
    )
    def test_directory_gone_once_the_lock_is_taken(self, tmp_path: Path, mocker: MockerFixture, storage_gone: bool):
        """The deletion comes after the creation: the creation is done, the collection does not exist; the loss of the
        collections directory is critical.
        """
        with Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR") as own:
            collections = own._adapter.collections_resource
            directory = collections if storage_gone else collections / "own"
            taker = TakeAwayUnderTheLock(CollectionLock.get_result, directory, tmp_path / "away")
            mocker.patch.object(CollectionLock, "get_result", autospec=True, side_effect=taker)
            if storage_gone:
                critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
                with pytest.raises(DekerCriticalError) as error:
                    own.create_collection("own", self.SCHEMA)
                assert str(error.value) == f"Storage directory {collections} doesn't exist"
                critical.assert_called_once_with(f"Storage directory {collections} doesn't exist")
                assert not collections.exists()
            else:
                assert own.create_collection("own", self.SCHEMA).name == "own"
                assert not directory.exists()
                assert own.get_collection("own") is None
                assert (tmp_path / "away" / "own.json").read_text() == ""  # held, never written

    @staticmethod
    def take(adapter, name: str) -> None:
        """Take the lock of the creation of a collection of the name and release it; the name is not checked here.

        :param adapter: the collection adapter
        :param name: name of the collection
        """
        lock = CollectionLock()
        lock.instance = adapter
        lock.args = [adapter, SimpleNamespace(name=name)]
        try:
            lock.check_existing_lock((), {"timeout": 1, "token": DefaultToken()})
        finally:
            lock.release()

    @pytest.mark.timeout(10)
    def test_directory_that_cannot_be_made_is_not_retried(self, tmp_path: Path):
        """A directory that cannot be made under the collections directory is refused at once, nothing made. The
        collection refuses such a name before; the lock is called directly.
        """
        with Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR") as own:
            collections = own._adapter.collections_resource
            with pytest.raises(FileNotFoundError) as error:
                self.take(own._adapter, "a/b")
            assert error.value.filename == str(collections / "a" / "b")
            assert list(collections.iterdir()) == []

    @pytest.mark.timeout(10)
    def test_directory_made_again_by_another_creator_meanwhile(self, tmp_path: Path, mocker: MockerFixture):
        """The json file is not found because the deletion took the directory away, and another creator made the
        directory again before the creator looked: the creator goes on and creates the collection, no raw error.
        """
        with Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR") as own:
            directory = own._adapter.collections_resource / "own"
            remade = RemadeBeforeTheOpen(Flock.acquire, directory, tmp_path / "away")
            mocker.patch.object(Flock, "acquire", autospec=True, side_effect=remade)
            collection = own.create_collection("own", self.SCHEMA)
            assert remade.calls == 2
            assert json.loads((directory / "own.json").read_text()) == json.loads(json.dumps(collection.as_dict))

    def test_collection_being_created_is_not_found(self, client: Client, name: str):
        """The json file of a collection being created is empty: the collection does not exist yet."""
        holder = self.hold(client, name)
        try:
            assert client.get_collection(name) is None
            assert name not in [collection.name for collection in client]
            with pytest.raises(DekerCollectionNotExistsError) as error:
                client._adapter.read(name)
            assert str(error.value) == f"{name} collection doesn't exist"
        finally:
            holder.release()

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
        lock = ArrayCreationLock(
            inserted_varray._adapter, inserted_varray, collection, timeout=Sentinel, token=DefaultToken()
        )
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
        lock = ArrayCreationLock(
            inserted_varray._adapter, inserted_varray, collection, timeout=Sentinel, token=DefaultToken()
        )
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
            lock = ArrayCreationLock(
                inserted_varray._adapter, inserted_varray, collection, timeout=Sentinel, token=DefaultToken()
            )
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
        lock = ArrayCreationLock(adapter, inserted_varray, collection, timeout=Sentinel, token=DefaultToken())
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

    def test_lock_of_a_missing_directory_is_refused_as_not_existing(
        self, inserted_varray: VArray, main_dir: Path, tmp_path: Path
    ):
        """The directory of a deleted VArray is gone: the VArray does not exist, and the thread lock is freed."""
        collection = inserted_varray._VArray__collection
        main_dir.rename(tmp_path / main_dir.name)
        lock = ArrayCreationLock(
            inserted_varray._adapter, inserted_varray, collection, timeout=Sentinel, token=DefaultToken()
        )
        with pytest.raises(DekerInstanceNotExistsError) as error:
            with lock:
                pass
        assert str(error.value) == f"{main_dir} doesn't exist"
        assert lock.flock is None
        assert lock.thread_lock is None
        assert not ArrayCreationLock._thread_locks[main_dir].locked()


class LockProbe:
    """Stands in for a method of the adapter and notes whether the path was locked when it was called."""

    def __init__(self, path: Path, original):
        self.path = path
        self.original = original
        self.held: list[bool] = []

    def __call__(self, array: VArray) -> Path:
        flock = Flock(self.path)
        try:
            flock.acquire()
        except DekerLockError:
            self.held.append(True)
        else:
            self.held.append(False)
            flock.release()
        return self.original(array)


class TestUpdateMetaAttributeLock:
    """The exclusive flock on the json file of a VArray while its custom attributes are updated.

    The update replaces the file; the lock holds the file that lies at the path once the flock is taken, so the
    next writer works on the file the holder has written. The directory is left to the creation of the Arrays.
    """

    @pytest.fixture
    def lock(self, inserted_varray: VArray) -> UpdateMetaAttributeLock:
        lock = UpdateMetaAttributeLock()
        lock.instance = inserted_varray._adapter
        lock.args = (inserted_varray._adapter, inserted_varray)
        lock.kwargs = {}
        return lock

    def test_lock_path_is_the_main_file(self, lock: UpdateMetaAttributeLock, inserted_varray: VArray):
        adapter = inserted_varray._adapter
        main_dir = get_main_path(inserted_varray.id, adapter.collection_path / adapter.data_dir)
        assert lock.get_path() == main_dir / (inserted_varray.id + adapter.file_ext)

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
                lock.check_existing_lock(func_args=(), func_kwargs=dict(NOT_GIVEN))
            assert lock.lock is None
        finally:
            holder.release()
        lock.check_existing_lock(func_args=(), func_kwargs=dict(NOT_GIVEN))
        assert lock.lock is not None
        lock.release()

    def test_update_of_the_attributes_holds_the_file(self, inserted_varray_with_attributes: VArray, mocker):
        """The adapter method runs under the lock: the json file is held while it works."""
        varray = inserted_varray_with_attributes
        adapter = varray._adapter
        probe = LockProbe(adapter._get_main_path_to_file(varray), adapter._get_main_path_to_file)
        mocker.patch.object(adapter, "_get_main_path_to_file", side_effect=probe)
        varray.update_custom_attributes({"custom_attribute": 0.9})
        assert probe.held == [True]

    def test_update_of_the_attributes_does_not_wait_for_the_creation_of_arrays(
        self, inserted_varray_with_attributes: VArray, mocker: MockerFixture
    ):
        """The creation of an Array holds the directory of the VArray; the update of its attributes goes on."""
        varray = inserted_varray_with_attributes
        mocker.patch.object(varray._adapter.ctx.config, "write_lock_timeout", 1)
        creation = ArrayCreationLock(
            varray._adapter, varray, varray._VArray__collection, timeout=Sentinel, token=DefaultToken()
        )
        start = time.monotonic()
        with creation:
            varray.update_custom_attributes({"custom_attribute": 0.9})
        assert time.monotonic() - start < 1
        assert varray.custom_attributes["custom_attribute"] == 0.9
        assert varray._adapter.read_meta(varray)["custom_attributes"]["custom_attribute"] == 0.9

    @pytest.mark.timeout(120)
    def test_updaters_of_two_processes_lose_no_update(
        self, inserted_varray_with_attributes: VArray, root_path: Path, mp_context: BaseContext
    ):
        """Every update replaces the file: each updater works on the file the previous one has written."""
        varray = inserted_varray_with_attributes
        start = varray.custom_attributes["custom_attribute"]
        times = 2000
        args = (embedded_uri(root_path), varray._VArray__collection.name, varray.id, times)
        processes = [mp_context.Process(target=add_under_the_update_lock, args=args) for _ in range(2)]
        for process in processes:
            process.start()
        for process in processes:
            process.join(100)
        assert [process.exitcode for process in processes] == [0, 0]
        assert varray._adapter.read_meta(varray)["custom_attributes"]["custom_attribute"] == start + 2 * times

    def test_writer_of_a_missing_file_is_refused_as_not_existing(self, lock: UpdateMetaAttributeLock, tmp_path: Path):
        """The json file of a deleted VArray is gone: the VArray does not exist, without waiting."""
        path = lock.get_path()
        path.rename(tmp_path / path.name)
        with pytest.raises(DekerInstanceNotExistsError) as error:
            lock.check_existing_lock(func_args=(), func_kwargs=dict(NOT_GIVEN))
        assert str(error.value) == f"{path} doesn't exist"
        assert lock.lock is None


if __name__ == "__main__":
    pytest.main()


class TestWaitingLocksTakeTheTimeoutAndTheTokenOfTheCall:
    """The waiting locks take the limit of the wait and the token from the decorated call."""

    @pytest.fixture
    def held_file(self, write_array_lock: WriteArrayLock, inserted_array: Array):
        write_array_lock.kwargs = {"array": inserted_array}
        holder = Flock(write_array_lock.get_path(), shared=True)
        holder.acquire()
        yield
        holder.release()

    @pytest.mark.timeout(10)
    def test_timeout_of_the_call_overrides_the_client(
        self, write_array_lock: WriteArrayLock, inserted_array: Array, held_file, mocker: MockerFixture
    ):
        mocker.patch.object(write_array_lock.instance.ctx.config, "write_lock_timeout", 60)
        start = time.monotonic()
        with pytest.raises(DekerLockError):
            write_array_lock.check_existing_lock(
                func_args=[], func_kwargs={"array": inserted_array, "timeout": 1, "token": DefaultToken()}
            )
        assert time.monotonic() - start < 2.5
        assert write_array_lock.lock is None

    @pytest.mark.timeout(10)
    def test_cancelled_token_stops_the_wait(self, write_array_lock: WriteArrayLock, inserted_array: Array, held_file):
        token = SimpleToken()
        canceller = threading.Timer(0.3, token.cancel)
        canceller.start()
        start = time.monotonic()
        with pytest.raises(TokenCancelledError):
            write_array_lock.check_existing_lock(
                func_args=[], func_kwargs={"array": inserted_array, "timeout": 30, "token": token}
            )
        canceller.join()
        assert time.monotonic() - start < 3
        assert write_array_lock.lock is None

    @pytest.mark.timeout(10)
    def test_creation_lock_cancelled_while_it_waits_frees_the_thread_lock(self, inserted_varray: VArray):
        adapter = inserted_varray._adapter
        main_dir = get_main_path(inserted_varray.id, adapter.collection_path / adapter.data_dir)
        holder = Flock(main_dir)
        holder.acquire()
        token = SimpleToken()
        token.cancel()
        try:
            lock = ArrayCreationLock(
                adapter, inserted_varray, inserted_varray._VArray__collection, timeout=30, token=token
            )
            with pytest.raises(TokenCancelledError):
                with lock:
                    pass
            assert lock.thread_lock is None
            assert not ArrayCreationLock._thread_locks[main_dir].locked()
        finally:
            holder.release()

    @pytest.mark.timeout(10)
    def test_creation_lock_takes_the_timeout_of_the_call(self, inserted_varray: VArray):
        adapter = inserted_varray._adapter
        main_dir = get_main_path(inserted_varray.id, adapter.collection_path / adapter.data_dir)
        holder = Flock(main_dir)
        holder.acquire()
        try:
            lock = ArrayCreationLock(
                adapter, inserted_varray, inserted_varray._VArray__collection, timeout=1, token=DefaultToken()
            )
            assert lock.timeout == 1
            start = time.monotonic()
            with pytest.raises(DekerLockError):
                with lock:
                    pass
            assert time.monotonic() - start < 2.5
        finally:
            holder.release()


MISSING = "Storage directory {} doesn't exist"


class TestLocksTellAMissingStorageFromAMissingFile:
    """A lock that does not find its file tells a missing collections directory of the storage (critical) from a
    missing file (``DekerInstanceNotExistsError`` with the text as before). Every test works on a storage of its own.
    """

    @pytest.fixture
    def own_client(self, tmp_path: Path):
        """Client of a storage of the test's own.

        :param tmp_path: directory of the test
        """
        with Client(embedded_uri(tmp_path / "storage")) as client:
            yield client

    @staticmethod
    def check(call, collections: Path, storage_gone: bool, message: str, log: pytest.LogCaptureFixture) -> None:
        """Call the lock and check what it tells.

        :param call: the call of the lock
        :param collections: the collections directory of the storage
        :param storage_gone: the directory is taken away
        :param message: the text of ``DekerInstanceNotExistsError`` for a missing file
        :param log: capture of the Deker loggers
        """
        if storage_gone:
            with pytest.raises(DekerCriticalError) as error:
                call()
            assert str(error.value) == MISSING.format(collections)
            critical = [record.getMessage() for record in log.records if record.levelname == "CRITICAL"]
            assert critical == [MISSING.format(collections)]
            assert not collections.exists()
        else:
            with pytest.raises(DekerInstanceNotExistsError) as not_existing:
                call()
            assert str(not_existing.value) == message
            assert [record for record in log.records if record.levelname == "CRITICAL"] == []

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="file gone")]
    )
    def test_main_file_not_found_by_the_flock(
        self, own_client: Client, array_schema: ArraySchema, deker_log: pytest.LogCaptureFixture, storage_gone: bool
    ):
        array = own_client.create_collection("own", array_schema).create()
        adapter = array._adapter
        collections = adapter.collection_path.parent
        path = adapter._get_main_path_to_file(array)
        os.remove(path)
        if storage_gone:
            collections.rename(collections.with_name("collections-away"))
        self.check(lambda: adapter.read_meta(array), collections, storage_gone, f"{path} doesn't exist", deker_log)

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="file gone")]
    )
    def test_main_file_gone_once_the_flock_is_taken(
        self,
        own_client: Client,
        array_schema: ArraySchema,
        deker_log: pytest.LogCaptureFixture,
        mocker: MockerFixture,
        storage_gone: bool,
    ):
        array = own_client.create_collection("own", array_schema).create()
        adapter = array._adapter
        collections = adapter.collection_path.parent
        path = adapter._get_main_path_to_file(array)
        stat = os.stat

        def gone_at_the_path(target, *args, **kwargs):
            if str(target) == str(path):
                if storage_gone:
                    collections.rename(collections.with_name("collections-away"))
                raise FileNotFoundError(2, "No such file or directory")
            return stat(target, *args, **kwargs)

        mocker.patch.object(os, "stat", side_effect=gone_at_the_path)
        self.check(lambda: adapter.read_meta(array), collections, storage_gone, f"{path} doesn't exist", deker_log)

    @pytest.mark.parametrize(
        "storage_gone", [pytest.param(True, id="collections gone"), pytest.param(False, id="file gone")]
    )
    def test_directory_of_the_varray_not_found_by_the_creation_lock(
        self, own_client: Client, varray_schema, deker_log: pytest.LogCaptureFixture, storage_gone: bool
    ):
        collection = own_client.create_collection("own", varray_schema)
        varray = collection.create()
        adapter = varray._adapter
        collections = adapter.collection_path.parent
        directory = get_main_path(varray.id, adapter.collection_path / adapter.data_dir)
        os.remove(directory / (varray.id + adapter.file_ext))
        directory.rmdir()
        if storage_gone:
            collections.rename(collections.with_name("collections-away"))
        lock = ArrayCreationLock(adapter, varray, collection, timeout=Sentinel, token=DefaultToken())

        def enter() -> None:
            with lock:
                pass

        self.check(enter, collections, storage_gone, f"{directory} doesn't exist", deker_log)

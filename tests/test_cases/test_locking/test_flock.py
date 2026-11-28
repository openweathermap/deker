import fcntl
import multiprocessing
import os
import shutil
import stat
import time

from concurrent.futures import ProcessPoolExecutor
from multiprocessing.context import BaseContext
from pathlib import Path

import h5py
import pytest

from deker.errors import DekerLockError
from deker.locks import Flock


@pytest.fixture
def hdf_path(root_path: Path):
    """Path to temporary hdf5 file for Flock tests.

    :param root_path: temporary root path
    """
    path = root_path / "flocked.hdf5"
    with h5py.File(path, "w"):
        pass
    yield path
    os.remove(path)


class TestFlock:
    def test_flock_init(self, hdf_path):
        """Tests Flock __init__.

        :param hdf_path: Path to temporary hdf5 file
        """
        flock = Flock(hdf_path)
        assert flock
        assert flock.file == hdf_path
        assert flock.fd is None

    def test_flock_wrong_file_descriptor(self, hdf_path):
        """Tests fcntl.flock does not work with h5py.File descriptor.

        :param hdf_path: Path to temporary hdf5 file
        """
        with pytest.raises(TypeError):
            with h5py.File(hdf_path, "r") as fd:
                assert not fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_flock_acquire(self, hdf_path):
        """Tests Flock acquire lock.

        :param hdf_path: Path to temporary hdf5 file
        """
        flock = Flock(hdf_path)
        try:
            flock.acquire()
            assert flock.file == hdf_path
            assert flock.fd is not None
            with pytest.raises(BlockingIOError):
                with open(hdf_path) as fd:
                    assert not fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            flock.release()

    def test_flock_release(self, hdf_path):
        """Tests Flock release lock.

        :param hdf_path: Path to temporary hdf5 file
        """
        flock = Flock(hdf_path)
        flock.acquire()
        assert flock.file == hdf_path
        assert flock.fd is not None
        with pytest.raises(BlockingIOError):
            with open(hdf_path) as fd:
                assert not fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        flock.release()
        with open(hdf_path) as fd:
            assert not fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_flock_context_manager(self, hdf_path):
        """Tests Flock __enter__ and __exit__.

        :param hdf_path: Path to temporary hdf5 file
        """
        with Flock(hdf_path) as flock:
            assert flock.file == hdf_path
            assert flock.fd is not None
            with pytest.raises(DekerLockError):
                Flock(hdf_path).acquire()
        flock1 = Flock(hdf_path)
        try:
            flock1.acquire()
        finally:
            flock1.release()

    def test_flock_acquire_missing_file_raises(self, tmp_path: Path):
        """Tests Flock.acquire does not pretend to hold a lock on a file that does not exist.

        :param tmp_path: temporary directory
        """
        flock = Flock(tmp_path / "missing.hdf5")
        with pytest.raises(FileNotFoundError):
            flock.acquire()
        assert flock.fd is None

    @pytest.mark.parametrize("name", ["collection.json", "collection.lock"])
    def test_flock_creates_a_missing_file_only_when_asked(self, tmp_path: Path, name: str):
        """Tests Flock.acquire creates a missing file with ``create``, empty and with the mode ``open`` gives a new
        file; without it a missing file is an error, whatever its name.

        :param tmp_path: temporary directory
        :param name: name of the file
        """
        path = tmp_path / name
        with pytest.raises(FileNotFoundError):
            Flock(path).acquire()
        assert not path.exists()
        flock = Flock(path, create=True)
        try:
            flock.acquire()
            assert path.read_text() == ""
            assert flock.fd is not None
            umask = os.umask(0)
            os.umask(umask)
            assert stat.S_IMODE(path.stat().st_mode) == 0o666 & ~umask
        finally:
            flock.release()

    def test_flock_never_creates_the_directory_of_a_file(self, tmp_path: Path):
        """The directory of the file is never created: a file in a missing one is not made.

        :param tmp_path: temporary directory
        """
        path = tmp_path / "collections" / "collection.json"
        flock = Flock(path, create=True)
        with pytest.raises(FileNotFoundError):
            flock.acquire()
        assert flock.fd is None
        assert not path.parent.exists()

    def test_flock_locks_a_directory(self, tmp_path: Path):
        """Tests a directory is locked like a file: the second holder is refused while the first one holds it.

        :param tmp_path: temporary directory
        """
        directory = tmp_path / "locked"
        directory.mkdir()
        with Flock(directory) as flock:
            assert flock.fd is not None
            with pytest.raises(DekerLockError):
                Flock(directory).acquire()
            fd = os.open(directory, os.O_RDONLY)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(fd)
        assert flock.fd is None

    def test_flock_releases_a_directory(self, tmp_path: Path):
        """Tests a released directory can be taken again.

        :param tmp_path: temporary directory
        """
        directory = tmp_path / "locked"
        directory.mkdir()
        flock = Flock(directory)
        flock.acquire()
        flock.release()
        with Flock(directory):
            pass

    def test_flock_descriptor_of_the_held_file(self, hdf_path):
        """The descriptor of the file the lock holds."""
        flock = Flock(hdf_path)
        flock.acquire()
        try:
            assert flock.descriptor == flock.fd
            assert os.path.samestat(os.fstat(flock.descriptor), os.stat(hdf_path))
        finally:
            flock.release()

    @pytest.mark.parametrize("taken", [False, True], ids=["never-taken", "released"])
    def test_flock_descriptor_of_a_lock_not_held_is_refused(self, hdf_path, taken: bool):
        """A lock that holds nothing has no descriptor to give."""
        flock = Flock(hdf_path)
        if taken:
            flock.acquire()
            flock.release()
        with pytest.raises(DekerLockError) as error:
            assert flock.descriptor is None  # never reached: the property raises
        assert str(error.value) == f"{hdf_path} is not locked"
        assert flock.fd is None

    def test_flock_release_closes_the_descriptor(self, hdf_path):
        """Tests the descriptor is closed on release, so nothing keeps the file open.

        :param hdf_path: Path to temporary hdf5 file
        """
        flock = Flock(hdf_path)
        flock.acquire()
        fd = flock.fd
        flock.release()
        assert flock.fd is None
        with pytest.raises(OSError):
            os.fstat(fd)

    def test_multiple_sync_processes_flock(self, mp_context: BaseContext):
        """Tests multiple processes cannot acquire lock on the same file."""
        cores = multiprocessing.cpu_count() // 2
        processes = cores if cores > 2 else 2
        with ProcessPoolExecutor(max_workers=processes) as ex:
            p = mp_context.Process(
                target=os.system,
                args=(f"python {os.path.dirname(__file__)}/long_mpt.py -c sync",),
            )
            p.start()
            time.sleep(1)

            try:
                command = f"python {os.path.dirname(__file__)}/mpt.py -c sync"
                results = ex.map(os.system, [command] * (processes - 1))
                results = list(results)
                print(results)
                assert results.count(256) == (processes - 1), results
            finally:
                try:
                    p.terminate()
                    p.join()
                    p.close()
                except Exception:
                    p.kill()
                path = Path("/tmp/mp_test")
                if path.exists():
                    shutil.rmtree(path)


if __name__ == "__main__":
    pytest.main()

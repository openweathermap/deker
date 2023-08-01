import fcntl
import multiprocessing
import os
import shutil
import time

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import h5py
import pytest

from deker.errors import DekerLockError
from deker.locks import Flock


@pytest.fixture()
def hdf_path(root_path: Path):
    """Path to temporary hdf5 file for Flock tests.

    :param root_path: temporary root path
    """
    path = root_path / "flocked.hdf5"
    with h5py.File(path, "w") as f:
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
                with open(hdf_path, "r") as fd:
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
            with open(hdf_path, "r") as fd:
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

    def test_multiple_sync_processes_flock(self):
        """Tests multiple processes cannot acquire lock on the same file."""
        cores = multiprocessing.cpu_count() // 2
        processes = cores if cores > 2 else 2
        with ProcessPoolExecutor(max_workers=processes) as ex:
            p = multiprocessing.Process(
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

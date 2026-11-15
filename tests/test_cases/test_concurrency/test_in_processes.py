"""
This module contains tests for multiprocessing and locking functionality.

The tests in this file cover scenarios where multiple processes are attempting to perform operations
on Array, VArray and Collection objects concurrently, and it verifies that the locking mechanisms work
correctly.
"""

import os
import time
import traceback

from collections.abc import Callable
from multiprocessing import Event, Queue, cpu_count
from multiprocessing.context import BaseContext
from multiprocessing.queues import Queue as QueueType
from multiprocessing.synchronize import Event as EventType
from pathlib import Path
from time import sleep
from typing import Literal
from unittest.mock import patch

import numpy as np
import pytest

from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from deker_tools.slices import create_shape_from_slice, slice_converter

from tests.parameters.uri import embedded_uri

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerLockError
from deker.locks import (
    CollectionLock,
    Flock,
    ReadArrayLock,
    UpdateMetaAttributeLock,
    WriteArrayLock,
    WriteVarrayLock,
)
from deker.schemas import ArraySchema, DimensionSchema, VArraySchema
from deker.types import LocksExtensions, Numeric

UPDATED_CUSTOM_ATTR = 20
cores = cpu_count() // 2
WORKERS = cores if cores > 2 else 2


def wait_unlock(func: Callable, lock_set: Event, funcs_finished: Event, wait: bool = True):
    """Method to patch Flock releasing.
    Sets lock_set event flag, returns results only after funcs_finished if wait param is True.

    :param func: function to wrap - Flock release
    :param lock_set: multiprocessing event indicating that the lock should be set
    :param funcs_finished: multiprocessing event indicating that all functions are finished
    :param wait: whether to wait for the function to complete before releasing the lock
    """

    def wrapper(*args, **kwargs):
        lock_set.set()
        if wait:
            funcs_finished.wait()
        result = func(*args, **kwargs)

        return result

    return wrapper


def call_array_method(
    collection_name: str,
    uri: str,
    id_: str,
    method: Literal["update", "clear", "update_meta_custom_attributes", "create"],
    lock_set: Event,
    funcs_finished: Event,
    wait: bool = True,
    is_virtual: bool = False,
    primary_attributes: dict = None,
    custom_attributes: dict = None,
    subset_slice: str = "[:]",
    fill_value: Numeric = 0,
):
    """Call a method on an Array object.

    :param collection_name: The name of the Collection containing the Array
    :param uri: Client Uri
    :param id_: Array id
    :param method: the method to call on the Array
    :param lock_set: multiprocessing event to set the lock
    :param funcs_finished: multiprocessing event indicating that all functions are finished
    :param wait: whether to wait for the function to complete before releasing the lock
    :param is_virtual: VArray flag
    :param primary_attributes: primary attributes dict
    :param custom_attributes: custom attributes dict
    :param subset_slice: slice string from slice_converter to make a subset from array
    :param fill_value: value to fill the subset
    """
    client = Client(uri, write_lock_timeout=1, loglevel="ERROR")
    collection = client.get_collection(collection_name)
    subset_slice = slice_converter[subset_slice]

    # Get Array object
    try:
        array = collection.filter({"id": id_}).last()
    except DekerLockError:
        return DekerLockError

    if is_virtual:
        lock = WriteVarrayLock
        schema = collection.varray_schema
    else:
        lock = WriteArrayLock
        schema = collection.array_schema
    try:
        if method == "update":
            # We patch method to wait for event
            with patch.object(lock, "release", wait_unlock(lock.release, lock_set, funcs_finished, wait)):
                data = np.ndarray(
                    shape=create_shape_from_slice(schema.shape, subset_slice),
                    dtype=schema.dtype,
                )
                try:
                    if data.shape == ():
                        data = fill_value
                    else:
                        data.fill(fill_value)
                except Exception:
                    raise
                array[subset_slice].update(data)

        elif method == "clear":
            with patch.object(lock, "release", wait_unlock(lock.release, lock_set, funcs_finished, wait)):
                array[subset_slice].clear()

        # Read used in varray only.
        elif method == "read":
            # patch release function to not release read lock until we're done
            with patch.object(
                ReadArrayLock,
                "release",
                wait_unlock(ReadArrayLock.release, lock_set, funcs_finished, wait),
            ):
                # patch timeout time
                array[subset_slice].read()

        elif method == "update_meta_custom_attributes":
            if wait:
                with patch.object(
                    UpdateMetaAttributeLock,
                    "release",
                    wait_unlock(UpdateMetaAttributeLock.release, lock_set, funcs_finished, wait),
                ):
                    array.update_custom_attributes(array.custom_attributes)
            else:
                array.update_custom_attributes(array.custom_attributes)
    except DekerLockError:
        return DekerLockError


def call_collection_method(path, name: str, method: str, lock_set: Event, func_finished: Event, wait: bool = False):
    """Call a method on a Collection object.

    :param path: Client Uri
    :param name: Collection name
    :param method: the method to call on the Array
    :param lock_set: multiprocessing event to set the lock
    :param func_finished: multiprocessing event indicating that function is finished
    :param wait: whether to wait for the function to complete before releasing the lock
    """
    client = Client(path, write_lock_timeout=1, loglevel="ERROR")
    schema = ArraySchema(dtype=int, dimensions=[DimensionSchema(name="x", size=2)])
    if method == "create":
        with patch.object(
            CollectionLock,
            "release",
            wait_unlock(CollectionLock.release, lock_set, func_finished, wait),
        ):
            try:
                client.create_collection(name, schema, None)
            except DekerLockError:
                return DekerLockError


def read_array(array_id: str, path, collection_name: str):
    """Read array method. We need it because of multiprocessing serialization."""
    client = Client(path, write_lock_timeout=1, loglevel="ERROR")
    collection = client.get_collection(collection_name)
    try:
        collection.filter({"id": array_id}).last()[:].read()
    except BaseException:
        traceback.print_exc()
        return Exception


class StorageHooks:
    """Settings of the storage adapter hooks of a child process; every process has its own copy of the class."""

    started: EventType | None = None
    seconds: float = 0.0
    queue: QueueType | None = None


_ORIGINAL_READ_DATA = HDF5StorageAdapter.read_data
_ORIGINAL_UPDATE_DATA = HDF5StorageAdapter.update_data


def slow_read_data(adapter: HDF5StorageAdapter, *args, **kwargs):
    """Read the data as the storage adapter does, holding the read lock for a while first.

    :param adapter: storage adapter
    :param args: arguments of read_data
    :param kwargs: keyword arguments of read_data
    :returns: the data
    """
    StorageHooks.started.set()
    sleep(StorageHooks.seconds)
    return _ORIGINAL_READ_DATA(adapter, *args, **kwargs)


def reporting_update_data(adapter: HDF5StorageAdapter, *args, **kwargs) -> None:
    """Write the data as the storage adapter does, reporting the moment the write started.

    :param adapter: storage adapter
    :param args: arguments of update_data
    :param kwargs: keyword arguments of update_data
    """
    StorageHooks.queue.put(time.monotonic())
    _ORIGINAL_UPDATE_DATA(adapter, *args, **kwargs)


_ORIGINAL_CREATE = HDF5StorageAdapter.create


def slow_create(adapter: HDF5StorageAdapter, *args, **kwargs) -> None:
    """Create the file as the storage adapter does, after a pause that widens the window between the check
    for the tile and its creation, so that concurrent writers collide for sure.

    :param adapter: storage adapter
    :param args: arguments of create
    :param kwargs: keyword arguments of create
    """
    sleep(StorageHooks.seconds)
    _ORIGINAL_CREATE(adapter, *args, **kwargs)


def read_array_slowly(uri: str, collection_name: str, array_id: str, started: Event, done: Event, seconds: float):
    """Read an Array holding the read lock for a while, as a long read does.

    :param uri: client uri
    :param collection_name: collection of the array
    :param array_id: id of the array
    :param started: event set once the read lock is held
    :param done: event set once the read lock is released
    :param seconds: how long the storage read takes
    """
    StorageHooks.started, StorageHooks.seconds = started, seconds
    with Client(uri, loglevel="ERROR") as client, patch.object(HDF5StorageAdapter, "read_data", slow_read_data):
        collection = client.get_collection(collection_name)
        collection.filter({"id": array_id}).last()[:].read()
    done.set()


def update_array_and_report(uri: str, collection_name: str, array_id: str, queue: Queue, timeout: int) -> None:
    """Update an Array and report when the storage write started, or the error.

    :param uri: client uri
    :param collection_name: collection of the array
    :param array_id: id of the array
    :param queue: queue for the moment the write started, or the exception
    :param timeout: write lock timeout of the client
    """
    StorageHooks.queue = queue
    try:
        with (
            Client(uri, write_lock_timeout=timeout, write_lock_check_interval=1, loglevel="ERROR") as client,
            patch.object(HDF5StorageAdapter, "update_data", reporting_update_data),
        ):
            collection = client.get_collection(collection_name)
            array = collection.filter({"id": array_id}).last()
            array[:].update(np.ones(array.shape, dtype=array.dtype))
    except Exception as e:
        queue.put(e)


def update_tile_region(
    uri: str, collection_name: str, varray_id: str, index_exp: str, value: float, go: Event, queue: Queue
):
    """Write one region of a VArray from another process, all processes starting together.

    :param uri: client uri
    :param collection_name: collection of the varray
    :param varray_id: id of the varray
    :param index_exp: region to write, as a string
    :param value: value to fill the region with
    :param go: start signal shared by the writers
    :param queue: queue for the result: ``None`` or the exception
    """
    StorageHooks.seconds = 0.5
    try:
        with (
            Client(uri, write_lock_timeout=30, write_lock_check_interval=1, loglevel="ERROR") as client,
            patch.object(HDF5StorageAdapter, "create", slow_create),
        ):
            collection = client.get_collection(collection_name)
            varray = collection.filter({"id": varray_id}).last()
            go.wait(30)
            varray[slice_converter[index_exp]].update(value)
        queue.put(None)
    except Exception as e:
        queue.put(e)


class TestReadersAndWriters:
    """The read lock lives in the kernel: a writer waits for a live reader and never for a dead one."""

    def test_writer_waits_for_a_reader_of_another_process(
        self, inserted_array: Array, array_collection: Collection, root_path: Path, mp_context: BaseContext
    ):
        """Tests a write starts only after the read of another process has ended."""
        manager = mp_context.Manager()
        started, done = manager.Event(), manager.Event()
        queue = manager.Queue()
        uri = str(embedded_uri(root_path))
        reader = mp_context.Process(
            target=read_array_slowly, args=(uri, array_collection.name, inserted_array.id, started, done, 1.5)
        )
        reader.start()
        try:
            assert started.wait(30)
            writer = mp_context.Process(
                target=update_array_and_report, args=(uri, array_collection.name, inserted_array.id, queue, 10)
            )
            writer.start()
            try:
                result = queue.get(timeout=30)
                assert not isinstance(result, Exception), result
                assert done.is_set(), "the write started while the reader was still reading"
            finally:
                writer.join(30)
        finally:
            reader.join(30)

    def test_writer_times_out_on_a_long_reader(
        self, inserted_array: Array, array_collection: Collection, root_path: Path, mp_context: BaseContext
    ):
        """Tests a write gives up with DekerLockError when the reader holds the file longer than the timeout."""
        manager = mp_context.Manager()
        started, done = manager.Event(), manager.Event()
        queue = manager.Queue()
        uri = str(embedded_uri(root_path))
        reader = mp_context.Process(
            target=read_array_slowly, args=(uri, array_collection.name, inserted_array.id, started, done, 4)
        )
        reader.start()
        try:
            assert started.wait(30)
            writer = mp_context.Process(
                target=update_array_and_report, args=(uri, array_collection.name, inserted_array.id, queue, 1)
            )
            writer.start()
            try:
                result = queue.get(timeout=30)
                assert isinstance(result, DekerLockError), result
            finally:
                writer.join(30)
        finally:
            reader.join(30)

    def test_writer_does_not_wait_for_a_dead_reader(
        self, inserted_array: Array, array_collection: Collection, root_path: Path, mp_context: BaseContext
    ):
        """Tests a reader killed in the middle of a read leaves no lock behind: the next write goes at once."""
        manager = mp_context.Manager()
        started, done = manager.Event(), manager.Event()
        queue = manager.Queue()
        uri = str(embedded_uri(root_path))
        reader = mp_context.Process(
            target=read_array_slowly, args=(uri, array_collection.name, inserted_array.id, started, done, 3600)
        )
        reader.start()
        assert started.wait(30)
        reader.kill()
        reader.join(30)

        before = time.monotonic()
        writer = mp_context.Process(
            target=update_array_and_report, args=(uri, array_collection.name, inserted_array.id, queue, 5)
        )
        writer.start()
        try:
            result = queue.get(timeout=30)
            assert not isinstance(result, Exception), result
            assert result - before < 4, "the writer waited for a reader that is dead"
        finally:
            writer.join(30)
        assert not list(inserted_array._Array__adapter._get_main_path_to_file(inserted_array).parent.glob("*lock"))


class TestTileCreation:
    """Two writers of a fresh VArray create its tile once; the data of both land in the one tile."""

    @pytest.mark.parametrize("writers", [2, 4])
    def test_concurrent_writers_create_one_tile(
        self, client: Client, root_path: Path, mp_context: BaseContext, writers: int
    ):
        schema = VArraySchema(
            dtype=float,
            dimensions=[DimensionSchema(name="x", size=writers * 4), DimensionSchema(name="y", size=4)],
            vgrid=(1, 1),  # one tile: every writer creates the same inner Array
        )
        collection = client.create_collection(f"tile_creation_{writers}", schema)
        try:
            varray = collection.create()
            manager = mp_context.Manager()
            go = manager.Event()
            queue = manager.Queue()
            uri = str(embedded_uri(root_path))
            processes = [
                mp_context.Process(
                    target=update_tile_region,
                    args=(uri, collection.name, varray.id, f"[{n * 4}:{n * 4 + 4}]", float(n + 1), go, queue),
                )
                for n in range(writers)
            ]
            for process in processes:
                process.start()
            go.set()
            try:
                results = [queue.get(timeout=60) for _ in processes]
            finally:
                for process in processes:
                    process.join(60)
            assert all(result is None for result in results), results

            tiles = [tile for tile in collection.arrays if tile._vid == varray.id]
            assert len(tiles) == 1
            expected = np.repeat(np.arange(1, writers + 1, dtype=float), 4)[:, np.newaxis] * np.ones((1, 4))
            assert np.array_equal(varray[:].read(), expected)
        finally:
            collection.delete()


class TestLocks:
    """Test if methods locks work correctly with multiple processes."""

    def test_array_write_lock(
        self, array_schema: ArraySchema, root_path, inserted_array: Array, mp_context: BaseContext
    ):
        """Test array lock."""
        manager = mp_context.Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()

        # Call method that locks array
        proc = mp_context.Process(
            target=call_array_method,
            args=(
                inserted_array.collection,
                str(embedded_uri(root_path)),
                inserted_array.id,
                "update",
                lock_set,
                func_finished,
            ),
        )
        proc.start()
        lock_set.wait()

        # Try to write to array:
        try:
            methods = ["update", "clear"]
            with mp_context.Pool(WORKERS) as pool:
                result = pool.starmap(
                    call_array_method,
                    [
                        (
                            inserted_array.collection,
                            str(embedded_uri(root_path)),
                            inserted_array.id,
                            method,
                            lock_set,
                            func_finished,
                            False,
                        )
                        for method in methods
                    ],
                )
                assert result.count(DekerLockError) == len(methods)
            func_finished.set()

        finally:
            proc.kill()

    def test_array_parallel_read(
        self, inserted_array: Array, array_collection: Collection, root_path: Path, mp_context: BaseContext
    ):
        """Test if we can read array in parallel."""
        args = (inserted_array.id, str(embedded_uri(root_path)), array_collection.name)
        with mp_context.Pool(WORKERS) as pool:
            result = pool.starmap(read_array, (args for _ in range(4)))

        assert result.count(Exception) == 0

    def test_varray_lock_wait_till_read_timeout(
        self, varray_schema: VArraySchema, root_path, inserted_varray_with_attributes: VArray, mp_context: BaseContext
    ):
        """Test varray read lock timeout."""
        manager = mp_context.Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()

        # Call read process to lock arrays for reading
        proc = mp_context.Process(
            target=call_array_method,
            args=(
                inserted_varray_with_attributes.collection,
                str(embedded_uri(root_path)),
                inserted_varray_with_attributes.id,
                "read",
                lock_set,
                func_finished,
                True,
                True,
            ),
        )
        proc.start()

        lock_set.wait()
        try:
            methods = ["update", "clear"]
            with mp_context.Pool(WORKERS) as pool:
                result = pool.starmap(
                    call_array_method,
                    [
                        (
                            inserted_varray_with_attributes.collection,
                            str(embedded_uri(root_path)),
                            inserted_varray_with_attributes.id,
                            method,
                            lock_set,
                            func_finished,
                            False,
                            True,
                        )
                        for method in methods
                    ],
                )

                assert result.count(DekerLockError) == len(methods)
            func_finished.set()
        finally:
            proc.kill()

    def test_varray_locks_inner_arrays(
        self, inserted_varray: VArray, root_path, varray_collection: Collection, mp_context: BaseContext
    ):
        """Test that as we lock varray, inner arrays also locked."""
        manager = mp_context.Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()

        # Call read process to lock arrays for reading
        proc = mp_context.Process(
            target=call_array_method,
            args=(
                inserted_varray.collection,
                str(embedded_uri(root_path)),
                inserted_varray.id,
                "update",
                lock_set,
                func_finished,
                True,
                True,
            ),
        )
        proc.start()
        lock_set.wait()
        with pytest.raises(DekerLockError):
            for _ in varray_collection.arrays:
                pass
        func_finished.set()

    def test_non_intersecting_vsubsets_update_OK(
        self, inserted_varray: VArray, root_path, varray_collection: Collection, mp_context: BaseContext
    ):
        """Test varray allows writing into non-intersecting subsets."""
        manager = mp_context.Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()
        value = -9999.9
        slices = (np.index_exp[:2, :2, :2], np.index_exp[8:, 8:, 8:])
        with mp_context.Pool(WORKERS) as pool:
            pool.starmap(
                call_array_method,
                [
                    (
                        inserted_varray.collection,
                        str(embedded_uri(root_path)),
                        inserted_varray.id,
                        "update",
                        lock_set,
                        func_finished,
                        False,
                        True,
                        None,
                        None,
                        slice_converter[subset_slice],
                        value,
                    )
                    for subset_slice in slices
                ],
            )

        try:
            for slice_ in slices:
                subset = inserted_varray[slice_]
                data = np.ndarray(shape=subset.shape, dtype=subset.dtype)
                data.fill(value)
                result = subset.read()
                assert np.all(result == data)
            assert np.all(inserted_varray[2:8].read() != value)
        except Exception:
            raise
        # finally:
        #     print(inserted_varray[:].read(), flush=True)

    def test_intersecting_vsubsets_update_fail(
        self, inserted_varray: VArray, root_path, varray_collection: Collection, mp_context: BaseContext
    ):
        """Test VArray not allows writing into intersecting subsets."""
        inserted_varray[:].update(np.ones(shape=inserted_varray.shape, dtype=inserted_varray.dtype))
        check_data = inserted_varray[:].read()
        manager = mp_context.Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()
        blocking_slice = np.index_exp[:4, :4, :4]
        blocking_value = -9999.9
        slices = (
            (np.index_exp[0:2, 1:3, 2:4], 100),  # shall be blocked
            (np.index_exp[2:3, 0:2, 0], 200),  # shall be blocked
            (np.index_exp[3, 2, 1], 300),  # shall be blocked
            (np.index_exp[0:6, 0:6, 0:6], 400),  # shall be blocked
            (np.index_exp[:5, 3:, :], 500),  # shall be blocked
            (np.index_exp[8:, 8:, 8:], blocking_value),  # shall proceed as non-locked
        )
        # Call read process to lock arrays
        proc = mp_context.Process(
            target=call_array_method,
            args=(
                inserted_varray.collection,
                str(embedded_uri(root_path)),
                inserted_varray.id,
                "update",
                lock_set,
                func_finished,
                True,
                True,
                None,
                None,
                slice_converter[blocking_slice],
                blocking_value,
            ),
        )
        proc.start()
        lock_set.wait()
        result = None
        try:
            with mp_context.Pool(WORKERS) as pool:
                result = pool.starmap(
                    call_array_method,
                    [
                        (
                            inserted_varray.collection,
                            str(embedded_uri(root_path)),
                            inserted_varray.id,
                            "update",
                            lock_set,
                            func_finished,
                            False,
                            True,
                            None,
                            None,
                            slice_converter[subset_slice],
                            val,
                        )
                        for subset_slice, val in slices
                    ],
                )
        except Exception:
            raise
        finally:
            func_finished.set()
            try:
                assert result.count(DekerLockError) == len(slices) - 1
                check_data[blocking_slice].fill(blocking_value)
                check_data[8:, 8:, 8:].fill(blocking_value)
                assert np.all(inserted_varray[:].read() == check_data)
            except Exception:
                raise
            finally:
                inserted_varray.delete()

    def test_varray_locks_release_arrays(
        self,
        inserted_varray: VArray,
        varray_collection: Collection,
        local_varray_adapter,
        local_array_adapter,
    ):
        """Test that varray lock release all flocks and delete varraylock"""
        # Check that sequential update are fine
        inserted_varray[:].update(np.zeros(shape=varray_collection.varray_schema.shape))

        # Check that there is no flocks or extra files.
        locks = []
        for root, _dirs, files in os.walk(varray_collection.path):
            for file in files:
                filename = f"{root}/{file}"
                if filename.endswith(local_varray_adapter.file_ext) or filename.endswith(local_array_adapter.file_ext):
                    lock = Flock(Path(filename))
                    lock.release()
                if filename.endswith(LocksExtensions.array_lock.value) or filename.endswith(
                    # type: ignore
                    LocksExtensions.varray_lock.value
                ):  # type: ignore
                    locks.append(filename)
        assert not locks

    def test_varray_update_meta_lock(
        self, varray_schema: VArraySchema, root_path, inserted_varray_with_attributes: VArray, mp_context: BaseContext
    ):
        """Test meta update is not available during lock."""
        manager = mp_context.Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()
        proc = mp_context.Process(
            target=call_array_method,
            args=(
                inserted_varray_with_attributes.collection,
                str(embedded_uri(root_path)),
                inserted_varray_with_attributes.id,
                "update_meta_custom_attributes",
                lock_set,
                func_finished,
                True,
                True,
            ),
        )
        proc.start()
        lock_set.wait()
        try:
            methods = ["update_meta_custom_attributes", "update_meta_custom_attributes"]
            with mp_context.Pool(WORKERS) as pool:
                result = pool.starmap(
                    call_array_method,
                    [
                        (
                            inserted_varray_with_attributes.collection,
                            str(embedded_uri(root_path)),
                            inserted_varray_with_attributes.id,
                            method,
                            lock_set,
                            func_finished,
                            False,
                            True,
                        )
                        for method in methods
                    ],
                )
                assert result.count(DekerLockError) == len(methods)
            func_finished.set()
        finally:
            proc.kill()

    def test_collection_create(self, client: Client, root_path: Path, mp_context: BaseContext):
        """Test if collection is locked on creation."""
        manager = mp_context.Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()
        name = "name"

        # Delete collection
        try:
            client.get_collection(name).delete()
        except Exception:
            pass

        proc = mp_context.Process(
            target=call_collection_method,
            args=(str(embedded_uri(root_path)), name, "create", lock_set, func_finished, True),
        )
        proc.start()
        lock_set.wait()
        try:
            methods = ["create"] * 3
            with mp_context.Pool(WORKERS) as pool:
                result = pool.starmap(
                    call_collection_method,
                    [
                        (str(embedded_uri(root_path)), name, method, lock_set, func_finished, False)
                        for method in methods
                    ],
                )
                assert result.count(DekerLockError) == len(methods)
                func_finished.set()
        finally:
            proc.kill()


if __name__ == "__main__":
    pytest.main()

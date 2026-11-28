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
from multiprocessing.synchronize import Barrier
from multiprocessing.synchronize import Event as EventType
from pathlib import Path
from queue import Empty
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
from deker.errors import DekerInstanceNotExistsError, DekerLockError, DekerValidationError, DekerVSubsetError
from deker.locks import (
    CollectionLock,
    Flock,
    ReadArrayLock,
    WriteArrayLock,
)
from deker.schemas import ArraySchema, DimensionSchema, VArraySchema
from deker.tools import get_main_path
from deker.types import Numeric

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

    # a VArray holds no lock of its own: its Arrays are held one by one through the Array write lock
    lock = WriteArrayLock
    schema = collection.varray_schema if is_virtual else collection.array_schema
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
                # wait before the json file is replaced: the lock holds the file at the path until then
                with patch(
                    "deker_local_adapters.varray_adapter.os.replace",
                    wait_unlock(os.replace, lock_set, funcs_finished, wait),
                ):
                    array.update_custom_attributes(array.custom_attributes)
            else:
                array.update_custom_attributes(array.custom_attributes)
    except (DekerLockError, DekerVSubsetError) as e:
        return type(e)


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
    for the Array and its creation, so that concurrent writers collide for sure.

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


def update_array_region(
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
            go.wait(60)
            varray[slice_converter[index_exp]].update(value)
        queue.put(None)
    except Exception as e:
        queue.put(e)


def slow_update_data(adapter: HDF5StorageAdapter, *args, **kwargs) -> None:
    """Write the data as the storage adapter does, after a pause that keeps the write lock held for a while.

    :param adapter: storage adapter
    :param args: arguments of update_data
    :param kwargs: keyword arguments of update_data
    """
    if StorageHooks.started is not None:
        StorageHooks.started.set()
    sleep(StorageHooks.seconds)
    _ORIGINAL_UPDATE_DATA(adapter, *args, **kwargs)


def write_varray_region(
    uri: str,
    collection_name: str,
    varray_id: str,
    index_exp: str,
    value: float,
    go: Barrier,
    queue: Queue,
    seconds: float,
    timeout: int,
    started: EventType | None = None,
) -> None:
    """Write one region of a VArray from another process, holding every Array write for a while.

    :param uri: client uri
    :param collection_name: collection of the varray
    :param varray_id: id of the varray
    :param index_exp: region to write, as a string
    :param value: value to fill the region with
    :param go: barrier the writers and the test pass together once every writer is ready
    :param queue: queue for the result: ``None`` or the exception
    :param seconds: how long every Array write takes
    :param timeout: write lock timeout of the client
    :param started: event set when the first Array write starts
    """
    StorageHooks.seconds = seconds
    StorageHooks.started = started
    try:
        with (
            Client(uri, write_lock_timeout=timeout, write_lock_check_interval=1, loglevel="ERROR") as client,
            patch.object(HDF5StorageAdapter, "update_data", slow_update_data),
        ):
            collection = client.get_collection(collection_name)
            varray = collection.filter({"id": varray_id}).last()
            go.wait(60)
            varray[slice_converter[index_exp]].update(value)
        queue.put(None)
    except Exception as e:
        queue.put(e)


def write_array_directly(
    uri: str,
    collection_name: str,
    varray_id: str,
    v_position: tuple,
    index_exp: str,
    value: float,
    go: Barrier,
    queue: Queue,
    seconds: float,
) -> None:
    """Write one region of an Array through the arrays manager, bypassing the VArray and its subset.

    :param uri: client uri
    :param collection_name: collection of the varray
    :param varray_id: id of the varray
    :param v_position: position of the Array
    :param index_exp: region of the Array to write, as a string
    :param value: value to fill the region with
    :param go: barrier the writers and the test pass together once every writer is ready
    :param queue: queue for the result: ``None`` or the exception
    :param seconds: how long the Array write takes
    """
    StorageHooks.seconds = seconds
    StorageHooks.started = None
    try:
        with (
            Client(uri, write_lock_timeout=30, write_lock_check_interval=1, loglevel="ERROR") as client,
            patch.object(HDF5StorageAdapter, "update_data", slow_update_data),
        ):
            collection = client.get_collection(collection_name)
            array = collection.arrays.filter({"vid": varray_id, "v_position": v_position}).last()
            go.wait(60)
            array[slice_converter[index_exp]].update(value)
        queue.put(None)
    except Exception as e:
        queue.put(e)


def create_array_directly(
    uri: str, collection_name: str, varray_id: str, v_position: tuple, go: Barrier, queue: Queue, seconds: float
) -> None:
    """Create an Array through the arrays manager, bypassing the VArray and its creation lock.

    :param uri: client uri
    :param collection_name: collection of the varray
    :param varray_id: id of the varray
    :param v_position: position of the Array
    :param go: barrier the creators and the test pass together once every creator is ready
    :param queue: queue for the result: the id of the created Array or the exception
    :param seconds: pause between the check for the file and its creation
    """
    StorageHooks.seconds = seconds
    try:
        with (
            Client(uri, loglevel="ERROR") as client,
            patch.object(HDF5StorageAdapter, "create", slow_create),
        ):
            collection = client.get_collection(collection_name)
            go.wait(60)
            array = collection.arrays.create({"vid": varray_id, "v_position": v_position})
        queue.put(array.id)
    except Exception as e:
        queue.put(e)


def hold_exclusive_flock(path: str, held: Event, release: Event) -> None:
    """Hold an exclusive flock on a data file, as the rebalancer does while it moves the file.

    :param path: file to hold
    :param held: set once the lock is held
    :param release: waited for before the lock is released
    """
    lock = Flock(Path(path))
    lock.acquire()
    held.set()
    release.wait(60)
    lock.release()


def try_exclusive_flock(path: str, queue: Queue) -> None:
    """Try an exclusive flock on a data file once, as the rebalancer does before it moves the file.

    :param path: file to try
    :param queue: queue for the result: ``True`` if the lock was taken, ``False`` if the file is held
    """
    lock = Flock(Path(path))
    try:
        lock.acquire()
    except DekerLockError:
        queue.put(False)
        return
    lock.release()
    queue.put(True)


def read_varray(uri: str, collection_name: str, varray_id: str, queue: Queue) -> None:
    """Read a whole VArray from another process.

    :param uri: client uri
    :param collection_name: collection of the varray
    :param varray_id: id of the varray
    :param queue: queue for the result: the data or the exception
    """
    try:
        with Client(uri, loglevel="ERROR") as client:
            collection = client.get_collection(collection_name)
            queue.put(collection.filter({"id": varray_id}).last()[:].read())
    except Exception as e:
        queue.put(e)


def leave(busy) -> None:
    """Announce the end of a write.

    :param busy: shared number of the writers that are writing
    """
    with busy.get_lock():
        busy.value -= 1


def enter(gate: EventType, busy) -> bool:
    """Announce a write and tell whether it may start.

    The writer counts itself busy before it looks at the gate, so whoever closes the gate and then sees nobody
    busy knows that no write is under way and none will start.

    :param gate: event that lets the writers work
    :param busy: shared number of the writers that are writing
    :returns: ``True`` if the write may start; ``False`` if the gate is closed, the writer is not busy then
    """
    with busy.get_lock():
        busy.value += 1
    if gate.is_set():
        return True
    leave(busy)
    gate.wait(1)
    return False


# the causes of a write of a VArray refused as written partly in the window of its deletion: an Array of the
# position whose main file is moved, its symlink not yet
PARTLY_WRITTEN_CAUSES = ("DekerInstanceNotExistsError(", "DekerBrokenSymlinkError(")


def write_the_current_varray(uri: str, collection_name: str, stop, gate, busy, current, results: Queue) -> None:
    """Write the whole current VArray, creating its Arrays, until told to stop; report how the writes ended.

    :param uri: client uri
    :param collection_name: collection of the VArrays
    :param stop: event that ends the loop
    :param gate: event that lets the writers work: cleared, it holds them between the writes
    :param busy: shared number of the writers that are writing
    :param current: shared id of the current VArray
    :param results: queue for the numbers of the written VArrays, of the refused writes, of the writes refused as
      written partly, and the other errors
    """
    written = refused = partly = 0
    failed: list[str] = []
    with Client(uri, loglevel="CRITICAL") as client:
        collection = client.get_collection(collection_name)
        while not stop.is_set():
            if not enter(gate, busy):
                continue
            try:
                varray = collection.filter({"id": current.value}).last()
                if varray is None:
                    refused += 1
                else:
                    varray[:].update(1.0)
                    written += 1
            except DekerInstanceNotExistsError:
                refused += 1
            except DekerVSubsetError as e:
                if all(cause.startswith(PARTLY_WRITTEN_CAUSES) for cause in e.exceptions):
                    partly += 1
                else:
                    failed.append(repr(e))
            except Exception as e:
                failed.append(repr(e))
            leave(busy)
    results.put((written, refused, partly, failed))


def array_main_file(collection: Collection, array: Array) -> Path:
    """Path of the main data file of an Array.

    :param collection: collection of the Array
    :param Array: the Array
    :returns: path of its hdf5 file
    """
    adapter = array._adapter
    return get_main_path(array.id, collection.path / adapter.data_dir) / (array.id + adapter.file_ext)


def run_writers(mp_context: BaseContext, target: Callable, args_per_writer: list) -> list:
    """Start one process per writer, release them together and collect their results.

    :param mp_context: multiprocessing context
    :param target: function of the writer process, taking the barrier and the queue between its arguments
    :param args_per_writer: arguments of every writer, without the barrier and the queue, as (before, after) tuples
    :returns: the results in the order of the writers
    """
    manager = mp_context.Manager()
    go = manager.Barrier(len(args_per_writer) + 1)
    queue = manager.Queue()
    processes = [
        mp_context.Process(target=target, args=(*before, go, queue, *after)) for before, after in args_per_writer
    ]
    for process in processes:
        process.start()
    go.wait(60)  # every writer has opened its client and found its array
    try:
        results = [queue.get(timeout=120) for _ in processes]
    finally:
        for process in processes:
            process.join(120)
    return results


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


class TestArrayCreation:
    """Two writers of a fresh VArray create its Array once; the data of both land in the one Array."""

    @pytest.mark.parametrize("writers", [2, 4])
    def test_concurrent_writers_create_one_array(
        self, client: Client, root_path: Path, mp_context: BaseContext, writers: int
    ):
        schema = VArraySchema(
            dtype=float,
            dimensions=[DimensionSchema(name="x", size=writers * 4), DimensionSchema(name="y", size=4)],
            vgrid=(1, 1),  # one Array: every writer creates the same inner Array
        )
        collection = client.create_collection(f"array_creation_{writers}", schema)
        try:
            varray = collection.create()
            manager = mp_context.Manager()
            go = manager.Event()
            queue = manager.Queue()
            uri = str(embedded_uri(root_path))
            processes = [
                mp_context.Process(
                    target=update_array_region,
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

            arrays = [array for array in collection.arrays if array._vid == varray.id]
            assert len(arrays) == 1
            expected = np.repeat(np.arange(1, writers + 1, dtype=float), 4)[:, np.newaxis] * np.ones((1, 4))
            assert np.array_equal(varray[:].read(), expected)
        finally:
            collection.delete()


@pytest.mark.timeout(120)
class TestConcurrentVArrayWriters:
    """Writers of one storage in different processes, started together.

    The VArray is 16 x 4 in four Arrays of 4 x 4 along x: Array k covers x in [4k, 4k + 4). Every Array write is
    held for a while, so the writers overlap in time for sure. Only the data files are locked, on write, for
    the time of the write of that file: writers of different files never wait for each other, writers of one
    file queue on it and a region is always written by one writer as a whole.
    """

    SIDE = 4
    HOLD = 0.3

    @pytest.fixture
    def varray(self, client: Client, request: pytest.FixtureRequest) -> VArray:
        schema = VArraySchema(
            dtype=float,
            dimensions=[DimensionSchema(name="x", size=4 * self.SIDE), DimensionSchema(name="y", size=self.SIDE)],
            vgrid=(4, 1),
            fill_value=0.0,
        )
        collection = client.create_collection(f"writers_{request.node.callspec.id}", schema)
        try:
            yield collection.create()
        finally:
            collection.delete()

    def write(self, mp_context: BaseContext, root_path: Path, varray: VArray, regions: list, timeout: int = 30) -> list:
        """Run one writer per region and return their results.

        :param mp_context: multiprocessing context
        :param root_path: storage root
        :param varray: the VArray to write
        :param regions: (index expression string, value) per writer
        :param timeout: write lock timeout of the writers
        :returns: results of the writers: ``None`` or the exception each
        """
        uri = str(embedded_uri(root_path))
        return run_writers(
            mp_context,
            write_varray_region,
            [
                ((uri, varray.collection, varray.id, index_exp, value), (self.HOLD, timeout))
                for index_exp, value in regions
            ],
        )

    def test_different_arrays(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """1. Different Arrays of one VArray: the writers never meet, both land."""
        results = self.write(mp_context, root_path, varray, [("[0:4]", 1.0), ("[8:12]", 2.0)])
        assert results == [None, None]
        data = varray[:].read()
        assert np.all(data[0:4] == 1.0)
        assert np.all(data[8:12] == 2.0)
        assert np.all(data[4:8] == 0.0) and np.all(data[12:16] == 0.0)

    def test_overlapping_array_sets(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """2. Array sets that overlap: both writers finish; a shared Array holds the data of one of them as a whole."""
        results = self.write(mp_context, root_path, varray, [("[0:8]", 1.0), ("[4:12]", 2.0)])
        assert results == [None, None]
        data = varray[:].read()
        assert np.all(data[0:4] == 1.0)
        assert np.all(data[8:12] == 2.0)
        shared = data[4:8]
        assert np.all(shared == 1.0) or np.all(shared == 2.0)
        assert np.all(data[12:16] == 0.0)

    def test_same_array_different_parts(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """3. One Array, different parts: the writers queue on the file, both parts land."""
        results = self.write(mp_context, root_path, varray, [("[0:2]", 1.0), ("[2:4]", 2.0)])
        assert results == [None, None]
        data = varray[:].read()
        assert np.all(data[0:2] == 1.0)
        assert np.all(data[2:4] == 2.0)
        assert np.all(data[4:] == 0.0)

    def test_same_array_same_part(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """4. One Array, the same part: the last writer wins and the part is entirely his."""
        results = self.write(mp_context, root_path, varray, [("[1:3, 1:3]", 1.0), ("[1:3, 1:3]", 2.0)])
        assert results == [None, None]
        data = varray[:].read()
        part = data[1:3, 1:3]
        assert np.all(part == 1.0) or np.all(part == 2.0)
        assert np.count_nonzero(data) == part.size

    def test_same_array_whole(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """5. One Array as a whole by two writers: the last one wins, the Array is entirely his."""
        results = self.write(mp_context, root_path, varray, [("[4:8]", 1.0), ("[4:8]", 2.0)])
        assert results == [None, None]
        data = varray[:].read()
        region = data[4:8]
        assert np.all(region == 1.0) or np.all(region == 2.0)
        assert np.all(data[0:4] == 0.0) and np.all(data[8:] == 0.0)

    def test_different_varrays(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """6. Different VArrays of one collection never meet."""
        collection = varray._VArray__collection
        other = collection.create()
        uri = str(embedded_uri(root_path))
        results = run_writers(
            mp_context,
            write_varray_region,
            [
                ((uri, collection.name, varray.id, "[0:8]", 1.0), (self.HOLD, 30)),
                ((uri, collection.name, other.id, "[0:8]", 2.0), (self.HOLD, 30)),
            ],
        )
        assert results == [None, None]
        assert np.all(varray[0:8].read() == 1.0) and np.all(varray[8:].read() == 0.0)
        assert np.all(other[0:8].read() == 2.0) and np.all(other[8:].read() == 0.0)

    def test_direct_writers_of_one_array(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """A writer that takes the Array through the arrays manager meets the same lock as a VArray writer."""
        varray[:].update(0.0)  # every Array exists now
        uri = str(embedded_uri(root_path))
        results = run_writers(
            mp_context,
            write_array_directly,
            [
                ((uri, varray.collection, varray.id, (1, 0), "[0:2]", 1.0), (self.HOLD,)),
                ((uri, varray.collection, varray.id, (1, 0), "[2:4]", 2.0), (self.HOLD,)),
            ],
        )
        assert results == [None, None]
        data = varray[:].read()
        assert np.all(data[4:6] == 1.0)
        assert np.all(data[6:8] == 2.0)

    def test_direct_and_varray_writers_of_one_array(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """A VArray writer and a direct writer of the same Array queue on its file, both parts land."""
        varray[:].update(0.0)
        uri = str(embedded_uri(root_path))
        manager = mp_context.Manager()
        go, queue = manager.Barrier(3), manager.Queue()
        processes = [
            mp_context.Process(
                target=write_varray_region,
                args=(uri, varray.collection, varray.id, "[4:6]", 1.0, go, queue, self.HOLD, 30),
            ),
            mp_context.Process(
                target=write_array_directly,
                args=(uri, varray.collection, varray.id, (1, 0), "[2:4]", 2.0, go, queue, self.HOLD),
            ),
        ]
        for process in processes:
            process.start()
        go.wait(60)
        try:
            results = [queue.get(timeout=60) for _ in processes]
        finally:
            for process in processes:
                process.join(60)
        assert results == [None, None]
        data = varray[:].read()
        assert np.all(data[4:6] == 1.0)
        assert np.all(data[6:8] == 2.0)

    def test_direct_creators_of_one_array(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """Two creators of one Array through the arrays manager: one creates it, the other is told it exists."""
        uri = str(embedded_uri(root_path))
        results = run_writers(
            mp_context,
            create_array_directly,
            [((uri, varray.collection, varray.id, (2, 0)), (0.5,)) for _ in range(2)],
        )
        created = [result for result in results if isinstance(result, str)]
        errors = [result for result in results if isinstance(result, Exception)]
        assert len(created) == 1, results
        assert len(errors) == 1 and isinstance(errors[0], DekerValidationError), results
        arrays = [array for array in varray._VArray__collection.arrays if array._vid == varray.id]
        assert [array.id for array in arrays] == created

    def test_rebalancer_lock_is_refused_during_a_write(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """A process holding the file as the rebalancer does cannot take it while a VArray writes the Array."""
        varray[:].update(0.0)
        collection = varray._VArray__collection
        array = collection.arrays.filter({"vid": varray.id, "v_position": (0, 0)}).last()
        path = array_main_file(collection, array)
        manager = mp_context.Manager()
        go, queue, started = manager.Barrier(2), manager.Queue(), manager.Event()
        writer = mp_context.Process(
            target=write_varray_region,
            args=(str(embedded_uri(root_path)), collection.name, varray.id, "[0:4]", 1.0, go, queue, 2.0, 30, started),
        )
        writer.start()
        go.wait(60)
        assert started.wait(60)  # the writer holds the Array now
        try:
            probe = mp_context.Process(target=try_exclusive_flock, args=(str(path), queue))
            probe.start()
            probe.join(30)
            assert queue.get(timeout=30) is False
        finally:
            writer.join(60)
        assert queue.get(timeout=30) is None
        probe = mp_context.Process(target=try_exclusive_flock, args=(str(path), queue))
        probe.start()
        probe.join(30)
        assert queue.get(timeout=30) is True

    def test_writer_and_reader_wait_for_the_rebalancer(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """While the rebalancer holds an Array, its reader fails at once and its writer waits; both pass afterwards."""
        varray[:].update(0.0)
        collection = varray._VArray__collection
        array = collection.arrays.filter({"vid": varray.id, "v_position": (0, 0)}).last()
        path = array_main_file(collection, array)
        manager = mp_context.Manager()
        held, release, queue = manager.Event(), manager.Event(), manager.Queue()
        holder = mp_context.Process(target=hold_exclusive_flock, args=(str(path), held, release))
        holder.start()
        assert held.wait(30)
        try:
            with pytest.raises(DekerLockError):
                varray[0:4].read()
            uri = str(embedded_uri(root_path))
            go = manager.Barrier(2)
            writer = mp_context.Process(
                target=write_varray_region, args=(uri, collection.name, varray.id, "[0:4]", 1.0, go, queue, 0.0, 30)
            )
            writer.start()
            go.wait(60)
            with pytest.raises(Empty):
                queue.get(timeout=2)  # the writer is waiting for the Array: no result while the file is held
            release.set()
            writer.join(60)
            assert queue.get(timeout=30) is None
        finally:
            release.set()
            holder.join(30)
        assert np.all(varray[0:4].read() == 1.0)

    def test_dead_writer_leaves_nothing_behind(self, mp_context: BaseContext, root_path: Path, varray: VArray):
        """A VArray writer killed in the middle of a write blocks nobody afterwards and no cleaning is needed."""
        varray[:].update(0.0)
        collection = varray._VArray__collection
        uri = str(embedded_uri(root_path))
        manager = mp_context.Manager()
        go, queue, started = manager.Barrier(2), manager.Queue(), manager.Event()
        writer = mp_context.Process(
            target=write_varray_region,
            args=(uri, collection.name, varray.id, "[0:8]", 1.0, go, queue, 300.0, 30, started),
        )
        writer.start()
        go.wait(60)
        assert started.wait(60)  # the writer holds its Arrays and sleeps inside the write
        os.kill(writer.pid, 9)
        writer.join(30)

        results = run_writers(
            mp_context, write_varray_region, [((uri, collection.name, varray.id, "[0:8]", 2.0), (0.0, 5))]
        )
        assert results == [None]
        reader = mp_context.Process(target=read_varray, args=(uri, collection.name, varray.id, queue))
        reader.start()
        reader.join(60)
        data = queue.get(timeout=30)
        assert not isinstance(data, Exception), data
        assert np.all(data[0:8] == 2.0)
        assert lock_files_in(collection) == []


def lock_files_in(collection: Collection) -> list:
    """Lock files on the disk of a collection.

    :param collection: the collection
    :returns: names of the lock files
    """
    return sorted(str(path.name) for path in Path(collection.path).rglob("*lock"))


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

    def test_varray_writer_times_out_on_a_long_reader(
        self, varray_schema: VArraySchema, root_path, inserted_varray_with_attributes: VArray, mp_context: BaseContext
    ):
        """A VArray writer waits for the reader of an Array and gives up by timeout: update and clear both raise
        DekerLockError, since every Array that failed was still held when the write lock timeout was over.

        The writers cover every Array partly: an Array that a clear covers entirely is moved to the trash, waiting for
        nobody.
        """
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
                            None,
                            None,
                            "[1:9, 1:9, 1:9]",
                        )
                        for method in methods
                    ],
                )

                assert result == [DekerLockError, DekerLockError]
            func_finished.set()
        finally:
            proc.kill()

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


class TestDeletionOfAVArrayAgainstItsWriters:
    """Three processes write a VArray, creating its Arrays, while it is deleted: nothing of it is left.

    The deletion never waits and never fails. A writer is refused with DekerInstanceNotExistsError, or, in the window
    of the deletion where the main file of an Array is moved and its symlink is not yet, with DekerVSubsetError
    "written partly" whose causes are DekerInstanceNotExistsError or DekerBrokenSymlinkError; any other error fails.
    """

    SECONDS = 6
    WRITERS = 3

    @pytest.mark.timeout(120)
    def test_nothing_of_a_deleted_varray_is_left(self, client: Client, root_path: Path, mp_context: BaseContext):
        schema = VArraySchema(dtype=float, dimensions=[DimensionSchema(name="x", size=4)], vgrid=(2,))
        collection = client.create_collection(f"deleted_varray_{mp_context.get_start_method()}", schema)
        trash = root_path / client._Client__ctx.config.trash_directory
        manager = mp_context.Manager()
        stop, gate, results = mp_context.Event(), mp_context.Event(), mp_context.Queue()
        busy, current = mp_context.Value("i", 0), manager.Value(str, "")
        arguments = (str(embedded_uri(root_path)), collection.name, stop, gate, busy, current, results)
        writers = [mp_context.Process(target=write_the_current_varray, args=arguments) for _ in range(self.WRITERS)]
        for writer in writers:
            writer.start()
        deletions = left = 0
        longest = 0.0
        try:
            deadline = time.monotonic() + self.SECONDS
            while time.monotonic() < deadline:
                varray = collection.create()
                current.value = varray.id
                gate.set()
                sleep(0.05)  # the writers are on their way
                start = time.monotonic()
                varray.delete()
                longest = max(longest, time.monotonic() - start)
                gate.clear()
                while busy.value:
                    sleep(0.0005)
                files = [
                    os.path.join(directory, name)
                    for directory, _, names in os.walk(collection.path)
                    for name in names
                    if name != f"{collection.name}.json"
                ]
                left += len(files)
                if files:
                    collection.clear()  # the counted leftovers are taken away: every one is counted once
                deletions += 1
        finally:
            stop.set()
            gate.set()
            totals = [results.get(timeout=60) for _ in writers]
            for writer in writers:
                writer.join(60)
            collection.delete()
        written, refused, partly = (sum(total[i] for total in totals) for i in range(3))
        failed = [error for total in totals for error in total[3]]
        print(
            f"\n  {mp_context.get_start_method()}: {deletions} deletions, the longest {longest:.3f} s; writes: "
            f"{written} done, {refused} refused as not existing, {partly} refused as written partly; "
            f"failures {failed[:5]}; files left {left}"
        )
        assert deletions > 0
        assert written > 0
        assert longest < 1  # nobody is waited for
        assert failed == []
        assert left == 0
        assert list(trash.iterdir()) == []

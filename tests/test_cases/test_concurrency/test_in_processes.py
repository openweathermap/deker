"""
This module contains tests for multiprocessing and locking functionality.

The tests in this file cover scenarios where multiple processes are attempting to perform operations
on Array, VArray and Collection objects concurrently, and it verifies that the locking mechanisms work
correctly.
"""
import os
import traceback

from multiprocessing import Event, Manager, Process, cpu_count
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Callable, Dict, Literal
from unittest.mock import patch

import h5py
import numpy as np
import pytest

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
from deker.tools import get_paths
from deker.types import LocksExtensions


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
    primary_attributes: Dict = None,
    custom_attributes: Dict = None,
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
    """
    client = Client(uri, write_lock_timeout=1, loglevel="ERROR")
    collection = client.get_collection(collection_name)

    # Get Array object
    if method == "create":
        with patch.object(
            Flock, "release", wait_unlock(Flock.release, lock_set, funcs_finished, wait)
        ):
            with patch("deker.ABC.base_array.get_id", lambda *a: id_):
                try:
                    array = collection.create(primary_attributes, custom_attributes)
                except DekerLockError:
                    return DekerLockError
                except Exception:
                    traceback.print_exc()
                    return None
        return
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
            with patch.object(
                lock, "release", wait_unlock(lock.release, lock_set, funcs_finished, wait)
            ):
                array[:].update(np.zeros(shape=schema.shape))

        elif method == "clear":
            with patch.object(
                lock, "release", wait_unlock(lock.release, lock_set, funcs_finished, wait)
            ):
                array[:].clear()

        # Read used in varray only.
        elif method == "read":
            # patch release function to not release read lock until we're done
            with patch.object(
                ReadArrayLock,
                "release",
                wait_unlock(ReadArrayLock.release, lock_set, funcs_finished, wait),
            ):
                # patch timeout time
                array[:].read()

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


def call_collection_method(
    path, name: str, method: str, lock_set: Event, func_finished: Event, wait: bool = False
):
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
    except:
        traceback.print_exc()
        return Exception


class TestLocks:
    """Test if methods locks work correctly with multiple processes."""

    def test_array_write_lock(self, array_schema: ArraySchema, root_path, inserted_array: Array):
        """Test array lock."""
        manager = Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()

        # Call method that locks array
        proc = Process(
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
            with Pool(WORKERS) as pool:
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
        self, inserted_array: Array, array_collection: Collection, root_path
    ):
        """Test if we can read array in parallel."""
        args = (inserted_array.id, str(embedded_uri(root_path)), array_collection.name)
        with Pool(WORKERS) as pool:
            result = pool.starmap(read_array, (args for _ in range(4)))

        assert result.count(Exception) == 0

    def test_varray_lock_wait_till_read_timeout(
        self,
        varray_schema: VArraySchema,
        root_path,
        inserted_varray_with_attributes: VArray,
    ):
        """Test varray read lock timeout."""
        manager = Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()

        # Call read process to lock arrays for reading
        proc = Process(
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
            with Pool(WORKERS) as pool:
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
        self, inserted_varray: VArray, root_path, varray_collection: Collection
    ):
        """Test that as we lock varray, inner varrays also locked."""
        manager = Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()

        # Call read process to lock arrays for reading
        proc = Process(
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
        try:
            for array in varray_collection.arrays:
                with pytest.raises(DekerLockError):
                    array[:].update(np.zeros(shape=varray_collection.array_schema.shape))
            func_finished.set()
        except Exception:
            proc.kill()

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
        inserted_varray[:].update(np.zeros(shape=varray_collection.varray_schema.shape))

        # Check that there is no flocks or extra files.
        locks = []
        for root, dirs, files in os.walk(varray_collection.path):
            for file in files:
                filename = f"{root}/{file}"
                if filename.endswith(local_varray_adapter.file_ext) or filename.endswith(
                    local_array_adapter.file_ext
                ):
                    lock = Flock(Path(filename))
                    lock.release()
                if filename.endswith(LocksExtensions.array_lock.value) or filename.endswith(
                    # type: ignore
                    LocksExtensions.varray_lock.value
                ):  # type: ignore
                    locks.append(filename)
        assert not locks

    def test_varray_update_meta_lock(
        self,
        varray_schema: VArraySchema,
        root_path,
        inserted_varray_with_attributes: VArray,
    ):
        """Test meta update is not available during lock."""
        manager = Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()
        proc = Process(
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
            with Pool(WORKERS) as pool:
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

    def test_varray_with_attributes_create_lock(
        self,
        client: Client,
        array_schema_with_attributes: ArraySchema,
        root_path,
        varray_with_attributes: VArray,
    ):
        """Test create lock."""
        manager = Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()
        proc = Process(
            target=call_array_method,
            args=(
                varray_with_attributes.collection,
                str(embedded_uri(root_path)),
                varray_with_attributes.id,
                "create",
                lock_set,
                func_finished,
                True,
                True,
                varray_with_attributes.primary_attributes,
                varray_with_attributes.custom_attributes,
            ),
        )
        proc.start()
        lock_set.wait()
        try:
            methods = ["create"] * 3
            with Pool(WORKERS) as pool:
                result = pool.starmap(
                    call_array_method,
                    [
                        (
                            varray_with_attributes.collection,
                            str(embedded_uri(root_path)),
                            varray_with_attributes.id,
                            method,
                            lock_set,
                            func_finished,
                            False,
                            True,
                            varray_with_attributes.primary_attributes,
                            varray_with_attributes.custom_attributes,
                        )
                        for method in methods
                    ],
                )
                assert result.count(DekerLockError) == len(methods)
                func_finished.set()
        finally:
            proc.kill()

    def test_collection_create(self, client: Client, root_path):
        """Test if collection is locked on creation."""
        manager = Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()
        name = "name"

        # Delete collection
        try:
            client.get_collection(name).delete()
        except Exception:
            pass

        proc = Process(
            target=call_collection_method,
            args=(str(embedded_uri(root_path)), name, "create", lock_set, func_finished, True),
        )
        proc.start()
        lock_set.wait()
        try:
            methods = ["create"] * 3
            with Pool(WORKERS) as pool:
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


class TestMethods:
    """Test if methods work correctly with multiple processes."""

    def test_array_with_attributes_create_multiple_processes(
        self,
        client: Client,
        array_schema_with_attributes: ArraySchema,
        root_path,
        array_with_attributes: Array,
        array_data: np.ndarray,
        storage_adapter,
        ctx,
    ):
        manager = Manager()
        lock_set = manager.Event()
        func_finished = manager.Event()

        methods = ["create"] * 3
        with Pool(WORKERS - 1) as pool:
            pool.starmap(
                call_array_method,
                [
                    (
                        array_with_attributes.collection,
                        str(embedded_uri(root_path)),
                        array_with_attributes.id,
                        method,
                        lock_set,
                        func_finished,
                        False,
                        False,
                        array_with_attributes.primary_attributes,
                        array_with_attributes.custom_attributes,
                    )
                    for method in methods
                ],
            )
        lock_set.wait()
        func_finished.set()

        paths = get_paths(
            array_with_attributes,
            root_path / ctx.config.collections_directory / array_with_attributes.collection,
        )
        filename = str(array_with_attributes.id) + storage_adapter.file_ext
        file_path = paths.main / filename
        symlink = paths.symlink / filename
        assert file_path.exists()
        assert symlink.exists()
        array_with_attributes[:].update(array_data)

        with h5py.File(file_path) as f:
            assert np.allclose(f["data"][:], np.asarray(array_data), equal_nan=True)


if __name__ == "__main__":
    pytest.main()

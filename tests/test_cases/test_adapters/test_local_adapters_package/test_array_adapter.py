import json
import os
import threading
import time

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import h5py
import numpy as np
import pytest

from cantok import DefaultToken, SimpleToken
from deker_local_adapters import AdaptersFactory, LocalArrayAdapter
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from deker_tools.cancel import TokenCancelledError, cancellation_scope, read_token, write_token
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.parameters.index_exp_params import valid_index_exp_params

from deker.arrays import Array, VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerArrayTypeError, DekerLockError, DekerValidationError
from deker.flock import Flock
from deker.schemas import ArraySchema, DimensionSchema
from deker.tools import get_paths


class TestArrayAdapter:
    """Class for testing local array adapter."""

    @pytest.mark.parametrize(
        "data",
        [
            np.ones(shape=(10, 10, 10)),
            np.ones(shape=(10, 10, 10)).tolist(),
            np.ndarray(shape=(10, 10, 10), dtype=float),
            np.ndarray(shape=(10, 10, 10), dtype=float).tolist(),
        ],
    )
    def test_array_adapter_creates_array(self, root_path, array: Array, data: Any, factory):
        """Tests if array adapter creates array properly.

        :param root_path: temporary collection root path
        :param array: Pre created array
        :param data: array data from parameters
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        filename = str(array.id) + factory.ctx.storage_adapter.file_ext
        file_path = paths.main / filename
        symlink = paths.symlink / filename
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        assert array_adapter.create(array, timeout=Sentinel, token=DefaultToken()) is array
        assert file_path.exists()
        array[:].update(data)
        assert symlink.exists()

        with h5py.File(file_path) as f:
            assert np.allclose(f["data"][:], np.asarray(data), equal_nan=True)

    def test_array_adapter_doesnt_create_with_same_primary_attrs(
        self, array_with_attributes: Array, array_collection_with_attributes: Collection
    ):
        array_adapter = array_with_attributes._adapter
        array_adapter.create(array_with_attributes, timeout=Sentinel, token=DefaultToken())
        new_array = Array(
            collection=array_collection_with_attributes,
            adapter=array_adapter,
            primary_attributes=array_with_attributes.primary_attributes,
            custom_attributes=array_with_attributes.custom_attributes,
        )
        with pytest.raises(DekerValidationError):
            assert array_adapter.create(new_array, timeout=Sentinel, token=DefaultToken())

    def test_array_adapter_fails_to_create_file_exists(self, root_path, array: Array, factory):
        """Tests if array adapter creates array properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        paths.create()
        filename = str(array.id) + factory.ctx.storage_adapter.file_ext
        file_path = paths.main / filename
        file_path.write_text("foreign")
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        with pytest.raises(DekerArrayError) as e:
            array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert "already exists" in str(e.value)
        assert file_path.read_text() == "foreign"  # refused in one step, never opened for writing
        assert not (paths.symlink / filename).exists()

    @pytest.mark.parametrize(
        ("data", "raised"),
        [
            (np.ones(shape=(3, 2, 2)), IndexError),
            (np.ones(shape=(2, 2, 2), dtype=int), DekerArrayTypeError),
            (
                [[[1.0, 1.0], [1.0, 1.0]], [[1.0, 1.0], [1.0, 1.0]], [[1.0, 1.0], [1.0, 1.0]]],
                IndexError,
            ),
            (
                (((1.0, 1.0), (1.0, 1.0)), ((1.0, 1.0), (1.0, 1.0)), ((1.0, 1.0), (1.0, 1.0))),
                IndexError,
            ),
            ([[["a", "b"], ["c", "d"]], [["e", "f"], ["g", "h"]]], DekerArrayTypeError),
            ("1,2,3", DekerArrayTypeError),
        ],
    )
    def test_array_adapter_raises_on_create(
        self,
        root_path,
        array: Array,
        factory,
        data: Any,
        raised: Exception,
    ):
        """Tests if array adapter raises on array creation.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        :param data: data to update the array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        paths = get_paths(array, coll_path)
        filename = str(array.id) + array_adapter.storage_adapter.file_ext
        file_path = paths.main / filename
        symlink = paths.symlink / filename

        with pytest.raises(raised):
            array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
            array[:].update(data)
            assert not file_path.exists()
            assert not symlink.exists()

    @pytest.mark.parametrize(
        "data",
        [
            np.ones(shape=(10, 10, 10)),
            np.ones(shape=(10, 10, 10)).tolist(),
            np.ndarray(shape=(10, 10, 10)),
            np.ndarray(shape=(10, 10, 10)).tolist(),
            np.empty(shape=(10, 10, 10)),
            np.empty(shape=(10, 10, 10)).tolist(),
        ],
    )
    def test_array_adapter_updates_array_data(self, root_path, array: Array, factory, data: Any):
        """Tests if array adapter updates array data properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        :param data: data to update the array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = paths.main / (array.id + array_adapter.storage_adapter.file_ext)
        array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        array[:].update(data)
        assert os.path.exists(filename)
        update_data = np.zeros(shape=(10, 10, 10))
        array_adapter.update(array, np.index_exp[:], update_data, timeout=Sentinel, token=DefaultToken())
        with h5py.File(filename) as f:
            assert (f["data"][:] == update_data).all()

    def test_array_adapter_raises_on_update_no_data(self, root_path, array: Array, factory):
        """Tests if array adapter raises on update if data is None.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = paths.main / (array.id + array_adapter.storage_adapter.file_ext)
        array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert os.path.exists(filename)
        with pytest.raises(DekerArrayError, match="shall not be None"):
            array_adapter.update(array, np.index_exp[:], None, timeout=Sentinel, token=DefaultToken())

    @pytest.mark.parametrize(
        "data",
        [
            np.ones(shape=(10, 10, 10)),
            np.ones(shape=(10, 10, 10)).tolist(),
        ],
    )
    def test_array_adapter_reads_data_from_array(
        self,
        root_path,
        array: Array,
        factory: AdaptersFactory,
        data: Any,
    ):
        """Tests if array adapter reads array data properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        :param data: data to update the array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert os.path.exists(filename)
        array[:].update(data)
        result = array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())
        assert (result == np.asarray(data)).all()

    def test_array_adapter_reads_cleared_data_from_array(
        self,
        root_path: Path,
        array: Array,
        factory: AdaptersFactory,
    ):
        """Tests if array adapter reads cleared array data properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert os.path.exists(filename)
        result = array_adapter.read_data(array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())
        assert (np.isnan(result)).all()
        assert result.dtype == array.dtype

    @pytest.mark.parametrize(
        ("metakey", "expected"),
        [
            ("primary_attributes", {}),
            ("custom_attributes", {}),
        ],
    )
    def test_array_adapter_reads_metadata_from_array(
        self,
        root_path,
        array: Array,
        factory: AdaptersFactory,
        metakey: Any,
        expected: Any,
    ):
        """Tests if array adapter reads array metadata properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        :param factory: AdaptersFactory
        :param metakey: key in metadata
        :param expected: expected value of metakey
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert os.path.exists(filename)
        meta = array.read_meta()
        assert isinstance(meta, dict)
        assert meta[metakey] == expected

    def test_array_adapter_deletes_array(self, root_path, inserted_array: Array, factory):
        """Tests if array adapter deletes array properly.

        :param root_path: temporary array_collection root path
        :param inserted_array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / inserted_array.collection
        paths = get_paths(inserted_array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = paths.main / (inserted_array.id + factory.ctx.storage_adapter.file_ext)
        mainfile = paths.main / filename
        array_adapter.delete(inserted_array, timeout=Sentinel, token=DefaultToken())
        assert not os.path.exists(paths.symlink / filename)
        assert not os.path.exists(mainfile)
        assert not paths.main.exists()

    @pytest.mark.timeout(60)
    def test_array_adapter_delete_waits_for_a_reader(self, root_path, inserted_array: Array, factory):
        """A delete holds the file exclusively: it waits for the reader to leave and only then removes the files."""
        coll_path = root_path / factory.ctx.config.collections_directory / inserted_array.collection
        paths = get_paths(inserted_array, coll_path)
        filename = inserted_array.id + factory.ctx.storage_adapter.file_ext
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        reader = Flock(paths.main / filename, shared=True)
        reader.acquire()
        with ThreadPoolExecutor(max_workers=1) as pool:
            deletion = pool.submit(array_adapter.delete, inserted_array, timeout=Sentinel, token=DefaultToken())
            with pytest.raises(FutureTimeoutError):
                deletion.result(timeout=0.5)  # the reader is still there
            assert (paths.main / filename).exists()
            reader.release()
            assert deletion.result(timeout=10) is None
        assert not (paths.main / filename).exists()
        assert not (paths.symlink / filename).exists()

    @pytest.mark.timeout(60)
    def test_array_adapter_delete_times_out_on_a_long_reader(
        self, root_path, inserted_array: Array, factory, mocker: MockerFixture
    ):
        """A delete gives up by the write lock timeout and leaves the files in place."""
        coll_path = root_path / factory.ctx.config.collections_directory / inserted_array.collection
        paths = get_paths(inserted_array, coll_path)
        filename = inserted_array.id + factory.ctx.storage_adapter.file_ext
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        mocker.patch.object(array_adapter.ctx.config, "write_lock_timeout", 1)
        with Flock(paths.main / filename, shared=True), pytest.raises(DekerLockError):
            array_adapter.delete(inserted_array, timeout=Sentinel, token=DefaultToken())
        assert (paths.main / filename).exists()
        assert (paths.symlink / filename).exists()

    def test_array_adapter_deletes_not_existing_array(
        self,
        root_path,
        inserted_array: Array,
        factory,
    ):
        """Tests if array adapter deletes array properly even if files not found.

        :param root_path: temporary array_collection root path
        :param inserted_array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / inserted_array.collection
        paths = get_paths(inserted_array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = inserted_array.id + factory.ctx.storage_adapter.file_ext
        mainfile = paths.main / filename
        assert mainfile.exists()
        symfile = paths.symlink / filename
        assert symfile.exists()
        for f in (symfile, mainfile):
            os.remove(f)
        array_adapter.delete(inserted_array, timeout=Sentinel, token=DefaultToken())
        assert not os.path.exists(filename)

    def test_array_adapter_clear(
        self,
        root_path,
        inserted_array: Array,
        factory,
    ):
        """Tests clearing data from array.

        :param root_path: temporary array_collection root path
        :param inserted_array: Pre created array.
        """
        coll_path = root_path / factory.ctx.config.collections_directory / inserted_array.collection
        paths = get_paths(inserted_array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, HDF5StorageAdapter)
        filename = paths.main / (inserted_array.id + factory.ctx.storage_adapter.file_ext)
        assert filename.exists()
        array_adapter.clear(inserted_array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())
        with h5py.File(filename, "r+") as f:
            ds = f.get("data")
            assert ds is None

    def test_array_adapter_get_paths(
        self,
        array_with_attributes: Array,
    ):
        """Tests getting array path from attributes.

        :param array_with_attributes: Array which contains some primary attributes
        """
        main_tree, rest = array_with_attributes.id.split("-", 1)
        main_tree = os.path.sep.join(s for s in main_tree)
        main_path = (
            array_with_attributes._adapter.collection_path / array_with_attributes._adapter.data_dir / main_tree / rest
        )

        sym_path = (
            array_with_attributes._adapter.collection_path
            / array_with_attributes._adapter.symlinks_dir
            / str(array_with_attributes.primary_attributes.get("primary_attribute"))
        )

        adapter_paths = get_paths(array_with_attributes, array_with_attributes._adapter.collection_path)

        assert adapter_paths.main == main_path
        assert adapter_paths.symlink == sym_path

    def test_array_adapter_update_custom_attributes(self, array_with_attributes: Array):
        new_custom_attributes = {
            "custom_attribute": 0.6,
            "time_attr_name": datetime.now(timezone.utc),
        }
        adapter = array_with_attributes._adapter
        adapter.create(array_with_attributes, timeout=Sentinel, token=DefaultToken())
        adapter.update_meta_custom_attributes(
            array_with_attributes, new_custom_attributes, timeout=Sentinel, token=DefaultToken()
        )
        assert array_with_attributes.custom_attributes == new_custom_attributes
        meta = array_with_attributes.read_meta()
        ar = Array._create_from_meta(
            array_with_attributes._Array__collection,
            meta,
            array_with_attributes._adapter,
        )
        assert ar.custom_attributes == new_custom_attributes

    def test_array_adapter_iter(self, array_collection: Collection):
        arrays = [array_collection.create() for _ in range(10)]
        metas = [json.loads(array._create_meta()) for array in arrays]
        adapter: LocalArrayAdapter = arrays[0]._adapter
        inner_metas = [*adapter]
        assert len(inner_metas) == len(metas)
        assert all(inner_meta in metas for inner_meta in inner_metas)

    def test_array_clear_with_fill_value(self, client: Client):
        schema = ArraySchema(dtype=int, dimensions=[DimensionSchema(name="x", size=2)])
        coll = client.create_collection("test", schema=schema)
        array = coll.create()
        try:
            data = np.asarray([np.iinfo(schema.dtype).min] * 2).reshape(2)
            array[:].update(data)
            array[:].clear()
            result = array[:].read()
            assert result.dtype == data.dtype
            assert np.array_equal(result, data)
        finally:
            coll.delete()

    def test_array_raises_varray_error(self, varray_collection: Collection):
        _id = str(uuid4())
        with pytest.raises(DekerArrayError) as e:
            varray_collection.arrays.create({"vid": _id, "v_position": (0, 0, 0)})

        assert f"VArray with id={_id} doesn't exist" == str(e.value)


class TestArrayAdapterEdges:
    @pytest.fixture
    def varray_arrays_adapter(self, varray_collection: Collection, ctx) -> LocalArrayAdapter:
        """Adapter of the Arrays (Arrays) that belong to the VArray collection."""
        from concurrent.futures import ThreadPoolExecutor

        return LocalArrayAdapter(
            collection_path=varray_collection.path,
            collection_options=varray_collection.options,
            ctx=ctx,
            storage_adapter=HDF5StorageAdapter,
            executor=ThreadPoolExecutor(max_workers=1),
        )

    def test_symlink_filename_of_an_existing_array(
        self, varray_arrays_adapter: LocalArrayAdapter, inserted_varray: VArray, varray_collection: Collection
    ):
        symlink = next(iter(varray_arrays_adapter._adapter_iter(inserted_varray.id)))
        position = tuple(int(i) for i in symlink.parent.name.split("-"))
        filename = varray_arrays_adapter._get_symlink_filename(
            inserted_varray.id, position, varray_collection.array_schema.primary_attributes
        )
        assert filename == symlink

    def test_symlink_filename_of_an_empty_array_directory(
        self, varray_arrays_adapter: LocalArrayAdapter, inserted_varray: VArray, varray_collection: Collection
    ):
        symlink = next(iter(varray_arrays_adapter._adapter_iter(inserted_varray.id)))
        position = tuple(int(i) for i in symlink.parent.name.split("-"))
        symlink.unlink()
        assert (
            varray_arrays_adapter._get_symlink_filename(
                inserted_varray.id, position, varray_collection.array_schema.primary_attributes
            )
            is None
        )

    def test_symlink_filename_of_a_missing_array(
        self, varray_arrays_adapter: LocalArrayAdapter, inserted_varray: VArray, varray_collection: Collection
    ):
        position = (99,) * len(inserted_varray.vgrid)
        assert (
            varray_arrays_adapter._get_symlink_filename(
                inserted_varray.id, position, varray_collection.array_schema.primary_attributes
            )
            is None
        )

    def test_adapter_iter_skips_foreign_files(
        self, varray_arrays_adapter: LocalArrayAdapter, inserted_varray: VArray, varray_collection: Collection
    ):
        root = varray_collection.path / varray_arrays_adapter.symlinks_dir / inserted_varray.id
        (root / "stray.lock").write_text("x")
        symlinks = list(varray_arrays_adapter._adapter_iter(inserted_varray.id))
        assert symlinks
        assert all(s.name.endswith(varray_arrays_adapter.file_ext) for s in symlinks)


if __name__ == "__main__":
    pytest.main()


class TestArrayAdapterBounds:
    """The bounds of every index expression DEKER accepts go through the adapter and its locks unchanged."""

    @staticmethod
    def adapter_of(root_path: Path, factory: AdaptersFactory, array: Array) -> LocalArrayAdapter:
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        return factory.get_array_adapter(coll_path, HDF5StorageAdapter)

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_read_data_returns_the_cells_of_the_bounds(
        self, root_path: Path, factory: AdaptersFactory, inserted_array: Array, array_data: np.ndarray, index_exp
    ):
        read = self.adapter_of(root_path, factory, inserted_array).read_data(
            inserted_array, index_exp, timeout=Sentinel, token=DefaultToken()
        )
        assert np.ndim(read) == np.ndim(array_data[index_exp])
        assert np.array_equal(np.asarray(read), array_data[index_exp])

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_update_writes_the_cells_of_the_bounds_only(
        self, root_path: Path, factory: AdaptersFactory, inserted_array: Array, array_data: np.ndarray, index_exp
    ):
        adapter = self.adapter_of(root_path, factory, inserted_array)
        expected = array_data.copy()
        expected[index_exp] = -9999
        adapter.update(
            inserted_array,
            index_exp,
            np.full(np.shape(array_data[index_exp]), -9999.0),
            timeout=Sentinel,
            token=DefaultToken(),
        )
        assert np.array_equal(
            np.asarray(adapter.read_data(inserted_array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())),
            expected,
        )

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_clear_empties_the_cells_of_the_bounds_only(
        self, root_path: Path, factory: AdaptersFactory, inserted_array: Array, array_data: np.ndarray, index_exp
    ):
        adapter = self.adapter_of(root_path, factory, inserted_array)
        expected = array_data.copy()
        expected[index_exp] = np.nan
        adapter.clear(inserted_array, index_exp, timeout=Sentinel, token=DefaultToken())
        assert np.array_equal(
            np.asarray(adapter.read_data(inserted_array, np.index_exp[:], timeout=Sentinel, token=DefaultToken())),
            expected,
            equal_nan=True,
        )


class CancelScopeAndCall:
    """Stand for a method of the storage adapter: cancel the scope, then do the real work.

    :param scope: token of the cancellation scope
    :param method: the real method
    """

    def __init__(self, scope: SimpleToken, method):
        self.scope = scope
        self.method = method

    def __call__(self, *args, **kwargs):
        self.scope.cancel()
        return self.method(*args, **kwargs)


def write_update(adapter, array, **kwargs):
    adapter.update(array, np.index_exp[0, 0, 0], 7.0, **kwargs)


def write_clear(adapter, array, **kwargs):
    adapter.clear(array, np.index_exp[0, 0, 0], **kwargs)


def write_attributes(adapter, array, **kwargs):
    adapter.update_meta_custom_attributes(array, {}, **kwargs)


def write_delete(adapter, array, **kwargs):
    adapter.delete(array, **kwargs)


WRITES = {
    "update": write_update,
    "clear": write_clear,
    "update_meta_custom_attributes": write_attributes,
    "delete": write_delete,
}


class TestArrayAdapterCancellation:
    """The array adapter stops a cancelled operation before its file is started, never after."""

    @pytest.fixture
    def main_file(self, inserted_array: Array) -> Path:
        return inserted_array._adapter._get_main_path_to_file(inserted_array)

    @pytest.mark.parametrize("operation", list(WRITES))
    def test_cancelled_token_changes_nothing(self, inserted_array: Array, main_file: Path, operation):
        before = (main_file.stat().st_mtime_ns, main_file.read_bytes())
        token = SimpleToken()
        token.cancel()
        with pytest.raises(TokenCancelledError):
            WRITES[operation](inserted_array._adapter, inserted_array, timeout=Sentinel, token=token)
        assert (main_file.stat().st_mtime_ns, main_file.read_bytes()) == before
        holder = Flock(main_file)
        holder.acquire()  # nothing holds the file
        holder.release()

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("operation", list(WRITES))
    def test_cancelled_while_waiting_for_the_lock_changes_nothing(
        self, inserted_array: Array, main_file: Path, operation
    ):
        before = main_file.read_bytes()
        scope = SimpleToken()
        with cancellation_scope(scope):
            token = write_token()
        canceller = threading.Timer(0.3, scope.cancel)
        with Flock(main_file, shared=True):
            canceller.start()
            with pytest.raises(TokenCancelledError):
                WRITES[operation](inserted_array._adapter, inserted_array, timeout=30, token=token)
        canceller.join()
        assert main_file.read_bytes() == before

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("operation", list(WRITES))
    def test_timeout_of_the_call_limits_the_wait(self, inserted_array: Array, main_file: Path, operation):
        start = time.monotonic()
        with Flock(main_file, shared=True), pytest.raises(DekerLockError):
            WRITES[operation](inserted_array._adapter, inserted_array, timeout=1, token=DefaultToken())
        assert time.monotonic() - start < 2.5
        assert main_file.exists()

    @pytest.mark.parametrize(
        ("operation", "storage_method"),
        [
            ("update", "update_data"),
            ("clear", "clear_data"),
            ("update_meta_custom_attributes", "update_meta_custom_attributes"),
        ],
    )
    def test_cancelled_after_the_file_is_started_goes_to_its_end(
        self, inserted_array: Array, mocker: MockerFixture, operation, storage_method
    ):
        adapter = inserted_array._adapter
        scope = SimpleToken()
        with cancellation_scope(scope):
            token = write_token()
        real = getattr(adapter.storage_adapter, storage_method)
        mocker.patch.object(adapter.storage_adapter, storage_method, side_effect=CancelScopeAndCall(scope, real))
        WRITES[operation](adapter, inserted_array, timeout=Sentinel, token=token)
        assert scope.cancelled
        if operation == "update":
            assert inserted_array[0, 0, 0].read() == 7.0
        if operation == "clear":
            assert np.isnan(inserted_array[0, 0, 0].read())

    def test_read_cancelled_while_the_file_is_read_returns_no_data(self, inserted_array: Array, mocker: MockerFixture):
        adapter = inserted_array._adapter
        scope = SimpleToken()
        with cancellation_scope(scope):
            token = read_token(60)
        real = adapter.storage_adapter.read_data
        read = mocker.patch.object(adapter.storage_adapter, "read_data", side_effect=CancelScopeAndCall(scope, real))
        with pytest.raises(TokenCancelledError):
            adapter.read_data(inserted_array, np.index_exp[0, 0, 0], timeout=Sentinel, token=token)
        read.assert_called_once()

    def test_read_with_a_cancelled_token_does_not_open_the_file(self, inserted_array: Array, mocker: MockerFixture):
        adapter = inserted_array._adapter
        read = mocker.spy(adapter.storage_adapter, "read_data")
        token = SimpleToken()
        token.cancel()
        with pytest.raises(TokenCancelledError):
            adapter.read_data(inserted_array, np.index_exp[0, 0, 0], timeout=Sentinel, token=token)
        read.assert_not_called()

    def test_create_with_a_cancelled_token_creates_no_file(self, array: Array):
        token = SimpleToken()
        token.cancel()
        with pytest.raises(TokenCancelledError):
            array._adapter.create(array, timeout=Sentinel, token=token)
        assert not array._adapter._get_main_path_to_file(array).exists()


class TestDeleteAllByVidCancellation:
    """The Arrays of a VArray are deleted all or none when the deletion is cancelled."""

    @pytest.fixture
    def array_adapter(self, inserted_varray: VArray):
        return inserted_varray._VArray__array_adapter

    def arrays_left(self, inserted_varray: VArray, array_adapter) -> int:
        return len(list(array_adapter._adapter_iter(inserted_varray.id)))

    def test_cancelled_before_the_start_deletes_nothing(self, inserted_varray: VArray, array_adapter):
        before = self.arrays_left(inserted_varray, array_adapter)
        scope = SimpleToken()
        with cancellation_scope(scope):
            token = write_token()
        scope.cancel()
        with pytest.raises(TokenCancelledError):
            array_adapter.delete_all_by_vid(
                inserted_varray.id, inserted_varray._VArray__collection, timeout=Sentinel, token=token
            )
        assert self.arrays_left(inserted_varray, array_adapter) == before > 0

    def test_cancelled_after_the_first_array_deletes_every_array(
        self, inserted_varray: VArray, array_adapter, mocker: MockerFixture
    ):
        scope = SimpleToken()
        with cancellation_scope(scope):
            token = write_token()
        mocker.patch.object(array_adapter, "_delete", side_effect=CancelScopeAndCall(scope, array_adapter._delete))
        array_adapter.delete_all_by_vid(
            inserted_varray.id, inserted_varray._VArray__collection, timeout=Sentinel, token=token
        )
        assert scope.cancelled
        assert self.arrays_left(inserted_varray, array_adapter) == 0

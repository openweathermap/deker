import json
import os

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import h5py
import numpy as np
import pytest

from deker_local_adapters import HDF5StorageAdapter, LocalArrayAdapter

from deker.arrays import Array
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerArrayTypeError, DekerValidationError
from deker.schemas import ArraySchema, DimensionSchema
from deker.tools import create_array_from_meta, get_paths


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
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        array_adapter.create(array)
        assert file_path.exists()
        array[:].update(data)
        assert symlink.exists()

        with h5py.File(file_path) as f:
            assert np.allclose(f["data"][:], np.asarray(data), equal_nan=True)

    def test_array_adapter_doesnt_create_with_same_primary_attrs(
        self, array_with_attributes: Array, array_collection_with_attributes: Collection
    ):
        array_adapter = array_with_attributes._adapter
        array_adapter.create(array_with_attributes)
        new_array = Array(
            collection=array_collection_with_attributes,
            adapter=array_adapter,
            primary_attributes=array_with_attributes.primary_attributes,
            custom_attributes=array_with_attributes.custom_attributes,
        )
        with pytest.raises(DekerValidationError):
            assert array_adapter.create(new_array)

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
        file_path.write_text("")
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        with pytest.raises(DekerArrayError) as e:
            array_adapter.create(array)

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
        :param data: array data from options
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        filename = str(array.id) + factory.ctx.storage_adapter.file_ext
        file_path = paths.main / filename
        symlink = paths.symlink / filename
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        with pytest.raises(raised):  # type: ignore
            array_adapter.create(array)
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
        :param data: array data from options
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array)
        array[:].update(data)
        assert os.path.exists(filename)
        update_data = np.zeros(shape=(10, 10, 10))
        array_adapter.update(array, np.index_exp[:], update_data)
        with h5py.File(filename) as f:
            assert (f["data"][:] == update_data).all()

    def test_array_adapter_raises_on_update_no_data(self, root_path, array: Array, factory):
        """Tests if array adapter raises on update if data is None.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array)
        assert os.path.exists(filename)
        with pytest.raises(ValueError):
            assert array_adapter.update(array, np.index_exp[:], None)

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
        factory,
        data: Any,
    ):
        """Tests if array adapter reads array data properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        :param data: array data from options
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array)
        assert os.path.exists(filename)
        array[:].update(data)
        result = array_adapter.read_data(array, np.index_exp[:])
        assert (result == np.asarray(data)).all()

    def test_array_adapter_reads_cleared_data_from_array(
        self,
        root_path,
        array: Array,
        factory,
    ):
        """Tests if array adapter reads cleared array data properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array)
        assert os.path.exists(filename)
        result = array_adapter.read_data(array, np.index_exp[:])
        assert (np.isnan(result)).all()  # type: ignore[arg-type]
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
        factory,
        metakey: Any,
        expected: Any,
    ):
        """Tests if array adapter reads array metadata properly.

        :param root_path: temporary array_collection root path
        :param array: Pre created array
        :param metakey: key in metadata
        :param expected: expected value of metakey
        """
        coll_path = root_path / factory.ctx.config.collections_directory / array.collection
        paths = get_paths(array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = paths.main / (array.id + factory.ctx.storage_adapter.file_ext)
        array_adapter.create(array)
        assert os.path.exists(filename)
        meta = array.read_meta()
        assert isinstance(meta, dict)
        assert meta[metakey] == expected  # type: ignore[literal-required]

    def test_array_adapter_deletes_array(self, root_path, inserted_array: Array, factory):
        """Tests if array adapter deletes array properly.

        :param root_path: temporary array_collection root path
        :param inserted_array: Pre created array
        """
        coll_path = root_path / factory.ctx.config.collections_directory / inserted_array.collection
        paths = get_paths(inserted_array, coll_path)
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = paths.main / (inserted_array.id + factory.ctx.storage_adapter.file_ext)
        mainfile = paths.main / filename
        array_adapter.delete(inserted_array)
        assert not os.path.exists(paths.symlink / filename)
        assert not os.path.exists(mainfile)
        assert not paths.main.exists()

    def test_array_adapter_deletes_non_existing_array(
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
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = inserted_array.id + factory.ctx.storage_adapter.file_ext
        mainfile = paths.main / filename
        assert mainfile.exists()
        symfile = paths.symlink / filename
        assert symfile.exists()
        for f in (symfile, mainfile):
            os.remove(f)
        array_adapter.delete(inserted_array)
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
        array_adapter = factory.get_array_adapter(coll_path, storage_adapter=HDF5StorageAdapter)
        filename = paths.main / (inserted_array.id + factory.ctx.storage_adapter.file_ext)
        assert filename.exists()
        array_adapter.clear(inserted_array, np.index_exp[:])
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
        main_tree = os.path.sep.join((s for s in main_tree))
        main_path = (
            array_with_attributes._adapter.collection_path
            / array_with_attributes._adapter.data_dir
            / main_tree
            / rest
        )

        sym_path = (
            array_with_attributes._adapter.collection_path
            / array_with_attributes._adapter.symlinks_dir
            / str(array_with_attributes.primary_attributes.get("primary_attribute"))
        )

        adapter_paths = get_paths(
            array_with_attributes, array_with_attributes._adapter.collection_path
        )

        assert adapter_paths.main == main_path
        assert adapter_paths.symlink == sym_path

    def test_array_adapter_update_custom_attributes(self, array_with_attributes: Array):
        new_custom_attributes = {
            "custom_attribute": 0.6,
            "time_attr_name": datetime.now(timezone.utc),
        }
        adapter = array_with_attributes._adapter
        adapter.create(array_with_attributes)
        adapter.update_meta_custom_attributes(array_with_attributes, new_custom_attributes)
        assert array_with_attributes.custom_attributes == new_custom_attributes
        meta = array_with_attributes.read_meta()
        ar = create_array_from_meta(
            array_with_attributes._Array__collection,  # type: ignore[attr-defined]
            meta,  # type: ignore[arg-type]
            array_with_attributes._adapter,
        )
        assert ar.custom_attributes == new_custom_attributes

    def test_array_adapter_iter(self, array_collection: Collection):
        arrays = [array_collection.create() for _ in range(10)]
        metas = [json.loads(array._create_meta()) for array in arrays]
        adapter: LocalArrayAdapter = arrays[0]._adapter
        inner_metas = [meta for meta in adapter]
        assert len(inner_metas) == len(metas)
        assert all(inner_meta in metas for inner_meta in inner_metas)

    def test_array_clear_with_fill_value(self, client: Client):
        schema = ArraySchema(dtype=int, dimensions=[DimensionSchema(name="x", size=2)])
        coll = client.create_collection("test", schema=schema)
        array = coll.create()
        try:
            data = np.asarray([np.iinfo(schema.dtype).min] * 2).reshape(2)  # type: ignore[arg-type,type-var]
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


if __name__ == "__main__":
    pytest.main()

import json
import os

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from deker_local_adapters import LocalVArrayAdapter
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from deker_tools.path import is_empty

from deker.arrays import VArray
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerValidationError
from deker.tools import get_paths


class TestVArrayAdapterMethods:
    def test_varray_adapter_init(self, array_collection, ctx, executor):
        adapter = LocalVArrayAdapter(array_collection.path, ctx, HDF5StorageAdapter, executor)
        assert adapter

    def test_varray_adapter_create_varray(self, varray: VArray):
        find = varray._VArray__collection.filter({"id": varray.id})
        array = find.first()
        assert array is None
        adapter = varray._adapter
        assert adapter.create(varray) is varray
        array = find.first()
        assert array is not None

    def test_varray_adapter_create_varray_fail_on_file_exists(self, varray: VArray):
        adapter = varray._adapter
        adapter.create(varray)
        with pytest.raises(DekerArrayError):
            assert adapter.create(varray)

    def test_varray_adapter_leaves_a_foreign_main_file_untouched(self, varray: VArray):
        adapter = varray._adapter
        paths = get_paths(varray, adapter.collection_path)
        paths.create()
        filename = varray.id + adapter.file_ext
        (paths.main / filename).write_text("foreign")
        with pytest.raises(DekerArrayError) as e:
            adapter.create(varray)
        assert "already exists" in str(e.value)
        assert (paths.main / filename).read_text() == "foreign"
        assert not (paths.symlink / filename).exists()

    def test_varray_adapter_delete_varray(self, varray: VArray):
        """Test delete without inner arrays."""
        find = varray._VArray__collection.filter({"id": varray.id})
        array = find.first()
        assert array is None
        adapter = varray._adapter
        adapter.create(varray)
        array = find.first()
        assert array is not None
        paths = get_paths(varray, varray._VArray__collection.path)
        adapter.delete(varray)
        assert find.first() is None
        assert not (paths.symlink / array.id).exists()
        assert not (paths.main / array.id).exists()
        assert not paths.main.exists()

    def test_varray_adapter_clears_varray_directories(self, varray: VArray, ctx):
        """Test delete without inner arrays."""
        find = varray._VArray__collection.filter({"id": varray.id})
        array = find.first()
        assert array is None
        adapter = varray._adapter
        adapter.create(varray)
        array = find.first()
        assert array is not None

        for exp in (np.index_exp[:5, :5, :5], np.index_exp[5:, 5:, 5:]):
            vsubset = array[exp]
            update_data = np.ones(shape=(5, 5, 5), dtype=array.dtype)
            vsubset.update(update_data)

        paths = get_paths(varray, varray._VArray__collection.path)
        adapter.delete(varray)
        assert find.first() is None
        assert not (paths.symlink / array.id).exists()
        assert not (paths.main / array.id).exists()
        assert not paths.main.exists()
        assert not (varray._VArray__collection.path / ctx.config.array_data_directory / array.id).exists()

        assert not is_empty(varray._VArray__collection.path / ctx.config.array_symlinks_directory)

    def test_varray_adapter_delete_varray_and_arrays(self, varray: VArray, ctx):
        """Test delete varray with all its contents."""
        find = varray._VArray__collection.filter({"id": varray.id})
        array = find.first()
        assert array is None
        adapter = varray._adapter
        adapter.create(varray)
        array = find.first()
        assert array is not None

        for exp in (np.index_exp[:5, :5, :5], np.index_exp[5:, 5:, 5:]):
            vsubset = array[exp]
            update_data = np.ones(shape=(5, 5, 5), dtype=array.dtype)
            vsubset.update(update_data)

        paths = get_paths(varray, varray._VArray__collection.path)
        array.delete()
        assert find.first() is None
        assert not (paths.symlink / array.id).exists()
        assert not (paths.main / array.id).exists()
        assert not paths.main.exists()
        assert not (varray._VArray__collection.path / ctx.config.array_data_directory / array.id).exists()

        assert is_empty(varray._VArray__collection.path / ctx.config.array_symlinks_directory)

    def test_varray_adapter_read_meta_from_varray(self, varray: VArray):
        """Test read meta."""
        adapter: LocalVArrayAdapter = varray._adapter
        adapter.create(varray)
        meta = adapter.read_meta(varray)
        assert meta
        assert meta["id"] == varray.id

    def test_varray_adapter_read_meta_from_varray_path(self, varray: VArray):
        """Test __iter__."""
        adapter: LocalVArrayAdapter = varray._adapter
        adapter.create(varray)
        paths = get_paths(varray, varray._VArray__collection.path)
        meta = adapter.read_meta(paths.main / (varray.id + adapter.file_ext))
        assert meta
        assert meta["id"] == varray.id

    def test_varray_adapter_update_custom_attributes(self, varray_with_attributes: VArray):
        new_custom_attributes = {
            "custom_attribute": 0.6,
            "time_attr_name": datetime.now(timezone.utc),
        }
        adapter: LocalVArrayAdapter = varray_with_attributes._adapter
        adapter.create(varray_with_attributes)
        adapter.update_meta_custom_attributes(varray_with_attributes, new_custom_attributes)
        assert varray_with_attributes.custom_attributes == new_custom_attributes
        meta = varray_with_attributes.read_meta()
        ar = VArray._create_from_meta(
            varray_with_attributes._VArray__collection,
            meta,
            varray_adapter=adapter,
            array_adapter=varray_with_attributes._VArray__array_adapter,
        )
        assert ar.custom_attributes == new_custom_attributes

    def test_varray_adapter_update_custom_attributes_replaces_the_file_in_one_step(
        self, varray_with_attributes: VArray, mocker
    ):
        """The new metadata is written next to the json file and moved over it in one step.

        A reader of the metadata never meets a half-written file.
        """
        adapter: LocalVArrayAdapter = varray_with_attributes._adapter
        adapter.create(varray_with_attributes)
        main = get_paths(varray_with_attributes, varray_with_attributes._VArray__collection.path).main / (
            varray_with_attributes.id + adapter.file_ext
        )
        replace = mocker.spy(os, "replace")
        adapter.update_meta_custom_attributes(varray_with_attributes, {"custom_attribute": 0.7})
        replace.assert_called_once()
        source, target = replace.call_args.args
        assert Path(target) == main
        assert Path(source).parent == main.parent
        assert not Path(source).exists()
        assert adapter.read_meta(varray_with_attributes)["custom_attributes"]["custom_attribute"] == 0.7

    def test_varray_adapter_update_custom_attributes_keeps_the_file_when_the_write_fails(
        self, varray_with_attributes: VArray, mocker
    ):
        """A failed update leaves the old metadata readable and no temporary file behind."""
        adapter: LocalVArrayAdapter = varray_with_attributes._adapter
        adapter.create(varray_with_attributes)
        before = adapter.read_meta(varray_with_attributes)
        main = get_paths(varray_with_attributes, varray_with_attributes._VArray__collection.path).main / (
            varray_with_attributes.id + adapter.file_ext
        )
        mocker.patch.object(os, "replace", side_effect=OSError("disk is full"))
        with pytest.raises(OSError, match="disk is full"):
            adapter.update_meta_custom_attributes(varray_with_attributes, {"custom_attribute": 0.7})
        assert adapter.read_meta(varray_with_attributes) == before
        assert [path.name for path in main.parent.iterdir()] == [main.name]


class TestActions:
    def test_varray_adapter_clear(self, root_path, inserted_varray: VArray, ctx):
        """Tests clear method is not implemented.

        :param root_path: temporary collection root path
        :param inserted_varray: Pre created VArray.
        """
        varray_adapter = inserted_varray._adapter
        paths = get_paths(
            inserted_varray,
            root_path / ctx.config.collections_directory / inserted_varray.collection,
        )
        filename = paths.main / (str(inserted_varray.id) + varray_adapter.file_ext)
        assert filename.exists()
        with pytest.raises(NotImplementedError):
            varray_adapter.clear(inserted_varray, np.index_exp[:])

    def test_varray_adapter_read_data(self, root_path, inserted_varray: VArray, ctx):
        """Tests read data method is not implemented.

        :param root_path: temporary collection root path
        :param inserted_varray: Pre created VArray.
        """
        varray_adapter = inserted_varray._adapter
        paths = get_paths(
            inserted_varray,
            root_path / ctx.config.collections_directory / inserted_varray.collection,
        )
        filename = paths.main / (inserted_varray.id + varray_adapter.file_ext)
        assert filename.exists()
        with pytest.raises(NotImplementedError):
            assert varray_adapter.read_data(inserted_varray, np.index_exp[:])

    def test_varray_adapter_update(self, root_path, inserted_varray: VArray, ctx):
        """Tests update method is not implemented.

        :param root_path: temporary collection root path
        :param inserted_varray: Pre created VArray.
        """
        varray_adapter = inserted_varray._adapter
        paths = get_paths(
            inserted_varray,
            root_path / ctx.config.collections_directory / inserted_varray.collection,
        )
        filename = paths.main / (inserted_varray.id + varray_adapter.file_ext)
        assert filename.exists()
        with pytest.raises(NotImplementedError):
            varray_adapter.update(inserted_varray, np.index_exp[:], None)

    def test_varray_adapter_iter(self, varray_collection: Collection):
        varrays = [varray_collection.create() for _ in range(10)]
        metas = [json.loads(varray._create_meta()) for varray in varrays]
        adapter: LocalVArrayAdapter = varrays[0]._adapter
        inner_metas = [*adapter]
        assert len(inner_metas) == len(metas)
        assert all(inner_meta in metas for inner_meta in inner_metas)

    def test_array_adapter_doesnt_create_with_same_primary_attrs(
        self,
        varray_with_attributes: VArray,
        varray_collection_with_attributes: Collection,
    ):
        varray_adapter = varray_with_attributes._adapter
        array_adapter = varray_with_attributes._VArray__array_adapter
        varray_adapter.create(varray_with_attributes)
        new_array = VArray(
            collection=varray_collection_with_attributes,
            adapter=varray_adapter,
            array_adapter=array_adapter,
            primary_attributes=varray_with_attributes.primary_attributes,
            custom_attributes=varray_with_attributes.custom_attributes,
        )
        with pytest.raises(DekerValidationError):
            assert varray_adapter.create(new_array)


if __name__ == "__main__":
    pytest.main()

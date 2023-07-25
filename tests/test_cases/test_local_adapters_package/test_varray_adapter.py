import json

from datetime import datetime, timezone

import numpy as np
import pytest

from deker_local_adapters import LocalVArrayAdapter
from deker_tools.path import is_empty

from deker.arrays import VArray
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerValidationError
from deker.tools import create_array_from_meta, get_paths


class TestVArrayAdapterMethods:
    def test_varray_adapter_init(self, array_collection, ctx, executor):
        adapter = LocalVArrayAdapter(
            array_collection.path, ctx, array_collection._storage_adapter, executor
        )
        assert adapter

    def test_varray_adapter_create_varray(self, varray: VArray):
        find = varray._VArray__collection.filter({"id": varray.id})  # type: ignore[attr-defined]
        array = find.first()
        assert array is None
        adapter = varray._adapter
        adapter.create(varray)
        array = find.first()
        assert array is not None

    def test_varray_adapter_create_varray_fail_on_file_exists(self, varray: VArray):
        adapter = varray._adapter
        adapter.create(varray)
        with pytest.raises(DekerArrayError):
            assert adapter.create(varray)

    def test_varray_adapter_delete_varray(self, varray: VArray):
        """Test delete without inner arrays."""
        find = varray._VArray__collection.filter({"id": varray.id})  # type: ignore[attr-defined]
        array = find.first()
        assert array is None
        adapter = varray._adapter
        adapter.create(varray)
        array = find.first()
        assert array is not None
        paths = get_paths(varray, varray._VArray__collection.path)  # type: ignore[attr-defined]
        adapter.delete(varray)
        assert find.first() is None
        assert not (paths.symlink / array.id).exists()
        assert not (paths.main / array.id).exists()
        assert not paths.main.exists()

    def test_varray_adapter_clears_varray_directories(self, varray: VArray, ctx):
        """Test delete without inner arrays."""
        find = varray._VArray__collection.filter({"id": varray.id})  # type: ignore[attr-defined]
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

        paths = get_paths(varray, varray._VArray__collection.path)  # type: ignore[attr-defined]
        adapter.delete(varray)
        assert find.first() is None
        assert not (paths.symlink / array.id).exists()
        assert not (paths.main / array.id).exists()
        assert not paths.main.exists()
        assert not (
            varray._VArray__collection.path / ctx.config.array_data_directory / array.id
        ).exists()

        assert not is_empty(
            varray._VArray__collection.path
            / ctx.config.array_symlinks_directory
            # type: ignore[attr-defined]
        )

    def test_varray_adapter_delete_varray_and_arrays(self, varray: VArray, ctx):
        """Test delete varray with all its contents."""
        find = varray._VArray__collection.filter({"id": varray.id})  # type: ignore[attr-defined]
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

        paths = get_paths(varray, varray._VArray__collection.path)  # type: ignore[attr-defined]
        array.delete()
        assert find.first() is None
        assert not (paths.symlink / array.id).exists()
        assert not (paths.main / array.id).exists()
        assert not paths.main.exists()
        assert not (
            varray._VArray__collection.path / ctx.config.array_data_directory / array.id
        ).exists()

        assert is_empty(
            varray._VArray__collection.path
            / ctx.config.array_symlinks_directory
            # type: ignore[attr-defined]
        )

    def test_varray_adapter_read_meta_from_varray(self, varray: VArray):
        """Test read meta."""
        adapter: LocalVArrayAdapter = varray._adapter  # type: ignore[attr-defined]
        adapter.create(varray)
        meta = adapter.read_meta(varray)
        assert meta
        assert meta["id"] == varray.id

    def test_varray_adapter_read_meta_from_varray_path(self, varray: VArray):
        """Test __iter__."""
        adapter: LocalVArrayAdapter = varray._adapter  # type: ignore[attr-defined]
        adapter.create(varray)
        paths = get_paths(varray, varray._VArray__collection.path)  # type: ignore[attr-defined]
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
        ar = create_array_from_meta(
            varray_with_attributes._VArray__collection,  # type: ignore[attr-defined]
            meta,  # type: ignore[arg-type]
            varray_adapter=adapter,
            array_adapter=varray_with_attributes._VArray__array_adapter,
            # type: ignore[attr-defined]
        )
        assert ar.custom_attributes == new_custom_attributes


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
        inner_metas = [meta for meta in adapter]
        assert len(inner_metas) == len(metas)
        assert all(inner_meta in metas for inner_meta in inner_metas)

    def test_array_adapter_doesnt_create_with_same_primary_attrs(
        self,
        varray_with_attributes: VArray,
        varray_collection_with_attributes: Collection,
    ):
        varray_adapter = varray_with_attributes._adapter
        array_adapter = varray_with_attributes._VArray__array_adapter  # type: ignore[attr-defined]
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

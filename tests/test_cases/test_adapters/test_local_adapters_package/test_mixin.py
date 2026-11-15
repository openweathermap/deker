import os

import pytest

from deker_local_adapters import LocalArrayAdapter
from deker_local_adapters.errors import DekerBrokenSymlinkError
from deker_local_adapters.varray_adapter import LocalVArrayAdapter

from deker.arrays import VArray
from deker.collection import Collection
from deker.tools.path import get_symlink_path


@pytest.mark.parametrize("adapter_", ["local_array_adapter", "local_varray_adapter"])
def test_broken_symlink(adapter_: str, request, local_array_adapter, local_varray_adapter):
    adapter = request.getfixturevalue(adapter_)
    if isinstance(adapter, LocalVArrayAdapter):
        collection = request.getfixturevalue("varray_collection")
        array = request.getfixturevalue("varray_with_attributes")
        schema = request.getfixturevalue("varray_schema_with_attributes")
        symlink_dir = "varray_symlinks"
    else:
        collection = request.getfixturevalue("array_collection")
        array = request.getfixturevalue("array_with_attributes")
        schema = request.getfixturevalue("array_schema_with_attributes")
        symlink_dir = "array_symlinks"

    symlinks_path = get_symlink_path(
        path_to_symlink_dir=collection.path / symlink_dir,
        primary_attributes_schema=schema.primary_attributes,
        primary_attributes=array.primary_attributes,
    )
    symlinks_path.mkdir(parents=True, exist_ok=True)
    path_for_file = symlinks_path / "test.txt"

    path_for_file.write_text("foo")

    os.symlink(path_for_file, symlinks_path / "symlink")
    path_for_file.unlink()
    with pytest.raises(DekerBrokenSymlinkError):
        adapter.get_by_primary_attributes(
            array.primary_attributes,
            schema,
            collection,
            local_array_adapter,
            local_varray_adapter if isinstance(adapter, LocalVArrayAdapter) else None,
        )


class TestMixinEdges:
    def test_get_varray_by_id(self, varray_collection: Collection, inserted_varray: VArray):
        found = varray_collection.filter({"id": inserted_varray.id}).first()
        assert isinstance(found, VArray)
        assert found.id == inserted_varray.id

    def test_get_varray_by_primary_attributes(
        self, varray_collection_with_attributes: Collection, inserted_varray_with_attributes: VArray
    ):
        found = varray_collection_with_attributes.filter(inserted_varray_with_attributes.primary_attributes).last()
        assert isinstance(found, VArray)
        assert found.id == inserted_varray_with_attributes.id

    def test_get_array_by_id(self, array_collection: Collection, inserted_array):
        found = array_collection.filter({"id": inserted_array.id}).first()
        assert found.id == inserted_array.id

    def test_subdirectory_in_symlink_directory_is_skipped(
        self,
        local_array_adapter: LocalArrayAdapter,
        array_collection: Collection,
        array_schema_with_attributes,
        array_with_attributes,
    ):
        from deker.tools.path import get_symlink_path

        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection.path / local_array_adapter.symlinks_dir,
            primary_attributes_schema=array_schema_with_attributes.primary_attributes,
            primary_attributes=array_with_attributes.primary_attributes,
        )
        (symlinks_path / "subdir").mkdir(parents=True)
        assert (
            local_array_adapter.get_by_primary_attributes(
                array_with_attributes.primary_attributes,
                array_schema_with_attributes,
                array_collection,
                local_array_adapter,
                None,
            )
            is None
        )

    def test_delete_when_the_main_directory_is_already_gone(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        import shutil

        from deker.tools import get_paths

        paths = get_paths(inserted_array, array_collection.path)
        shutil.rmtree(paths.main)
        local_array_adapter.delete(inserted_array)
        assert not (paths.symlink / (inserted_array.id + local_array_adapter.file_ext)).exists()

    def test_empty_symlink_directory_finds_nothing(
        self,
        local_array_adapter: LocalArrayAdapter,
        array_collection: Collection,
        array_schema_with_attributes,
        array_with_attributes,
    ):
        from deker.tools.path import get_symlink_path

        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection.path / local_array_adapter.symlinks_dir,
            primary_attributes_schema=array_schema_with_attributes.primary_attributes,
            primary_attributes=array_with_attributes.primary_attributes,
        )
        symlinks_path.mkdir(parents=True, exist_ok=True)
        assert (
            local_array_adapter.get_by_primary_attributes(
                array_with_attributes.primary_attributes,
                array_schema_with_attributes,
                array_collection,
                local_array_adapter,
                None,
            )
            is None
        )

    def test_iter_skips_foreign_files(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        data_dir = array_collection.path / local_array_adapter.data_dir
        (data_dir / "stray.txt").write_text("x")
        metas = list(local_array_adapter)
        assert [meta["id"] for meta in metas] == [inserted_array.id]

    def test_delete_removes_empty_parent_directories_only(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        from deker.tools import get_paths

        paths = get_paths(inserted_array, array_collection.path)
        neighbour = paths.main.parent / "neighbour"
        neighbour.mkdir(parents=True)
        inserted_array.delete()
        assert not paths.main.exists()
        assert neighbour.exists()  # a non-empty parent is left alone
        assert (array_collection.path / local_array_adapter.data_dir).exists()

    def test_delete_of_a_symlink_without_target(
        self, local_array_adapter: LocalArrayAdapter, inserted_array, array_collection: Collection
    ):
        from deker.tools import get_paths

        paths = get_paths(inserted_array, array_collection.path)
        os.remove(paths.main / (inserted_array.id + local_array_adapter.file_ext))
        local_array_adapter.delete(inserted_array)  # the target is already gone, the symlink is removed anyway
        assert not (paths.symlink / (inserted_array.id + local_array_adapter.file_ext)).exists()

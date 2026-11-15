import os

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from pathlib import Path
from threading import Event

import pytest

from deker_local_adapters import LocalArrayAdapter
from deker_local_adapters.errors import DekerBrokenSymlinkError
from deker_local_adapters.varray_adapter import LocalVArrayAdapter
from pytest_mock import MockerFixture

from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.errors import DekerLockError, DekerValidationError
from deker.flock import Flock
from deker.tools import get_paths
from deker.tools.path import get_symlink_path


class HeldCheck:
    """Replacement of ``check_symlink_existence`` that reports being inside the creation and waits to be let go.

    :param original: the replaced method
    """

    def __init__(self, original) -> None:
        self.original = original
        self.inside = Event()
        self.go = Event()

    def __call__(self, file: Path) -> None:
        self.inside.set()
        assert self.go.wait(10)
        self.original(file)


def twin(array: Array | VArray) -> Array | VArray:
    """Make another (V)Array with the same primary attributes.

    :param array: the array to copy the attributes from
    :returns: a new, not yet created, (V)Array
    """
    if isinstance(array, VArray):
        return VArray(
            array._VArray__collection,
            array._adapter,
            array._VArray__array_adapter,
            primary_attributes=array.primary_attributes,
            custom_attributes=array.custom_attributes,
        )
    return Array(
        array._Array__collection,
        array._adapter,
        primary_attributes=array.primary_attributes,
        custom_attributes=array.custom_attributes,
    )


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


@pytest.mark.parametrize("array_", ["array_with_attributes", "varray_with_attributes"])
class TestSymlinkDirectoryLock:
    """Creation of a (V)Array with primary attributes holds the directory of its symlink exclusively."""

    @pytest.mark.timeout(60)
    def test_second_creator_waits_and_is_refused(self, array_: str, request, mocker: MockerFixture):
        """The second creator of the same primary attributes waits for the first one and then learns they are taken."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        held = HeldCheck(adapter.check_symlink_existence)
        mocker.patch.object(adapter, "check_symlink_existence", held)
        other = twin(array)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(adapter.create, array)
            assert held.inside.wait(10)
            second = pool.submit(adapter.create, other)
            with pytest.raises(FutureTimeoutError):
                second.result(timeout=0.5)  # the directory is held by the first creator
            held.go.set()
            assert first.result(timeout=10) is array
            with pytest.raises(DekerValidationError):
                second.result(timeout=10)
        paths = get_paths(array, adapter.collection_path)
        assert os.listdir(paths.symlink) == [array.id + adapter.file_ext]
        assert not (get_paths(other, adapter.collection_path).main / (other.id + adapter.file_ext)).exists()

    @pytest.mark.timeout(60)
    def test_creator_gives_up_after_the_timeout(self, array_: str, request, mocker: MockerFixture):
        """A creator that cannot take the directory within the timeout fails and leaves no files."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        mocker.patch.object(adapter.ctx.config, "write_lock_timeout", 1)
        paths = get_paths(array, adapter.collection_path)
        paths.create()
        with Flock(paths.symlink), pytest.raises(DekerLockError):
            adapter.create(array)
        assert os.listdir(paths.symlink) == []
        assert not (paths.main / (array.id + adapter.file_ext)).exists()

    def test_lock_is_released_after_creation(self, array_: str, request):
        """The directory is free once the (V)Array is created."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        adapter.create(array)
        paths = get_paths(array, adapter.collection_path)
        with Flock(paths.symlink):
            pass

    def test_lock_is_released_after_a_failed_creation(self, array_: str, request):
        """The directory is free once a creator is refused."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        adapter.create(array)
        with pytest.raises(DekerValidationError):
            adapter.create(twin(array))
        with Flock(get_paths(array, adapter.collection_path).symlink):
            pass

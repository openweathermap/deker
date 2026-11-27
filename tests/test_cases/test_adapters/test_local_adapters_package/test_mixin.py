import errno
import os

from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from functools import partial
from pathlib import Path
from threading import Event

import pytest

from cantok import DefaultToken
from deker_local_adapters import LocalArrayAdapter
from deker_local_adapters.errors import DekerBrokenSymlinkError
from deker_local_adapters.varray_adapter import LocalVArrayAdapter
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from deker.arrays import Array, VArray
from deker.collection import Collection
from deker.errors import DekerFilterError, DekerLockError, DekerValidationError
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
        local_array_adapter.delete(inserted_array, timeout=Sentinel, token=DefaultToken())
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
        local_array_adapter.delete(
            inserted_array, timeout=Sentinel, token=DefaultToken()
        )  # the target is already gone, the symlink is removed anyway
        assert not (paths.symlink / (inserted_array.id + local_array_adapter.file_ext)).exists()


class TestDeleteOfNeighbouringArrays:
    """Two Arrays whose ids share the first characters share the directories of the data tree.

    Deleting them at once from two threads is a race on those directories: the one that loses the removal
    of a shared directory must still remove the directories of its own symlink.
    """

    REPEATS = 50

    @staticmethod
    def _neighbours(array_collection: Collection, local_array_adapter: LocalArrayAdapter) -> tuple[Array, Array]:
        """Create two Arrays whose main files share the directories of the data tree.

        :param array_collection: the collection
        :param local_array_adapter: the adapter of the collection
        :returns: the Arrays
        """
        arrays = tuple(
            Array(array_collection, local_array_adapter, id_=f"8c{tail}0000-0000-4000-8000-{n:012d}")
            for n, tail in enumerate(("aa", "bb"))
        )
        for array in arrays:
            local_array_adapter.create(array, timeout=Sentinel, token=DefaultToken())
        return arrays

    @staticmethod
    def _directories_left(array_collection: Collection, local_array_adapter: LocalArrayAdapter) -> list[Path]:
        """List every directory under the data and the symlinks directories of the collection.

        :param array_collection: the collection
        :param local_array_adapter: the adapter of the collection
        :returns: the directories, empty when the deletion was clean
        """
        return [
            Path(dirpath) / name
            for directory in (local_array_adapter.data_dir, local_array_adapter.symlinks_dir)
            for dirpath, dirnames, _ in os.walk(array_collection.path / directory)
            for name in dirnames
        ]

    def test_two_threads_leave_no_directories(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter
    ):
        """Whatever the order of the two threads, nothing stays under the data and the symlinks directories."""
        with ThreadPoolExecutor(max_workers=2) as executor:
            for _ in range(self.REPEATS):
                arrays = self._neighbours(array_collection, local_array_adapter)
                list(executor.map(partial(local_array_adapter.delete, timeout=Sentinel, token=DefaultToken()), arrays))
                assert self._directories_left(array_collection, local_array_adapter) == []

    def test_a_directory_removed_by_the_neighbour_is_skipped(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """A shared directory that is gone by the time of rmdir was removed by the other thread: the climb goes on."""
        first, second = self._neighbours(array_collection, local_array_adapter)
        shared = array_collection.path / local_array_adapter.data_dir / "8" / "c"
        local_array_adapter.delete(first, timeout=Sentinel, token=DefaultToken())
        assert shared.exists()
        real_rmdir = os.rmdir

        def rmdir_lost_to_the_neighbour(path: str | os.PathLike, **kwargs: object) -> None:
            real_rmdir(path, **kwargs)
            if Path(path) == shared:
                raise FileNotFoundError(2, "the neighbour was first", str(path))

        mocker.patch("deker_local_adapters.mixin.os.rmdir", rmdir_lost_to_the_neighbour)
        local_array_adapter.delete(second, timeout=Sentinel, token=DefaultToken())
        assert self._directories_left(array_collection, local_array_adapter) == []

    def test_a_directory_filled_meanwhile_is_left(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """A directory that is not empty anymore at rmdir (something was created in it) stays, without an error."""
        first, second = self._neighbours(array_collection, local_array_adapter)
        shared = array_collection.path / local_array_adapter.data_dir / "8" / "c"
        local_array_adapter.delete(first, timeout=Sentinel, token=DefaultToken())
        real_rmdir = os.rmdir

        def rmdir_of_a_filled_directory(path: str | os.PathLike, **kwargs: object) -> None:
            if Path(path) == shared:
                (shared / "newcomer").mkdir()
                raise OSError(errno.ENOTEMPTY, "Directory not empty", str(path))
            real_rmdir(path, **kwargs)

        mocker.patch("deker_local_adapters.mixin.os.rmdir", rmdir_of_a_filled_directory)
        local_array_adapter.delete(second, timeout=Sentinel, token=DefaultToken())
        assert (shared / "newcomer").exists()
        left = [shared.parent, shared, shared / "newcomer"]
        assert self._directories_left(array_collection, local_array_adapter) == left
        symlink = array_collection.path / local_array_adapter.symlinks_dir / (second.id + local_array_adapter.file_ext)
        assert not symlink.exists()

    def test_other_errors_of_rmdir_are_raised(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """Any other failure of rmdir is an error of the deletion, as before."""
        array = array_collection.create()
        mocker.patch("deker_local_adapters.mixin.os.rmdir", side_effect=PermissionError(errno.EACCES, "no"))
        with pytest.raises(PermissionError):
            local_array_adapter.delete(array, timeout=Sentinel, token=DefaultToken())

    def test_a_data_file_removed_meanwhile_is_not_an_error(
        self, array_collection: Collection, local_array_adapter: LocalArrayAdapter, mocker: MockerFixture
    ):
        """The data file may disappear between the existence check and the removal: the symlink is still removed."""
        array = array_collection.create()
        paths = get_paths(array, array_collection.path)
        file = array.id + local_array_adapter.file_ext
        real_remove = os.remove

        def remove_of_a_file_gone_meanwhile(path: str | os.PathLike, *args: object, **kwargs: object) -> None:
            real_remove(path, *args, **kwargs)
            raise FileNotFoundError(2, "gone meanwhile", str(path))

        mocker.patch("deker_local_adapters.mixin.os.remove", remove_of_a_file_gone_meanwhile)
        local_array_adapter.delete(array, timeout=Sentinel, token=DefaultToken())
        assert not (paths.main / file).exists()
        assert not (paths.symlink / file).exists()
        assert self._directories_left(array_collection, local_array_adapter) == []


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
            first = pool.submit(adapter.create, array, timeout=Sentinel, token=DefaultToken())
            assert held.inside.wait(10)
            second = pool.submit(adapter.create, other, timeout=Sentinel, token=DefaultToken())
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
            adapter.create(array, timeout=Sentinel, token=DefaultToken())
        assert os.listdir(paths.symlink) == []
        assert not (paths.main / (array.id + adapter.file_ext)).exists()

    def test_lock_is_released_after_creation(self, array_: str, request):
        """The directory is free once the (V)Array is created."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        adapter.create(array, timeout=Sentinel, token=DefaultToken())
        paths = get_paths(array, adapter.collection_path)
        with Flock(paths.symlink):
            pass

    def test_lock_is_released_after_a_failed_creation(self, array_: str, request):
        """The directory is free once a creator is refused."""
        array = request.getfixturevalue(array_)
        adapter = array._adapter
        adapter.create(array, timeout=Sentinel, token=DefaultToken())
        with pytest.raises(DekerValidationError):
            adapter.create(twin(array), timeout=Sentinel, token=DefaultToken())
        with Flock(get_paths(array, adapter.collection_path).symlink):
            pass


class TestGetIdByPrimaryAttributes:
    """The id of an array by its primary attributes, without reading its metadata: a VArray finds its Arrays so."""

    def test_finds_the_id_of_an_array(
        self, array_collection_with_attributes: Collection, inserted_array_with_attributes: Array
    ):
        adapter = inserted_array_with_attributes._adapter
        found = adapter.get_id_by_primary_attributes(
            inserted_array_with_attributes.primary_attributes, array_collection_with_attributes.array_schema
        )
        assert found == inserted_array_with_attributes.id

    def test_finds_the_id_of_a_varray(
        self, varray_collection_with_attributes: Collection, inserted_varray_with_attributes: VArray
    ):
        adapter = inserted_varray_with_attributes._adapter
        found = adapter.get_id_by_primary_attributes(
            inserted_varray_with_attributes.primary_attributes, varray_collection_with_attributes.varray_schema
        )
        assert found == inserted_varray_with_attributes.id

    def test_finds_the_id_of_an_array_of_a_varray(self, inserted_varray: VArray):
        """The Arrays of a VArray are found by its id and the position in the vgrid."""
        subset = inserted_varray[:]
        vposition = subset._VSubset__arrays[0].vposition
        collection = inserted_varray._VArray__collection
        array = collection.arrays.filter({"vid": inserted_varray.id, "v_position": vposition}).last()
        found = subset._VSubset__array_adapter.get_id_by_primary_attributes(
            {"vid": inserted_varray.id, "v_position": vposition}, collection.array_schema
        )
        assert found == array.id

    def test_does_not_read_the_metadata(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        mocker: MockerFixture,
    ):
        adapter = inserted_array_with_attributes._adapter
        read_meta = mocker.spy(adapter, "read_meta")
        storage_read_meta = mocker.spy(adapter.storage_adapter, "read_meta")
        adapter.get_id_by_primary_attributes(
            inserted_array_with_attributes.primary_attributes, array_collection_with_attributes.array_schema
        )
        assert read_meta.call_count == 0
        assert storage_read_meta.call_count == 0

    def test_returns_none_without_an_array(
        self, array_collection_with_attributes: Collection, array_with_attributes: Array
    ):
        """An array that was never created has no symlink directory; an empty directory is the same."""
        adapter = array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        attributes = array_with_attributes.primary_attributes
        assert adapter.get_id_by_primary_attributes(attributes, schema) is None
        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection_with_attributes.path / adapter.symlinks_dir,
            primary_attributes_schema=schema.primary_attributes,
            primary_attributes=attributes,
        )
        symlinks_path.mkdir(parents=True, exist_ok=True)
        assert adapter.get_id_by_primary_attributes(attributes, schema) is None

    def test_returns_none_after_the_array_is_deleted(
        self, array_collection_with_attributes: Collection, inserted_array_with_attributes: Array
    ):
        adapter = inserted_array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        attributes = inserted_array_with_attributes.primary_attributes
        inserted_array_with_attributes.delete()
        assert adapter.get_id_by_primary_attributes(attributes, schema) is None

    def test_a_broken_symlink_raises(self, array_collection_with_attributes: Collection, array_with_attributes: Array):
        adapter = array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        symlinks_path = get_symlink_path(
            path_to_symlink_dir=array_collection_with_attributes.path / adapter.symlinks_dir,
            primary_attributes_schema=schema.primary_attributes,
            primary_attributes=array_with_attributes.primary_attributes,
        )
        symlinks_path.mkdir(parents=True, exist_ok=True)
        target = symlinks_path / "target.hdf5"
        target.write_text("gone")
        os.symlink(target, symlinks_path / "link.hdf5")
        target.unlink()
        with pytest.raises(DekerBrokenSymlinkError):
            adapter.get_id_by_primary_attributes(array_with_attributes.primary_attributes, schema)

    def test_finds_the_same_array_as_the_filter(
        self, array_collection_with_attributes: Collection, inserted_array_with_attributes: Array
    ):
        """Both ways of looking an array up agree: the id and the whole array found by the primary attributes."""
        adapter = inserted_array_with_attributes._adapter
        schema = array_collection_with_attributes.array_schema
        attributes = inserted_array_with_attributes.primary_attributes
        found = adapter.get_by_primary_attributes(attributes, schema, array_collection_with_attributes, adapter, None)
        assert found is not None
        assert adapter.get_id_by_primary_attributes(attributes, schema) == found.id

    def test_a_schema_without_primary_attributes_is_refused(self, array_collection: Collection, inserted_array: Array):
        """Without primary attributes nothing identifies an array: the first file of the directory is not the answer."""
        adapter = inserted_array._adapter
        with pytest.raises(DekerFilterError, match="primary attributes"):
            adapter.get_id_by_primary_attributes({}, array_collection.array_schema)

    @pytest.mark.parametrize(
        ("attributes", "message"),
        [
            pytest.param({}, "primary_attribute is missing", id="missing"),
            pytest.param({"primary_attribute": 1, "extra": 2}, "don't exist in schema: 'extra'$", id="extra"),
            pytest.param(
                {"primary_attribute": 1, "zeta": 2, "alpha": 3},
                "^Some arguments don't exist in schema: 'alpha', 'zeta'$",
                id="two_extra",
            ),
        ],
    )
    def test_incomplete_or_extra_attributes_are_refused(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        attributes: dict,
        message: str,
    ):
        adapter = inserted_array_with_attributes._adapter
        with pytest.raises(DekerFilterError, match=message):
            adapter.get_id_by_primary_attributes(attributes, array_collection_with_attributes.array_schema)


class TestGetByPrimaryAttributesChecksTheAttributes:
    """The whole-array search refuses an empty, incomplete or oversized set of attributes, like filter does."""

    def test_a_schema_without_primary_attributes_is_refused(self, array_collection: Collection, inserted_array: Array):
        adapter = inserted_array._adapter
        with pytest.raises(DekerFilterError, match="primary attributes"):
            adapter.get_by_primary_attributes({}, array_collection.array_schema, array_collection, adapter, None)

    @pytest.mark.parametrize(
        ("attributes", "message"),
        [
            pytest.param({}, "primary_attribute is missing", id="missing"),
            pytest.param({"primary_attribute": 1, "extra": 2}, "don't exist in schema: 'extra'$", id="extra"),
            pytest.param(
                {"primary_attribute": 1, "zeta": 2, "alpha": 3},
                "^Some arguments don't exist in schema: 'alpha', 'zeta'$",
                id="two_extra",
            ),
        ],
    )
    def test_incomplete_or_extra_attributes_are_refused(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        attributes: dict,
        message: str,
    ):
        adapter = inserted_array_with_attributes._adapter
        with pytest.raises(DekerFilterError, match=message):
            adapter.get_by_primary_attributes(
                attributes,
                array_collection_with_attributes.array_schema,
                array_collection_with_attributes,
                adapter,
                None,
            )

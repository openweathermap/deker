"""Direct tests of the HDF5 storage adapter: the ``empty_cells`` counter and the ``data`` dataset life cycle."""

import json

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import pytest

from deker_local_adapters.storage_adapters.hdf5.hdf5_options import HDF5CompressionOpts, HDF5Options
from deker_local_adapters.storage_adapters.hdf5.hdf5_storage_adapter import HDF5StorageAdapter
from deker_tools.slices import create_shape_from_slice
from pytest_mock import MockerFixture

from tests.parameters.index_exp_params import array_step_cases, invalid_index_params, valid_index_exp_params

from deker.errors import DekerArrayError

SHAPE = (100,)
DTYPE = np.float64
FILL = np.nan
TOTAL = 100


@pytest.fixture
def adapter() -> HDF5StorageAdapter:
    return HDF5StorageAdapter()


@pytest.fixture
def array_file(tmp_path: Path, adapter: HDF5StorageAdapter) -> Path:
    path = tmp_path / "array.hdf5"
    adapter.create(path, SHAPE, {"id": "test"})
    return path


def empty_cells(path: Path) -> int:
    with h5py.File(path, "r", locking=False) as f:
        return int(f["empty_cells"][()])


def has_data(path: Path) -> bool:
    with h5py.File(path, "r", locking=False) as f:
        return "data" in f


def real_values(path: Path) -> int:
    with h5py.File(path, "r", locking=False) as f:
        return int(np.count_nonzero(~np.isnan(f["data"][()])))


def write(adapter: HDF5StorageAdapter, path: Path, bounds, data) -> None:
    adapter.update_data(path, bounds, np.asarray(data, dtype=DTYPE), DTYPE, SHAPE, FILL, None)


class TestEmptyCellsCounter:
    def test_new_array_is_fully_empty(self, array_file):
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_first_write_counts_written_cells(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        assert empty_cells(array_file) == 50
        assert real_values(array_file) == 50

    def test_overwrite_does_not_count_twice(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        write(adapter, array_file, np.index_exp[0:50], np.arange(50) + 1)
        assert empty_cells(array_file) == 50

    def test_writing_fill_values_over_data_frees_cells(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        write(adapter, array_file, np.index_exp[0:25], np.full(25, FILL))
        assert empty_cells(array_file) == 75
        assert real_values(array_file) == 25

    def test_writing_fill_values_everywhere_deletes_dataset(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        write(adapter, array_file, np.index_exp[0:50], np.full(50, FILL))
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_writing_fill_values_into_empty_array_creates_nothing(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.full(50, FILL))
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_mixed_data_counts_only_fill_values(self, adapter, array_file):
        data = np.arange(50, dtype=DTYPE)
        data[::2] = FILL
        write(adapter, array_file, np.index_exp[0:50], data)
        assert empty_cells(array_file) == 75
        assert real_values(array_file) == 25

    def test_single_cell_write(self, adapter, array_file):
        adapter.update_data(array_file, np.index_exp[7], np.float64(1.5), DTYPE, SHAPE, FILL, None)
        assert empty_cells(array_file) == 99
        adapter.update_data(array_file, np.index_exp[7], np.float64(FILL), DTYPE, SHAPE, FILL, None)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_non_nan_fill_value(self, adapter, tmp_path):
        path = tmp_path / "zeros.hdf5"
        adapter.create(path, SHAPE, {"id": "zeros"})
        adapter.update_data(path, np.index_exp[0:10], np.arange(1, 11, dtype=DTYPE), DTYPE, SHAPE, 0.0, None)
        assert empty_cells(path) == 90
        adapter.update_data(path, np.index_exp[0:10], np.zeros(10, dtype=DTYPE), DTYPE, SHAPE, 0.0, None)
        assert empty_cells(path) == TOTAL
        assert not has_data(path)


class TestClearData:
    def test_clearing_an_empty_region_twice_keeps_the_data(self, adapter, array_file):
        """Regression: the counter used to grow blindly and the second clear deleted the live cells 25:50."""
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert empty_cells(array_file) == 75
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert empty_cells(array_file) == 75
        assert has_data(array_file)
        assert real_values(array_file) == 25

    def test_clearing_everything_piece_by_piece_deletes_dataset(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        adapter.clear_data(array_file, SHAPE, np.index_exp[25:50], FILL)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_clearing_the_whole_array_deletes_dataset(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        adapter.clear_data(array_file, SHAPE, np.index_exp[:], FILL)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)

    def test_clearing_an_array_without_data_is_a_no_op(self, adapter, array_file):
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert empty_cells(array_file) == TOTAL
        assert not has_data(array_file)


class TestDriftedCounter:
    """Files written by older versions may carry a wrong counter; the deletion must never trust it blindly."""

    @staticmethod
    def set_counter(path: Path, value: int) -> None:
        with h5py.File(path, "r+", locking=False) as f:
            f["empty_cells"][()] = value

    def test_overcounted_counter_does_not_delete_live_data_on_clear(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        self.set_counter(array_file, 90)  # true value is 50
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:25], FILL)
        assert has_data(array_file)
        assert real_values(array_file) == 25
        assert empty_cells(array_file) == 75  # corrected by the scan

    def test_overcounted_counter_does_not_delete_live_data_on_update(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        self.set_counter(array_file, 95)  # true value is 50
        write(adapter, array_file, np.index_exp[0:10], np.full(10, FILL))
        assert has_data(array_file)
        assert real_values(array_file) == 40
        assert empty_cells(array_file) == 60  # corrected by the scan

    def test_undercounted_counter_stays_safe(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        self.set_counter(array_file, 0)  # true value is 50
        adapter.clear_data(array_file, SHAPE, np.index_exp[0:50], FILL)
        assert has_data(array_file)  # the counter says 50, the scan is not even reached
        assert empty_cells(array_file) == 50
        assert real_values(array_file) == 0


class TestStorageAdapterBrokenFiles:
    @pytest.fixture
    def file_without_meta(self, tmp_path: Path) -> Path:
        path = tmp_path / "broken.hdf5"
        HDF5StorageAdapter().create(path, (10,), {"id": "broken"})
        with h5py.File(path, "r+", locking=False) as f:
            del f["meta"]
        return path

    def test_read_meta_raises_without_meta(self, file_without_meta):
        with pytest.raises(DekerArrayError, match="No metadata"):
            HDF5StorageAdapter().read_meta(file_without_meta)

    def test_update_meta_raises_without_meta(self, file_without_meta):
        with pytest.raises(DekerArrayError, match="No metadata"):
            HDF5StorageAdapter().update_meta_custom_attributes(file_without_meta, {"a": 1})


class TestEmptyArrayRead:
    """``read_data`` on an array without a ``data`` dataset allocates only the requested region."""

    def test_slice_returns_fill_values_of_subset_shape(self, adapter, array_file):
        data = adapter.read_data(array_file, SHAPE, np.index_exp[10:30], FILL, DTYPE)
        assert data.shape == (20,)
        assert data.dtype == DTYPE
        assert np.all(np.isnan(data))

    def test_full_read_returns_array_shape(self, adapter, array_file):
        data = adapter.read_data(array_file, SHAPE, np.index_exp[...], FILL, DTYPE)
        assert data.shape == SHAPE

    def test_integer_index_returns_scalar_like_a_stored_dataset(self, adapter, array_file):
        empty = adapter.read_data(array_file, SHAPE, np.index_exp[5], FILL, DTYPE)
        write(adapter, array_file, np.index_exp[0:50], np.arange(50))
        stored = adapter.read_data(array_file, SHAPE, np.index_exp[5], FILL, DTYPE)
        assert type(empty) is type(stored)
        assert np.ndim(empty) == 0
        assert np.isnan(empty)

    def test_non_nan_fill_value(self, adapter, tmp_path):
        path = tmp_path / "ints.hdf5"
        adapter.create(path, (4, 4), {"id": "ints"})
        data = adapter.read_data(path, (4, 4), np.index_exp[1:3, :], -1, np.int32)
        assert data.shape == (2, 4)
        assert data.dtype == np.int32
        assert np.all(data == -1)


class TestDatasetCreation:
    """The first write creates the dataset with a fill value instead of a full-size in-memory array."""

    def test_dataset_carries_fill_value(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:10], np.arange(10))
        with h5py.File(array_file, "r", locking=False) as f:
            assert np.isnan(f["data"].fillvalue)

    def test_unwritten_region_reads_as_fill_value(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[10:20], np.arange(10))
        data = adapter.read_data(array_file, SHAPE, np.index_exp[...], FILL, DTYPE)
        assert np.array_equal(data[10:20], np.arange(10))
        assert np.all(np.isnan(data[:10]))
        assert np.all(np.isnan(data[20:]))

    def test_non_nan_fill_value_is_stored_in_dataset(self, adapter, tmp_path):
        path = tmp_path / "ints.hdf5"
        shape = (10, 10)
        adapter.create(path, shape, {"id": "ints"})
        adapter.update_data(path, np.index_exp[0, 0:5], np.arange(5, dtype=np.int32), np.int32, shape, -1, None)
        with h5py.File(path, "r", locking=False) as f:
            assert f["data"].fillvalue == -1
            stored = f["data"][()]
        assert np.array_equal(stored[0, 0:5], np.arange(5))
        assert np.count_nonzero(stored == -1) == 95

    def test_chunked_dataset_materialises_only_written_chunks(self, adapter, tmp_path):
        path = tmp_path / "chunked.hdf5"
        shape = (1000, 1000)
        adapter.create(path, shape, {"id": "chunked"})
        options = HDF5Options(
            chunks=(100, 100),
            compression_opts=HDF5CompressionOpts(compression="gzip", compression_opts=1),
        )
        adapter.update_data(path, np.index_exp[0:100, 0:100], np.ones((100, 100)), DTYPE, shape, FILL, options)
        with h5py.File(path, "r", locking=False) as f:
            ds = f["data"]
            assert ds.chunks == (100, 100)
            assert ds.compression == "gzip"
            # one chunk out of a hundred is written: storage is a fraction of the full array
            assert ds.id.get_storage_size() < ds.nbytes / 10
            assert np.all(ds[0:100, 0:100] == 1)
            assert np.all(np.isnan(ds[100:200, 100:200]))

    def test_scalar_data_is_broadcast_on_creation(self, adapter, array_file):
        adapter.update_data(array_file, np.index_exp[0:10], np.asarray(7.0), DTYPE, SHAPE, FILL, None)
        assert real_values(array_file) == 10
        assert empty_cells(array_file) == 90


class TestFileCreation:
    """``create`` makes the file in one step: an existing file is refused, never reopened or truncated."""

    def test_existing_file_is_refused_and_left_untouched(self, adapter, tmp_path):
        path = tmp_path / "taken.hdf5"
        path.write_bytes(b"foreign")
        with pytest.raises(FileExistsError):
            adapter.create(path, SHAPE, {"id": "test"})
        assert path.read_bytes() == b"foreign"

    def test_created_file_is_refused_on_the_second_creation(self, adapter, array_file):
        with pytest.raises(FileExistsError):
            adapter.create(array_file, SHAPE, {"id": "other"})
        with h5py.File(array_file, "r", locking=False) as f:
            assert json.loads(f["meta"][()]) == {"id": "test"}


class TestEmptyCellsCounterAccess:
    """The counter of empty cells is read, counted from the data and written for the integrity check."""

    def test_new_file_counter_and_data_agree(self, adapter, array_file):
        assert adapter.read_empty_cells(array_file) == TOTAL
        assert adapter.count_empty_cells(array_file, SHAPE, FILL) == TOTAL

    def test_written_file_counter_and_data_agree(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:30], np.ones(30))
        assert adapter.read_empty_cells(array_file) == TOTAL - 30
        assert adapter.count_empty_cells(array_file, SHAPE, FILL) == TOTAL - 30

    @pytest.mark.parametrize("stored", [0, 5, 1000])
    def test_drifted_counter_is_read_as_stored_and_counted_from_the_data(self, adapter, array_file, stored: int):
        write(adapter, array_file, np.index_exp[0:30], np.ones(30))
        with h5py.File(array_file, "r+", locking=False) as f:
            f["empty_cells"][()] = stored
        assert adapter.read_empty_cells(array_file) == stored
        assert adapter.count_empty_cells(array_file, SHAPE, FILL) == TOTAL - 30

    def test_write_empty_cells_replaces_the_counter_only(self, adapter, array_file):
        write(adapter, array_file, np.index_exp[0:30], np.ones(30))
        adapter.write_empty_cells(array_file, 42)
        assert adapter.read_empty_cells(array_file) == 42
        assert real_values(array_file) == 30  # the data is untouched

    def test_non_nan_fill_value_is_counted(self, adapter, tmp_path):
        path = tmp_path / "zeros.hdf5"
        adapter.create(path, SHAPE, {"id": "test"})
        adapter.update_data(path, np.index_exp[0:10], np.ones(10), DTYPE, SHAPE, 0.0, None)
        assert adapter.count_empty_cells(path, SHAPE, 0.0) == TOTAL - 10

    def test_file_without_counter_raises(self, adapter, array_file):
        with h5py.File(array_file, "r+", locking=False) as f:
            del f["empty_cells"]
        with pytest.raises(DekerArrayError):
            adapter.read_empty_cells(array_file)


class TestMetaDataset:
    """The ``meta`` dataset is sized exactly to its UTF-8 payload and survives non-ASCII content."""

    @staticmethod
    def meta_bytes(path: Path) -> tuple[bytes, int]:
        with h5py.File(path, "r", locking=False) as f:
            ds = f["meta"]
            return ds[()], ds.dtype.itemsize

    def test_created_meta_is_sized_to_payload(self, adapter, tmp_path):
        path = tmp_path / "meta.hdf5"
        meta = {"id": "test", "custom_attributes": {"name": "температура"}}
        adapter.create(path, SHAPE, meta)
        stored, itemsize = self.meta_bytes(path)
        assert stored == json.dumps(meta, default=str).encode("utf-8")
        assert itemsize == len(stored)
        assert adapter.read_meta(path) == meta

    @pytest.mark.parametrize("metadata", ['{"id": "raw"}', b'{"id": "raw"}'])
    def test_create_accepts_prepared_metadata(self, adapter, tmp_path, metadata):
        path = tmp_path / "meta.hdf5"
        adapter.create(path, SHAPE, metadata)
        assert adapter.read_meta(path) == {"id": "raw"}

    def test_updated_meta_is_sized_to_payload(self, adapter, array_file):
        attributes = {"station": "Москва", "height": 10}
        assert adapter.update_meta_custom_attributes(array_file, attributes) == attributes
        stored, itemsize = self.meta_bytes(array_file)
        assert itemsize == len(stored)
        assert adapter.read_meta(array_file)["custom_attributes"] == attributes


class TestFileSpace:
    """New files persist their free-space manager, so ``meta`` updates reuse space instead of growing the file."""

    def test_new_file_uses_persistent_free_space_manager(self, array_file):
        with h5py.File(array_file, "r", locking=False) as f:
            strategy, persist, _ = f.id.get_create_plist().get_file_space_strategy()
        assert strategy == h5py.h5f.FSPACE_STRATEGY_FSM_AGGR
        assert persist

    @staticmethod
    def update_meta_many_times(adapter: HDF5StorageAdapter, path: Path, times: int) -> list[int]:
        sizes = []
        for i in range(times):
            adapter.update_meta_custom_attributes(path, {"i": i % 10, "pad": "x" * (i % 5)})
            sizes.append(path.stat().st_size)
        return sizes

    def test_meta_updates_do_not_grow_the_file(self, adapter, array_file):
        warmup = self.update_meta_many_times(adapter, array_file, 10)
        later = self.update_meta_many_times(adapter, array_file, 50)
        assert max(later) <= max(warmup)

    def test_legacy_file_grows_on_meta_updates(self, adapter, tmp_path):
        """Contrast: without the persistent free-space manager every ``meta`` update leaks the old space."""
        path = tmp_path / "legacy.hdf5"
        with h5py.File(path, "w", locking=False) as f:
            f.create_dataset("meta", data=b'{"id": "legacy"}', dtype="S16", shape=())
            f.create_dataset("empty_cells", data=TOTAL, shape=())
        warmup = self.update_meta_many_times(adapter, path, 10)
        later = self.update_meta_many_times(adapter, path, 50)
        assert min(later) > max(warmup)

    def test_old_files_without_the_strategy_are_still_served(self, adapter, tmp_path):
        path = tmp_path / "legacy.hdf5"
        with h5py.File(path, "w", locking=False) as f:
            f.create_dataset("meta", data=b'{"id": "legacy"}', dtype="S16", shape=())
            f.create_dataset("empty_cells", data=TOTAL, shape=())
        assert adapter.read_meta(path) == {"id": "legacy"}
        write(adapter, path, np.index_exp[0:10], np.arange(10))
        assert adapter.update_meta_custom_attributes(path, {"a": 1}) == {"a": 1}
        assert real_values(path) == 10


class DatasetAccessLog:
    """Record which HDF5 datasets are read from and written to through h5py while a test runs.

    The h5py hooks must be plain functions (a bound method set on a class is not re-bound to the dataset),
    so the hooks report to the log that is active for the current test.
    """

    active: "DatasetAccessLog | None" = None

    def __init__(self) -> None:
        self.reads: list[str] = []
        self.writes: list[str] = []

    @property
    def data_reads(self) -> int:
        """Number of reads of the ``data`` dataset."""
        return self.reads.count("/data")

    @property
    def data_writes(self) -> int:
        """Number of writes to the ``data`` dataset."""
        return self.writes.count("/data")


_dataset_read = h5py.Dataset.__getitem__
_dataset_write = h5py.Dataset.__setitem__


def _record_read(dataset: h5py.Dataset, args: Any, new_dtype: Any = None) -> Any:
    """Record a read of the dataset in the active log and perform it.

    :param dataset: dataset being read
    :param args: index expression
    :param new_dtype: dtype to read as
    :returns: the read data
    """
    if DatasetAccessLog.active is not None:
        DatasetAccessLog.active.reads.append(dataset.name)
    return _dataset_read(dataset, args, new_dtype)


def _record_write(dataset: h5py.Dataset, args: Any, value: Any) -> None:
    """Record a write to the dataset in the active log and perform it.

    :param dataset: dataset being written
    :param args: index expression
    :param value: data to write
    """
    if DatasetAccessLog.active is not None:
        DatasetAccessLog.active.writes.append(dataset.name)
    _dataset_write(dataset, args, value)


@pytest.fixture
def access(mocker: MockerFixture) -> Iterator[DatasetAccessLog]:
    mocker.patch.object(h5py.Dataset, "__getitem__", _record_read)
    mocker.patch.object(h5py.Dataset, "__setitem__", _record_write)
    log = DatasetAccessLog()
    DatasetAccessLog.active = log
    try:
        yield log
    finally:
        DatasetAccessLog.active = None


def make_file(adapter: HDF5StorageAdapter, tmp_path: Path, shape: tuple[int, ...]) -> Path:
    """Create an array file of the given shape without data.

    :param adapter: storage adapter
    :param tmp_path: directory for the file
    :param shape: array shape
    :returns: path to the file
    """
    path = tmp_path / "array.hdf5"
    adapter.create(path, shape, {"id": "test"})
    return path


def preload(path: Path, values: np.ndarray) -> None:
    """Put a ``data`` dataset into the file directly, with a counter matching its content.

    :param path: path to the file
    :param values: full-size array of values; ``NaN`` cells count as empty
    """
    with h5py.File(path, "r+", locking=False) as f:
        f.create_dataset("data", data=values, fillvalue=FILL)
        f["empty_cells"][()] = int(np.count_nonzero(np.isnan(values)))


def stored(path: Path) -> np.ndarray:
    """Read the whole ``data`` dataset.

    :param path: path to the file
    :returns: stored values
    """
    with h5py.File(path, "r", locking=False) as f:
        return f["data"][()]


def region_data(shape: tuple[int, ...], bounds: Any) -> np.ndarray:
    """Build distinct real values for the region selected by ``bounds``.

    :param shape: array shape
    :param bounds: index expression
    :returns: array of the region shape with values ``1..n``
    """
    region_shape = create_shape_from_slice(shape, bounds)
    size = int(np.prod(region_shape, dtype=int))
    return np.arange(1, size + 1, dtype=DTYPE).reshape(region_shape)


def real_values_everywhere(shape: tuple[int, ...]) -> np.ndarray:
    """Build a full-size array without a single fill value.

    :param shape: array shape
    :returns: array with values ``1..n``
    """
    size = int(np.prod(shape, dtype=int))
    return np.arange(1, size + 1, dtype=DTYPE).reshape(shape)


def every_other_cell_filled(values: np.ndarray) -> np.ndarray:
    """Copy of ``values`` with every other cell replaced by the fill value; the first cell always keeps its value.

    :param values: source array
    :returns: the copy
    """
    mixed = values.copy()
    mixed.reshape(-1)[1::2] = FILL
    return mixed


# Index expressions that select every cell of the array: the region is the whole file
FULL_OVERWRITE_CASES = [
    pytest.param((100,), np.index_exp[:], id="1d-colon"),
    pytest.param((100,), np.index_exp[...], id="1d-ellipsis"),
    pytest.param((100,), np.index_exp[0:100], id="1d-explicit"),
    pytest.param((100,), np.index_exp[-100:], id="1d-negative-start"),
    pytest.param((100,), np.index_exp[::1], id="1d-step-one"),
    pytest.param((10, 10), np.index_exp[:], id="2d-colon"),
    pytest.param((10, 10), np.index_exp[:, :], id="2d-colon-colon"),
    pytest.param((10, 10), np.index_exp[..., :], id="2d-ellipsis-colon"),
    pytest.param((10, 10), np.index_exp[0:10, 0:10], id="2d-explicit"),
    pytest.param((4, 5, 5), np.index_exp[:], id="3d-colon"),
    pytest.param((4, 5, 5), np.index_exp[0:4, ...], id="3d-explicit-ellipsis"),
    pytest.param((1, 100), np.index_exp[0], id="2d-single-row-int"),
    pytest.param((1, 100), np.index_exp[0, :], id="2d-single-row-int-colon"),
    pytest.param((1, 100), np.index_exp[-1], id="2d-single-row-negative-int"),
    pytest.param((1, 1, 100), np.index_exp[0, 0], id="3d-two-ints"),
    pytest.param((1,), np.index_exp[0], id="single-cell-int"),
    pytest.param((1,), np.index_exp[:], id="single-cell-colon"),
    pytest.param((3, 4, 5, 6), np.index_exp[:], id="4d-colon"),
    pytest.param((3, 4, 5, 6), np.index_exp[0:3, ..., 0:6], id="4d-explicit-ellipsis"),
    pytest.param((1, 4, 1, 6), np.index_exp[0, :, 0], id="4d-ints-on-the-axes-of-one"),
]

# Index expressions that leave at least one cell untouched: the region must be read before the write.
# A stride over the whole axis selects every other cell, so it is not a full overwrite either.
PARTIAL_WRITE_CASES = [
    pytest.param((100,), np.index_exp[0:50], 50, id="1d-half"),
    pytest.param((100,), np.index_exp[::2], 50, id="1d-stride-over-the-whole-axis"),
    pytest.param((100,), np.index_exp[1::3], 33, id="1d-stride-with-offset"),
    pytest.param((100,), np.index_exp[0:50:2], 25, id="1d-stride-in-a-half"),
    pytest.param((100,), np.index_exp[0:99], 99, id="1d-all-but-last"),
    pytest.param((100,), np.index_exp[1:], 99, id="1d-all-but-first"),
    pytest.param((100,), np.index_exp[-1], 1, id="1d-last-cell-int"),
    pytest.param((100,), np.index_exp[7], 1, id="1d-one-cell-int"),
    pytest.param((10, 10), np.index_exp[0], 10, id="2d-first-row-int"),
    pytest.param((10, 10), np.index_exp[:, 0:5], 50, id="2d-half-columns"),
    pytest.param((10, 10), np.index_exp[::2, :], 50, id="2d-stride-rows"),
    pytest.param((10, 10), np.index_exp[::2, ::2], 25, id="2d-stride-both-axes"),
    pytest.param((4, 5, 5), np.index_exp[:, ::2, :], 60, id="3d-stride-middle-axis"),
    pytest.param((10, 10), np.index_exp[0:9, :], 90, id="2d-all-but-last-row"),
    pytest.param((4, 5, 5), np.index_exp[1:3], 50, id="3d-middle-slab"),
    pytest.param((4, 5, 5), np.index_exp[:, :, 0], 20, id="3d-first-column"),
    pytest.param((1, 100), np.index_exp[0, 0:50], 50, id="2d-single-row-half"),
    pytest.param((2, 100), np.index_exp[0], 100, id="2d-one-of-two-rows"),
    pytest.param((3, 4, 5, 6), np.index_exp[1], 120, id="4d-one-slab"),
    pytest.param((3, 4, 5, 6), np.index_exp[:, ::2, :, 1:5], 120, id="4d-strided-and-ranged"),
    pytest.param((3, 4, 5, 6), np.index_exp[..., 0], 60, id="4d-last-axis-int"),
]


class TestFullOverwrite:
    """A write that covers every cell of the file replaces its content: the old content is not read for the counter."""

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_data_replaces_the_content_without_reading_it(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        preload(path, every_other_cell_filled(real_values_everywhere(shape)))
        data = region_data(shape, bounds)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert empty_cells(path) == 0
        assert np.array_equal(stored(path).reshape(-1), data.reshape(-1))
        assert reads == 0
        assert writes == 1

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_data_into_an_empty_array_creates_the_dataset_without_reading(
        self, adapter, tmp_path, access, shape, bounds
    ):
        path = make_file(adapter, tmp_path, shape)
        data = region_data(shape, bounds)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert empty_cells(path) == 0
        assert np.array_equal(stored(path).reshape(-1), data.reshape(-1))
        assert reads == 0
        assert writes == 1

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_fill_values_delete_the_dataset_without_reading_or_writing_it(
        self, adapter, tmp_path, access, shape, bounds
    ):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))
        data = np.full(create_shape_from_slice(shape, bounds), FILL, dtype=DTYPE)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 0
        assert writes == 0

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_fill_values_into_an_empty_array_create_nothing(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        data = np.full(create_shape_from_slice(shape, bounds), FILL, dtype=DTYPE)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 0
        assert writes == 0

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_mixed_data_counts_the_fill_values_of_the_new_data_only(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))
        data = every_other_cell_filled(region_data(shape, bounds))
        expected_empty = int(np.count_nonzero(np.isnan(data)))

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert empty_cells(path) == expected_empty
        assert real_values(path) == int(np.prod(shape, dtype=int)) - expected_empty
        assert reads == 0
        assert writes == 1

    @pytest.mark.parametrize("drift", [0, 3, 100_000], ids=["counter-zero", "counter-too-small", "counter-too-big"])
    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_drifted_counter_is_replaced_by_the_count_of_the_new_data(
        self, adapter, tmp_path, access, shape, bounds, drift
    ):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))
        TestDriftedCounter.set_counter(path, drift)
        data = every_other_cell_filled(region_data(shape, bounds))
        expected_empty = int(np.count_nonzero(np.isnan(data)))

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads = access.data_reads

        assert empty_cells(path) == expected_empty
        assert reads == 0

    @pytest.mark.parametrize("drift", [0, 3, 100_000], ids=["counter-zero", "counter-too-small", "counter-too-big"])
    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_drifted_counter_does_not_keep_a_dataset_of_fill_values(
        self, adapter, tmp_path, access, shape, bounds, drift
    ):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))
        TestDriftedCounter.set_counter(path, drift)
        data = np.full(create_shape_from_slice(shape, bounds), FILL, dtype=DTYPE)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 0
        assert writes == 0

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_scalar_fills_every_cell(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        preload(path, every_other_cell_filled(real_values_everywhere(shape)))

        adapter.update_data(path, bounds, np.float64(5.0), DTYPE, shape, FILL, None)
        reads = access.data_reads

        assert empty_cells(path) == 0
        assert np.all(stored(path) == 5.0)
        assert reads == 0

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_scalar_fill_value_deletes_the_dataset(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))

        adapter.update_data(path, bounds, np.float64(FILL), DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 0
        assert writes == 0

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_non_nan_fill_value(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        total = int(np.prod(shape, dtype=int))
        data = region_data(shape, bounds)

        adapter.update_data(path, bounds, data, DTYPE, shape, 0.0, None)
        assert empty_cells(path) == 0
        assert access.data_reads == 0

        adapter.update_data(path, bounds, np.zeros_like(data), DTYPE, shape, 0.0, None)
        assert not has_data(path)
        assert empty_cells(path) == total
        assert access.data_reads == 0


class TestPartialWrite:
    """A write that leaves cells untouched counts by the difference in its region, so the region is read once."""

    @pytest.mark.parametrize(("shape", "bounds", "count"), PARTIAL_WRITE_CASES)
    def test_fill_values_over_data_free_exactly_the_region(self, adapter, tmp_path, access, shape, bounds, count):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))
        data = np.full(create_shape_from_slice(shape, bounds), FILL, dtype=DTYPE)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads = access.data_reads

        assert has_data(path)
        assert empty_cells(path) == count
        assert real_values(path) == int(np.prod(shape, dtype=int)) - count
        assert reads == 1

    @pytest.mark.parametrize(("shape", "bounds", "count"), PARTIAL_WRITE_CASES)
    def test_repeated_fill_values_do_not_count_twice(self, adapter, tmp_path, access, shape, bounds, count):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))
        data = np.full(create_shape_from_slice(shape, bounds), FILL, dtype=DTYPE)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads = access.data_reads

        assert has_data(path)
        assert empty_cells(path) == count
        assert reads == 2

    @pytest.mark.parametrize(("shape", "bounds", "count"), PARTIAL_WRITE_CASES)
    def test_data_over_fill_values_takes_exactly_the_region(self, adapter, tmp_path, access, shape, bounds, count):
        path = make_file(adapter, tmp_path, shape)
        total = int(np.prod(shape, dtype=int))
        preload(path, np.full(shape, FILL, dtype=DTYPE))
        data = region_data(shape, bounds)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads = access.data_reads

        assert empty_cells(path) == total - count
        assert real_values(path) == count
        assert reads == 1

    @pytest.mark.parametrize(("shape", "bounds", "count"), PARTIAL_WRITE_CASES)
    def test_data_into_an_empty_array_does_not_read(self, adapter, tmp_path, access, shape, bounds, count):
        path = make_file(adapter, tmp_path, shape)
        total = int(np.prod(shape, dtype=int))
        data = region_data(shape, bounds)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads = access.data_reads

        assert empty_cells(path) == total - count
        assert real_values(path) == count
        assert reads == 0

    @pytest.mark.parametrize(("shape", "bounds", "count"), PARTIAL_WRITE_CASES)
    def test_fill_values_into_an_empty_array_create_nothing(self, adapter, tmp_path, access, shape, bounds, count):
        path = make_file(adapter, tmp_path, shape)
        data = np.full(create_shape_from_slice(shape, bounds), FILL, dtype=DTYPE)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads, writes = access.data_reads, access.data_writes

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 0
        assert writes == 0

    @pytest.mark.parametrize(("shape", "bounds", "count"), PARTIAL_WRITE_CASES)
    def test_last_real_cells_cleared_by_a_partial_write_delete_the_dataset(
        self, adapter, tmp_path, access, shape, bounds, count
    ):
        """Only the region holds data; writing fill values into it empties the file, which is verified by a scan."""
        path = make_file(adapter, tmp_path, shape)
        values = np.full(shape, FILL, dtype=DTYPE)
        values[bounds] = region_data(shape, bounds)
        preload(path, values)
        data = np.full(create_shape_from_slice(shape, bounds), FILL, dtype=DTYPE)

        adapter.update_data(path, bounds, data, DTYPE, shape, FILL, None)
        reads = access.data_reads

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 2  # the region before the write and the scan before the deletion


class TestClearWholeArray:
    """Clearing every cell of the file deletes the dataset: the content is not read."""

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_whole_array_is_deleted_without_reading(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        preload(path, every_other_cell_filled(real_values_everywhere(shape)))

        adapter.clear_data(path, shape, bounds, FILL)
        reads, writes = access.data_reads, access.data_writes

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 0
        assert writes == 0

    @pytest.mark.parametrize(("shape", "bounds"), FULL_OVERWRITE_CASES)
    def test_whole_empty_array_is_a_no_op(self, adapter, tmp_path, access, shape, bounds):
        path = make_file(adapter, tmp_path, shape)

        adapter.clear_data(path, shape, bounds, FILL)
        reads, writes = access.data_reads, access.data_writes

        assert not has_data(path)
        assert empty_cells(path) == int(np.prod(shape, dtype=int))
        assert reads == 0
        assert writes == 0

    @pytest.mark.parametrize(("shape", "bounds", "count"), PARTIAL_WRITE_CASES)
    def test_partial_clear_reads_the_region_and_keeps_the_rest(self, adapter, tmp_path, access, shape, bounds, count):
        path = make_file(adapter, tmp_path, shape)
        preload(path, real_values_everywhere(shape))

        adapter.clear_data(path, shape, bounds, FILL)
        reads, writes = access.data_reads, access.data_writes

        assert has_data(path)
        assert empty_cells(path) == count
        assert real_values(path) == int(np.prod(shape, dtype=int)) - count
        assert reads == 1
        assert writes == 1


if __name__ == "__main__":
    pytest.main()


# Index expressions with a negative step, which HDF5 cannot select itself: the adapter reverses the data instead
NEGATIVE_STEP_CASES = [
    pytest.param((100,), np.index_exp[::-1], id="1d-reversed"),
    pytest.param((100,), np.index_exp[::-2], id="1d-reversed-every-other"),
    pytest.param((100,), np.index_exp[8:2:-2], id="1d-reversed-range"),
    pytest.param((100,), np.index_exp[-1::-3], id="1d-reversed-from-the-end"),
    pytest.param((100,), np.index_exp[50::-1], id="1d-reversed-half"),
    pytest.param((10, 10), np.index_exp[::-1], id="2d-reversed-rows"),
    pytest.param((10, 10), np.index_exp[::-1, ::2], id="2d-reversed-rows-strided-columns"),
    pytest.param((10, 10), np.index_exp[::2, ::-1], id="2d-strided-rows-reversed-columns"),
    pytest.param((10, 10), np.index_exp[::-1, ::-1], id="2d-reversed-both"),
    pytest.param((10, 10), np.index_exp[3, ::-3], id="2d-int-and-reversed"),
    pytest.param((10, 10), np.index_exp[9:1:-2, 1:9:3], id="2d-reversed-range-and-strided"),
    pytest.param((4, 5, 5), np.index_exp[::-1, :, ::-2], id="3d-two-axes-reversed"),
    pytest.param((4, 5, 5), np.index_exp[::-1, 2, 4:0:-1], id="3d-reversed-int-reversed"),
    pytest.param((1,), np.index_exp[::-1], id="single-cell-reversed"),
    pytest.param((3, 4, 5, 6), np.index_exp[::-1], id="4d-reversed-first"),
    pytest.param((3, 4, 5, 6), np.index_exp[::-1, ::2, ::-3, ::-1], id="4d-mixed"),
    pytest.param((3, 4, 5, 6), np.index_exp[2, ::-1, 1, 5:0:-2], id="4d-ints-and-reversed"),
    pytest.param((3, 4, 5, 6), np.index_exp[..., ::-2], id="4d-ellipsis-reversed-last"),
]


class TestNegativeStep:
    """A negative step selects the same cells as its positive twin, in the reversed order, exactly as in numpy."""

    @pytest.mark.parametrize(("shape", "bounds"), NEGATIVE_STEP_CASES)
    def test_read_follows_numpy(self, adapter, tmp_path, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        values = real_values_everywhere(shape)
        preload(path, values)

        data = adapter.read_data(path, shape, bounds, FILL, DTYPE)

        assert np.array_equal(data, values[bounds])

    @pytest.mark.parametrize(("shape", "bounds"), NEGATIVE_STEP_CASES)
    def test_read_of_an_empty_array_has_the_shape_of_the_selection(self, adapter, tmp_path, shape, bounds):
        path = make_file(adapter, tmp_path, shape)

        data = adapter.read_data(path, shape, bounds, FILL, DTYPE)

        assert np.shape(data) == np.full(shape, FILL)[bounds].shape
        assert np.all(np.isnan(data))

    @pytest.mark.parametrize(("shape", "bounds"), NEGATIVE_STEP_CASES)
    def test_update_follows_numpy(self, adapter, tmp_path, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        values = real_values_everywhere(shape)
        preload(path, values)
        new_data = -region_data(shape, bounds)
        expected = values.copy()
        expected[bounds] = new_data

        adapter.update_data(path, bounds, new_data, DTYPE, shape, FILL, None)

        assert np.array_equal(stored(path), expected)
        assert empty_cells(path) == 0

    @pytest.mark.parametrize(("shape", "bounds"), NEGATIVE_STEP_CASES)
    def test_update_into_an_empty_array_follows_numpy(self, adapter, tmp_path, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        new_data = -region_data(shape, bounds)
        expected = np.full(shape, FILL, dtype=DTYPE)
        expected[bounds] = new_data

        adapter.update_data(path, bounds, new_data, DTYPE, shape, FILL, None)

        assert np.array_equal(stored(path), expected, equal_nan=True)
        assert empty_cells(path) == int(np.count_nonzero(np.isnan(expected)))

    @pytest.mark.parametrize(("shape", "bounds"), NEGATIVE_STEP_CASES)
    def test_update_with_a_scalar(self, adapter, tmp_path, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        values = real_values_everywhere(shape)
        preload(path, values)
        expected = values.copy()
        expected[bounds] = -7.0

        adapter.update_data(path, bounds, np.float64(-7.0), DTYPE, shape, FILL, None)

        assert np.array_equal(stored(path), expected)

    @pytest.mark.parametrize(("shape", "bounds"), NEGATIVE_STEP_CASES)
    def test_fill_values_count_exactly_the_selection(self, adapter, tmp_path, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        values = real_values_everywhere(shape)
        preload(path, values)
        expected = values.copy()
        expected[bounds] = FILL
        selected = int(np.count_nonzero(np.isnan(expected)))

        adapter.update_data(
            path, bounds, np.full(np.shape(expected[bounds]), FILL, dtype=DTYPE), DTYPE, shape, FILL, None
        )

        assert empty_cells(path) == selected
        if selected == int(np.prod(shape, dtype=int)):
            assert not has_data(path)
        else:
            assert np.array_equal(stored(path), expected, equal_nan=True)

    @pytest.mark.parametrize(("shape", "bounds"), NEGATIVE_STEP_CASES)
    def test_clear_follows_numpy(self, adapter, tmp_path, shape, bounds):
        path = make_file(adapter, tmp_path, shape)
        values = real_values_everywhere(shape)
        preload(path, values)
        expected = values.copy()
        expected[bounds] = FILL
        selected = int(np.count_nonzero(np.isnan(expected)))

        adapter.clear_data(path, shape, bounds, FILL)

        assert empty_cells(path) == selected
        if selected == int(np.prod(shape, dtype=int)):
            assert not has_data(path)
        else:
            assert np.array_equal(stored(path), expected, equal_nan=True)


BOUNDS_SHAPE = (10, 10, 10)
OVERWRITE = -9999.0


def filled_file(adapter: HDF5StorageAdapter, path: Path, shape: tuple[int, ...]) -> np.ndarray:
    """Create an array file of the shape with every cell holding its own ordinal, and return that data.

    :param adapter: the storage adapter
    :param path: the file to create
    :param shape: the shape of the array
    :returns: the data written
    """
    adapter.create(path, shape, {"id": "test"})
    data = np.arange(np.prod(shape), dtype=DTYPE).reshape(shape)
    adapter.update_data(path, np.index_exp[:], data, DTYPE, shape, FILL, None)
    return data


def whole(adapter: HDF5StorageAdapter, path: Path, shape: tuple[int, ...]) -> np.ndarray:
    """Read the whole array of the file.

    :param adapter: the storage adapter
    :param path: the file
    :param shape: the shape of the array
    :returns: the data
    """
    return np.asarray(adapter.read_data(path, shape, np.index_exp[:], FILL, DTYPE))


class TestBounds:
    """Every index expression DEKER accepts selects the same cells as numpy does, on reads, writes and clears.

    Negative steps are the forward twin plus a flip, an integer index yields a scalar, an ellipsis stands for the
    axes it replaces. The expressions DEKER refuses never change the file: numpy-like ones that select nothing
    are a no-op, the others are an IndexError.
    """

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_read_selects_the_cells_numpy_selects(self, adapter, tmp_path, index_exp):
        data = filled_file(adapter, tmp_path / "array.hdf5", BOUNDS_SHAPE)
        read = adapter.read_data(tmp_path / "array.hdf5", BOUNDS_SHAPE, index_exp, FILL, DTYPE)
        assert np.ndim(read) == np.ndim(data[index_exp])
        assert np.array_equal(np.asarray(read), data[index_exp])

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_read_of_an_empty_array_has_the_shape_numpy_gives(self, adapter, tmp_path, index_exp):
        """Without a dataset only the requested region is allocated: fill values of the shape of the selection."""
        path = tmp_path / "array.hdf5"
        adapter.create(path, BOUNDS_SHAPE, {"id": "test"})
        read = adapter.read_data(path, BOUNDS_SHAPE, index_exp, FILL, DTYPE)
        reference = np.empty(BOUNDS_SHAPE)[index_exp]
        assert np.ndim(read) == reference.ndim
        assert np.shape(read) == reference.shape
        assert np.all(np.isnan(read))

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_update_writes_the_cells_numpy_selects_and_no_others(self, adapter, tmp_path, index_exp):
        path = tmp_path / "array.hdf5"
        data = filled_file(adapter, path, BOUNDS_SHAPE)
        expected = data.copy()
        expected[index_exp] = OVERWRITE
        adapter.update_data(
            path, index_exp, np.full(np.shape(data[index_exp]), OVERWRITE), DTYPE, BOUNDS_SHAPE, FILL, None
        )
        assert np.array_equal(whole(adapter, path, BOUNDS_SHAPE), expected)

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_clear_empties_the_cells_numpy_selects_and_no_others(self, adapter, tmp_path, index_exp):
        path = tmp_path / "array.hdf5"
        data = filled_file(adapter, path, BOUNDS_SHAPE)
        expected = data.copy()
        expected[index_exp] = FILL
        adapter.clear_data(path, BOUNDS_SHAPE, index_exp, FILL)
        assert np.array_equal(whole(adapter, path, BOUNDS_SHAPE), expected, equal_nan=True)
        assert empty_cells(path) == int(np.isnan(expected).sum())

    @pytest.mark.parametrize(("shape", "index_exp"), array_step_cases())
    def test_stepped_bounds_read_write_and_clear_like_numpy(self, adapter, tmp_path, shape, index_exp):
        """Steps of any sign and size on arrays of one to four dimensions, uneven against the step."""
        path = tmp_path / "array.hdf5"
        data = filled_file(adapter, path, shape)
        assert np.array_equal(np.asarray(adapter.read_data(path, shape, index_exp, FILL, DTYPE)), data[index_exp])
        expected = data.copy()
        expected[index_exp] = OVERWRITE
        adapter.update_data(path, index_exp, np.full(np.shape(data[index_exp]), OVERWRITE), DTYPE, shape, FILL, None)
        assert np.array_equal(whole(adapter, path, shape), expected)
        expected[index_exp] = FILL
        adapter.clear_data(path, shape, index_exp, FILL)
        assert np.array_equal(whole(adapter, path, shape), expected, equal_nan=True)

    @pytest.mark.parametrize("index_exp", invalid_index_params)
    def test_refused_bounds_never_change_the_file(self, adapter, tmp_path, index_exp):
        """The storage adapter is below the validation of DEKER: a refused expression selects nothing or fails."""
        path = tmp_path / "array.hdf5"
        data = filled_file(adapter, path, BOUNDS_SHAPE)
        outcomes = []
        for operation in (
            (adapter.read_data, (path, BOUNDS_SHAPE, index_exp, FILL, DTYPE)),
            (adapter.update_data, (path, index_exp, np.full((), OVERWRITE), DTYPE, BOUNDS_SHAPE, FILL, None)),
            (adapter.clear_data, (path, BOUNDS_SHAPE, index_exp, FILL)),
        ):
            method, arguments = operation
            try:
                result = method(*arguments)
            except IndexError:
                outcomes.append("refused")
            else:
                assert result is None or np.size(result) == 0
                outcomes.append("nothing selected")
        assert len(set(outcomes)) == 1, outcomes  # the three operations agree on the expression
        assert np.array_equal(whole(adapter, path, BOUNDS_SHAPE), data)
        assert empty_cells(path) == 0

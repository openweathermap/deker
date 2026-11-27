import time

from collections import OrderedDict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from typing import Any

import h5py
import hdf5plugin
import numpy as np
import pytest

from cantok import AbstractToken, DefaultToken, SimpleToken
from deker_local_adapters import HDF5CompressionOpts, HDF5Options
from deker_tools.cancel import cancellation_scope
from deker_tools.slices import positive_bounds
from deker_tools.typings import Sentinel
from pytest_mock import MockerFixture

from tests.fixtures.collection import SteppedCollections
from tests.parameters.index_exp_params import STEPPED_INDEX_EXPS, valid_index_exp_params, varray_step_cases

from deker.arrays import VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerArrayError,
    DekerCancelledError,
    DekerLockError,
    DekerSubsetError,
    DekerTimeoutError,
    DekerValidationError,
    DekerVSubsetError,
)
from deker.flock import Flock
from deker.schemas import AttributeSchema, DimensionSchema, TimeDimensionSchema, VArraySchema
from deker.subset import Subset, VSubset, _array_positions, _ArrayBounds, _split_axis
from deker.tools import get_main_path
from deker.types.private.classes import ArrayPosition, ArrayPositionedData
from deker.types.private.typings import Slice
from deker.types.public.classes import Scale


class TestVArraySubset:
    @pytest.mark.parametrize(
        ("slice_", "result"),
        [
            (
                (slice(None, None, None)),
                [
                    ArrayPosition(
                        vposition=(0, 0),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(0, 5), slice(0, 2)),
                    ),
                    ArrayPosition(
                        vposition=(0, 1),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(0, 5), slice(2, 4)),
                    ),
                    ArrayPosition(
                        vposition=(0, 2),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(0, 5), slice(4, 6)),
                    ),
                    ArrayPosition(
                        vposition=(0, 3),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(0, 5), slice(6, 8)),
                    ),
                    ArrayPosition(
                        vposition=(0, 4),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(0, 5), slice(8, 10)),
                    ),
                    ArrayPosition(
                        vposition=(1, 0),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(5, 10), slice(0, 2)),
                    ),
                    ArrayPosition(
                        vposition=(1, 1),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(5, 10), slice(2, 4)),
                    ),
                    ArrayPosition(
                        vposition=(1, 2),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(5, 10), slice(4, 6)),
                    ),
                    ArrayPosition(
                        vposition=(1, 3),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(5, 10), slice(6, 8)),
                    ),
                    ArrayPosition(
                        vposition=(1, 4),
                        bounds=(slice(None, None), slice(None, None)),
                        data_slice=(slice(5, 10), slice(8, 10)),
                    ),
                ],
            ),
            (
                (slice(3, 6), slice(0, 5)),
                [
                    ArrayPosition(
                        vposition=(0, 0),
                        bounds=(slice(3, 5), slice(None, None, None)),
                        data_slice=(slice(0, 2), slice(0, 2)),
                    ),
                    ArrayPosition(
                        vposition=(0, 1),
                        bounds=(slice(3, 5), slice(None, None, None)),
                        data_slice=(slice(0, 2), slice(2, 4)),
                    ),
                    ArrayPosition(
                        vposition=(0, 2),
                        bounds=(slice(3, 5), slice(0, 1)),
                        data_slice=(slice(0, 2), slice(4, 5)),
                    ),
                    ArrayPosition(
                        vposition=(1, 0),
                        bounds=(slice(0, 1), slice(None, None, None)),
                        data_slice=(slice(2, 3), slice(0, 2)),
                    ),
                    ArrayPosition(
                        vposition=(1, 1),
                        bounds=(slice(0, 1), slice(None, None, None)),
                        data_slice=(slice(2, 3), slice(2, 4)),
                    ),
                    ArrayPosition(
                        vposition=(1, 2),
                        bounds=(slice(0, 1), slice(0, 1)),
                        data_slice=(slice(2, 3), slice(4, 5)),
                    ),
                ],
            ),
            (
                (0, slice(1, 5)),
                [
                    ArrayPosition(
                        vposition=(0, 0),
                        bounds=(0, slice(1, 2, None)),
                        data_slice=(slice(0, 1, None),),
                    ),
                    ArrayPosition(
                        vposition=(0, 1),
                        bounds=(0, slice(None, None, None)),
                        data_slice=(slice(1, 3, None),),
                    ),
                    ArrayPosition(
                        vposition=(0, 2),
                        bounds=(0, slice(0, 1, None)),
                        data_slice=(slice(3, 4, None),),
                    ),
                ],
            ),
        ],
    )
    def test_subset_calc_array_2dimension(self, slice_: tuple[slice, ...], result: tuple, client: Client, name: str):
        """Test correctness of arrays calculation in vsubset for 2 dimensional array."""
        dimensions = [
            DimensionSchema(name="x", size=10),
            DimensionSchema(name="y", size=10),
        ]

        array_schema = VArraySchema(
            dtype=float,
            dimensions=dimensions,  # type: ignore[arg-type]
            vgrid=(2, 5),
            attributes=[
                AttributeSchema(name="cl", dtype=float, primary=False),
            ],
        )
        new_collection = client.create_collection(name, schema=array_schema)
        varray = new_collection.create(primary_attributes={}, custom_attributes={})

        assert varray[slice_]._VSubset__arrays == result
        new_collection.delete()

    @pytest.mark.parametrize(
        ("slice_", "result"),
        [
            (
                slice(None, None),
                [
                    ArrayPosition(
                        vposition=(0, 0, 0),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(0, 5),) * 3,
                    ),
                    ArrayPosition(
                        vposition=(0, 0, 1),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(0, 5), slice(0, 5), slice(5, 10)),
                    ),
                    ArrayPosition(
                        vposition=(0, 1, 0),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(0, 5), slice(5, 10), slice(0, 5)),
                    ),
                    ArrayPosition(
                        vposition=(0, 1, 1),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(0, 5), slice(5, 10), slice(5, 10)),
                    ),
                    ArrayPosition(
                        vposition=(1, 0, 0),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(5, 10), slice(0, 5), slice(0, 5)),
                    ),
                    ArrayPosition(
                        vposition=(1, 0, 1),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(5, 10), slice(0, 5), slice(5, 10)),
                    ),
                    ArrayPosition(
                        vposition=(1, 1, 0),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(5, 10), slice(5, 10), slice(0, 5)),
                    ),
                    ArrayPosition(
                        vposition=(1, 1, 1),
                        bounds=(slice(None, None),) * 3,
                        data_slice=(slice(5, 10),) * 3,
                    ),
                ],
            ),
        ],
    )
    def test_subset_calc_array_3dimension(self, slice_: tuple[slice, ...], result: tuple, client: Client, name: str):
        """Test correctness of arrays calculation in vsubset for 3 dimensional array."""
        dimensions = [
            DimensionSchema(name="x", size=10),
            DimensionSchema(name="y", size=10),
            DimensionSchema(name="z", size=10),
        ]

        array_schema = VArraySchema(
            dtype=float,
            dimensions=dimensions,  # type: ignore[arg-type]
            vgrid=(2, 2, 2),
            attributes=[
                AttributeSchema(name="cl", dtype=float, primary=False),
            ],
        )
        new_collection = client.create_collection(name, schema=array_schema)
        varray = new_collection.create(primary_attributes={}, custom_attributes={})

        assert varray[slice_]._VSubset__arrays == result
        new_collection.delete()

    def test_read_does_not_describe_the_subsets_when_the_log_is_off(self, inserted_varray: VArray, mocker):
        """Neither the VSubset nor the Subsets of its Arrays are turned into strings at the ERROR level."""
        vsubset_described = mocker.spy(VSubset, "__str__")
        subset_described = mocker.spy(Subset, "__str__")
        inserted_varray[0].read()
        assert vsubset_described.call_count == 0
        assert subset_described.call_count == 0

    def test_read_describes_the_subsets_when_the_log_is_on(self, inserted_varray: VArray, deker_log, mocker):
        vsubset_described = mocker.spy(VSubset, "__str__")
        inserted_varray[0].read()
        assert vsubset_described.call_count > 0
        assert any("data read" in record.message for record in deker_log.records)

    @pytest.mark.parametrize(
        ("index_exp", "bounds"),
        [(0, "[0]"), ((slice(0, 2), 1), "[0:2, 1]"), ((slice(None), slice(None, None, 2)), "[:, ::2]")],
    )
    def test_repr_names_the_bounds_the_shape_and_the_varray(
        self, inserted_varray: VArray, index_exp: Slice, bounds: str
    ):
        """The repr of a VSubset carries its bounds, its shape and the id of its VArray."""
        vsubset = inserted_varray[index_exp]
        assert repr(vsubset) == f"VSubset(bounds={bounds}, shape={vsubset.shape}, array={inserted_varray.id})"

    @pytest.mark.parametrize(
        ("index_exp", "bounds"),
        [(0, "[0]"), ((slice(0, 2), 1), "[0:2, 1]"), ((slice(None), slice(None, None, 2)), "[:, ::2]")],
    )
    def test_str_is_the_varray_with_the_bounds(self, inserted_varray: VArray, index_exp: Slice, bounds: str):
        """The str of a VSubset is the str of its VArray followed by the bounds."""
        assert str(inserted_varray[index_exp]) == f"{inserted_varray!s}{bounds}"

    def test_vsubset_update_and_read_data(self, varray: VArray):
        """Test vsubset update and read methods."""
        find = varray._VArray__collection.filter({"id": varray.id})  # type: ignore[attr-defined]
        array = find.first()
        assert array is None
        varray._adapter.create(varray, timeout=Sentinel, token=DefaultToken())
        array = find.first()
        assert array is not None
        data = np.ones(shape=varray.shape, dtype=varray.dtype)
        vsubset: VSubset = varray[:]
        vsubset.update(data)
        result = vsubset.read()
        assert data.tolist() == result.tolist()

    @pytest.mark.parametrize(
        "index_exp", [np.index_exp[:], np.index_exp[0], np.index_exp[1:3, 1:3], np.index_exp[0, 0]]
    )
    def test_vsubset_update_with_scalar_fills_subset(self, varray: VArray, index_exp: Slice):
        """Tests if a scalar fills the whole VSubset, as it does for an Array subset."""
        varray._adapter.create(varray, timeout=Sentinel, token=DefaultToken())
        vsubset: VSubset = varray[index_exp]
        vsubset.update(5)
        result = vsubset.read()
        assert np.all(np.asarray(result) == 5)
        assert np.asarray(result).shape == vsubset.shape

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_update_vsubset_raises_on_no_data(self, varray_collection: Collection, index_exp: Slice):
        """Tests if subset updates itself correctly.

        :param varray_collection: Pre created collection
        :param index_exp: Slice for a varray
        """
        array: VArray = varray_collection.create()
        with pytest.raises(DekerArrayError):
            assert array[:].update(None)

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_read_vsubset_nans(self, varray_collection: Collection, index_exp: Slice):
        varray = varray_collection.create()
        vsubset = varray[index_exp]
        data = vsubset.read()
        assert np.isnan(data).all()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_clear_vsubset(self, varray_collection: Collection, array_data: np.ndarray, index_exp: Slice):
        varray: VArray = varray_collection.create()
        vsubset: VSubset = varray[:]
        vsubset.update(array_data)
        try:
            vsubset: VSubset = varray[index_exp]
            data = vsubset.read()
            assert (data == array_data[index_exp]).all()
            vsubset.clear()
            data = vsubset.read()
            assert np.isnan(data).all()
        finally:
            varray.delete()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_clear_vsubset_with_no_data(self, varray_collection: Collection, index_exp: Slice):
        varray = varray_collection.create()
        try:
            vsubset = varray[index_exp]
            vsubset.clear()
            data = vsubset.read()
            assert np.isnan(data).all()
        finally:
            varray.delete()

    def test_vsubset_read_no_data(self, varray_collection: Collection):
        varray = varray_collection.create()
        vsubset = varray[:]
        data = vsubset.read()
        assert np.isnan(data).all()

    def test_vsubset_arrays_calc_1dim(self, client: Client):
        """Test correctness of array calculation for 1 dimensional array"""
        dimensions = [
            DimensionSchema(name="x", size=6),
        ]

        array_schema = VArraySchema(
            dtype=float,
            dimensions=dimensions,  # type: ignore[arg-type]
            vgrid=(3,),
        )
        collection = client.create_collection("1dim_col", array_schema)
        varray = collection.create()
        data = np.zeros(shape=(6,))
        varray[:].update(data)
        # Check full:
        assert varray[:]._VSubset__arrays == [
            ArrayPosition((0,), (slice(None, None),), (slice(0, 2),)),
            ArrayPosition((1,), (slice(None, None),), (slice(2, 4),)),
            ArrayPosition((2,), (slice(None, None),), (slice(4, 6),)),
        ]
        # Check part:
        assert varray[:3]._VSubset__arrays == [
            ArrayPosition((0,), (slice(None, None),), (slice(0, 2),)),
            ArrayPosition((1,), (slice(0, 1),), (slice(2, 3),)),
        ]

        # Check one:
        subset = varray[3]
        assert subset._VSubset__arrays == [
            ArrayPosition(
                (1,),
                (1,),
                (),
            ),
        ]
        assert subset.read() == data[3]
        collection.delete()

    def test_vsubset_arrays_calc_2dim(self, client: Client):
        """Test correctness of array calculation for 2 dimensional array"""
        dimensions = [
            DimensionSchema(name="x", size=6),
            DimensionSchema(name="y", size=6),
        ]

        array_schema = VArraySchema(
            dtype=float,
            dimensions=dimensions,  # type: ignore[arg-type]
            vgrid=(3, 3),
        )
        coll = client.create_collection("2dim_col", array_schema)
        varray = coll.create()
        # Check full:
        assert varray[:]._VSubset__arrays == [
            ArrayPosition((0, 0), (slice(None, None), slice(None, None)), (slice(0, 2), slice(0, 2))),
            ArrayPosition((0, 1), (slice(None, None), slice(None, None)), (slice(0, 2), slice(2, 4))),
            ArrayPosition((0, 2), (slice(None, None), slice(None, None)), (slice(0, 2), slice(4, 6))),
            ArrayPosition((1, 0), (slice(None, None), slice(None, None)), (slice(2, 4), slice(0, 2))),
            ArrayPosition((1, 1), (slice(None, None), slice(None, None)), (slice(2, 4), slice(2, 4))),
            ArrayPosition((1, 2), (slice(None, None), slice(None, None)), (slice(2, 4), slice(4, 6))),
            ArrayPosition((2, 0), (slice(None, None), slice(None, None)), (slice(4, 6), slice(0, 2))),
            ArrayPosition((2, 1), (slice(None, None), slice(None, None)), (slice(4, 6), slice(2, 4))),
            ArrayPosition((2, 2), (slice(None, None), slice(None, None)), (slice(4, 6), slice(4, 6))),
        ]
        # Check half:
        assert varray[:3, :]._VSubset__arrays == [
            ArrayPosition((0, 0), (slice(None, None), slice(None, None)), (slice(0, 2), slice(0, 2))),
            ArrayPosition((0, 1), (slice(None, None), slice(None, None)), (slice(0, 2), slice(2, 4))),
            ArrayPosition((0, 2), (slice(None, None), slice(None, None)), (slice(0, 2), slice(4, 6))),
            ArrayPosition((1, 0), (slice(0, 1), slice(None, None)), (slice(2, 3), slice(0, 2))),
            ArrayPosition((1, 1), (slice(0, 1), slice(None, None)), (slice(2, 3), slice(2, 4))),
            ArrayPosition((1, 2), (slice(0, 1), slice(None, None)), (slice(2, 3), slice(4, 6))),
        ]

        # Check part
        assert varray[1:5, 1:5]._VSubset__arrays == [
            ArrayPosition((0, 0), (slice(1, 2), slice(1, 2)), (slice(0, 1), slice(0, 1))),
            ArrayPosition(
                (0, 1),
                (
                    slice(1, 2),
                    slice(None, None),
                ),
                (slice(0, 1), slice(1, 3)),
            ),
            ArrayPosition(
                (0, 2),
                (
                    slice(1, 2),
                    slice(0, 1),
                ),
                (slice(0, 1), slice(3, 4)),
            ),
            ArrayPosition(
                (1, 0),
                (
                    slice(None, None),
                    slice(1, 2),
                ),
                (slice(1, 3), slice(0, 1)),
            ),
            ArrayPosition((1, 1), (slice(None, None), slice(None, None)), (slice(1, 3), slice(1, 3))),
            ArrayPosition(
                (1, 2),
                (
                    slice(None, None),
                    slice(0, 1),
                ),
                (slice(1, 3), slice(3, 4)),
            ),
            ArrayPosition(
                (2, 0),
                (
                    slice(0, 1),
                    slice(1, 2),
                ),
                (slice(3, 4), slice(0, 1)),
            ),
            ArrayPosition(
                (2, 1),
                (
                    slice(0, 1),
                    slice(None, None),
                ),
                (slice(3, 4), slice(1, 3)),
            ),
            ArrayPosition((2, 2), (slice(0, 1), slice(0, 1)), (slice(3, 4), slice(3, 4))),
        ]

        # Check one dimension
        assert varray[1, :]._VSubset__arrays == [
            ArrayPosition((0, 0), (1, slice(None, None)), (slice(0, 2),)),
            ArrayPosition(
                (0, 1),
                (
                    1,
                    slice(None, None),
                ),
                (slice(2, 4),),
            ),
            ArrayPosition(
                (0, 2),
                (
                    1,
                    slice(None, None),
                ),
                (slice(4, 6),),
            ),
        ]
        coll.delete()

    def test_vsubset_arrays_calc_3dim(self, client):
        """Tests vsubset array calculation for 3 dimensional array."""
        dimensions = [
            DimensionSchema(name="x", size=6),
            DimensionSchema(name="y", size=6),
            DimensionSchema(name="z", size=6),
        ]
        array_schema = VArraySchema(
            dtype=np.int8,
            dimensions=dimensions,
            vgrid=(3, 3, 3),
        )
        coll = client.create_collection(name="3dim", schema=array_schema)
        varray = coll.create()
        data = np.arange(6 * 6 * 6).astype(np.int8).reshape((6, 6, 6))  # wraps past 127 like numpy 1 did
        varray[:].update(data)

        # Check full
        assert (varray[:].read() == data[:]).all()

        # Check half
        half = np.index_exp[:3, :, :]
        assert (varray[half].read() == data[half]).all()

        # Check quarter
        quarter = np.index_exp[:3, :3, :]
        assert (varray[quarter].read() == data[quarter]).all()

        # Check part
        part = np.index_exp[:3, :3, :3]
        assert (varray[part].read() == data[part]).all()

        # Check one
        one = np.index_exp[1, 2, 3]
        assert varray[one].read() == data[one]

        # Check write
        value = np.int8(-1)
        varray[one].update(value)
        assert varray[one].read() == value

        coll.delete()

    def test_update_reports_the_exceptions_of_its_array_workers(self, inserted_varray: VArray, mocker: MockerFixture):
        """Every Array is written by a worker; the failures of the workers are collected into one error."""
        mocker.patch.object(VSubset, "_VSubset__update_array_data", side_effect=OSError("disk full"))
        subset = inserted_varray[:]
        with pytest.raises(DekerVSubsetError) as info:
            subset.update(np.zeros(subset.shape))
        error = info.value
        arrays = len(subset._VSubset__arrays)
        assert (
            error.message == f"{subset!s} is written partly: {arrays} of {arrays} Arrays failed, the others are written"
        )
        assert len(error.exceptions) == arrays
        assert all(text.startswith("OSError('disk full')") for text in error.exceptions)
        assert all("Traceback" in text for text in error.exceptions)
        assert "1) OSError('disk full')" in str(error)

    def test_update_raises_the_lock_error_when_every_failure_is_a_lock(
        self, inserted_varray: VArray, mocker: MockerFixture
    ):
        """When every failed Array failed on its lock, the write raises DekerLockError: it can simply be repeated."""
        mocker.patch.object(VSubset, "_VSubset__update_array_data", side_effect=DekerLockError("held by a writer"))
        subset = inserted_varray[:]
        arrays = len(subset._VSubset__arrays)
        with pytest.raises(DekerLockError) as info:
            subset.update(np.zeros(subset.shape))
        assert not isinstance(info.value, DekerVSubsetError)
        assert f"{subset!s} is written partly: {arrays} of {arrays} Arrays were not released in time" in str(info.value)
        assert "held by a writer" in str(info.value)

    def test_update_reports_a_lock_error_among_other_failures_as_a_group(
        self, inserted_varray: VArray, mocker: MockerFixture
    ):
        """A lock error next to another failure is not retryable as a whole: the group is raised, with both."""
        subset = inserted_varray[:]
        arrays = len(subset._VSubset__arrays)
        failures = [DekerLockError("held by a writer")] + [OSError("disk full")] * (arrays - 1)
        mocker.patch.object(VSubset, "_VSubset__update_array_data", side_effect=failures)
        with pytest.raises(DekerVSubsetError) as info:
            subset.update(np.zeros(subset.shape))
        assert len(info.value.exceptions) == arrays
        assert any(text.startswith("DekerLockError('held by a writer')") for text in info.value.exceptions)
        assert any(text.startswith("OSError('disk full')") for text in info.value.exceptions)


def array_main_file(array) -> Path:
    """Main file of an Array.

    :param array: Array of a VArray
    :returns: path to the hdf5 file of the Array
    """
    adapter = array._adapter
    return get_main_path(array.id, adapter.collection_path / adapter.data_dir) / (array.id + adapter.file_ext)


def is_free(path: Path) -> bool:
    """Tell whether a file can be taken exclusively right now.

    :param path: path to the file
    :returns: ``True`` if nobody holds the file
    """
    flock = Flock(path)
    try:
        flock.acquire()
    except DekerLockError:
        return False
    flock.release()
    return True


class HeldStorageCall:
    """Replacement of a storage adapter method that records what is held while it runs and waits to be let go.

    :param original: the replaced bound method
    :param files: main files of the Arrays, by their v_position in the vgrid
    """

    def __init__(self, original, files: dict[tuple[int, ...], Path]) -> None:
        self.original = original
        self.files = files
        self.inside = Event()
        self.go = Event()
        self.free: dict[tuple[int, ...], bool] = {}
        self.lock_files: list[str] = []

    def __call__(self, path: Path, *args, **kwargs):
        if not self.inside.is_set():
            self.free = {vpos: is_free(file) for vpos, file in self.files.items()}
            self.lock_files = sorted(str(file.name) for file in path.parent.rglob("*lock"))
            self.inside.set()
            assert self.go.wait(10)
        return self.original(path, *args, **kwargs)


class TestFindArray:
    """A VSubset finds the Arrays of its VArray by their position in the vgrid, without reading their metadata."""

    @pytest.fixture
    def held_array(self, inserted_varray: VArray) -> Iterator[tuple[tuple[int, ...], Flock]]:
        """Holds the first Array as a writer of another process does; the Array is released afterwards."""
        subset = inserted_varray[:]
        vpos = subset._VSubset__arrays[0].vposition
        array = subset._create_array_from_vposition(vpos)
        with Flock(array_main_file(array)) as flock:
            yield vpos, flock

    def test_find_array_does_not_read_the_metadata(self, inserted_varray: VArray, mocker: MockerFixture):
        subset = inserted_varray[:]
        adapter = subset._VSubset__array_adapter
        read_meta = mocker.spy(adapter, "read_meta")
        storage_read_meta = mocker.spy(adapter.storage_adapter, "read_meta")
        array = subset._create_array_from_vposition(subset._VSubset__arrays[0].vposition)
        assert array is not None
        assert read_meta.call_count == 0
        assert storage_read_meta.call_count == 0

    def test_found_array_equals_the_one_built_from_the_metadata(self, inserted_varray: VArray):
        """The Array built from the VArray has the id, the attributes and the dimensions of the one on disk."""
        subset = inserted_varray[:]
        vpos = subset._VSubset__arrays[-1].vposition
        array = subset._create_array_from_vposition(vpos)
        from_meta = inserted_varray._VArray__collection.arrays.filter(
            {"vid": inserted_varray.id, "v_position": vpos}
        ).last()
        assert array is not None and from_meta is not None
        assert array.as_dict == from_meta.as_dict
        assert array._vid == inserted_varray.id
        assert array._v_position == vpos

    def test_found_array_of_a_timed_varray_gets_its_time_attributes(self, inserted_varray_with_attributes: VArray):
        """The start of the time dimension of an inner Array is computed from the position, as on creation."""
        subset = inserted_varray_with_attributes[:]
        vpos = subset._VSubset__arrays[-1].vposition
        array = subset._create_array_from_vposition(vpos)
        from_meta = inserted_varray_with_attributes._VArray__collection.arrays.filter(
            {"vid": inserted_varray_with_attributes.id, "v_position": vpos}
        ).last()
        assert array is not None and from_meta is not None
        assert array.custom_attributes == from_meta.custom_attributes
        assert [d.as_dict for d in array.dimensions] == [d.as_dict for d in from_meta.dimensions]

    def test_find_array_returns_none_for_a_missing_position(self, varray_collection: Collection):
        varray = varray_collection.create()
        try:
            subset = varray[:]
            assert subset._create_array_from_vposition(subset._VSubset__arrays[0].vposition) is None
        finally:
            varray.delete()

    def test_find_array_ignores_a_writer_of_another_process(self, inserted_varray: VArray, held_array):
        """The search does not touch the file, so a writer holding the Array does not stop it."""
        vpos, _ = held_array
        subset = inserted_varray[:]
        array = subset._create_array_from_vposition(vpos)
        assert array is not None
        assert array._v_position == vpos

    def test_update_waits_for_the_writer_of_another_process_to_leave(self, inserted_varray: VArray, held_array):
        """The write lock of the Array waits for the writer, and the data is written once it leaves."""
        vpos, flock = held_array
        data = np.full(inserted_varray.shape, 7, dtype=inserted_varray.dtype)
        with ThreadPoolExecutor(1) as executor:
            writing = executor.submit(inserted_varray[:].update, data)
            time.sleep(0.5)
            assert not writing.done()
            flock.release()
            writing.result(timeout=30)
        assert (inserted_varray[:].read() == data).all()

    def test_update_times_out_on_an_array_held_by_another_writer(self, inserted_varray: VArray, held_array):
        """When the writer does not leave in time, that Array fails and the error names the lock."""
        vpos, _ = held_array
        subset = inserted_varray[:]
        config = subset._VSubset__adapter.ctx.config
        timeout, interval = config.write_lock_timeout, config.write_lock_check_interval
        config.write_lock_timeout, config.write_lock_check_interval = 1, 1
        try:
            started = time.monotonic()
            with pytest.raises(DekerLockError) as info:
                subset.update(np.ones(inserted_varray.shape, dtype=inserted_varray.dtype))
            assert time.monotonic() - started >= 1
            assert f"1 of {len(subset._VSubset__arrays)} Arrays were not released in time" in str(info.value)
            assert not isinstance(info.value, DekerVSubsetError)
        finally:
            config.write_lock_timeout, config.write_lock_check_interval = timeout, interval


@pytest.mark.timeout(60)
class TestVSubsetWritesArrayByArray:
    """A VArray write holds each Array only while it is written and leaves no files behind."""

    @pytest.fixture
    def files(self, inserted_varray: VArray) -> dict[tuple[int, ...], Path]:
        """Main files of the Arrays by their v_position in the vgrid."""
        subset = inserted_varray[:]
        return {
            position.vposition: array_main_file(subset._create_array_from_vposition(position.vposition))
            for position in subset._VSubset__arrays
        }

    @pytest.mark.parametrize("method", ["update_data", "clear_data"])
    def test_only_the_written_array_is_held(
        self, inserted_varray: VArray, files: dict[tuple[int, ...], Path], mocker: MockerFixture, method: str
    ):
        """While an Array is written, that Array is held exclusively, the other Arrays are free, no file is made."""
        storage = inserted_varray._VArray__array_adapter.storage_adapter
        held = HeldStorageCall(getattr(storage, method), files)
        mocker.patch.object(storage, method, held)
        subset = inserted_varray[0:2, 0:2, 0:2]  # one Array: (0, 0, 0)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(subset.update, 1.0) if method == "update_data" else pool.submit(subset.clear)
            assert held.inside.wait(10)
            assert held.free == {vpos: vpos != (0, 0, 0) for vpos in files}
            assert held.lock_files == []
            held.go.set()
            assert future.result(timeout=10) is None
        assert all(is_free(file) for file in files.values())

    def test_update_leaves_no_files_behind(self, inserted_varray: VArray, files: dict[tuple[int, ...], Path]):
        """A VArray write touches nothing but the files of its Arrays."""
        collection_path = inserted_varray._VArray__collection.path
        before = sorted(collection_path.rglob("*"))
        inserted_varray[:].update(2.0)
        assert sorted(collection_path.rglob("*")) == before
        assert all(is_free(file) for file in files.values())
        assert np.all(inserted_varray[:].read() == 2.0)

    def test_writers_of_one_array_queue_on_it(
        self,
        inserted_varray: VArray,
        array_data: np.ndarray,
        files: dict[tuple[int, ...], Path],
        mocker: MockerFixture,
    ):
        """Two threads writing different parts of one Array wait for each other; both parts land."""
        storage = inserted_varray._VArray__array_adapter.storage_adapter
        held = HeldStorageCall(storage.update_data, files)
        mocker.patch.object(storage, "update_data", held)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(inserted_varray[0:2, 0:5, 0:5].update, 1.0)
            assert held.inside.wait(10)
            second = pool.submit(inserted_varray[2:5, 0:5, 0:5].update, 2.0)
            with pytest.raises(FutureTimeoutError):
                second.result(timeout=0.5)  # the Array is held by the first writer
            held.go.set()
            assert first.result(timeout=10) is None
            assert second.result(timeout=10) is None
        data = inserted_varray[:].read()
        assert np.all(data[0:2, 0:5, 0:5] == 1.0)
        assert np.all(data[2:5, 0:5, 0:5] == 2.0)
        assert np.array_equal(data[5:, :, :], array_data[5:, :, :])  # the other Arrays are untouched


class TestSplitAxis:
    """Parts of the Arrays a subset bound selects along one axis, with their slices of the subset data."""

    @pytest.mark.parametrize(
        ("bound", "size", "array_size", "expected"),
        [
            # whole axis of 3 Arrays of 2 cells: a whole Array is the plain slice(None, None)
            (
                slice(None),
                6,
                2,
                [
                    (0, slice(None, None), slice(0, 2)),
                    (1, slice(None, None), slice(2, 4)),
                    (2, slice(None, None), slice(4, 6)),
                ],
            ),
            # the second Array is cut
            (slice(0, 3), 6, 2, [(0, slice(None, None), slice(0, 2)), (1, slice(0, 1), slice(2, 3))]),
            # the first Array is skipped, the last one is cut
            (slice(2, 5), 6, 2, [(1, slice(None, None), slice(0, 2)), (2, slice(0, 1), slice(2, 3))]),
            # negative bounds are resolved against the axis
            (slice(-4, -1), 6, 2, [(1, slice(None, None), slice(0, 2)), (2, slice(0, 1), slice(2, 3))]),
            # one Array for the whole axis
            (slice(None), 6, 6, [(0, slice(None, None), slice(0, 6))]),
            # a step inside the Arrays: every other cell, the phase carries over the Array border
            (
                slice(None, None, 2),
                6,
                2,
                [(0, slice(0, 1, 2), slice(0, 1)), (1, slice(0, 1, 2), slice(1, 2)), (2, slice(0, 1, 2), slice(2, 3))],
            ),
            (
                slice(1, None, 2),
                6,
                2,
                [(0, slice(1, 2, 2), slice(0, 1)), (1, slice(1, 2, 2), slice(1, 2)), (2, slice(1, 2, 2), slice(2, 3))],
            ),
            # a step that does not divide the Array size: the first cell of an Array depends on the phase
            (
                slice(None, None, 3),
                10,
                4,
                [(0, slice(0, 4, 3), slice(0, 2)), (1, slice(2, 3, 3), slice(2, 3)), (2, slice(1, 2, 3), slice(3, 4))],
            ),
            # a step larger than an Array jumps over Arrays
            (
                slice(None, None, 5),
                12,
                2,
                [(0, slice(0, 1, 5), slice(0, 1)), (2, slice(1, 2, 5), slice(1, 2)), (5, slice(0, 1, 5), slice(2, 3))],
            ),
            # a step larger than the axis selects the first cell only
            (slice(None, None, 100), 12, 4, [(0, slice(0, 1, 100), slice(0, 1))]),
            # start inside an Array with a step
            (slice(5, 12, 4), 12, 4, [(1, slice(1, 2, 4), slice(0, 1)), (2, slice(1, 2, 4), slice(1, 2))]),
            # integers drop the axis
            (0, 6, 2, [(0, 0, None)]),
            (3, 6, 2, [(1, 1, None)]),
            (-1, 6, 2, [(2, 1, None)]),
        ],
    )
    def test_parts_follow_the_selected_positions(self, bound, size, array_size, expected):
        """Tests if the parts walk the selected positions Array by Array and their data slices continue each other."""
        assert _split_axis(bound, size, array_size) == [_ArrayBounds(*part) for part in expected]

    @pytest.mark.parametrize(
        ("size", "array_size"),
        [(6, 1), (6, 2), (6, 3), (6, 6), (7, 1), (7, 7), (12, 1), (12, 3), (12, 4), (24, 2), (24, 6), (24, 24)],
    )
    @pytest.mark.parametrize(
        "bound",
        [
            slice(None),
            slice(1, None),
            slice(None, -1),
            slice(2, 5),
            slice(None, None, 2),
            slice(1, None, 3),
            slice(None, None, 5),
            slice(None, None, 7),
            slice(3, 20, 4),
        ],
    )
    def test_parts_cover_the_selection_exactly(self, bound, size, array_size):
        """Tests if the cells of the parts are exactly the selected cells, in order, whatever the Arrays."""
        axis = np.arange(size)
        expected = axis[bound]
        collected = []
        for part in _split_axis(bound, size, array_size):
            piece = axis[part.position * array_size : (part.position + 1) * array_size]
            cells = piece[part.bounds]
            assert part.data_slice is not None
            assert part.data_slice == slice(len(collected), len(collected) + len(cells))
            collected.extend(cells)
        assert collected == expected.tolist()

    def test_negative_step_is_rejected(self):
        with pytest.raises(DekerVSubsetError):
            _split_axis(slice(None, None, -1), 6, 2)


class TestArrayPositions:
    """Arrays a subset covers, with their bounds and data slices, for several axes."""

    @pytest.mark.parametrize(
        ("bounds", "shape", "arrays_shape", "expected"),
        [
            # 2 dimensions, 4 whole Arrays of 5x5
            (
                (slice(None), slice(None)),
                (10, 10),
                (5, 5),
                [
                    ((0, 0), (slice(None, None), slice(None, None)), (slice(0, 5), slice(0, 5))),
                    ((0, 1), (slice(None, None), slice(None, None)), (slice(0, 5), slice(5, 10))),
                    ((1, 0), (slice(None, None), slice(None, None)), (slice(5, 10), slice(0, 5))),
                    ((1, 1), (slice(None, None), slice(None, None)), (slice(5, 10), slice(5, 10))),
                ],
            ),
            # the subset [0:7, 2:8] cuts every Array
            (
                (slice(0, 7), slice(2, 8)),
                (10, 10),
                (5, 5),
                [
                    ((0, 0), (slice(None, None), slice(2, 5)), (slice(0, 5), slice(0, 3))),
                    ((0, 1), (slice(None, None), slice(0, 3)), (slice(0, 5), slice(3, 6))),
                    ((1, 0), (slice(0, 2), slice(2, 5)), (slice(5, 7), slice(0, 3))),
                    ((1, 1), (slice(0, 2), slice(0, 3)), (slice(5, 7), slice(3, 6))),
                ],
            ),
            # an integer index drops its axis from the data slices and selects one row of Arrays
            (
                (7, slice(None)),
                (10, 10),
                (5, 5),
                [((1, 0), (2, slice(None, None)), (slice(0, 5),)), ((1, 1), (2, slice(None, None)), (slice(5, 10),))],
            ),
            # a step jumping over the second Array of the first axis
            (
                (slice(None, None, 6), slice(None, None, 3)),
                (12, 6),
                (4, 3),
                [
                    ((0, 0), (slice(0, 1, 6), slice(0, 1, 3)), (slice(0, 1), slice(0, 1))),
                    ((0, 1), (slice(0, 1, 6), slice(0, 1, 3)), (slice(0, 1), slice(1, 2))),
                    ((1, 0), (slice(2, 3, 6), slice(0, 1, 3)), (slice(1, 2), slice(0, 1))),
                    ((1, 1), (slice(2, 3, 6), slice(0, 1, 3)), (slice(1, 2), slice(1, 2))),
                ],
            ),
        ],
    )
    def test_arrays_with_bounds_and_data_slices(self, bounds, shape, arrays_shape, expected):
        positions = _array_positions(bounds, shape, arrays_shape)
        assert [tuple(position) for position in positions] == expected

    @pytest.mark.parametrize(
        ("shape", "arrays_shape"),
        [((12,), (4,)), ((12, 10), (4, 5)), ((12, 10, 8), (4, 2, 4)), ((6, 5, 4, 6), (2, 1, 2, 3))],
        ids=["1d", "2d", "3d", "4d"],
    )
    def test_arrays_assemble_the_numpy_selection(self, shape, arrays_shape):
        """Tests if the data slices of the Arrays assemble the numpy selection of a stepped expression."""
        data = np.arange(np.prod(shape)).reshape(shape)
        ndim = len(shape)
        for index_exp in STEPPED_INDEX_EXPS[ndim]:
            forward, flipped = positive_bounds(shape, index_exp)
            expected = data[forward]
            assembled = np.empty(expected.shape, dtype=data.dtype)
            for vposition, bounds, data_slice in _array_positions(forward, shape, arrays_shape):
                piece = data[tuple(slice(p * t, (p + 1) * t) for p, t in zip(vposition, arrays_shape))]
                assembled[data_slice] = piece[bounds]
            assert np.array_equal(assembled, expected), index_exp
            assert np.array_equal(np.flip(assembled, flipped), data[index_exp]), index_exp


class TestVSubsetArrayWorkers:
    """Methods that read, update and clear one inner Array of the subset."""

    @pytest.mark.parametrize("index_exp", [np.index_exp[:], np.index_exp[0:5, 0:5, 0:5], np.index_exp[7, 1:3, :]])
    def test_read_array_data_of_existing_arrays(
        self, inserted_varray: VArray, array_data: np.ndarray, index_exp: Slice
    ):
        """Tests if every Array of a written VArray is read with its slice of the subset data."""
        vsubset: VSubset = inserted_varray[index_exp]
        expected = array_data[index_exp]
        for array_pos in vsubset._VSubset__arrays:
            data_slice, data = vsubset._VSubset__read_array_data(array_pos, timeout=Sentinel, token=DefaultToken())
            assert data_slice == array_pos.data_slice
            assert np.array_equal(np.asarray(data), np.asarray(expected)[data_slice])

    @pytest.mark.parametrize("index_exp", [np.index_exp[:], np.index_exp[0:5, 0:5, 0:5], np.index_exp[2, 3:9, 1]])
    def test_read_array_data_of_missing_arrays(self, varray: VArray, index_exp: Slice):
        """Tests if a missing Array is read as its fill value with the shape of its bounds."""
        varray._adapter.create(varray, timeout=Sentinel, token=DefaultToken())
        vsubset: VSubset = varray[index_exp]
        for array_pos in vsubset._VSubset__arrays:
            data_slice, data = vsubset._VSubset__read_array_data(array_pos, timeout=Sentinel, token=DefaultToken())
            assert data_slice == array_pos.data_slice
            assert np.all(np.isnan(data))
            assert np.asarray(data).shape == np.asarray(np.empty(varray.arrays_shape)[array_pos.bounds]).shape

    @pytest.mark.parametrize("bounds", [(99, 0, 0), (0, 99, 0), (0, 0, -99)])
    def test_read_array_data_raises_on_wrong_bounds(self, inserted_varray: VArray, bounds: tuple):
        """Tests if an index outside the Array is rejected."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        wrong = ArrayPosition(array_pos.vposition, bounds, array_pos.data_slice)
        with pytest.raises(IndexError):
            vsubset._VSubset__read_array_data(wrong, timeout=Sentinel, token=DefaultToken())

    def test_update_array_data_creates_missing_array(self, varray: VArray):
        """Tests if updating a missing Array creates it with the data."""
        varray._adapter.create(varray, timeout=Sentinel, token=DefaultToken())
        vsubset: VSubset = varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        assert vsubset._create_array_from_vposition(array_pos.vposition) is None
        data = np.full(varray.arrays_shape, 7.0, dtype=varray.dtype)
        vsubset._VSubset__update_array_data(
            ArrayPositionedData(array_pos.vposition, array_pos.bounds, data), timeout=Sentinel, token=DefaultToken()
        )
        array = vsubset._create_array_from_vposition(array_pos.vposition)
        assert array is not None
        assert np.array_equal(array[:].read(), data)

    def test_update_array_data_updates_existing_array(self, inserted_varray: VArray):
        """Tests if an existing Array is updated in place."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[-1]
        data = np.full(inserted_varray.arrays_shape, -1.0, dtype=inserted_varray.dtype)
        vsubset._VSubset__update_array_data(
            ArrayPositionedData(array_pos.vposition, array_pos.bounds, data), timeout=Sentinel, token=DefaultToken()
        )
        array = vsubset._create_array_from_vposition(array_pos.vposition)
        assert np.array_equal(array[:].read(), data)

    @pytest.mark.parametrize(
        ("dimensions", "vgrid", "time_index"),
        [
            # 1D: the time axis is the only one
            ([("t", 6)], (3,), 0),
            # 2D: time first / time last, split along time only, along the other axis only, along both
            ([("t", 8), ("x", 4)], (4, 1), 0),
            ([("x", 4), ("t", 8)], (1, 4), 1),
            ([("x", 6), ("t", 6)], (3, 1), 1),
            ([("x", 6), ("t", 6)], (2, 3), 1),
            # 3D: time in the middle, uneven splits
            ([("x", 4), ("t", 12), ("y", 6)], (2, 4, 3), 1),
            ([("x", 4), ("t", 12), ("y", 6)], (1, 6, 1), 1),
            ([("x", 9), ("t", 10), ("y", 4)], (3, 5, 2), 1),
            # 4D: time last
            ([("a", 4), ("b", 4), ("c", 2), ("t", 8)], (2, 2, 1, 4), 3),
        ],
    )
    def test_update_array_data_sets_time_attribute_of_new_arrays(self, client: Client, dimensions, vgrid, time_index):
        """Tests if every new Array starts where its first cell is on the VArray time axis.

        The Array attribute is ``start + step * array_size * position``, so an Array's own time axis
        is the matching piece of the VArray time axis, whatever the number of dimensions and the grid.
        """
        step = timedelta(hours=1)
        schema = VArraySchema(
            dtype=float,
            dimensions=[
                TimeDimensionSchema(name=name, size=size, start_value="$start", step=step)
                if i == time_index
                else DimensionSchema(name=name, size=size)
                for i, (name, size) in enumerate(dimensions)
            ],
            vgrid=vgrid,
            attributes=[AttributeSchema(name="start", dtype=datetime, primary=False)],
        )
        collection = client.create_collection(f"arrays_time_{len(dimensions)}d_{'_'.join(map(str, vgrid))}", schema)
        try:
            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            varray = collection.create(custom_attributes={"start": start})
            vsubset: VSubset = varray[:]
            array_size = varray.arrays_shape[time_index]
            assert len(vsubset._VSubset__arrays) == np.prod(vgrid)
            for array_pos in vsubset._VSubset__arrays:
                data = np.zeros(varray.arrays_shape, dtype=varray.dtype)
                vsubset._VSubset__update_array_data(
                    ArrayPositionedData(array_pos.vposition, array_pos.bounds, data),
                    timeout=Sentinel,
                    token=DefaultToken(),
                )
                array = vsubset._create_array_from_vposition(array_pos.vposition)
                expected = start + step * array_size * array_pos.vposition[time_index]
                assert array.custom_attributes["parent.start"] == expected
                assert array.dimensions[time_index].start_value == expected
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("dimensions", "vgrid", "time_index"),
        [
            ([("t", 6)], (3,), 0),
            ([("t", 8), ("x", 4)], (4, 2), 0),
            ([("x", 6), ("t", 6)], (2, 3), 1),
            ([("x", 4), ("t", 12), ("y", 6)], (2, 4, 3), 1),
            ([("a", 4), ("b", 4), ("c", 2), ("t", 8)], (2, 2, 1, 4), 3),
        ],
    )
    def test_arrays_time_axes_match_the_varray_time_axis(self, client: Client, dimensions, vgrid, time_index):
        """Tests if the time axis of every Array, read as an Array, is the matching piece of the VArray time axis."""
        step = timedelta(minutes=30)
        schema = VArraySchema(
            dtype=float,
            dimensions=[
                TimeDimensionSchema(name=name, size=size, start_value="$start", step=step)
                if i == time_index
                else DimensionSchema(name=name, size=size)
                for i, (name, size) in enumerate(dimensions)
            ],
            vgrid=vgrid,
            attributes=[AttributeSchema(name="start", dtype=datetime, primary=False)],
        )
        collection = client.create_collection(f"arrays_axis_{len(dimensions)}d_{'_'.join(map(str, vgrid))}", schema)
        try:
            varray = collection.create(custom_attributes={"start": datetime(2024, 6, 1, tzinfo=timezone.utc)})
            varray[:].update(np.zeros(varray.shape, dtype=varray.dtype))
            time_name = dimensions[time_index][0]
            varray_axis = varray[:].describe()[time_name]
            array_size = varray.arrays_shape[time_index]
            arrays = [array for array in collection.arrays if array._vid == varray.id]
            assert len(arrays) == np.prod(vgrid)
            for array in arrays:
                position = array.primary_attributes["v_position"][time_index]
                first_cell = position * array_size
                assert array[:].describe()[time_name] == varray_axis[first_cell : first_cell + array_size]
        finally:
            collection.delete()

    @pytest.mark.parametrize(
        ("data", "error"),
        [
            (np.zeros((2, 2, 2)), IndexError),  # wrong shape
            (np.zeros((5, 5, 5), dtype=np.int8), DekerArrayError),  # wrong dtype
            ("text", DekerArrayError),  # not an array at all
        ],
    )
    def test_update_array_data_raises_on_wrong_data(self, inserted_varray: VArray, data, error):
        """Tests if data that does not fit the Array is rejected."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        with pytest.raises(error):
            vsubset._VSubset__update_array_data(
                ArrayPositionedData(array_pos.vposition, array_pos.bounds, data), timeout=Sentinel, token=DefaultToken()
            )

    def test_clear_array_deletes_fully_covered_array(self, inserted_varray: VArray):
        """Tests if an Array fully covered by the subset is deleted."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        vsubset._VSubset__clear_array(array_pos, timeout=Sentinel, token=DefaultToken())
        assert vsubset._create_array_from_vposition(array_pos.vposition) is None

    def test_clear_array_clears_partially_covered_array(self, inserted_varray: VArray, array_data: np.ndarray):
        """Tests if an Array partially covered by the subset is cleared only inside the bounds and kept."""
        vsubset: VSubset = inserted_varray[0:2, 0:2, 0:2]
        array_pos = vsubset._VSubset__arrays[0]
        vsubset._VSubset__clear_array(array_pos, timeout=Sentinel, token=DefaultToken())
        array = vsubset._create_array_from_vposition(array_pos.vposition)
        assert array is not None
        read = array[:].read()
        assert np.all(np.isnan(read[0:2, 0:2, 0:2]))
        assert np.array_equal(read[2:, 2:, 2:], array_data[2:5, 2:5, 2:5])

    def test_clear_array_ignores_missing_array(self, varray: VArray):
        """Tests if clearing an Array that was never written does nothing and raises nothing."""
        varray._adapter.create(varray, timeout=Sentinel, token=DefaultToken())
        vsubset: VSubset = varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        vsubset._VSubset__clear_array(array_pos, timeout=Sentinel, token=DefaultToken())
        assert vsubset._create_array_from_vposition(array_pos.vposition) is None


class TestVSubsetForXArray:
    @pytest.fixture(scope="class")
    def full_described_varray(self, client) -> VArray:
        dims = [
            TimeDimensionSchema(
                name="dt",
                size=24,
                start_value=datetime(2023, 1, 1, tzinfo=timezone.utc),
                step=timedelta(hours=1),
            ),
            DimensionSchema(name="y", size=721, scale=Scale(start_value=90.0, step=-0.25, name="lat")),
            DimensionSchema(name="x", size=1440, scale=Scale(start_value=-180.0, step=0.25, name="lon")),
            DimensionSchema(name="layers", size=3, labels=["temp", "pres", "pcp"]),
        ]

        attrs = [
            AttributeSchema(name="ca", dtype=int, primary=False),
            AttributeSchema(name="pa", dtype=int, primary=True),
        ]
        schema = VArraySchema(dtype=float, dimensions=dims, attributes=attrs, vgrid=(24, 1, 1, 1))
        options = HDF5Options(compression_opts=HDF5CompressionOpts(**hdf5plugin.Zstd(2)))
        col = client.create_collection("described_coll", schema, options)
        try:
            array = col.create({"pa": 1}, {"ca": 0})
            yield array
        finally:
            col.delete()

    @pytest.fixture(scope="class")
    def not_described_varray(self, client) -> VArray:
        dims = [
            TimeDimensionSchema(
                name="dt",
                size=24,
                start_value=datetime(2023, 1, 1, tzinfo=timezone.utc),
                step=timedelta(hours=1),
            ),
            DimensionSchema(name="y", size=721),
            DimensionSchema(name="x", size=1440),
            DimensionSchema(name="layers", size=3),
        ]

        attrs = [
            AttributeSchema(name="ca", dtype=int, primary=False),
            AttributeSchema(name="pa", dtype=int, primary=True),
        ]
        schema = VArraySchema(dtype=float, dimensions=dims, attributes=attrs, vgrid=(24, 1, 1, 1))
        options = HDF5Options(compression_opts=HDF5CompressionOpts(**hdf5plugin.Zstd(2)))
        col = client.create_collection("not_described_coll", schema, options)
        try:
            array = col.create({"pa": 1}, {"ca": 0})
            yield array
        finally:
            col.delete()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_vsubset_description_on_read_xarray(self, varray_collection: Collection, index_exp: Slice):
        varray: VArray = varray_collection.create()
        vsubset = varray[index_exp]

        if vsubset.shape == ():
            with pytest.raises(DekerSubsetError):
                assert vsubset.read_xarray()
        else:
            try:
                xarray = vsubset.read_xarray()
                assert xarray.attrs["primary_attributes"] == varray.primary_attributes
                assert xarray.attrs["custom_attributes"] == varray.custom_attributes

                ds = xarray.to_dataset()
                assert ds

                # cannot set assert conversion to pandas.DataFrame as all data is np.NaN
                # and cannot be compared with pandas.DataFrame suggested methods
                xarray.to_dataframe()  # so just trying to create it
            except Exception as e:
                raise AssertionError(e)

    @pytest.mark.parametrize(
        ("index_exp", "expected"),
        [
            (
                np.index_exp[:],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, 0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": [90.0],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, 0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": [-180],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, 1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["pres"],
                    }
                ),
            ),
            (
                np.index_exp[0, 1, 2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.75],
                        "x": [-179.5],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0, 1, 2, 0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.75],
                        "x": [-179.5],
                        "layers": ["temp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.75],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2, 2:3],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.75],
                        "x": [-179.5],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2, 2:3, 0:1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.75],
                        "x": [-179.5],
                        "layers": ["temp"],
                    }
                ),
            ),
            (
                np.index_exp[6:12],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(6, 12)
                        ],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, 6:12],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": [88.5, 88.25, 88.0, 87.75, 87.5, 87.25],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, 6:12],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": [-178.5, -178.25, -178.0, -177.75, -177.5, -177.25],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, :2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": np.around(np.arange(90, -91, -0.25), 2).tolist()[:721],
                        "x": np.around(np.arange(-180, 180, 0.25), 2).tolist()[:1440],
                        "layers": ["temp", "pres"],
                    }
                ),
            ),
            (
                np.index_exp[22:, 719:, 1438:, 1:],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, 22, tzinfo=timezone.utc),
                            datetime(2023, 1, 1, 23, tzinfo=timezone.utc),
                        ],
                        "y": [-89.75, -90.0],
                        "x": [179.5, 179.75],
                        "layers": ["pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[-1, -1, -1, -1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 23, 0, tzinfo=timezone.utc)],
                        "y": [-90.0],
                        "x": [179.75],
                        "layers": ["pcp"],
                    }
                ),
            ),
            (
                np.index_exp[-2, -2, -2, -2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 22, 0, tzinfo=timezone.utc)],
                        "y": [-89.75],
                        "x": [179.5],
                        "layers": ["pres"],
                    }
                ),
            ),
            (
                np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, 16.5, "pcp"],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc)],
                        "y": [-3.5],
                        "x": [16.5],
                        "layers": ["pcp"],
                    }
                ),
            ),
        ],
    )
    def test_vsubset_description(self, full_described_varray: VArray, index_exp: Slice, expected):
        subset = full_described_varray[index_exp]
        assert subset.describe() == expected

    @pytest.mark.parametrize(
        ("index_exp", "expected"),
        [
            (
                np.index_exp[:],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": list(range(721)),
                        "x": list(range(1440)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": list(range(721)),
                        "x": list(range(1440)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, 0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": [0],
                        "x": list(range(1440)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, 0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": list(range(721)),
                        "x": [0],
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, 1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": list(range(721)),
                        "x": list(range(1440)),
                        "layers": [1],
                    }
                ),
            ),
            (
                np.index_exp[0, 1, 2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [1],
                        "x": [2],
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[0, 1, 2, 0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [1],
                        "x": [2],
                        "layers": [0],
                    }
                ),
            ),
            (
                np.index_exp[0:1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": list(range(721)),
                        "x": list(range(1440)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [1],
                        "x": list(range(1440)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2, 2:3],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [1],
                        "x": [2],
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2, 2:3, 0:1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [1],
                        "x": [2],
                        "layers": [0],
                    }
                ),
            ),
            (
                np.index_exp[6:12],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(6, 12)
                        ],
                        "y": list(range(721)),
                        "x": list(range(1440)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, 6:12],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": [6, 7, 8, 9, 10, 11],
                        "x": list(range(1440)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, 6:12],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": list(range(721)),
                        "x": [6, 7, 8, 9, 10, 11],
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, :2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n for n in range(24)],
                        "y": list(range(721)),
                        "x": list(range(1440)),
                        "layers": [0, 1],
                    }
                ),
            ),
            (
                np.index_exp[22:, 719:, 1438:, 1:],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, 22, tzinfo=timezone.utc),
                            datetime(2023, 1, 1, 23, tzinfo=timezone.utc),
                        ],
                        "y": [719, 720],
                        "x": [1438, 1439],
                        "layers": [1, 2],
                    }
                ),
            ),
            (
                np.index_exp[-1, -1, -1, -1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 23, 0, tzinfo=timezone.utc)],
                        "y": [720],
                        "x": [1439],
                        "layers": [2],
                    }
                ),
            ),
            (
                np.index_exp[-2, -2, -2, -2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 22, 0, tzinfo=timezone.utc)],
                        "y": [719],
                        "x": [1438],
                        "layers": [1],
                    }
                ),
            ),
        ],
    )
    def test_vsubset_index_description(self, not_described_varray: VArray, index_exp: Slice, expected):
        subset = not_described_varray[index_exp]
        assert subset.describe() == expected

    @pytest.mark.parametrize(
        "index_exp",
        [
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, 16.5, "pcp"],
            np.index_exp[0, 0, 0, 4],
            np.index_exp[::0],
            np.index_exp[::True],
        ],
    )
    def test_not_described_subset_raises(self, not_described_varray, index_exp):
        with pytest.raises(IndexError):
            assert not_described_varray[index_exp]

    @pytest.mark.parametrize(
        "index_exp",
        [
            np.index_exp[1.0, -3.5, 16.5, "pcp"],
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), "-3.5", 16.5, "pcp"],
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, "16.5", "pcp"],
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, 16.5, 1.0],
            np.index_exp[0, 0, 0, 4],
            np.index_exp[::0],
            np.index_exp[::True],
        ],
    )
    def test_described_vsubset_raises(self, full_described_varray, index_exp):
        with pytest.raises(IndexError):
            assert full_described_varray[index_exp]

    @pytest.mark.parametrize(
        "index_exp",
        [
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, 16.5, "pcp"],
            np.index_exp[1.0, -3.5, 16.5, "pcp"],
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), "-3.5", 16.5, "pcp"],
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, "16.5", "pcp"],
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, 16.5, 1.0],
            np.index_exp[0, 0, 0, 4],
            np.index_exp[::0],
            np.index_exp[::True],
        ],
    )
    def test_not_described_vsubset_raises(self, not_described_varray, index_exp):
        with pytest.raises(IndexError):
            assert not_described_varray[index_exp]


@pytest.fixture
def stepped_data(stepped_varray_collection: Collection) -> np.ndarray:
    """Distinct values for every cell of the VArray of the collection."""
    shape = stepped_varray_collection.varray_schema.shape
    return np.arange(1, np.prod(shape, dtype=int) + 1, dtype=float).reshape(shape)


@pytest.fixture
def stepped_varray(stepped_varray_collection: Collection, stepped_data: np.ndarray) -> Iterator[VArray]:
    """A VArray with data in every cell."""
    varray = stepped_varray_collection.create()
    varray[:].update(stepped_data)
    try:
        yield varray
    finally:
        varray.delete()


class TestVSubsetSteps:
    """Reading, writing and clearing a stepped subset of a VArray gives exactly what numpy gives, on every grid."""

    @pytest.mark.parametrize(
        ("filled_stepped_varray", "index_exp"), varray_step_cases(), indirect=["filled_stepped_varray"]
    )
    def test_read(self, filled_stepped_varray: VArray, index_exp: Slice):
        vsubset = filled_stepped_varray[index_exp]
        expected = SteppedCollections.data(filled_stepped_varray.shape)[index_exp]
        assert vsubset.shape == expected.shape
        assert np.array_equal(vsubset.read(), expected)

    @pytest.mark.parametrize(
        ("stepped_varray_collection", "index_exp"), varray_step_cases(), indirect=["stepped_varray_collection"]
    )
    def test_update(self, stepped_varray: VArray, stepped_data: np.ndarray, index_exp: Slice):
        vsubset = stepped_varray[index_exp]
        new_data = -1.0 - np.arange(np.prod(vsubset.shape, dtype=int), dtype=float).reshape(vsubset.shape)
        expected = stepped_data.copy()
        expected[index_exp] = new_data

        vsubset.update(new_data)

        assert np.array_equal(vsubset.read(), new_data)
        assert np.array_equal(stepped_varray[:].read(), expected)

    @pytest.mark.parametrize(
        ("stepped_varray_collection", "index_exp"),
        varray_step_cases(every_grid=False),
        indirect=["stepped_varray_collection"],
    )
    def test_update_with_a_scalar(self, stepped_varray: VArray, stepped_data: np.ndarray, index_exp: Slice):
        vsubset = stepped_varray[index_exp]
        expected = stepped_data.copy()
        expected[index_exp] = -7.0

        vsubset.update(-7.0)

        assert np.array_equal(stepped_varray[:].read(), expected)

    @pytest.mark.parametrize(
        ("stepped_varray_collection", "index_exp"), varray_step_cases(), indirect=["stepped_varray_collection"]
    )
    def test_clear(self, stepped_varray: VArray, stepped_data: np.ndarray, index_exp: Slice):
        vsubset = stepped_varray[index_exp]
        expected = stepped_data.copy()
        expected[index_exp] = np.nan

        vsubset.clear()

        assert np.all(np.isnan(vsubset.read()))
        assert np.array_equal(stepped_varray[:].read(), expected, equal_nan=True)

    @pytest.mark.parametrize(
        ("stepped_varray_collection", "index_exp"), varray_step_cases(), indirect=["stepped_varray_collection"]
    )
    def test_update_into_an_empty_varray(
        self, stepped_varray_collection: Collection, stepped_data: np.ndarray, index_exp: Slice
    ):
        """Tests if a stepped write creates only the Arrays it touches and leaves the other cells empty."""
        varray = stepped_varray_collection.create()
        try:
            vsubset = varray[index_exp]
            new_data = stepped_data[index_exp]
            expected = np.full(varray.shape, np.nan)
            expected[index_exp] = new_data

            vsubset.update(new_data)

            assert np.array_equal(varray[:].read(), expected, equal_nan=True)
        finally:
            varray.delete()

    @pytest.mark.parametrize(
        ("stepped_varray_collection", "index_exp"),
        varray_step_cases(every_grid=False),
        indirect=["stepped_varray_collection"],
    )
    def test_wrong_shape_raises(self, stepped_varray: VArray, index_exp: Slice):
        vsubset = stepped_varray[index_exp]
        wrong = np.zeros(tuple(n + 1 for n in vsubset.shape))
        with pytest.raises(IndexError):
            vsubset.update(wrong)


class DatasetAccessLog:
    """Record which HDF5 datasets are read through h5py while a test runs.

    The h5py hook must be a plain function (a bound method set on a class is not re-bound to the dataset),
    so the hook reports to the log that is active for the current test.
    """

    active: "DatasetAccessLog | None" = None

    def __init__(self) -> None:
        self.reads: list[str] = []

    @property
    def data_reads(self) -> int:
        """Number of reads of the ``data`` datasets of the Arrays."""
        return self.reads.count("/data")


_dataset_read = h5py.Dataset.__getitem__


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


@pytest.fixture
def access(mocker: MockerFixture) -> Iterator[DatasetAccessLog]:
    mocker.patch.object(h5py.Dataset, "__getitem__", _record_read)
    log = DatasetAccessLog()
    DatasetAccessLog.active = log
    try:
        yield log
    finally:
        DatasetAccessLog.active = None


@pytest.fixture
def split_varray(client: Client) -> Iterator[VArray]:
    """A VArray of shape (15, 10) split into three Arrays (5, 10) along the first axis, with data in every cell."""
    schema = VArraySchema(
        dtype=float,
        dimensions=[DimensionSchema(name="x", size=15), DimensionSchema(name="y", size=10)],
        vgrid=(3, 1),
    )
    collection = client.create_collection("split_varray_reads", schema)
    try:
        varray = collection.create()
        varray[:].update(np.arange(1, 151, dtype=float).reshape(15, 10))
        yield varray
    finally:
        collection.delete()


# Subsets of the (15, 10) VArray and the number of Arrays they cover only partially
ARRAY_COVERAGE_CASES = [
    pytest.param(np.index_exp[:], 0, id="whole-varray"),
    pytest.param(np.index_exp[0:5], 0, id="first-Array-whole"),
    pytest.param(np.index_exp[5:15], 0, id="last-two-Arrays-whole"),
    pytest.param(np.index_exp[2:15], 1, id="first-Array-partial-rest-whole"),
    pytest.param(np.index_exp[0:7], 1, id="first-Array-whole-second-partial"),
    pytest.param(np.index_exp[2:7], 2, id="two-Arrays-partial"),
    pytest.param(np.index_exp[:, 0:5], 3, id="every-Array-partial"),
    pytest.param(np.index_exp[3], 1, id="one-row-of-the-first-Array"),
    pytest.param(np.index_exp[4:11], 2, id="outer-Arrays-partial-middle-whole"),
]


class TestVSubsetArrayReads:
    """A write or a clear reads an Array only when it covers the Array partially; a fully covered Array is replaced."""

    @pytest.mark.parametrize(("index_exp", "partial_arrays"), ARRAY_COVERAGE_CASES)
    def test_update_reads_only_partially_covered_arrays(
        self, split_varray: VArray, access: DatasetAccessLog, index_exp: Slice, partial_arrays: int
    ):
        before = split_varray[:].read()
        data = np.full(before[index_exp].shape, -1.0)
        reads_before = access.data_reads  # the Arrays read by the line above

        split_varray[index_exp].update(data)
        reads = access.data_reads - reads_before

        expected = before.copy()
        expected[index_exp] = data
        assert np.array_equal(split_varray[:].read(), expected)
        assert reads == partial_arrays

    @pytest.mark.parametrize(("index_exp", "partial_arrays"), ARRAY_COVERAGE_CASES)
    def test_clear_reads_only_partially_covered_arrays(
        self, split_varray: VArray, access: DatasetAccessLog, index_exp: Slice, partial_arrays: int
    ):
        before = split_varray[:].read()
        reads_before = access.data_reads  # the Arrays read by the line above

        split_varray[index_exp].clear()
        reads = access.data_reads - reads_before

        expected = before.copy()
        expected[index_exp] = np.nan
        assert np.array_equal(split_varray[:].read(), expected, equal_nan=True)
        assert reads == partial_arrays


if __name__ == "__main__":
    pytest.main()


class CancelOnFirstCall:
    """Stand for a method of an adapter: the first call cancels the scope, every call does the real work.

    :param scope: token of the cancellation scope
    :param method: the real method
    """

    def __init__(self, scope: SimpleToken, method):
        self.scope = scope
        self.method = method
        self.calls = 0

    def __call__(self, *args, **kwargs):
        self.calls += 1
        if self.calls == 1:
            self.scope.cancel()
        return self.method(*args, **kwargs)


def read_slowly(*args, **kwargs):
    """Stand for a read of the storage that takes two seconds."""
    time.sleep(2)
    return np.zeros((5, 5, 5))


class TestVSubsetTimeoutAndCancellation:
    """A VSubset reads and writes its Arrays under one token of the operation."""

    @pytest.fixture
    def one_worker(self, inserted_varray: VArray, mocker) -> Iterator[None]:
        """Run the Arrays of the VArray one after another, so the order of the cancellation is known."""
        executor = ThreadPoolExecutor(1)
        mocker.patch.object(inserted_varray._adapter, "executor", executor)
        yield
        executor.shutdown()

    @pytest.mark.parametrize("operation", ["read", "update", "clear"])
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [pytest.param({"timeout": 5}, 5, id="given"), pytest.param({}, Sentinel, id="not-given")],
    )
    def test_timeout_reaches_the_adapter_of_every_array(
        self, inserted_varray: VArray, mocker, operation, kwargs, expected
    ):
        adapter = inserted_varray._VArray__array_adapter
        method = {"read": "read_data", "update": "update", "clear": "clear"}[operation]
        adapter_call = mocker.patch.object(adapter, method, return_value=np.zeros((3, 3, 3)))
        args = (1.0,) if operation == "update" else ()
        getattr(inserted_varray[0:3, 0:3, 0:3], operation)(*args, **kwargs)
        assert adapter_call.call_count == 1
        assert adapter_call.call_args.kwargs["timeout"] == expected
        assert isinstance(adapter_call.call_args.kwargs["token"], AbstractToken)

    @pytest.mark.parametrize("operation", ["read", "update", "clear"])
    @pytest.mark.parametrize("timeout", [0, -1, 1.5, True, "10", None])
    def test_invalid_timeout_is_rejected(self, inserted_varray: VArray, operation, timeout):
        args = (1.0,) if operation == "update" else ()
        with pytest.raises(DekerValidationError):
            getattr(inserted_varray[:], operation)(*args, timeout=timeout)

    @pytest.mark.parametrize("operation", ["read", "update", "clear"])
    def test_cancelled_scope_stops_before_any_array(
        self, inserted_varray: VArray, array_data: np.ndarray, mocker, operation
    ):
        adapter = inserted_varray._VArray__array_adapter
        spies = [mocker.spy(adapter, method) for method in ("read_data", "update", "clear", "delete", "create")]
        token = SimpleToken()
        token.cancel()
        args = (1.0,) if operation == "update" else ()
        with cancellation_scope(token), pytest.raises(DekerCancelledError):
            getattr(inserted_varray[:], operation)(*args)
        assert all(spy.call_count == 0 for spy in spies)
        np.testing.assert_array_equal(inserted_varray[:].read(), array_data)

    @pytest.mark.timeout(20)
    def test_read_cancelled_after_the_first_array_reads_no_other(self, inserted_varray: VArray, one_worker, mocker):
        adapter = inserted_varray._VArray__array_adapter
        scope = SimpleToken()
        first_read = CancelOnFirstCall(scope, adapter.read_data)
        mocker.patch.object(adapter, "read_data", side_effect=first_read)
        with cancellation_scope(scope), pytest.raises(DekerCancelledError):
            inserted_varray[:].read()
        assert first_read.calls == 1

    @pytest.mark.timeout(20)
    def test_read_over_its_timeout_returns_no_data(self, inserted_varray: VArray, mocker):
        adapter = inserted_varray._VArray__array_adapter
        mocker.patch.object(adapter.storage_adapter, "read_data", side_effect=read_slowly)
        start = time.monotonic()
        with pytest.raises(DekerTimeoutError):
            inserted_varray[:].read(timeout=1)
        assert time.monotonic() - start < 10

    @pytest.mark.timeout(20)
    def test_write_cancelled_after_the_first_file_is_started_goes_to_its_end(
        self, inserted_varray: VArray, one_worker, mocker
    ):
        adapter = inserted_varray._VArray__array_adapter
        scope = SimpleToken()
        first_write = CancelOnFirstCall(scope, adapter.storage_adapter.update_data)
        mocker.patch.object(adapter.storage_adapter, "update_data", side_effect=first_write)
        with cancellation_scope(scope):
            inserted_varray[:].update(7.0)
        assert first_write.calls == 8
        np.testing.assert_array_equal(inserted_varray[:].read(), np.full((10, 10, 10), 7.0))

    @pytest.mark.timeout(20)
    def test_write_cancelled_before_any_file_changes_nothing(
        self, inserted_varray: VArray, array_data: np.ndarray, mocker
    ):
        adapter = inserted_varray._VArray__array_adapter
        update_data = mocker.spy(adapter.storage_adapter, "update_data")
        token = SimpleToken()
        token.cancel()
        with cancellation_scope(token), pytest.raises(DekerCancelledError):
            inserted_varray[:].update(7.0)
        update_data.assert_not_called()
        np.testing.assert_array_equal(inserted_varray[:].read(), array_data)

    @pytest.mark.timeout(20)
    def test_write_cancelled_after_its_start_before_any_file_changes_nothing(
        self, inserted_varray: VArray, array_data: np.ndarray, mocker
    ):
        """The scope is cancelled after the entry check of the write: every Array stops at its own check."""
        adapter = inserted_varray._VArray__array_adapter
        scope = SimpleToken()
        process_data = CancelOnFirstCall(scope, adapter._process_data)
        mocker.patch.object(adapter, "_process_data", side_effect=process_data)
        update_data = mocker.spy(adapter.storage_adapter, "update_data")
        with cancellation_scope(scope), pytest.raises(DekerCancelledError) as error:
            inserted_varray[:].update(7.0)
        assert type(error.value) is DekerCancelledError
        assert process_data.calls == 1
        update_data.assert_not_called()
        np.testing.assert_array_equal(inserted_varray[:].read(), array_data)

    @pytest.mark.timeout(20)
    @pytest.mark.parametrize("setting", [False, True], ids=["argument", "client-setting"])
    def test_write_waits_for_a_held_array_up_to_its_timeout(self, inserted_varray: VArray, mocker, setting):
        adapter = inserted_varray._VArray__array_adapter
        kwargs = {"timeout": 1}
        if setting:
            mocker.patch.object(adapter.ctx.config, "write_lock_timeout", 1)
            kwargs = {}
        held = inserted_varray[0:5, 0:5, 0:5]._VSubset__arrays[0]
        array = inserted_varray[:]._create_array_from_vposition(held.vposition)
        holder = Flock(adapter._get_main_path_to_file(array), shared=True)
        holder.acquire()
        try:
            start = time.monotonic()
            with pytest.raises(DekerLockError, match="written partly: 1 of 8"):
                inserted_varray[:].update(7.0, **kwargs)
            assert time.monotonic() - start < 2.5
        finally:
            holder.release()

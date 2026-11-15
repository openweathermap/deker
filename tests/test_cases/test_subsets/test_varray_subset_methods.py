import time
import traceback

from collections import OrderedDict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from random import randint
from typing import Any

import h5py
import hdf5plugin
import numpy as np
import pytest

from deker_local_adapters import HDF5CompressionOpts, HDF5Options
from deker_tools.slices import positive_bounds
from pytest_mock import MockerFixture

from tests.parameters.index_exp_params import STEPPED_INDEX_EXPS, valid_index_exp_params, varray_step_cases
from tests.plugins.collection import SteppedCollections

from deker.arrays import VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerSubsetError, DekerVSubsetError
from deker.schemas import AttributeSchema, DimensionSchema, TimeDimensionSchema, VArraySchema
from deker.subset import VSubset, _split_axis, _tile_positions, _TileBounds
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

    def test_vsubset_update_and_read_data(self, varray: VArray):
        """Test vsubset update and read methods."""
        find = varray._VArray__collection.filter({"id": varray.id})  # type: ignore[attr-defined]
        array = find.first()
        assert array is None
        varray._adapter.create(varray)
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
        varray._adapter.create(varray)
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

    def test_DekerVSubsetError_exception_group(self, inserted_varray: VArray, root_path, varray_collection: Collection):
        """Test raise of DekerVSubsetError."""
        excs = [ValueError, TypeError, FileExistsError, FileNotFoundError, EOFError]
        sleep_time = randint(1, 5)

        def raise_exc(exc):
            time.sleep(sleep_time)
            raise exc(f"This is {exc.__name__}")

        def catch_errors(threads_results):
            exceptions = []
            for result in threads_results:
                try:
                    result.result()
                except Exception as e:
                    exceptions.append(repr(e) + "\n" + traceback.format_exc(-1))
            if exceptions:
                raise DekerVSubsetError("ExceptionGroup works", exceptions)

        timer = time.monotonic()
        with ThreadPoolExecutor(len(excs)) as pool:
            futures = [pool.submit(raise_exc, exc) for exc in excs]

        with pytest.raises(DekerVSubsetError):
            assert catch_errors(threads_results=futures)
        # catch_errors(threads_results=futures)

        test_time = int(time.monotonic() - timer)
        assert test_time <= sleep_time


class TestSplitAxis:
    """Parts of the tiles a subset bound selects along one axis, with their slices of the subset data."""

    @pytest.mark.parametrize(
        ("bound", "size", "tile_size", "expected"),
        [
            # whole axis of 3 tiles of 2 cells: a whole tile is the plain slice(None, None)
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
            # the second tile is cut
            (slice(0, 3), 6, 2, [(0, slice(None, None), slice(0, 2)), (1, slice(0, 1), slice(2, 3))]),
            # the first tile is skipped, the last one is cut
            (slice(2, 5), 6, 2, [(1, slice(None, None), slice(0, 2)), (2, slice(0, 1), slice(2, 3))]),
            # negative bounds are resolved against the axis
            (slice(-4, -1), 6, 2, [(1, slice(None, None), slice(0, 2)), (2, slice(0, 1), slice(2, 3))]),
            # one tile for the whole axis
            (slice(None), 6, 6, [(0, slice(None, None), slice(0, 6))]),
            # a step inside the tiles: every other cell, the phase carries over the tile border
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
            # a step that does not divide the tile size: the first cell of a tile depends on the phase
            (
                slice(None, None, 3),
                10,
                4,
                [(0, slice(0, 4, 3), slice(0, 2)), (1, slice(2, 3, 3), slice(2, 3)), (2, slice(1, 2, 3), slice(3, 4))],
            ),
            # a step larger than a tile jumps over tiles
            (
                slice(None, None, 5),
                12,
                2,
                [(0, slice(0, 1, 5), slice(0, 1)), (2, slice(1, 2, 5), slice(1, 2)), (5, slice(0, 1, 5), slice(2, 3))],
            ),
            # a step larger than the axis selects the first cell only
            (slice(None, None, 100), 12, 4, [(0, slice(0, 1, 100), slice(0, 1))]),
            # start inside a tile with a step
            (slice(5, 12, 4), 12, 4, [(1, slice(1, 2, 4), slice(0, 1)), (2, slice(1, 2, 4), slice(1, 2))]),
            # integers drop the axis
            (0, 6, 2, [(0, 0, None)]),
            (3, 6, 2, [(1, 1, None)]),
            (-1, 6, 2, [(2, 1, None)]),
        ],
    )
    def test_parts_follow_the_selected_positions(self, bound, size, tile_size, expected):
        """Tests if the parts walk the selected positions tile by tile and their data slices continue each other."""
        assert _split_axis(bound, size, tile_size) == [_TileBounds(*part) for part in expected]

    @pytest.mark.parametrize(
        ("size", "tile_size"),
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
    def test_parts_cover_the_selection_exactly(self, bound, size, tile_size):
        """Tests if the cells of the parts are exactly the selected cells, in order, whatever the tiles."""
        axis = np.arange(size)
        expected = axis[bound]
        collected = []
        for part in _split_axis(bound, size, tile_size):
            tile = axis[part.position * tile_size : (part.position + 1) * tile_size]
            cells = tile[part.bounds]
            assert part.data_slice is not None
            assert part.data_slice == slice(len(collected), len(collected) + len(cells))
            collected.extend(cells)
        assert collected == expected.tolist()

    def test_negative_step_is_rejected(self):
        with pytest.raises(DekerVSubsetError):
            _split_axis(slice(None, None, -1), 6, 2)


class TestTilePositions:
    """Tiles a subset covers, with their bounds and data slices, for several axes."""

    @pytest.mark.parametrize(
        ("bounds", "shape", "arrays_shape", "expected"),
        [
            # 2 dimensions, 4 whole tiles of 5x5
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
            # the subset [0:7, 2:8] cuts every tile
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
            # an integer index drops its axis from the data slices and selects one row of tiles
            (
                (7, slice(None)),
                (10, 10),
                (5, 5),
                [((1, 0), (2, slice(None, None)), (slice(0, 5),)), ((1, 1), (2, slice(None, None)), (slice(5, 10),))],
            ),
            # a step jumping over the second tile of the first axis
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
    def test_tiles_with_bounds_and_data_slices(self, bounds, shape, arrays_shape, expected):
        positions = _tile_positions(bounds, shape, arrays_shape)
        assert [tuple(position) for position in positions] == expected

    @pytest.mark.parametrize(
        ("shape", "arrays_shape"),
        [((12,), (4,)), ((12, 10), (4, 5)), ((12, 10, 8), (4, 2, 4)), ((6, 5, 4, 6), (2, 1, 2, 3))],
        ids=["1d", "2d", "3d", "4d"],
    )
    def test_tiles_assemble_the_numpy_selection(self, shape, arrays_shape):
        """Tests if the data slices of the tiles assemble the numpy selection of a stepped expression."""
        data = np.arange(np.prod(shape)).reshape(shape)
        ndim = len(shape)
        for index_exp in STEPPED_INDEX_EXPS[ndim]:
            forward, flipped = positive_bounds(shape, index_exp)
            expected = data[forward]
            assembled = np.empty(expected.shape, dtype=data.dtype)
            for vposition, bounds, data_slice in _tile_positions(forward, shape, arrays_shape):
                tile = data[tuple(slice(p * t, (p + 1) * t) for p, t in zip(vposition, arrays_shape))]
                assembled[data_slice] = tile[bounds]
            assert np.array_equal(assembled, expected), index_exp
            assert np.array_equal(np.flip(assembled, flipped), data[index_exp]), index_exp


class TestVSubsetArrayWorkers:
    """Methods that read, update and clear one inner Array of the subset."""

    @pytest.mark.parametrize("index_exp", [np.index_exp[:], np.index_exp[0:5, 0:5, 0:5], np.index_exp[7, 1:3, :]])
    def test_read_array_data_of_existing_tiles(self, inserted_varray: VArray, array_data: np.ndarray, index_exp: Slice):
        """Tests if every tile of a written VArray is read with its slice of the subset data."""
        vsubset: VSubset = inserted_varray[index_exp]
        expected = array_data[index_exp]
        for array_pos in vsubset._VSubset__arrays:
            data_slice, data = vsubset._VSubset__read_array_data(array_pos)
            assert data_slice == array_pos.data_slice
            assert np.array_equal(np.asarray(data), np.asarray(expected)[data_slice])

    @pytest.mark.parametrize("index_exp", [np.index_exp[:], np.index_exp[0:5, 0:5, 0:5], np.index_exp[2, 3:9, 1]])
    def test_read_array_data_of_missing_tiles(self, varray: VArray, index_exp: Slice):
        """Tests if a missing tile is read as its fill value with the shape of its bounds."""
        varray._adapter.create(varray)
        vsubset: VSubset = varray[index_exp]
        for array_pos in vsubset._VSubset__arrays:
            data_slice, data = vsubset._VSubset__read_array_data(array_pos)
            assert data_slice == array_pos.data_slice
            assert np.all(np.isnan(data))
            assert np.asarray(data).shape == np.asarray(np.empty(varray.arrays_shape)[array_pos.bounds]).shape

    @pytest.mark.parametrize("bounds", [(99, 0, 0), (0, 99, 0), (0, 0, -99)])
    def test_read_array_data_raises_on_wrong_bounds(self, inserted_varray: VArray, bounds: tuple):
        """Tests if an index outside the tile is rejected."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        wrong = ArrayPosition(array_pos.vposition, bounds, array_pos.data_slice)
        with pytest.raises(IndexError):
            vsubset._VSubset__read_array_data(wrong)

    def test_update_array_data_creates_missing_tile(self, varray: VArray):
        """Tests if updating a missing tile creates it with the data."""
        varray._adapter.create(varray)
        vsubset: VSubset = varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        assert vsubset._create_array_from_vposition(array_pos.vposition) is None
        data = np.full(varray.arrays_shape, 7.0, dtype=varray.dtype)
        vsubset._VSubset__update_array_data(ArrayPositionedData(array_pos.vposition, array_pos.bounds, data))
        array = vsubset._create_array_from_vposition(array_pos.vposition)
        assert array is not None
        assert np.array_equal(array[:].read(), data)

    def test_update_array_data_updates_existing_tile(self, inserted_varray: VArray):
        """Tests if an existing tile is updated in place."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[-1]
        data = np.full(inserted_varray.arrays_shape, -1.0, dtype=inserted_varray.dtype)
        vsubset._VSubset__update_array_data(ArrayPositionedData(array_pos.vposition, array_pos.bounds, data))
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
    def test_update_array_data_sets_time_attribute_of_new_tiles(self, client: Client, dimensions, vgrid, time_index):
        """Tests if every new tile starts where its first cell is on the VArray time axis.

        The tile attribute is ``start + step * tile_size * position``, so a tile's own time axis
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
        collection = client.create_collection(f"tiles_time_{len(dimensions)}d_{'_'.join(map(str, vgrid))}", schema)
        try:
            start = datetime(2024, 1, 1, tzinfo=timezone.utc)
            varray = collection.create(custom_attributes={"start": start})
            vsubset: VSubset = varray[:]
            tile_size = varray.arrays_shape[time_index]
            assert len(vsubset._VSubset__arrays) == np.prod(vgrid)
            for array_pos in vsubset._VSubset__arrays:
                data = np.zeros(varray.arrays_shape, dtype=varray.dtype)
                vsubset._VSubset__update_array_data(ArrayPositionedData(array_pos.vposition, array_pos.bounds, data))
                tile = vsubset._create_array_from_vposition(array_pos.vposition)
                expected = start + step * tile_size * array_pos.vposition[time_index]
                assert tile.custom_attributes["parent.start"] == expected
                assert tile.dimensions[time_index].start_value == expected
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
    def test_tiles_time_axes_match_the_varray_time_axis(self, client: Client, dimensions, vgrid, time_index):
        """Tests if the time axis of every tile, read as an Array, is the matching piece of the VArray time axis."""
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
        collection = client.create_collection(f"tiles_axis_{len(dimensions)}d_{'_'.join(map(str, vgrid))}", schema)
        try:
            varray = collection.create(custom_attributes={"start": datetime(2024, 6, 1, tzinfo=timezone.utc)})
            varray[:].update(np.zeros(varray.shape, dtype=varray.dtype))
            time_name = dimensions[time_index][0]
            varray_axis = varray[:].describe()[time_name]
            tile_size = varray.arrays_shape[time_index]
            tiles = [tile for tile in collection.arrays if tile._vid == varray.id]
            assert len(tiles) == np.prod(vgrid)
            for tile in tiles:
                position = tile.primary_attributes["v_position"][time_index]
                first_cell = position * tile_size
                assert tile[:].describe()[time_name] == varray_axis[first_cell : first_cell + tile_size]
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
        """Tests if data that does not fit the tile is rejected."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        with pytest.raises(error):
            vsubset._VSubset__update_array_data(ArrayPositionedData(array_pos.vposition, array_pos.bounds, data))

    def test_clear_array_deletes_fully_covered_tile(self, inserted_varray: VArray):
        """Tests if a tile fully covered by the subset is deleted."""
        vsubset: VSubset = inserted_varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        vsubset._VSubset__clear_array(array_pos)
        assert vsubset._create_array_from_vposition(array_pos.vposition) is None

    def test_clear_array_clears_partially_covered_tile(self, inserted_varray: VArray, array_data: np.ndarray):
        """Tests if a tile partially covered by the subset is cleared only inside the bounds and kept."""
        vsubset: VSubset = inserted_varray[0:2, 0:2, 0:2]
        array_pos = vsubset._VSubset__arrays[0]
        vsubset._VSubset__clear_array(array_pos)
        array = vsubset._create_array_from_vposition(array_pos.vposition)
        assert array is not None
        tile = array[:].read()
        assert np.all(np.isnan(tile[0:2, 0:2, 0:2]))
        assert np.array_equal(tile[2:, 2:, 2:], array_data[2:5, 2:5, 2:5])

    def test_clear_array_ignores_missing_tile(self, varray: VArray):
        """Tests if clearing a tile that was never written does nothing and raises nothing."""
        varray._adapter.create(varray)
        vsubset: VSubset = varray[:]
        array_pos = vsubset._VSubset__arrays[0]
        vsubset._VSubset__clear_array(array_pos)
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
            array = col.create({"pa": 1, "ca": 0})
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
            array = col.create({"pa": 1, "ca": 0})
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
        """Tests if a stepped write creates only the tiles it touches and leaves the other cells empty."""
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
        """Number of reads of the ``data`` datasets of the tiles."""
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
def tiled_varray(client: Client) -> Iterator[VArray]:
    """A VArray of shape (15, 10) split into three tiles (5, 10) along the first axis, with data in every cell."""
    schema = VArraySchema(
        dtype=float,
        dimensions=[DimensionSchema(name="x", size=15), DimensionSchema(name="y", size=10)],
        vgrid=(3, 1),
    )
    collection = client.create_collection("tiled_varray_reads", schema)
    try:
        varray = collection.create()
        varray[:].update(np.arange(1, 151, dtype=float).reshape(15, 10))
        yield varray
    finally:
        collection.delete()


# Subsets of the (15, 10) VArray and the number of tiles they cover only partially
TILE_COVERAGE_CASES = [
    pytest.param(np.index_exp[:], 0, id="whole-varray"),
    pytest.param(np.index_exp[0:5], 0, id="first-tile-whole"),
    pytest.param(np.index_exp[5:15], 0, id="last-two-tiles-whole"),
    pytest.param(np.index_exp[2:15], 1, id="first-tile-partial-rest-whole"),
    pytest.param(np.index_exp[0:7], 1, id="first-tile-whole-second-partial"),
    pytest.param(np.index_exp[2:7], 2, id="two-tiles-partial"),
    pytest.param(np.index_exp[:, 0:5], 3, id="every-tile-partial"),
    pytest.param(np.index_exp[3], 1, id="one-row-of-the-first-tile"),
    pytest.param(np.index_exp[4:11], 2, id="outer-tiles-partial-middle-whole"),
]


class TestVSubsetTileReads:
    """A write or a clear reads a tile only when it covers the tile partially; a fully covered tile is replaced."""

    @pytest.mark.parametrize(("index_exp", "partial_tiles"), TILE_COVERAGE_CASES)
    def test_update_reads_only_partially_covered_tiles(
        self, tiled_varray: VArray, access: DatasetAccessLog, index_exp: Slice, partial_tiles: int
    ):
        before = tiled_varray[:].read()
        data = np.full(before[index_exp].shape, -1.0)
        reads_before = access.data_reads  # the tiles read by the line above

        tiled_varray[index_exp].update(data)
        reads = access.data_reads - reads_before

        expected = before.copy()
        expected[index_exp] = data
        assert np.array_equal(tiled_varray[:].read(), expected)
        assert reads == partial_tiles

    @pytest.mark.parametrize(("index_exp", "partial_tiles"), TILE_COVERAGE_CASES)
    def test_clear_reads_only_partially_covered_tiles(
        self, tiled_varray: VArray, access: DatasetAccessLog, index_exp: Slice, partial_tiles: int
    ):
        before = tiled_varray[:].read()
        reads_before = access.data_reads  # the tiles read by the line above

        tiled_varray[index_exp].clear()
        reads = access.data_reads - reads_before

        expected = before.copy()
        expected[index_exp] = np.nan
        assert np.array_equal(tiled_varray[:].read(), expected, equal_nan=True)
        assert reads == partial_tiles


if __name__ == "__main__":
    pytest.main()

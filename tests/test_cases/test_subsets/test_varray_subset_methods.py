from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Tuple

import hdf5plugin
import numpy as np
import pytest

from deker_local_adapters import HDF5CompressionOpts, HDF5Options

from tests.parameters.index_exp_params import valid_index_exp_params

from deker import Scale
from deker.arrays import VArray
from deker.client import Client
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerSubsetError
from deker.schemas import AttributeSchema, DimensionSchema, TimeDimensionSchema, VArraySchema
from deker.subset import VSubset
from deker.types.private.classes import ArrayPosition
from deker.types.private.typings import Slice


class TestVArraySubset:
    @pytest.mark.parametrize(
        "slice_,result",
        (
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
        ),
    )
    def test_subset_calc_array_2dimension(
        self, slice_: Tuple[slice, ...], result: tuple, client: Client, name: str
    ):
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
        "slice_,result",
        (
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
        ),
    )
    def test_subset_calc_array_3dimension(
        self, slice_: Tuple[slice, ...], result: tuple, client: Client, name: str
    ):
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

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_update_vsubset_raises_on_no_data(
        self, varray_collection: Collection, index_exp: Slice
    ):
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
    def test_clear_vsubset(
        self, varray_collection: Collection, array_data: np.ndarray, index_exp: Slice
    ):
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
            ArrayPosition(
                (0, 0), (slice(None, None), slice(None, None)), (slice(0, 2), slice(0, 2))
            ),
            ArrayPosition(
                (0, 1), (slice(None, None), slice(None, None)), (slice(0, 2), slice(2, 4))
            ),
            ArrayPosition(
                (0, 2), (slice(None, None), slice(None, None)), (slice(0, 2), slice(4, 6))
            ),
            ArrayPosition(
                (1, 0), (slice(None, None), slice(None, None)), (slice(2, 4), slice(0, 2))
            ),
            ArrayPosition(
                (1, 1), (slice(None, None), slice(None, None)), (slice(2, 4), slice(2, 4))
            ),
            ArrayPosition(
                (1, 2), (slice(None, None), slice(None, None)), (slice(2, 4), slice(4, 6))
            ),
            ArrayPosition(
                (2, 0), (slice(None, None), slice(None, None)), (slice(4, 6), slice(0, 2))
            ),
            ArrayPosition(
                (2, 1), (slice(None, None), slice(None, None)), (slice(4, 6), slice(2, 4))
            ),
            ArrayPosition(
                (2, 2), (slice(None, None), slice(None, None)), (slice(4, 6), slice(4, 6))
            ),
        ]
        # Check half:
        assert varray[:3, :]._VSubset__arrays == [
            ArrayPosition(
                (0, 0), (slice(None, None), slice(None, None)), (slice(0, 2), slice(0, 2))
            ),
            ArrayPosition(
                (0, 1), (slice(None, None), slice(None, None)), (slice(0, 2), slice(2, 4))
            ),
            ArrayPosition(
                (0, 2), (slice(None, None), slice(None, None)), (slice(0, 2), slice(4, 6))
            ),
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
            ArrayPosition(
                (1, 1), (slice(None, None), slice(None, None)), (slice(1, 3), slice(1, 3))
            ),
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
        data = np.asarray(range(6 * 6 * 6), dtype=np.int8).reshape((6, 6, 6))
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
            DimensionSchema(
                name="y", size=361, scale=Scale(start_value=90.0, step=-0.5, name="lat")
            ),
            DimensionSchema(
                name="x", size=720, scale=Scale(start_value=-180.0, step=0.5, name="lon")
            ),
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
            DimensionSchema(name="y", size=361),
            DimensionSchema(name="x", size=720),
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
    def test_vsubset_description_on_read_xarray(
        self, varray_collection: Collection, index_exp: Slice
    ):
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
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, 0],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": [90.0],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, 0],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": [-180],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, 1],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["pres"],
                    }
                ),
            ),
            (
                np.index_exp[0, 1, 2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.5],
                        "x": [-179.0],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0, 1, 2, 0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.5],
                        "x": [-179.0],
                        "layers": ["temp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.5],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2, 2:3],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.5],
                        "x": [-179.0],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[0:1, 1:2, 2:3, 0:1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": [89.5],
                        "x": [-179.0],
                        "layers": ["temp"],
                    }
                ),
            ),
            (
                np.index_exp[6:12],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(6, 12)
                        ],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, 6:12],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": [87.0, 86.5, 86.0, 85.5, 85.0, 84.5],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, 6:12],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": [-177.0, -176.5, -176.0, -175.5, -175.0, -174.5],
                        "layers": ["temp", "pres", "pcp"],
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, :2],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": np.around(np.arange(90, -91, -0.5), 1).tolist()[:361],
                        "x": np.around(np.arange(-180, 180, 0.5), 1).tolist()[:720],
                        "layers": ["temp", "pres"],
                    }
                ),
            ),
            (
                np.index_exp[22:, 359:, 718:, 1:],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(22, 24)
                        ],
                        "y": [-89.5, -90.0],
                        "x": [179.0, 179.5],
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
                        "x": [179.5],
                        "layers": ["pcp"],
                    }
                ),
            ),
            (
                np.index_exp[-2, -2, -2, -2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 22, 0, tzinfo=timezone.utc)],
                        "y": [-89.5],
                        "x": [179.0],
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
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": list(range(361)),
                        "x": list(range(720)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[0],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, tzinfo=timezone.utc)],
                        "y": list(range(361)),
                        "x": list(range(720)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, 0],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": [0],
                        "x": list(range(720)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, 0],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": list(range(361)),
                        "x": [0],
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, 1],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": list(range(361)),
                        "x": list(range(720)),
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
                        "y": list(range(361)),
                        "x": list(range(720)),
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
                        "x": list(range(720)),
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
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(6, 12)
                        ],
                        "y": list(range(361)),
                        "x": list(range(720)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, 6:12],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": [6, 7, 8, 9, 10, 11],
                        "x": list(range(720)),
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, 6:12],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": list(range(361)),
                        "x": [6, 7, 8, 9, 10, 11],
                        "layers": list(range(3)),
                    }
                ),
            ),
            (
                np.index_exp[:, :, :, :2],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1) * n
                            for n in range(24)
                        ],
                        "y": list(range(361)),
                        "x": list(range(720)),
                        "layers": [0, 1],
                    }
                ),
            ),
            (
                np.index_exp[22:, 359:, 718:, 1:],
                OrderedDict(
                    {
                        "dt": [
                            datetime(2023, 1, 1, 22, tzinfo=timezone.utc),
                            datetime(2023, 1, 1, 23, tzinfo=timezone.utc),
                        ],
                        "y": [359, 360],
                        "x": [718, 719],
                        "layers": [1, 2],
                    }
                ),
            ),
            (
                np.index_exp[-1, -1, -1, -1],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 23, 0, tzinfo=timezone.utc)],
                        "y": [360],
                        "x": [719],
                        "layers": [2],
                    }
                ),
            ),
            (
                np.index_exp[-2, -2, -2, -2],
                OrderedDict(
                    {
                        "dt": [datetime(2023, 1, 1, 22, 0, tzinfo=timezone.utc)],
                        "y": [359],
                        "x": [718],
                        "layers": [1],
                    }
                ),
            ),
        ],
    )
    def test_vsubset_index_description(
        self, not_described_varray: VArray, index_exp: Slice, expected
    ):
        subset = not_described_varray[index_exp]
        assert subset.describe() == expected

    @pytest.mark.parametrize(
        "index_exp",
        [
            np.index_exp[datetime(2023, 1, 1, 11, 0, tzinfo=timezone.utc), -3.5, 16.5, "pcp"],
            np.index_exp[0, 0, 0, 4],
            np.index_exp[::-1],
            np.index_exp[::2],
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
            np.index_exp[::-1],
            np.index_exp[::2],
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
            np.index_exp[::-1],
            np.index_exp[::2],
        ],
    )
    def test_not_described_vsubset_raises(self, not_described_varray, index_exp):
        with pytest.raises(IndexError):
            assert not_described_varray[index_exp]


if __name__ == "__main__":
    pytest.main()

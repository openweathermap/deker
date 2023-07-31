from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import h5py
import hdf5plugin
import numpy as np
import pytest

from deker_local_adapters import HDF5CompressionOpts, HDF5Options
from numpy import ndarray

from tests.parameters.index_exp_params import valid_index_exp_params

from deker import ArraySchema, AttributeSchema, DimensionSchema, Scale, TimeDimensionSchema
from deker.arrays import Array
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerSubsetError
from deker.subset import Subset
from deker.tools import get_paths
from deker.types.private.typings import Slice


class TestArraySubsetMethods:
    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_subset_shape_is_correct(
        self, inserted_array: Array, array_data: ndarray, index_exp: Slice
    ):
        """Tests if subset shape is correct.

        :param inserted_array: Pre created array
        :param array_data: Data of array
        :param index_exp: Slice for an array
        """
        subset = inserted_array[index_exp]
        assert subset
        assert subset.shape == array_data[index_exp].shape

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_subset_read(self, inserted_array: Array, array_data: ndarray, index_exp: Slice):
        """Tests if subset reads correctly.

        :param inserted_array: Pre created array
        :param array_data: Data of array
        :param index_exp: Slice for an array
        """
        subset = inserted_array[index_exp]
        read = subset.read()
        assert (read == array_data[index_exp]).all()  # type: ignore

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_subset_clear(
        self,
        array_collection: Collection,
        array_data: ndarray,
        root_path: Path,
        index_exp: list,
        ctx,
    ):
        """Tests if subset clears itself correctly.

        :param array_collection: Pre created array_collection
        :param root_path: Path to collections directory
        :param index_exp: Slice for an array
        """
        array: Array = array_collection.create()
        array[:].update(array_data)
        try:
            subset: Subset = array[index_exp]
            data = subset.read()
            subset.clear()
            paths = get_paths(
                array, root_path / ctx.config.collections_directory / array.collection
            )
            filename = paths.main / (array.id + ctx.storage_adapter.file_ext)

            with h5py.File(filename) as f:
                ds = f.get("data")
                if data.shape == (10, 10, 10):
                    assert ds is None
                else:
                    assert np.isnan(ds[index_exp]).all()
                    array_mask = np.zeros(array.shape)
                    array_mask[index_exp] = np.nan
                    assert not np.isnan(ds[array_mask == 0]).all()

        finally:
            array.delete()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_update_subset(
        self, array_collection: Collection, array_data: ndarray, index_exp: Slice
    ):
        """Tests if subset updates itself correctly.

        :param array_collection: Pre created collection
        :param array_data: Data of array
        :param index_exp: Slice for an array
        """
        array: Array = array_collection.create()
        array[:].update(array_data)
        try:
            subset = array[index_exp]
            data = subset.read()
            if isinstance(data, ndarray):
                data[:] = -9999
            else:
                data = -9999
            subset.update(data)

            new_data = subset.read()

            if isinstance(new_data, ndarray):
                assert (new_data == data).all()  # type: ignore
                if data.shape != (10, 10, 10):
                    array_mask = np.zeros(array.shape)
                    array_mask[:] = -5
                    array_mask[index_exp] = np.nan
                    ds = array[:].read()
                    assert (ds[array_mask == -5] != -9999).all()
            else:
                assert new_data == data
        finally:
            array.delete()

    def test_update_subset_raises_on_no_data(self, array_collection: Collection):
        """Tests if subset updates itself correctly.

        :param array_collection: Pre created collection
        """
        array: Array = array_collection.create()
        with pytest.raises(DekerArrayError):
            assert array[:].update(None)

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_read_subset_nans(self, array_collection: Collection, index_exp: Slice):
        """Tests if empty subset can be read correctly."""
        array = array_collection.create()
        subset = array[index_exp]
        data = subset.read()
        assert np.isnan(data).all()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_clear_subset_with_no_data(self, array_collection: Collection, index_exp: Slice):
        array = array_collection.create()
        try:
            subset = array[index_exp]
            subset.clear()
            data = subset.read()
            assert np.isnan(data).all()
        finally:
            array.delete()


class TestSubsetForXArray:
    @pytest.fixture(scope="class")
    def full_described_array(self, client) -> Array:
        """Test if an Array with described scaled dimensions can be created."""
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
        schema = ArraySchema(dtype=float, dimensions=dims, attributes=attrs)
        options = HDF5Options(compression_opts=HDF5CompressionOpts(**hdf5plugin.Zstd(2)))
        col = client.create_collection("described_coll", schema, options)
        try:
            array = col.create({"pa": 1, "ca": 0})
            yield array
        finally:
            col.delete()

    @pytest.fixture(scope="class")
    def not_described_array(self, client) -> Array:
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
        schema = ArraySchema(dtype=float, dimensions=dims, attributes=attrs)
        options = HDF5Options(compression_opts=HDF5CompressionOpts(**hdf5plugin.Zstd(2)))
        col = client.create_collection("not_described_coll", schema, options)
        try:
            array = col.create({"pa": 1, "ca": 0})
            yield array
        finally:
            col.delete()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_subset_description_on_read_xarray(
        self, array_collection: Collection, index_exp: Slice
    ):
        array: Array = array_collection.create()
        subset = array[index_exp]

        if subset.shape == ():
            with pytest.raises(DekerSubsetError):
                assert subset.read_xarray()
        else:
            try:
                xarray = subset.read_xarray()
                assert xarray.attrs["primary_attributes"] == array.primary_attributes
                assert xarray.attrs["custom_attributes"] == array.custom_attributes
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
                            datetime(2023, 1, 1, 22, tzinfo=timezone.utc),
                            datetime(2023, 1, 1, 23, tzinfo=timezone.utc),
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
    def test_subset_description(self, full_described_array: Array, index_exp: Slice, expected):
        """Test if subset description is correct."""
        subset = full_described_array[index_exp]
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
    def test_subset_index_description(self, not_described_array: Array, index_exp: Slice, expected):
        """Test if undescribed array returns correct description."""
        subset = not_described_array[index_exp]
        assert subset.describe() == expected

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
    def test_described_subset_raises(self, full_described_array, index_exp):
        """Test if described array raises on incorrect index expression."""
        with pytest.raises(IndexError):
            assert full_described_array[index_exp]

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
    def test_not_described_subset_raises(self, not_described_array, index_exp):
        with pytest.raises(IndexError):
            assert not_described_array[index_exp]


if __name__ == "__main__":
    pytest.main()

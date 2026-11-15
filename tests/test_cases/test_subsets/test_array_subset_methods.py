from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import h5py
import hdf5plugin
import numpy as np
import pytest

from deker_local_adapters import HDF5CompressionOpts, HDF5Options
from numpy import ndarray

from tests.parameters.index_exp_params import array_step_cases, valid_index_exp_params
from tests.plugins.collection import SteppedCollections

from deker import ArraySchema, AttributeSchema, DimensionSchema, Scale, TimeDimensionSchema
from deker.arrays import Array
from deker.collection import Collection
from deker.errors import DekerArrayError, DekerSubsetError
from deker.subset import Subset
from deker.tools import get_paths
from deker.types.private.typings import Slice


class TestArraySubsetMethods:
    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_subset_shape_is_correct(self, inserted_array: Array, array_data: ndarray, index_exp: Slice):
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
            paths = get_paths(array, root_path / ctx.config.collections_directory / array.collection)
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
    def test_update_subset(self, array_collection: Collection, array_data: ndarray, index_exp: Slice):
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


# The described array of TestSubsetForXArray: dt 24 x 1h from 2023-01-01, y 721 x -0.25 from 90, x 1440 x 0.25
# from -180, layers temp/pres/pcp
def _hours(*positions: int) -> list[datetime]:
    """Time coordinates of the described array at the given positions.

    :param positions: indexes on the time axis
    :returns: datetimes
    """
    return [datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(hours=n) for n in positions]


# Integer steps: valid on the described and on the not described array alike
stepped_description_params = [
    pytest.param(
        np.index_exp[::6],
        {"dt": _hours(0, 6, 12, 18), "layers": ["temp", "pres", "pcp"]},
        id="time-every-6th",
    ),
    pytest.param(
        np.index_exp[::-6],
        {"dt": _hours(23, 17, 11, 5), "layers": ["temp", "pres", "pcp"]},
        id="time-backward-every-6th",
    ),
    pytest.param(
        np.index_exp[2:10:3, 0:9:4, -4::1, ::2],
        {
            "dt": _hours(2, 5, 8),
            "y": [90.0, 89.0, 88.0],
            "x": [179.0, 179.25, 179.5, 179.75],
            "layers": ["temp", "pcp"],
        },
        id="every-axis",
    ),
    pytest.param(
        np.index_exp[5, ::-360, 1439:1430:-3, ::-1],
        {"dt": _hours(5), "y": [-90.0, 0.0, 90.0], "x": [179.75, 179.0, 178.25], "layers": ["pcp", "pres", "temp"]},
        id="backward-scales-and-labels",
    ),
]

# Fancy steps: the described array only
fancy_stepped_description_params = [
    pytest.param(
        np.index_exp[
            datetime(2023, 1, 1, 20, tzinfo=timezone.utc) :: -timedelta(hours=10), 90.0:88.0:-1.0, :, "pcp":"temp":-1
        ],
        {"dt": _hours(20, 10, 0), "y": [90.0, 89.0], "layers": ["pcp", "pres"]},
        id="fancy-steps",
    ),
]


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
            DimensionSchema(name="y", size=721, scale=Scale(start_value=90.0, step=-0.25, name="lat")),
            DimensionSchema(name="x", size=1440, scale=Scale(start_value=-180.0, step=0.25, name="lon")),
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
            DimensionSchema(name="y", size=721),
            DimensionSchema(name="x", size=1440),
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
    def test_subset_description_on_read_xarray(self, array_collection: Collection, index_exp: Slice):
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
            np.index_exp[::0],
            np.index_exp[::True],
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
            np.index_exp[::0],
            np.index_exp[::True],
        ],
    )
    def test_not_described_subset_raises(self, not_described_array, index_exp):
        with pytest.raises(IndexError):
            assert not_described_array[index_exp]

    @pytest.mark.parametrize(("index_exp", "expected"), stepped_description_params + fancy_stepped_description_params)
    def test_described_subset(self, full_described_array: Array, index_exp: Slice, expected: dict):
        description = full_described_array[index_exp].describe()
        for name, values in expected.items():
            assert description[name] == values, name
        # the axes left out of the expression are described in full
        assert len(description["x"]) == (len(expected["x"]) if "x" in expected else 1440)

    @pytest.mark.parametrize(("index_exp", "expected"), stepped_description_params)
    def test_not_described_subset(self, not_described_array: Array, index_exp: Slice, expected: dict):
        """Tests if the axes without a scale or labels are described by their indexes."""
        shape = not_described_array.shape
        description = not_described_array[index_exp].describe()
        for n, (name, size) in enumerate(zip(("dt", "y", "x", "layers"), shape)):
            bound = index_exp[n] if n < len(index_exp) else slice(None)
            positions = [bound] if isinstance(bound, int) else list(range(*bound.indices(size)))
            if name == "dt":
                assert description[name] == _hours(*positions)
            else:
                assert description[name] == positions

    @pytest.mark.parametrize(("index_exp", "expected"), stepped_description_params + fancy_stepped_description_params)
    def test_read_xarray(self, full_described_array: Array, index_exp: Slice, expected: dict):
        """Tests if the coordinates of the xarray follow the stepped description."""
        pytest.importorskip("xarray")
        subset = full_described_array[index_exp]
        xarray = subset.read_xarray()
        # an integer index keeps its axis in the xarray, with a single coordinate
        assert xarray.shape == tuple(len(values) for values in subset.describe().values())
        assert np.array_equal(xarray.values.reshape(subset.shape), subset.read(), equal_nan=True)
        for name, values in expected.items():
            if name != "dt":  # time coordinates are converted by xarray, they are checked through describe()
                assert list(xarray.coords[name].values) == values, name


@pytest.fixture
def stepped_data(stepped_array_collection: Collection) -> np.ndarray:
    """Distinct values for every cell of the array of the collection."""
    shape = stepped_array_collection.array_schema.shape
    return np.arange(1, np.prod(shape, dtype=int) + 1, dtype=float).reshape(shape)


@pytest.fixture
def stepped_array(stepped_array_collection: Collection, stepped_data: np.ndarray) -> Array:
    """An Array with data in every cell."""
    array = stepped_array_collection.create()
    array[:].update(stepped_data)
    try:
        yield array
    finally:
        array.delete()


class TestSubsetSteps:
    """Reading, writing and clearing a stepped subset of an Array gives exactly what numpy gives, in 1D to 4D."""

    @pytest.mark.parametrize(
        ("filled_stepped_array", "index_exp"), array_step_cases(), indirect=["filled_stepped_array"]
    )
    def test_read(self, filled_stepped_array: Array, index_exp: Slice):
        subset = filled_stepped_array[index_exp]
        expected = SteppedCollections.data(filled_stepped_array.shape)[index_exp]
        assert subset.shape == expected.shape
        assert np.array_equal(subset.read(), expected)

    @pytest.mark.parametrize(
        ("stepped_array_collection", "index_exp"), array_step_cases(), indirect=["stepped_array_collection"]
    )
    def test_update(self, stepped_array: Array, stepped_data: np.ndarray, index_exp: Slice):
        subset = stepped_array[index_exp]
        new_data = -1.0 - np.arange(np.prod(subset.shape, dtype=int), dtype=float).reshape(subset.shape)
        expected = stepped_data.copy()
        expected[index_exp] = new_data

        subset.update(new_data)

        assert np.array_equal(subset.read(), new_data)
        assert np.array_equal(stepped_array[:].read(), expected)

    @pytest.mark.parametrize(
        ("stepped_array_collection", "index_exp"), array_step_cases(), indirect=["stepped_array_collection"]
    )
    def test_update_with_a_scalar(self, stepped_array: Array, stepped_data: np.ndarray, index_exp: Slice):
        subset = stepped_array[index_exp]
        expected = stepped_data.copy()
        expected[index_exp] = -7.0

        subset.update(-7.0)

        assert np.array_equal(stepped_array[:].read(), expected)

    @pytest.mark.parametrize(
        ("stepped_array_collection", "index_exp"), array_step_cases(), indirect=["stepped_array_collection"]
    )
    def test_clear(self, stepped_array: Array, stepped_data: np.ndarray, index_exp: Slice):
        subset = stepped_array[index_exp]
        expected = stepped_data.copy()
        expected[index_exp] = np.nan

        subset.clear()

        assert np.all(np.isnan(subset.read()))
        assert np.array_equal(stepped_array[:].read(), expected, equal_nan=True)

    @pytest.mark.parametrize(
        ("stepped_array_collection", "index_exp"), array_step_cases(), indirect=["stepped_array_collection"]
    )
    def test_update_into_an_empty_array(
        self, stepped_array_collection: Collection, stepped_data: np.ndarray, index_exp: Slice
    ):
        array = stepped_array_collection.create()
        try:
            subset = array[index_exp]
            new_data = stepped_data[index_exp]
            expected = np.full(array.shape, np.nan)
            expected[index_exp] = new_data

            subset.update(new_data)

            assert np.array_equal(array[:].read(), expected, equal_nan=True)
        finally:
            array.delete()

    @pytest.mark.parametrize(
        ("stepped_array_collection", "index_exp"), array_step_cases(), indirect=["stepped_array_collection"]
    )
    def test_wrong_shape_raises(self, stepped_array: Array, index_exp: Slice):
        subset = stepped_array[index_exp]
        wrong = np.zeros(tuple(n + 1 for n in subset.shape))
        with pytest.raises(IndexError):
            subset.update(wrong)


if __name__ == "__main__":
    pytest.main()

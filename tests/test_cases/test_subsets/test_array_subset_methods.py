import logging
import os
import threading
import time

from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import deker_local_adapters.collection_adapter
import deker_local_adapters.mixin
import h5py
import hdf5plugin
import numpy as np
import pytest

from cantok import AbstractToken, SimpleToken
from deker_local_adapters import HDF5CompressionOpts, HDF5Options
from deker_tools.cancel import cancellation_scope
from deker_tools.typings import Sentinel
from numpy import ndarray
from pytest_mock import MockerFixture

from tests.fixtures.collection import SteppedCollections
from tests.parameters.index_exp_params import array_step_cases, valid_index_exp_params
from tests.parameters.uri import embedded_uri
from tests.test_cases.test_arrays.test_array_methods import HeldEmptying

from deker import ArraySchema, AttributeSchema, DimensionSchema, Scale, TimeDimensionSchema
from deker.arrays import Array
from deker.client import Client
from deker.collection import Collection
from deker.errors import (
    DekerArrayError,
    DekerCancelledError,
    DekerCriticalError,
    DekerInstanceNotExistsError,
    DekerLockError,
    DekerSubsetError,
    DekerTimeoutError,
    DekerTrashError,
    DekerValidationError,
)
from deker.flock import Flock
from deker.subset import Subset
from deker.tools import get_main_path, get_paths
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

    def test_read_does_not_describe_the_subset_when_the_log_is_off(self, inserted_array: Array, mocker):
        """The messages of the log are built only when they are written: no subset string at the ERROR level."""
        described = mocker.spy(Subset, "__str__")
        inserted_array[0].read()
        assert described.call_count == 0

    def test_read_describes_the_subset_when_the_log_is_on(self, inserted_array: Array, deker_log, mocker):
        described = mocker.spy(Subset, "__str__")
        inserted_array[0].read()
        assert described.call_count > 0
        assert any("data read" in record.message for record in deker_log.records)

    @pytest.mark.parametrize(
        ("index_exp", "bounds"),
        [(0, "[0]"), ((slice(0, 2), 1), "[0:2, 1]"), ((slice(None), slice(None, None, 2)), "[:, ::2]")],
    )
    def test_repr_names_the_bounds_the_shape_and_the_array(self, inserted_array: Array, index_exp: Slice, bounds: str):
        """The repr of a Subset carries its bounds, its shape and the id of its Array."""
        subset = inserted_array[index_exp]
        assert repr(subset) == f"Subset(bounds={bounds}, shape={subset.shape}, array={inserted_array.id})"

    @pytest.mark.parametrize(
        ("index_exp", "bounds"),
        [(0, "[0]"), ((slice(0, 2), 1), "[0:2, 1]"), ((slice(None), slice(None, None, 2)), "[:, ::2]")],
    )
    def test_str_is_the_array_with_the_bounds(self, inserted_array: Array, index_exp: Slice, bounds: str):
        """The str of a Subset is the str of its Array followed by the bounds."""
        assert str(inserted_array[index_exp]) == f"{inserted_array!s}{bounds}"

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
            array = col.create({"pa": 1}, {"ca": 0})
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
            array = col.create({"pa": 1}, {"ca": 0})
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


def read_slowly(*args, **kwargs):
    """Stand for a read of the storage that takes two seconds."""
    time.sleep(2)
    return 0.0


SUBSET_OPERATIONS = {
    "read": ("read_data", ()),
    "update": ("update", (1.0,)),
    "clear": ("clear", ()),
}


class TestSubsetTimeoutAndCancellation:
    """A read is limited by its timeout as a whole; a write waits for the lock of the Array up to its timeout."""

    @pytest.mark.parametrize("operation", list(SUBSET_OPERATIONS))
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [pytest.param({"timeout": 5}, 5, id="given"), pytest.param({}, Sentinel, id="not-given")],
    )
    def test_timeout_reaches_the_adapter(self, inserted_array: Array, mocker, operation, kwargs, expected):
        method, args = SUBSET_OPERATIONS[operation]
        adapter_call = mocker.patch.object(inserted_array._adapter, method, return_value=0.0)
        getattr(inserted_array[0, 0, 0], operation)(*args, **kwargs)
        assert adapter_call.call_args.kwargs["timeout"] == expected
        assert isinstance(adapter_call.call_args.kwargs["token"], AbstractToken)

    @pytest.mark.parametrize(
        ("kwargs", "background", "empty"),
        [
            pytest.param({}, False, True, id="default"),
            pytest.param({"background": True}, True, True, id="background"),
            pytest.param({"_empty_trash": False}, False, False, id="no-emptying"),
            pytest.param({"background": True, "_empty_trash": False}, True, False, id="both"),
        ],
    )
    def test_flags_of_the_clearing_reach_the_adapter(
        self, inserted_array: Array, mocker, kwargs: dict, background: bool, empty: bool
    ):
        adapter_call = mocker.patch.object(inserted_array._adapter, "clear")
        assert inserted_array[:].clear(**kwargs) is None
        assert adapter_call.call_args.kwargs["background"] is background
        assert adapter_call.call_args.kwargs["_empty_trash"] is empty

    @pytest.mark.parametrize("operation", list(SUBSET_OPERATIONS))
    @pytest.mark.parametrize("timeout", [0, -1, 1.5, True, "10", None])
    def test_invalid_timeout_is_rejected_before_the_adapter(self, inserted_array: Array, mocker, operation, timeout):
        method, args = SUBSET_OPERATIONS[operation]
        adapter_call = mocker.patch.object(inserted_array._adapter, method, return_value=0.0)
        with pytest.raises(DekerValidationError):
            getattr(inserted_array[0, 0, 0], operation)(*args, timeout=timeout)
        adapter_call.assert_not_called()

    @pytest.mark.parametrize("operation", list(SUBSET_OPERATIONS))
    def test_cancelled_scope_stops_before_the_adapter(self, inserted_array: Array, mocker, operation):
        method, args = SUBSET_OPERATIONS[operation]
        adapter_call = mocker.patch.object(inserted_array._adapter, method, return_value=0.0)
        token = SimpleToken()
        token.cancel()
        with cancellation_scope(token), pytest.raises(DekerCancelledError) as error:
            getattr(inserted_array[0, 0, 0], operation)(*args)
        assert not isinstance(error.value, DekerTimeoutError)
        adapter_call.assert_not_called()

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("setting", [False, True], ids=["argument", "client-setting"])
    def test_read_over_its_timeout_returns_no_data(self, inserted_array: Array, mocker, setting):
        mocker.patch.object(inserted_array._adapter.storage_adapter, "read_data", side_effect=read_slowly)
        kwargs = {"timeout": 1}
        if setting:
            mocker.patch.object(inserted_array._adapter.ctx.config, "read_timeout", 1)
            kwargs = {}
        start = time.monotonic()
        with pytest.raises(DekerTimeoutError):
            inserted_array[0, 0, 0].read(**kwargs)
        assert 1.9 < time.monotonic() - start < 4

    @pytest.mark.timeout(10)
    def test_read_within_its_timeout_returns_the_data(self, inserted_array: Array, array_data: np.ndarray):
        assert inserted_array[0, 0, 0].read(timeout=60) == array_data[0, 0, 0]

    @pytest.mark.timeout(10)
    @pytest.mark.parametrize("operation", ["update", "clear"])
    @pytest.mark.parametrize("setting", [False, True], ids=["argument", "client-setting"])
    def test_write_waits_for_the_lock_up_to_its_timeout(
        self, inserted_array: Array, array_data: np.ndarray, mocker, operation, setting
    ):
        args = SUBSET_OPERATIONS[operation][1]
        kwargs = {"timeout": 1}
        if setting:
            mocker.patch.object(inserted_array._adapter.ctx.config, "write_lock_timeout", 1)
            kwargs = {}
        holder = Flock(inserted_array._adapter._get_main_path_to_file(inserted_array), shared=True)
        holder.acquire()
        try:
            start = time.monotonic()
            with pytest.raises(DekerLockError):
                getattr(inserted_array[0, 0, 0], operation)(*args, **kwargs)
            assert time.monotonic() - start < 2.5
        finally:
            holder.release()
        assert inserted_array[0, 0, 0].read() == array_data[0, 0, 0]


def main_file_of(array: Array) -> Path:
    """Main file of an Array.

    :param array: the Array
    :returns: path to its hdf5 file
    """
    adapter = array._adapter
    return get_main_path(array.id, adapter.collection_path / adapter.data_dir) / (array.id + adapter.file_ext)


class MoveAwayOnCheck:
    """Stand for ``is_deleted`` of the adapter: moves the main file away and answers that the Array is there.

    The file leaves between the check of the subset and the opening of the file by the lock.

    :param file: the main file
    :param target: where the file is moved to
    """

    def __init__(self, file: Path, target: Path) -> None:
        self.file = file
        self.target = target

    def __call__(self, array: Array) -> bool:
        self.file.rename(self.target / self.file.name)
        return False


class TestSubsetOfAnArrayWhoseFileIsGone:
    """An Array whose main file is gone does not exist: its subset raises DekerInstanceNotExistsError."""

    def test_directory_gone_between_the_check_and_the_lock_of_the_full_clearing(
        self, inserted_array: Array, tmp_path: Path, mocker
    ):
        """The full clearing locks the directory of the main file first: a directory gone is a missing Array."""
        file = main_file_of(inserted_array)
        directory = file.parent
        check = mocker.patch.object(
            inserted_array._adapter, "is_deleted", side_effect=MoveAwayOnCheck(directory, tmp_path)
        )
        with pytest.raises(DekerInstanceNotExistsError) as error:
            inserted_array[:].clear()
        assert str(error.value) == f"{directory} doesn't exist"
        assert check.call_count == 1
        (tmp_path / directory.name).rename(directory)

    @pytest.mark.parametrize("operation", ["read", "update", "clear"])
    def test_file_gone_before_the_call(self, inserted_array: Array, tmp_path: Path, operation: str):
        """The directory of the file stays: the subset still tells that the Array does not exist."""
        file = main_file_of(inserted_array)
        file.rename(tmp_path / file.name)
        subset = inserted_array[:]
        args = (1.0,) if operation == "update" else ()
        with pytest.raises(DekerInstanceNotExistsError) as error:
            getattr(subset, operation)(*args)
        assert str(error.value) == (
            f"{subset} doesn't exist, create new or get an instance again to be able to call Subset.{operation}"
        )
        assert file.parent.is_dir()

    @pytest.mark.parametrize("operation", ["read", "update", "clear"])
    def test_file_gone_between_the_check_and_the_lock(
        self, inserted_array: Array, tmp_path: Path, mocker, operation: str
    ):
        file = main_file_of(inserted_array)
        check = mocker.patch.object(inserted_array._adapter, "is_deleted", side_effect=MoveAwayOnCheck(file, tmp_path))
        args = (1.0,) if operation == "update" else ()
        with pytest.raises(DekerInstanceNotExistsError) as error:
            getattr(inserted_array[:], operation)(*args)
        assert str(error.value) == f"{file} doesn't exist"
        assert check.call_count == 1
        assert not file.exists()


def trash_of(array: Array) -> Path:
    """Find the trash of the storage of an Array.

    :param array: the Array
    :returns: the trash
    """
    return Path(array._adapter.uri.path) / array._adapter.ctx.config.trash_directory


def inode_of(path: Path) -> int:
    """Get the inode of a file: a full clearing puts a new file at the path of the main file.

    :param path: the file
    :returns: its inode
    """
    return os.stat(path).st_ino


class MoveAfterReplace:
    """Stand for ``os.replace``: replaces, then moves paths to the trash, as a deletion of the Array does meanwhile.

    :param real: the real ``os.replace``
    :param paths: what the deletion moves right after the replace, in its order
    :param trash: the trash of the storage
    """

    def __init__(self, real, paths: list[Path], trash: Path) -> None:
        self.real = real
        self.paths = paths
        self.trash = trash

    def __call__(self, source, target, *args, **kwargs):
        self.real(source, target, *args, **kwargs)
        for number, path in enumerate(self.paths):
            os.rename(path, self.trash / f"deleted-{number}")


class DeleteBeforeLink:
    """Stand for ``os.link``: the Array is deleted first, as a deletion that comes meanwhile does, then the link.

    :param real: the real ``os.link``
    :param array: the Array deleted
    """

    def __init__(self, real, array: Array) -> None:
        self.real = real
        self.array = array

    def __call__(self, *args, **kwargs):
        self.array.delete(_empty_trash=False)
        return self.real(*args, **kwargs)


class TestClearOfAWholeArray:
    """A clearing of every cell of an Array puts an empty file in place of its file: it waits for the writer and for
    another full clearing, never for the readers; the old file goes to the trash by a hard link."""

    @pytest.fixture
    def main(self, inserted_array: Array) -> Path:
        """Main file of the Array; the trash is empty before the test and after it.

        :param inserted_array: the Array with its data
        """
        trash = trash_of(inserted_array)
        assert list(trash.iterdir()) == []
        yield main_file_of(inserted_array)
        inserted_array._adapter.empty_trash(background=False)

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("index_exp", [np.index_exp[:], np.index_exp[...], np.index_exp[0:10, :, ::1]])
    def test_readers_are_not_waited_for(self, inserted_array: Array, main: Path, index_exp: Slice):
        before = inode_of(main)
        with Flock(main, shared=True):
            start = time.monotonic()
            assert inserted_array[index_exp].clear(timeout=5) is None
            assert time.monotonic() - start < 0.5
        assert inode_of(main) != before
        assert np.isnan(inserted_array[:].read()).all()
        assert list(trash_of(inserted_array).iterdir()) == []

    @pytest.mark.timeout(60)
    def test_writer_is_waited_for_up_to_the_timeout(self, inserted_array: Array, array_data: ndarray, main: Path):
        before = inode_of(main)
        with Flock(main):
            start = time.monotonic()
            with pytest.raises(DekerLockError) as error:
                inserted_array[:].clear(timeout=1)
            assert 0.9 < time.monotonic() - start < 2.5
        assert str(error.value) == f"{inserted_array} is locked by a writer"
        assert inode_of(main) == before
        assert np.array_equal(inserted_array[:].read(), array_data)
        assert os.listdir(main.parent) == [main.name]

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("directory", [False, True], ids=["writer", "another clearing"])
    def test_holder_that_leaves_within_the_timeout_lets_it_clear(
        self, inserted_array: Array, main: Path, directory: bool
    ):
        holder = Flock(main.parent if directory else main)
        holder.acquire()
        release = threading.Timer(1, holder.release)
        release.start()
        try:
            start = time.monotonic()
            assert inserted_array[:].clear(timeout=10) is None
            assert time.monotonic() - start > 0.9
        finally:
            release.join(30)
        assert np.isnan(inserted_array[:].read()).all()

    @pytest.mark.timeout(60)
    def test_another_clearing_is_waited_for_up_to_the_timeout(
        self, inserted_array: Array, array_data: ndarray, main: Path
    ):
        """Another full clearing holds the directory of the main file exclusively."""
        before = inode_of(main)
        with Flock(main.parent):
            start = time.monotonic()
            with pytest.raises(DekerLockError) as error:
                inserted_array[:].clear(timeout=1)
            assert 0.9 < time.monotonic() - start < 2.5
        assert str(error.value) == f"{inserted_array} is locked by another clearing"
        assert inode_of(main) == before
        assert np.array_equal(inserted_array[:].read(), array_data)

    @pytest.mark.timeout(60)
    def test_lock_of_the_directory_stops_neither_readers_nor_writers(
        self, inserted_array: Array, array_data: ndarray, main: Path
    ):
        with Flock(main.parent):
            start = time.monotonic()
            assert inserted_array[0, 0, 0].read(timeout=1) == array_data[0, 0, 0]
            inserted_array[1, 1, 1].update(-1.0, timeout=1)
            inserted_array[2, 2, 2].clear(timeout=1)
            assert time.monotonic() - start < 2
        assert inserted_array[1, 1, 1].read() == -1.0
        assert np.isnan(inserted_array[2, 2, 2].read())

    def test_directory_is_locked_before_the_file(self, inserted_array: Array, main: Path, mocker: MockerFixture):
        acquire = mocker.spy(Flock, "acquire")
        inserted_array[:].clear()
        assert [call.args[0].file for call in acquire.call_args_list] == [main.parent, main]
        assert [call.args[0].shared for call in acquire.call_args_list] == [False, True]

    def test_reader_that_holds_the_old_file_reads_the_old_data(
        self, inserted_array: Array, array_data: ndarray, main: Path
    ):
        with h5py.File(main, "r", locking=False) as old:
            inserted_array[:].clear()
            assert np.array_equal(old["data"][()], array_data)
        assert np.isnan(inserted_array[:].read()).all()

    def test_object_taken_before_reads_the_fill_value(self, inserted_array: Array, main: Path):
        taken_before = inserted_array._Array__collection.filter({"id": inserted_array.id}).last()
        inserted_array[:].clear()
        assert np.isnan(taken_before[:].read()).all()
        taken_before[0, 0, 0].update(5.0)
        assert inserted_array[0, 0, 0].read() == 5.0

    def test_symlink_keeps_its_text_and_leads_to_the_new_file(self, inserted_array_with_attributes: Array):
        array = inserted_array_with_attributes
        main = main_file_of(array)
        symlink = get_paths(array, array._adapter.collection_path).symlink / main.name
        text, attributes = symlink.readlink(), (dict(array.primary_attributes), dict(array.custom_attributes))
        array[:].clear()
        assert symlink.readlink() == text
        assert os.path.samefile(symlink, main)
        collection = array._Array__collection
        for found in (collection.filter({"id": array.id}).last(), collection.filter(array.primary_attributes).last()):
            assert found.id == array.id
            assert (dict(found.primary_attributes), dict(found.custom_attributes)) == attributes
            assert np.isnan(found[:].read()).all()
        array._adapter.empty_trash(background=False)

    def test_metadata_is_taken_from_the_file(self, inserted_array_with_attributes: Array):
        """The custom attributes updated through another object after this one was taken are kept."""
        stale = inserted_array_with_attributes
        collection = stale._Array__collection
        fresh = collection.filter({"id": stale.id}).last()
        fresh.update_custom_attributes({"custom_attribute": 9.5})
        stale[:].clear()
        assert collection.filter({"id": stale.id}).last().custom_attributes["custom_attribute"] == 9.5
        stale._adapter.empty_trash(background=False)

    def test_data_directory_holds_the_main_file_only(self, inserted_array: Array, main: Path):
        inserted_array[:].clear()
        assert os.listdir(main.parent) == [main.name]

    @pytest.mark.parametrize("call", ["link", "replace"])
    @pytest.mark.parametrize("error", [OSError("refused"), KeyboardInterrupt()], ids=["error", "interrupt"])
    def test_failure_before_the_replace_removes_the_new_file(
        self, inserted_array: Array, array_data: ndarray, main: Path, mocker: MockerFixture, call: str, error
    ):
        before = inode_of(main)
        mocker.patch.object(os, call, side_effect=error)
        with pytest.raises(type(error)) as raised:
            inserted_array[:].clear()
        assert raised.value is error
        mocker.stopall()
        assert os.listdir(main.parent) == [main.name]
        assert inode_of(main) == before
        assert np.array_equal(inserted_array[:].read(), array_data)
        linked = list(trash_of(inserted_array).iterdir())
        assert [inode_of(path) for path in linked] == ([before] if call == "replace" else [])

    @pytest.mark.parametrize("moved", [1, 2], ids=["symlink", "symlink and main file"])
    def test_deletion_after_the_replace_takes_the_new_file_to_the_trash(
        self, inserted_array: Array, main: Path, mocker: MockerFixture, moved: int
    ):
        """The deletion moves the symlink, then the main file; the clearing finds the symlink gone after the replace."""
        symlink = get_paths(inserted_array, inserted_array._adapter.collection_path).symlink / main.name
        stand = MoveAfterReplace(os.replace, [symlink, main][:moved], trash_of(inserted_array))
        mocker.patch.object(os, "replace", side_effect=stand)
        with pytest.raises(DekerInstanceNotExistsError) as error:
            inserted_array[:].clear()
        assert str(error.value) == f"{main} doesn't exist"
        assert not os.path.lexists(main)
        assert not os.path.lexists(symlink)
        assert not main.parent.exists()

    def test_deletion_before_the_link_stops_the_clearing(
        self, inserted_array: Array, main: Path, mocker: MockerFixture
    ):
        mocker.patch.object(os, "link", side_effect=DeleteBeforeLink(os.link, inserted_array))
        with pytest.raises(DekerInstanceNotExistsError) as error:
            inserted_array[:].clear()
        assert str(error.value) == f"{main} doesn't exist"
        assert not main.parent.exists()

    @pytest.mark.parametrize("background", [False, True])
    def test_without_emptying_the_trash_keeps_the_old_file_there(
        self, inserted_array: Array, main: Path, mocker: MockerFixture, background: bool
    ):
        before = inode_of(main)
        empty = mocker.spy(deker_local_adapters.mixin, "empty_trash_directory")
        thread = mocker.spy(threading.Thread, "start")
        assert inserted_array[:].clear(background=background, _empty_trash=False) is None
        empty.assert_not_called()
        thread.assert_not_called()
        assert [inode_of(path) for path in trash_of(inserted_array).iterdir()] == [before]

    def test_trash_is_emptied_once_by_default(self, inserted_array: Array, main: Path, mocker: MockerFixture):
        empty = mocker.spy(deker_local_adapters.mixin, "empty_trash_directory")
        inserted_array[:].clear()
        empty.assert_called_once_with(trash_of(inserted_array), background=False, logger=inserted_array._adapter.logger)
        assert list(trash_of(inserted_array).iterdir()) == []

    @pytest.mark.timeout(60)
    def test_background_returns_while_the_trash_is_emptied(
        self, inserted_array: Array, main: Path, mocker: MockerFixture
    ):
        before = inode_of(main)
        held = HeldEmptying(deker_local_adapters.collection_adapter._empty_trash_in_background)
        mocker.patch.object(deker_local_adapters.collection_adapter, "_empty_trash_in_background", side_effect=held)
        assert inserted_array[:].clear(background=True) is None
        assert held.started.wait(30)
        assert [inode_of(path) for path in trash_of(inserted_array).iterdir()] == [before]
        (thread,) = held.threads
        assert thread is not threading.current_thread()
        assert thread.daemon
        held.go.set()
        thread.join(30)
        assert list(trash_of(inserted_array).iterdir()) == []

    def test_failed_emptying_is_raised_after_the_clearing(self, inserted_array: Array, main: Path, client: Client):
        trash = trash_of(inserted_array)
        locked = trash / "locked"
        locked.mkdir()
        (locked / "kept").write_text("file")
        locked.chmod(0o500)
        try:
            with pytest.raises(DekerTrashError) as error:
                inserted_array[:].clear()
            assert str(error.value) == (
                f"The trash of the storage is not emptied: 1 files are not removed from {trash}. "
                "Reasons: Permission denied"
            )
            assert np.isnan(inserted_array[:].read()).all()
        finally:
            locked.chmod(0o700)
            client.empty_trash()

    def test_cancelled_before_the_start_changes_nothing(self, inserted_array: Array, array_data: ndarray, main: Path):
        before = inode_of(main)
        scope = SimpleToken()
        scope.cancel()
        with cancellation_scope(scope), pytest.raises(DekerCancelledError):
            inserted_array[:].clear()
        assert inode_of(main) == before
        assert np.array_equal(inserted_array[:].read(), array_data)
        assert list(trash_of(inserted_array).iterdir()) == []


class TestClearCriterionOfAWholeArray:
    """A subset is the whole Array when it selects every cell, whatever its shape: such a subset is cleared by the
    replacement of the file, without waiting for the readers; any other subset is cleared in place, waiting for them."""

    @pytest.fixture
    def flat(self, client: Client) -> Array:
        """An Array with an axis of size 1, written, in a collection of its own; deleted after the test.

        :param client: client of the storage
        """
        dimensions = [DimensionSchema(name="x", size=1), DimensionSchema(name="y", size=4)]
        collection = client.create_collection(
            "flat_" + str(os.getpid()), ArraySchema(dimensions=dimensions, dtype=float)
        )
        array = collection.create()
        array[:].update(np.arange(4, dtype=float).reshape(1, 4))
        yield array
        collection.delete()

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize(
        ("index_exp", "whole"),
        [
            pytest.param(np.index_exp[0], True, id="integer-index-on-an-axis-of-size-1"),
            pytest.param(np.index_exp[0, :], True, id="integer-index-and-slice"),
            pytest.param(np.index_exp[:, ::-1], True, id="reversed-axis"),
            pytest.param(np.index_exp[0, ::2], False, id="stride"),
            pytest.param(np.index_exp[:, 1:], False, id="part"),
        ],
    )
    def test_whole_by_the_cells(self, flat: Array, index_exp: Slice, whole: bool):
        main = main_file_of(flat)
        before = inode_of(main)
        with Flock(main, shared=True):
            if whole:
                assert flat[index_exp].clear(timeout=1) is None
            else:
                with pytest.raises(DekerLockError) as error:
                    flat[index_exp].clear(timeout=1)
                assert str(error.value) == f"{flat} is locked by its readers or a writer"
        assert (inode_of(main) != before) is whole
        expected = np.arange(4, dtype=float).reshape(1, 4)
        if whole:
            expected[:] = np.nan
        assert np.array_equal(flat[:].read(), expected, equal_nan=True)
        flat._adapter.empty_trash(background=False)


class TestClearInAStorageWithoutItsDirectories:
    """The clearing that does not find the trash or the collections directory of the storage is critical."""

    @pytest.fixture
    def array(self, tmp_path: Path) -> Array:
        """A written Array in a storage of the test's own.

        :param tmp_path: directory of the test
        """
        client = Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR")
        schema = ArraySchema(dimensions=[DimensionSchema(name="x", size=4)], dtype=float)
        array = client.create_collection("own", schema).create()
        array[:].update(np.zeros(4))
        yield array
        client.close()

    @pytest.mark.parametrize("directory", ["trash", "collections"])
    @pytest.mark.parametrize("index_exp", [np.index_exp[:], np.index_exp[1:]], ids=["whole", "part"])
    def test_missing_storage_directory_is_critical(
        self, array: Array, tmp_path: Path, mocker: MockerFixture, directory: str, index_exp: Slice
    ):
        gone = tmp_path / "storage" / directory
        gone.rename(tmp_path / "away")
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        with pytest.raises(DekerCriticalError) as error:
            array[index_exp].clear()
        assert str(error.value) == f"Storage directory {gone} doesn't exist"
        critical.assert_called_once_with(f"Storage directory {gone} doesn't exist")

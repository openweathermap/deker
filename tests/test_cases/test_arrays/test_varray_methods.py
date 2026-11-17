import os
import string

from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import shuffle
from typing import Any

import numpy as np
import pytest

from deker_local_adapters import HDF5StorageAdapter, LocalArrayAdapter, LocalVArrayAdapter
from deker_local_adapters.factory import AdaptersFactory
from numpy import ndarray

from tests.parameters.array_params import attributes_validation_params
from tests.parameters.index_exp_params import (
    invalid_index_params,
    invalid_step_index_exp_params,
    valid_index_exp_params,
)
from tests.parameters.uri import embedded_uri

from deker.arrays import VArray
from deker.client import Client
from deker.collection import Collection
from deker.dimensions import TimeDimension
from deker.errors import DekerMemoryError, DekerValidationError
from deker.schemas import DimensionSchema, TimeDimensionSchema, VArraySchema
from deker.tools import get_paths
from deker.tools.schema import create_dimensions
from deker.types import ArrayMeta
from deker.types.private.typings import FancySlice, Slice
from deker.types.public.classes import Scale


class TestVArrayMethods:
    @pytest.mark.parametrize("varray_init_params", attributes_validation_params())
    def test_varray_raises_on_validate_attributes(
        self,
        varray_init_params: dict,
        factory: AdaptersFactory,
        varray_collection_with_attributes: Collection,
    ):
        """Tests attributes validation on VArray creation.

        :param varray_init_params: parameters for Array creation
        :param factory: fixture
        :param varray_collection_with_attributes: fixture
        """

        varray_init_params["adapter"] = factory.get_varray_adapter(
            varray_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter
        )
        varray_init_params["array_adapter"] = factory.get_array_adapter(
            varray_collection_with_attributes.path,
            HDF5StorageAdapter,
            varray_collection_with_attributes.options,
        )
        varray_init_params["collection"] = varray_collection_with_attributes
        with pytest.raises(DekerValidationError):  # type: ignore[call-overload]
            VArray(**varray_init_params)

    def test_array_init(
        self,
        factory: AdaptersFactory,
        varray_collection_with_attributes: Collection,
    ):
        adapter = factory.get_varray_adapter(varray_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter)
        array_adapter = factory.get_array_adapter(
            varray_collection_with_attributes.path,
            HDF5StorageAdapter,
            varray_collection_with_attributes.options,
        )
        assert VArray(
            varray_collection_with_attributes,
            adapter,
            array_adapter,
            primary_attributes={"primary_attribute": 3},
            custom_attributes={
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        )

    @pytest.mark.parametrize(
        "varray_collection",
        [
            None,
            True,
            False,
            0,
            1,
            -1,
            0.1,
            -0.1,
            complex(0000000000.1),
            complex(-0000000000.1),
            "",
            " ",
            "         ",
            string.whitespace,
            string.printable,
            "a",
            [],
            ["a"],
            tuple(),
            tuple(
                "a",
            ),
            set(),
            {"a"},
            dict(),
            dict(collection="a"),
        ],
    )
    def test_array_init_fails(
        self,
        factory: AdaptersFactory,
        varray_collection: Any,
    ):
        mock_path = Path(".")
        adapter = factory.get_varray_adapter(mock_path, storage_adapter=HDF5StorageAdapter)
        array_adapter = factory.get_array_adapter(mock_path, storage_adapter=HDF5StorageAdapter)
        with pytest.raises(AttributeError):
            assert VArray(
                varray_collection,
                adapter,
                array_adapter,
                primary_attributes={"primary_attribute": 3},
                custom_attributes={
                    "custom_attribute": 0.5,
                    "time_attr_name": datetime.now(timezone.utc),
                },
            )

    def test_varray_shapes_and_vgrid(
        self,
        factory: AdaptersFactory,
        varray_collection_with_attributes: Collection,
    ):
        adapter = factory.get_varray_adapter(varray_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter)
        array_adapter = factory.get_array_adapter(
            varray_collection_with_attributes.path,
            HDF5StorageAdapter,
            varray_collection_with_attributes.options,
        )
        array = VArray(
            varray_collection_with_attributes,
            adapter,
            array_adapter,
            primary_attributes={"primary_attribute": 3},
            custom_attributes={
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        )
        assert isinstance(array.shape, tuple)
        assert array.shape == tuple([dim.size for dim in varray_collection_with_attributes.varray_schema.dimensions])
        assert array.shape == varray_collection_with_attributes.varray_schema.shape
        assert isinstance(array.named_shape, tuple)
        assert array.named_shape == tuple(
            (dim.name, dim.size) for dim in varray_collection_with_attributes.varray_schema.dimensions
        )
        assert array.vgrid == varray_collection_with_attributes.varray_schema.vgrid
        assert isinstance(array.vgrid, tuple)

        assert array.arrays_shape == tuple(np.asarray(array.shape) // np.asarray(array.vgrid))
        assert isinstance(array.arrays_shape, tuple)

    def test_varray_collection_property(
        self,
        factory: AdaptersFactory,
        varray_collection_with_attributes: Collection,
    ):
        adapter = factory.get_varray_adapter(varray_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter)
        array_adapter = factory.get_array_adapter(
            varray_collection_with_attributes.path,
            HDF5StorageAdapter,
            varray_collection_with_attributes.options,
        )
        array = VArray(
            varray_collection_with_attributes,
            adapter,
            array_adapter,
            primary_attributes={"primary_attribute": 3},
            custom_attributes={
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        )
        assert isinstance(array.collection, str)
        assert array.collection == varray_collection_with_attributes.name

    def test_delete_varray(self, root_path, inserted_varray: VArray, factory):
        """Tests array delete method.

        :param root_path: temporary array_collection root path
        :param inserted_varray: VArray object
        """
        collection = inserted_varray._VArray__collection
        adapter = factory.get_varray_adapter(collection.path, storage_adapter=HDF5StorageAdapter)
        array_adapter = factory.get_array_adapter(collection.path, HDF5StorageAdapter, collection.options)
        paths = get_paths(
            inserted_varray,
            root_path / adapter.ctx.config.collections_directory / inserted_varray.collection,
        )
        filename = paths.main / (inserted_varray.id + adapter.file_ext)
        assert filename.exists()
        inserted_varray.delete()
        assert not os.path.exists(filename)
        assert not (paths.symlink / inserted_varray.id).exists()
        assert not (paths.main / inserted_varray.id).exists()
        assert not paths.main.exists()
        assert not (
            inserted_varray._VArray__collection.path  # type: ignore[attr-defined]
            / array_adapter.data_dir
            / inserted_varray.id
        ).exists()
        # the symlinks of the Arrays live under the VArray id; the collection is shared with other tests
        assert not (
            inserted_varray._VArray__collection.path  # type: ignore[attr-defined]
            / array_adapter.symlinks_dir
            / inserted_varray.id
        ).exists()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_update_varray(self, inserted_varray: VArray, array_data: np.ndarray, index_exp):
        """Tests VArray update method.

        :param inserted_varray: VArray object
        :param array_data: Data to update array with
        """
        old_data = inserted_varray[:].read()
        assert (old_data == array_data).all()
        len_items = len(old_data.flatten())

        update = np.asarray(range(len_items, len_items + 1000), dtype=inserted_varray.dtype)
        update = update.reshape(inserted_varray.shape)
        update = update[index_exp]

        vsubset = inserted_varray[index_exp]
        vsubset.update(update)
        data = inserted_varray[index_exp].read()
        assert (data == update).all()

    def test_dimensions_are_built_once(self, inserted_array_with_attributes: VArray):
        """The dimensions of an array are built on the first access and then reused."""
        first = inserted_array_with_attributes.dimensions
        assert inserted_array_with_attributes.dimensions is first
        fresh = create_dimensions(
            inserted_array_with_attributes.schema.dimensions,
            {**inserted_array_with_attributes.primary_attributes, **inserted_array_with_attributes.custom_attributes},
        )
        assert [d.as_dict for d in first] == [d.as_dict for d in fresh]

    def test_dimensions_are_rebuilt_after_an_update_of_the_custom_attributes(
        self, inserted_array_with_attributes: VArray
    ):
        """A time dimension starting at a custom attribute follows the attribute after an update."""
        before = inserted_array_with_attributes.dimensions
        new_start = datetime(2030, 1, 1, tzinfo=timezone.utc)
        inserted_array_with_attributes.update_custom_attributes({"time_attr_name": new_start})
        after = inserted_array_with_attributes.dimensions
        assert after is not before
        assert [d.start_value for d in after if isinstance(d, TimeDimension)] == [new_start]
        assert [d.start_value for d in before if isinstance(d, TimeDimension)] != [new_start]

    def test_update_custom_attributes(self, inserted_array_with_attributes: VArray):
        """Tests array update method.

        :param inserted_array_with_attributes: Array object
        """
        new_custom_attributes = {
            "custom_attribute": 0.6,
            "time_attr_name": datetime.now(timezone.utc),
        }

        assert inserted_array_with_attributes.custom_attributes != new_custom_attributes
        inserted_array_with_attributes.update_custom_attributes(new_custom_attributes)
        assert inserted_array_with_attributes.custom_attributes == new_custom_attributes
        meta = inserted_array_with_attributes.read_meta()
        meta_time = datetime.fromisoformat(meta["custom_attributes"]["time_attr_name"])
        assert (
            meta_time
            == inserted_array_with_attributes.custom_attributes["time_attr_name"]
            == new_custom_attributes["time_attr_name"]
        )
        for d in inserted_array_with_attributes.dimensions:
            if isinstance(d, TimeDimension):
                assert d.start_value == new_custom_attributes["time_attr_name"]

    @pytest.mark.parametrize(
        "new_custom_attributes",
        [
            {},
            {"custom_attribute": "0.6", "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": "", "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": " ", "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": "       ", "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": "abc", "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": list(), "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": tuple(), "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": set(), "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": dict(), "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": [0], "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": (0,), "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": {0}, "time_attr_name": datetime.now(timezone.utc)},
            {"custom_attribute": {0: 0}, "time_attr_name": datetime.now(timezone.utc)},
            {
                "custom_attribute": datetime.now(timezone.utc),
                "time_attr_name": datetime.now(timezone.utc),
            },
            {"custom_attribute": 0.6, "time_attr_name": datetime.now(timezone.utc).isoformat()},
            {"custom_attribute": 0.6, "time_attr_name": timezone.utc},
            {"custom_attribute": 0.6, "time_attr_name": ""},
            {"custom_attribute": 0.6, "time_attr_name": " "},
            {"custom_attribute": 0.6, "time_attr_name": "       "},
            {"custom_attribute": 0.6, "time_attr_name": "abc"},
            {"custom_attribute": 0.6, "time_attr_name": list()},
            {"custom_attribute": 0.6, "time_attr_name": tuple()},
            {"custom_attribute": 0.6, "time_attr_name": set()},
            {"custom_attribute": 0.6, "time_attr_name": dict()},
            {"custom_attribute": 0.6, "time_attr_name": [0]},
            {"custom_attribute": 0.6, "time_attr_name": (0,)},
            {"custom_attribute": 0.6, "time_attr_name": {0}},
            {"custom_attribute": 0.6, "time_attr_name": {0: 0}},
        ],
    )
    def test_varray_update_custom_attributes_fail(
        self, inserted_varray_with_attributes: VArray, new_custom_attributes: dict
    ):
        """Tests VArray update method.

        :param inserted_varray_with_attributes: VArray object
        :param new_custom_attributes: Data to update array custom attributes
        """
        with pytest.raises(DekerValidationError):  # type: ignore[call-overload]
            inserted_varray_with_attributes.update_custom_attributes(new_custom_attributes)

    @pytest.mark.parametrize(
        ("new_custom_attributes", "skipped_attr"),
        [
            ({"custom_attribute": 0.7}, "time_attr_name"),
            ({"custom_attribute": None}, "time_attr_name"),
            ({"time_attr_name": datetime.now(timezone.utc)}, "custom_attribute"),
        ],
    )
    def test_varray_update_custom_attributes_update_just_passed_attrs(
        self,
        inserted_varray_with_attributes: VArray,
        new_custom_attributes: dict,
        skipped_attr: str,
    ):
        old_attrs = inserted_varray_with_attributes.custom_attributes
        new_attr_name = list(new_custom_attributes.keys())[0]
        inserted_varray_with_attributes.update_custom_attributes(new_custom_attributes)
        assert inserted_varray_with_attributes.custom_attributes[new_attr_name] == new_custom_attributes[new_attr_name]
        assert old_attrs[skipped_attr] == inserted_varray_with_attributes.custom_attributes[skipped_attr]

    @pytest.mark.parametrize("index_exp", invalid_index_params)
    def test_array_getitem_raises(self, inserted_varray: VArray, index_exp: Slice):
        """Test VArray raises IndexError on invalid slice expression (None, True, False, too many indexes).

        :param inserted_varray: Pre created VArray
        :param index_exp: slicing params list
        """
        with pytest.raises(IndexError):
            assert inserted_varray[index_exp]

    @pytest.mark.parametrize(
        ("fancy_index_exp", "index_exp"),
        [
            (np.index_exp[90.0, -180.0], np.index_exp[0, 0]),
            (np.index_exp[0.0, -180.0], np.index_exp[180, 0]),
            (np.index_exp[-90.0, -180.0], np.index_exp[360, 0]),
            (np.index_exp[90.0, 0.0], np.index_exp[0, 360]),
            (np.index_exp[0.0, 0.0], np.index_exp[180, 360]),
            (np.index_exp[-90.0, 0.0], np.index_exp[360, 360]),
            (np.index_exp[90.0, 179.5], np.index_exp[0, 719]),
            (np.index_exp[0.0, 179.5], np.index_exp[180, 719]),
            (np.index_exp[-90.0, 179.5], np.index_exp[360, 719]),
            (np.index_exp[90.0, 90.0], np.index_exp[0, 540]),
            (np.index_exp[-90.0, -90.0], np.index_exp[360, 180]),
            (np.index_exp[:, -90.0], np.index_exp[:, 180]),
            (np.index_exp[-90.0, :], np.index_exp[360, :]),
            (np.index_exp[..., "j"], np.index_exp[..., 9]),  # the index after an ellipsis is for the last axis
            (np.index_exp[-90.0, ...], np.index_exp[360, ...]),
            (np.index_exp[-90.0, 180], np.index_exp[360, 180]),
            (-89.5, 359),
            (np.index_exp[90.0:0.0, -180.0:0.0], np.index_exp[0:180, 0:360]),
            (np.index_exp[0.0:-90.0, 0.0:179.5], np.index_exp[180:-1, 360:-1]),
            (np.index_exp[88.5:-1.5, -179.5:0.5], np.index_exp[3:183, 1:361]),
            (np.index_exp[1.5:-88.5, -3.5:3.5], np.index_exp[177:-4, 353:367]),
            (np.index_exp[90.0, -180.0, "a"], np.index_exp[0, 0, 0]),
            (np.index_exp[0.0, -180.0, "b"], np.index_exp[180, 0, 1]),
            (np.index_exp[-90.0, -180.0, "c"], np.index_exp[360, 0, 2]),
            (np.index_exp[90.0, 0.0, "d"], np.index_exp[0, 360, 3]),
            (np.index_exp[0.0, 0.0, "e"], np.index_exp[180, 360, 4]),
            (np.index_exp[-90.0, 0.0, "f"], np.index_exp[360, 360, 5]),
            (np.index_exp[90.0, 179.5, "g"], np.index_exp[0, 719, 6]),
            (np.index_exp[0.0, 179.5, "h"], np.index_exp[180, 719, 7]),
            (np.index_exp[-90.0, 179.5, "i"], np.index_exp[360, 719, 8]),
            (np.index_exp[90.0, 90.0, "j"], np.index_exp[0, 540, 9]),
            (np.index_exp[90.0:0.0, -180.0:0.0, "b":"j"], np.index_exp[0:180, 0:360, 1:9]),
            (np.index_exp[0.0:-90.0, 0.0:179.5, "c":"f"], np.index_exp[180:-1, 360:-1, 2:5]),
            (np.index_exp[88.5:, :0.5, :"h"], np.index_exp[3:, :361, :7]),
            (np.index_exp[:-88.5, -3.5:, "d":], np.index_exp[:-4, 353:, 3:]),
            (np.index_exp[:, :, "a":"j"], np.index_exp[:, :, 0:9]),
            (np.index_exp[:, :, "b":"j"], np.index_exp[:, :, 1:9]),
            (np.index_exp[:, :, "c":"f"], np.index_exp[:, :, 2:5]),
            (np.index_exp[:, :, :"h"], np.index_exp[:, :, :7]),
            (np.index_exp[:, :, "d":], np.index_exp[:, :, 3:]),
        ],
    )
    def test_varray_fancy_getitem_ok(self, scaled_varray: VArray, index_exp: Slice, fancy_index_exp: FancySlice):
        """Tests subset fancy creation from array __getitem__.

        :param scaled_varray: Pre created Varray
        :param index_exp: slicing params list
        :param fancy_index_exp: slicing params list
        """
        fancy_subset = scaled_varray[fancy_index_exp]
        assert fancy_subset
        subset = scaled_varray[index_exp]
        assert subset
        fancy_data = fancy_subset.read()
        data = subset.read()
        assert (fancy_data == data).all()

    @pytest.mark.parametrize(
        "scale_index_exp",
        [
            "index",
            datetime.now(),
            datetime.now(tz=timezone.utc),
            0.1,
            0.2,
            0.5000001,
            -0.0000001,
            90.5,
            -90.5,
            np.index_exp[0.2:-90.0, 0.0:179.5],
            np.index_exp[0.5:-90.5, 0.0:179.5],
            np.index_exp[0.5:-90.0, 0.1:179.5],
            np.index_exp[0.5:-90.0, 0.0:180.0],
            np.index_exp[0.5:-90.0, 0.0:179.999999999999999],
            np.index_exp[0.0:-90.0:1.5],
            np.index_exp[0.0:-90.0:-1],
            np.index_exp[0.0:-90.0, 0.0:179.5:-1],
            np.index_exp[:, :, "а"],  # cyrillic
            np.index_exp[:, :, "aa"],
            np.index_exp[:, :, " a"],
            np.index_exp[:, :, "a "],
            np.index_exp[:, :, " a "],
            np.index_exp[:, :, "a":"z"],
            np.index_exp[:, :, 0.1],
            np.index_exp[:, :, -0.1],
        ],
    )
    def test_varray_scale_getitem_raises(self, scaled_varray: VArray, scale_index_exp: FancySlice):
        """Tests subset raises errors from array __getitem__.

        :param scaled_varray: Pre created Varray
        :param scale_index_exp: slicing params list
        """
        with pytest.raises(IndexError):
            scaled_varray[scale_index_exp]

    @pytest.mark.parametrize(
        ("fancy_index_exp", "index_exp"),
        [
            (np.index_exp[datetime(2023, 1, 1), datetime(2023, 1, 1)], np.index_exp[0, 0]),
            (np.index_exp[datetime(2023, 1, 15), datetime(2023, 1, 1, 12)], np.index_exp[14, 12]),
            (np.index_exp[datetime(2023, 1, 31), datetime(2023, 1, 1, 23)], np.index_exp[-1, -1]),
            (
                np.index_exp[
                    datetime(2023, 1, 2) : datetime(2023, 1, 30),
                    datetime(2023, 1, 1, 4) : datetime(2023, 1, 1, 23),
                ],
                np.index_exp[1:29, 4:23],
            ),
            (
                np.index_exp[: datetime(2023, 1, 15), datetime(2023, 1, 1, 12) :],
                np.index_exp[:14, 12:],
            ),
            (
                np.index_exp[datetime(2023, 1, 10) :, : datetime(2023, 1, 1, 4)],
                np.index_exp[9:, :4],
            ),
            (
                np.index_exp[datetime(2023, 1, 1).isoformat(), datetime(2023, 1, 1).isoformat()],
                np.index_exp[0, 0],
            ),
            (
                np.index_exp[datetime(2023, 1, 15).isoformat(), datetime(2023, 1, 1, 12).isoformat()],
                np.index_exp[14, 12],
            ),
            (
                np.index_exp[datetime(2023, 1, 31).isoformat(), datetime(2023, 1, 1, 23).isoformat()],
                np.index_exp[-1, -1],
            ),
            (
                np.index_exp[
                    datetime(2023, 1, 2).isoformat() : datetime(2023, 1, 30).isoformat(),
                    datetime(2023, 1, 1, 4).isoformat() : datetime(2023, 1, 1, 23).isoformat(),
                ],
                np.index_exp[1:29, 4:23],
            ),
            (
                np.index_exp[: datetime(2023, 1, 15).isoformat(), datetime(2023, 1, 1, 12).isoformat() :],
                np.index_exp[:14, 12:],
            ),
            (
                np.index_exp[datetime(2023, 1, 10).isoformat() :, : datetime(2023, 1, 1, 4).isoformat()],
                np.index_exp[9:, :4],
            ),
            (
                np.index_exp[
                    datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp(),
                    datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp(),
                ],
                np.index_exp[0, 0],
            ),
            (
                np.index_exp[
                    datetime(2023, 1, 15, tzinfo=timezone.utc).timestamp(),
                    datetime(2023, 1, 1, 12, tzinfo=timezone.utc).timestamp(),
                ],
                np.index_exp[14, 12],
            ),
            (
                np.index_exp[
                    datetime(2023, 1, 31, tzinfo=timezone.utc).timestamp(),
                    datetime(2023, 1, 1, 23, tzinfo=timezone.utc).timestamp(),
                ],
                np.index_exp[-1, -1],
            ),
            (
                np.index_exp[
                    datetime(2023, 1, 2, tzinfo=timezone.utc).timestamp() : datetime(
                        2023, 1, 30, tzinfo=timezone.utc
                    ).timestamp(),
                    datetime(2023, 1, 1, 4, tzinfo=timezone.utc).timestamp() : datetime(
                        2023, 1, 1, 23, tzinfo=timezone.utc
                    ).timestamp(),
                ],
                np.index_exp[1:29, 4:23],
            ),
            (
                np.index_exp[
                    : datetime(2023, 1, 15, tzinfo=timezone.utc).timestamp(),
                    datetime(2023, 1, 1, 12, tzinfo=timezone.utc).timestamp() :,
                ],
                np.index_exp[:14, 12:],
            ),
            (
                np.index_exp[
                    datetime(2023, 1, 10, tzinfo=timezone.utc).timestamp() :,
                    : datetime(2023, 1, 1, 4, tzinfo=timezone.utc).timestamp(),
                ],
                np.index_exp[9:, :4],
            ),
        ],
    )
    def test_timed_varray_fancy_getitem_ok(self, timed_varray: VArray, index_exp: Slice, fancy_index_exp: FancySlice):
        """Tests subset fancy creation from varray __getitem__.

        :param timed_varray: Pre created varray
        :param index_exp: slicing params list
        :param fancy_index_exp: slicing params list
        """
        fancy_subset = timed_varray[fancy_index_exp]
        assert fancy_subset
        subset = timed_varray[index_exp]
        assert subset
        fancy_data = fancy_subset.read()
        data = subset.read()
        assert (fancy_data == data).all()

    @pytest.mark.parametrize(
        "scale_index_exp",
        [
            "index",
            datetime.now(),
            datetime.now(tz=timezone.utc),
            0.1,
            0.2,
            0.5000001,
            -0.0000001,
            90.5,
            -90.5,
            np.index_exp[0.2:-90.0, 0.0:179.5],
            np.index_exp[0.5:-90.5, 0.0:179.5],
            np.index_exp[0.5:-90.0, 0.1:179.5],
            np.index_exp[0.5:-90.0, 0.0:180.0],
            np.index_exp[0.5:-90.0, 0.0:179.999999999999999],
            np.index_exp[0.0:-90.0:1.5],
            np.index_exp[0.0:-90.0:-1],
            np.index_exp[0.0:-90.0, 0.0:179.5:-1],
            np.index_exp[0.0:-90.0, 0.0:179.5:2],
            np.index_exp[:, "а"],
            np.index_exp[:, "aa"],
            np.index_exp[:, " a"],
            np.index_exp[:, "a "],
            np.index_exp[:, " a "],
            np.index_exp[:, "a":"z"],
            np.index_exp[:, 0.1],
            np.index_exp[:, -0.1],
            np.index_exp[datetime(2023, 1, 1) : datetime(2023, 1, 2).isoformat()],
            np.index_exp[datetime(2023, 1, 1).isoformat() : datetime(2023, 1, 2, tzinfo=timezone.utc).timestamp()],
            np.index_exp[datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp() : datetime(2023, 1, 2)],
            np.index_exp[
                datetime(2023, 1, 1, tzinfo=timezone(timedelta(hours=2))).timestamp(),
                datetime(2023, 1, 1, tzinfo=timezone(timedelta(hours=2))).timestamp(),
            ],
            np.index_exp[
                datetime(2023, 1, 15, tzinfo=timezone(timedelta(hours=2))).timestamp(),
                datetime(2023, 1, 1, 12, tzinfo=timezone(timedelta(hours=2))).timestamp(),
            ],
            np.index_exp[
                datetime(2023, 1, 31, tzinfo=timezone(timedelta(hours=2))).timestamp(),
                datetime(2023, 1, 1, 23, tzinfo=timezone(timedelta(hours=2))).timestamp(),
            ],
            np.index_exp[
                int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()),
                int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp()),
            ],
            np.index_exp[
                int(datetime(2023, 1, 15, tzinfo=timezone.utc).timestamp()),
                int(datetime(2023, 1, 1, 12, tzinfo=timezone.utc).timestamp()),
            ],
            np.index_exp[
                int(datetime(2023, 1, 31, tzinfo=timezone.utc).timestamp()),
                int(datetime(2023, 1, 1, 23, tzinfo=timezone.utc).timestamp()),
            ],
            np.index_exp[datetime(2023, 1, 1) :: timedelta(hours=36)],  # not a multiple of the step of one day
            np.index_exp[datetime(2023, 1, 1) :: timedelta(0)],
            np.index_exp[datetime(2023, 1, 1) :: 1.0],
            np.index_exp[timedelta(days=1) :: 2],
            np.index_exp[: timedelta(days=1) : 2],
        ],
    )
    def test_varray_time_getitem_raises(self, timed_varray: VArray, scale_index_exp: FancySlice):
        """Tests subset raises errors from array __getitem__.

        :param timed_varray: Pre created Varray
        :param scale_index_exp: slicing params list
        """
        with pytest.raises(IndexError):
            timed_varray[scale_index_exp]

    def test_varray_getitem_raises_memory_error(self, client, root_path):
        """Test array raise DekerMemoryError on too big slice expression and too low memory limit."""
        schema = VArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=1000000),
                DimensionSchema(name="y", size=1000000),
            ],
            dtype=float,
            vgrid=(10000, 10000),
        )
        col_name = "memory_excess"
        with Client(embedded_uri(root_path), memory_limit=100, loglevel="CRITICAL") as extra_client:
            big_collection = client.create_collection(col_name, schema)
            big_collection.create()

            collection = extra_client.get_collection(col_name)
            try:
                with pytest.raises(DekerMemoryError):
                    for array in collection:
                        assert array[:10000, :10000]
            finally:
                collection.delete()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_varray_getitem_ok(self, inserted_varray: VArray, array_data: ndarray, index_exp: Slice):
        """Tests subset creation from array __getitem__.

        :param inserted_varray: Pre created VArray
        :param array_data: Data of array
        :param index_exp: slicing params list
        """
        subset = inserted_varray[index_exp]
        assert subset
        data = subset.read()
        assert (data == array_data[index_exp]).all()

    def test_varray_as_dict(self, varray: VArray):
        array_dict = {
            "id": varray.id,
            "collection": varray.collection,
            "dimensions": tuple(dim.as_dict for dim in varray.dimensions),
            "shape": varray.shape,
            "named_shape": varray.named_shape,
            "primary_attributes": varray.primary_attributes,
            "custom_attributes": varray.custom_attributes,
            "vgrid": varray.vgrid,
        }
        assert varray.as_dict == array_dict

    @pytest.mark.parametrize(
        "id_",
        [
            True,
            False,
            1,
            -1,
            0.1,
            -0.1,
            complex(0000000000.1),
            complex(-0000000000.1),
            "",
            " ",
            "         ",
            string.whitespace,
            string.printable,
            "a",
            [],
            ["a"],
            tuple(),
            tuple(
                "a",
            ),
            set(),
            {"a"},
            dict(),
            dict(collection="a"),
            "c9b83915",
            "c9b83915-95e7-bf4a-65a7cf806edf",
            "c9b83915-95e7-5b8e-bf4a-bf4a-65a7cf806edf",
            "c9b83915:95e7:5b8e:bf4a:65a7cf806edf",
        ],
    )
    def test_array_raises_on_invalid_id(
        self,
        varray_collection: Collection,
        factory,
        id_: Any,
    ):
        varray_adapter = factory.get_varray_adapter(varray_collection.path, storage_adapter=HDF5StorageAdapter)
        array_adapter = factory.get_array_adapter(varray_collection.path, HDF5StorageAdapter, varray_collection.options)
        with pytest.raises(DekerValidationError):
            assert VArray(varray_collection, varray_adapter, array_adapter, id_=id_)

    def test_varray_str(self, varray: VArray, varray_with_attributes: VArray):
        arrays = (varray, varray_with_attributes)
        for ar in arrays:
            s = f"{ar.__class__.__name__}({ar.id!r}"  # noqa: F841  # the test asserts nothing

    def test_varray_repr(self, varray: VArray, varray_with_attributes: VArray):
        arrays = (varray, varray_with_attributes)
        for ar in arrays:
            s = f"{ar.__class__.__name__}(id={ar.id!r}, collection={ar.collection!r})"
            if ar.primary_attributes:
                s = s[:-1] + f", primary_attributes={ar.primary_attributes!r})"
            if ar.custom_attributes:
                s = s[:-1] + f", custom_attributes={ar.custom_attributes!r})"
            assert repr(ar) == s

    @pytest.mark.parametrize("index_exp", invalid_step_index_exp_params)
    def test_step_validator(self, varray: VArray, index_exp):
        """Tests if a zero or a boolean step is rejected."""
        with pytest.raises(IndexError):
            varray[index_exp]

    def test_create_from_meta_ordered(
        self,
        varray_collection_with_attributes: Collection,
        local_array_adapter: LocalArrayAdapter,
        local_varray_adapter: LocalVArrayAdapter,
        varray_with_attributes: VArray,
    ):
        meta: ArrayMeta = varray_with_attributes.as_dict

        primary_attribute_keys = list(meta["primary_attributes"].keys())
        shuffle(primary_attribute_keys)

        custom_attribute_keys = list(meta["custom_attributes"].keys())
        shuffle(custom_attribute_keys)

        primary_attributes, custom_attributes = {}, {}
        for key in primary_attribute_keys:
            primary_attributes[key] = meta["primary_attributes"][key]

        for key in custom_attribute_keys:
            custom_attributes[key] = meta["custom_attributes"][key]

        meta["primary_attributes"] = primary_attributes
        meta["custom_attributes"] = custom_attributes

        array = VArray._create_from_meta(
            varray_collection_with_attributes,
            meta=meta,
            array_adapter=local_array_adapter,
            varray_adapter=local_varray_adapter,
        )
        assert array.primary_attributes == varray_with_attributes.primary_attributes
        assert array.custom_attributes == varray_with_attributes.custom_attributes


# Described dimensions used by the step tests, split into Arrays along every axis
STEP_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
STEP_LABELS = ["a", "b", "c", "d", "e"]


def _step_dimensions():
    """Dimensions of the varray for the step tests: time (24 x 1h), scale down (10 x -0.5), scale up (10 x 0.5), labels.

    :returns: dimension schemas
    """
    return [
        TimeDimensionSchema(name="t", size=24, start_value=STEP_START, step=timedelta(hours=1)),
        DimensionSchema(name="y", size=10, scale=Scale(start_value=90.0, step=-0.5)),
        DimensionSchema(name="x", size=10, scale=Scale(start_value=-180.0, step=0.5)),
        DimensionSchema(name="layers", size=5, labels=STEP_LABELS),
    ]


# Fancy steps of the described varray and the plain integer slices they convert to
fancy_step_params = [
    pytest.param(
        np.index_exp[STEP_START + timedelta(hours=2) : STEP_START + timedelta(hours=10) : timedelta(hours=2)],
        (slice(2, 10, 2),),
        id="time-forward-2h",
    ),
    pytest.param(
        np.index_exp[STEP_START + timedelta(hours=10) : STEP_START + timedelta(hours=2) : -timedelta(hours=2)],
        (slice(10, 2, -2),),
        id="time-backward-2h",
    ),
    pytest.param(np.index_exp[:, 89.0:86.0:-1.0], (slice(None), slice(2, 8, 2)), id="scale-down-against-the-scale"),
    pytest.param(np.index_exp[:, 86.0:89.0:1.0], (slice(None), slice(8, 2, -2)), id="scale-down-along-the-scale"),
    pytest.param(
        np.index_exp[:, :, -176.0:-179.0:-1.5], (slice(None), slice(None), slice(8, 2, -3)), id="scale-up-backward"
    ),
    pytest.param(
        np.index_exp[:, :, :, "e":"a":-1],
        (slice(None), slice(None), slice(None), slice(4, 0, -1)),
        id="labels-backward",
    ),
    pytest.param(
        np.index_exp[STEP_START + timedelta(hours=3) :: timedelta(hours=4), 89.0::-1.0, ::3, "b"::2],
        (slice(3, None, 4), slice(2, None, 2), slice(None, None, 3), slice(1, None, 2)),
        id="every-axis-at-once",
    ),
]

invalid_fancy_step_params = [
    pytest.param(np.index_exp[:: timedelta(minutes=30)], id="time-step-not-a-multiple"),
    pytest.param(np.index_exp[:: timedelta(0)], id="time-zero-step"),
    pytest.param(np.index_exp[::1.0], id="time-float-step"),
    pytest.param(np.index_exp[timedelta(hours=1) :: 2], id="time-timedelta-start"),
    pytest.param(np.index_exp[:, ::0.3], id="scale-step-not-a-multiple"),
    pytest.param(np.index_exp[:, ::0.0], id="scale-zero-step"),
    pytest.param(np.index_exp[:, :: timedelta(hours=1)], id="scale-timedelta-step"),
    pytest.param(np.index_exp[:, :, :, ::1.0], id="labels-float-step"),
    pytest.param(np.index_exp[:, :, :, ::"b"], id="labels-string-step"),
    pytest.param(np.index_exp[:, 89.0:86.0:1.0], id="scale-step-walks-away-from-stop"),
]


class TestVArrayStepIndexing:
    """Steps in the index expressions of a VArray convert exactly as for an Array, whatever the grid."""

    @pytest.fixture(
        scope="class", params=[(1, 1, 1, 1), (4, 2, 5, 1), (3, 5, 2, 5)], ids=lambda v: "x".join(map(str, v))
    )
    def described_varray(self, request, client: Client) -> VArray:
        vgrid = request.param
        schema = VArraySchema(dtype=float, dimensions=_step_dimensions(), vgrid=vgrid)
        collection = client.create_collection(f"varray_steps_{'_'.join(map(str, vgrid))}", schema)
        try:
            varray = collection.create()
            varray[:].update(np.arange(24 * 10 * 10 * 5, dtype=float).reshape(24, 10, 10, 5))
            yield varray
        finally:
            collection.delete()

    @pytest.mark.parametrize(("index_exp", "bounds"), fancy_step_params)
    def test_fancy_steps_convert_to_integer_slices(self, described_varray: VArray, index_exp: FancySlice, bounds):
        subset = described_varray[index_exp]
        assert subset.bounds == bounds
        expected = described_varray[:].read()[bounds]
        assert subset.shape == expected.shape
        assert np.array_equal(subset.read(), expected)

    @pytest.mark.parametrize("index_exp", invalid_fancy_step_params)
    def test_steps_of_a_wrong_kind_raise(self, described_varray: VArray, index_exp: FancySlice):
        with pytest.raises(IndexError):
            described_varray[index_exp]

    @pytest.mark.parametrize("index_exp", invalid_step_index_exp_params)
    def test_zero_and_boolean_steps_raise(self, described_varray: VArray, index_exp: Slice):
        with pytest.raises(IndexError):
            described_varray[index_exp]


if __name__ == "__main__":
    pytest.main()

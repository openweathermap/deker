import gc
import logging
import os
import string
import sys
import threading
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import shuffle
from typing import Any

import deker_local_adapters.collection_adapter
import deker_local_adapters.mixin
import numpy as np
import pytest

from cantok import AbstractToken, SimpleToken
from deker_local_adapters import HDF5StorageAdapter, LocalArrayAdapter, LocalVArrayAdapter
from deker_local_adapters.factory import AdaptersFactory
from deker_tools.cancel import cancellation_scope
from deker_tools.typings import Sentinel
from numpy import ndarray
from pytest_mock import MockerFixture

from tests.parameters.array_params import attributes_validation_params
from tests.parameters.index_exp_params import (
    invalid_index_params,
    invalid_step_index_exp_params,
    valid_index_exp_params,
)
from tests.parameters.uri import embedded_uri

from deker.arrays import Array
from deker.client import Client
from deker.collection import Collection
from deker.dimensions import TimeDimension
from deker.errors import (
    DekerCancelledError,
    DekerCollectionAlreadyExistsError,
    DekerCriticalError,
    DekerInstanceNotExistsError,
    DekerMemoryError,
    DekerTrashError,
    DekerValidationError,
)
from deker.flock import Flock
from deker.schemas import ArraySchema, AttributeSchema, DimensionSchema, TimeDimensionSchema, VArraySchema
from deker.tools import get_paths
from deker.tools.schema import create_dimensions
from deker.types import ArrayMeta
from deker.types.private.typings import FancySlice, NumericDtypes, Slice
from deker.types.public.classes import Scale


@pytest.mark.asyncio
class TestArrayMethods:
    @pytest.mark.parametrize("array_init_params", attributes_validation_params())
    def test_array_raises_on_validate_attributes(
        self,
        array_init_params: dict,
        factory: AdaptersFactory,
        array_collection_with_attributes,
    ):
        """Tests attributes validation on Array creation.

        :param array_init_params: parameters for Array creation
        :param factory: fixture
        :param array_collection_with_attributes: fixture
        """
        array_init_params["adapter"] = factory.get_array_adapter(
            array_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter
        )
        array_init_params["collection"] = array_collection_with_attributes
        with pytest.raises(DekerValidationError):  # type: ignore[call-overload]
            Array(**array_init_params)

    def test_array_init(
        self,
        factory: AdaptersFactory,
        array_collection_with_attributes: Collection,
    ):
        assert Array(
            array_collection_with_attributes,
            factory.get_array_adapter(array_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter),
            primary_attributes={"primary_attribute": 3},
            custom_attributes={
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        )

    @pytest.mark.parametrize(
        "collection",
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
        collection: Any,
    ):
        with pytest.raises(AttributeError):
            assert Array(
                collection,
                factory.get_array_adapter(Path("path"), storage_adapter=HDF5StorageAdapter),
                primary_attributes={"primary_attribute": 3},
                custom_attributes={
                    "custom_attribute": 0.5,
                    "time_attr_name": datetime.now(timezone.utc),
                },
            )

    def test_array_shapes(
        self,
        factory: AdaptersFactory,
        array_collection_with_attributes: Collection,
    ):
        array = Array(
            array_collection_with_attributes,
            factory.get_array_adapter(array_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter),
            primary_attributes={"primary_attribute": 3},
            custom_attributes={
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        )
        assert isinstance(array.shape, tuple)
        assert array.shape == tuple([dim.size for dim in array_collection_with_attributes.array_schema.dimensions])
        assert array.shape == array_collection_with_attributes.array_schema.shape
        assert isinstance(array.named_shape, tuple)
        assert array.named_shape == tuple(
            (dim.name, dim.size) for dim in array_collection_with_attributes.array_schema.dimensions
        )

    def test_collection_property(
        self,
        factory: AdaptersFactory,
        array_collection_with_attributes,
    ):
        array = Array(
            array_collection_with_attributes,
            factory.get_array_adapter(array_collection_with_attributes.path, storage_adapter=HDF5StorageAdapter),
            primary_attributes={"primary_attribute": 3},
            custom_attributes={
                "custom_attribute": 0.5,
                "time_attr_name": datetime.now(timezone.utc),
            },
        )
        assert isinstance(array.collection, str)
        assert array.collection == array_collection_with_attributes.name

    @pytest.mark.parametrize("array_", ["inserted_array", "inserted_varray"])
    def test_private_collection_property(self, array_: str, request):
        """The adapters reach the collection object of an array, not only its name: its schemas, for one."""
        array = request.getfixturevalue(array_)
        collection = request.getfixturevalue("varray_collection" if array_ == "inserted_varray" else "array_collection")
        assert array._collection is collection

    def test_delete_array(self, root_path, inserted_array: Array, ctx):
        """Tests array delete method.

        :param root_path: temporary collection root path
        :param inserted_array: Array object
        """
        paths = get_paths(
            inserted_array,
            root_path / ctx.config.collections_directory / inserted_array.collection,
        )
        filename = paths.main / (inserted_array.id + ctx.storage_adapter.file_ext)
        assert filename.exists()
        inserted_array.delete()
        assert not os.path.exists(filename)

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_update_array(self, inserted_array: Array, array_data: np.ndarray, index_exp):
        """Tests array update method.

        :param inserted_array: Array object
        :param array_data: Data to update array with
        """
        old_data = inserted_array[:].read()
        assert (old_data == array_data).all()
        len_items = len(old_data.flatten())

        update = np.asarray(range(len_items, len_items + 1000), dtype=inserted_array.dtype)
        update = update.reshape(inserted_array.shape)
        update = update[index_exp]

        inserted_array[index_exp].update(update)
        data = inserted_array[index_exp].read()
        assert (data == update).all()

    def test_dimensions_are_built_once(self, inserted_array_with_attributes: Array):
        """The dimensions of an array are built on the first access and then reused."""
        first = inserted_array_with_attributes.dimensions
        assert inserted_array_with_attributes.dimensions is first
        fresh = create_dimensions(
            inserted_array_with_attributes.schema.dimensions,
            {**inserted_array_with_attributes.primary_attributes, **inserted_array_with_attributes.custom_attributes},
        )
        assert [d.as_dict for d in first] == [d.as_dict for d in fresh]

    def test_dimensions_are_rebuilt_after_an_update_of_the_custom_attributes(
        self, inserted_array_with_attributes: Array
    ):
        """A time dimension starting at a custom attribute follows the attribute after an update."""
        before = inserted_array_with_attributes.dimensions
        new_start = datetime(2030, 1, 1, tzinfo=timezone.utc)
        inserted_array_with_attributes.update_custom_attributes({"time_attr_name": new_start})
        after = inserted_array_with_attributes.dimensions
        assert after is not before
        assert [d.start_value for d in after if isinstance(d, TimeDimension)] == [new_start]
        assert [d.start_value for d in before if isinstance(d, TimeDimension)] != [new_start]

    def test_update_custom_attributes(self, inserted_array_with_attributes: Array):
        """Tests array update_custom_attributes method.

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

    @pytest.mark.parametrize("dtype", [datetime, str, tuple] + NumericDtypes)
    def test_custom_attributes_set_to_none(self, client: Client, dtype: object):
        """Tests array is created with custom attribute set to None.

        :param client: Deker Client instance
        """
        schema = ArraySchema(
            dimensions=[DimensionSchema(name="x", size=1)],
            dtype=int,
            attributes=[AttributeSchema(name="some_attr", dtype=dtype, primary=False)],
        )
        col_name = "test_custom_attrs_values_are_None"
        try:
            col = client.create_collection(col_name, schema)
        except DekerCollectionAlreadyExistsError:
            col = client.get_collection(col_name)
            col.clear()
        try:
            key = "custom_attributes"
            attrs = {key: {schema.attributes[0].name: None}}
            array = col.create(**attrs)
            assert array
            attr = getattr(array, key)
            assert attr[schema.attributes[0].name] is None
        except Exception:
            raise
        finally:
            col.delete()

    @pytest.mark.parametrize("dtype", NumericDtypes + [str, tuple, datetime])
    def test_custom_attributes_updated_to_none(self, client: Client, dtype: object):
        """Tests array custom attribute is updated to None.

        :param client: Deker Client instance
        """
        schema = ArraySchema(
            dimensions=[DimensionSchema(name="x", size=1)],
            dtype=int,
            attributes=[AttributeSchema(name="some_attr", dtype=dtype, primary=False)],
        )
        col_name = "test_custom_attrs_values_are_None"
        try:
            col = client.create_collection(col_name, schema)
        except DekerCollectionAlreadyExistsError:
            col = client.get_collection(col_name)
            col.clear()

        try:
            key = "custom_attributes"
            if dtype is tuple:
                val = (1,)
            elif dtype is datetime:
                val = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
            else:
                val = dtype(1)  # type: ignore
            attrs = {key: {schema.attributes[0].name: val}}
            array = col.create(**attrs)
            assert array
            attr = getattr(array, key)
            assert attr[schema.attributes[0].name] == val
            array.update_custom_attributes({schema.attributes[0].name: None})
            attr = getattr(array, key)
            assert attr[schema.attributes[0].name] is None
        except Exception:
            raise
        finally:
            col.delete()

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
    def test_update_custom_attributes_fail(self, inserted_array_with_attributes: Array, new_custom_attributes: dict):
        """Tests array update method.

        :param inserted_array_with_attributes: Array object
        :param new_custom_attributes: Data to update array custom attributes
        """
        with pytest.raises(DekerValidationError):  # type: ignore[call-overload]
            inserted_array_with_attributes.update_custom_attributes(new_custom_attributes)

    @pytest.mark.parametrize(
        ("new_custom_attributes", "message"),
        [
            pytest.param(
                {"zzz": 1},
                "Setting additional attributes not listed in schema is not allowed. Invalid attributes: ['zzz']",
                id="unknown",
            ),
            pytest.param(
                {"zzz": 1, "custom_attribute": 0.7},
                "Setting additional attributes not listed in schema is not allowed. Invalid attributes: ['zzz']",
                id="unknown_and_custom",
            ),
            pytest.param(
                {"primary_attribute": 5},
                "Key and custom attributes shall not have same names; invalid names: {'primary_attribute'}",
                id="primary",
            ),
            pytest.param(
                {"primary_attribute": 5, "custom_attribute": 0.7},
                "Key and custom attributes shall not have same names; invalid names: {'primary_attribute'}",
                id="primary_and_custom",
            ),
        ],
    )
    def test_update_custom_attributes_with_a_name_out_of_the_custom_ones_fails(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        new_custom_attributes: dict,
        message: str,
    ):
        """An update with a name that is not a custom attribute of the schema changes nothing.

        A primary name fails as a name both primary and custom: the update checks the given attributes as the custom
        ones against the primary attributes of the Array.
        """
        before = dict(inserted_array_with_attributes.custom_attributes)
        with pytest.raises(DekerValidationError) as error:
            # a copy: the parameters are shared by the runs
            inserted_array_with_attributes.update_custom_attributes(dict(new_custom_attributes))
        assert str(error.value) == message
        assert inserted_array_with_attributes.custom_attributes == before
        assert inserted_array_with_attributes.primary_attributes == {"primary_attribute": 3}
        stored = array_collection_with_attributes.filter({"primary_attribute": 3}).last()
        assert stored is not None
        assert stored.id == inserted_array_with_attributes.id
        assert stored.primary_attributes == {"primary_attribute": 3}
        assert stored.custom_attributes == before

    @pytest.mark.parametrize(
        ("new_custom_attributes", "skipped_attr"),
        [
            ({"custom_attribute": 0.7}, "time_attr_name"),
            ({"custom_attribute": None}, "time_attr_name"),
            ({"time_attr_name": datetime.now(timezone.utc)}, "custom_attribute"),
        ],
    )
    def test_update_custom_attributes_update_just_passed_attrs(
        self,
        array_collection_with_attributes: Collection,
        inserted_array_with_attributes: Array,
        new_custom_attributes: dict,
        skipped_attr: str,
    ):
        old_attrs = inserted_array_with_attributes.custom_attributes
        expected = {**old_attrs, **new_custom_attributes}
        new_attr_name = list(new_custom_attributes.keys())[0]
        inserted_array_with_attributes.update_custom_attributes(new_custom_attributes)
        assert inserted_array_with_attributes.custom_attributes[new_attr_name] == new_custom_attributes[new_attr_name]
        assert old_attrs[skipped_attr] == inserted_array_with_attributes.custom_attributes[skipped_attr]
        assert inserted_array_with_attributes.custom_attributes == expected
        stored = array_collection_with_attributes.filter({"primary_attribute": 3}).last()
        assert stored is not None
        assert stored.custom_attributes == expected

    @pytest.mark.parametrize("index_exp", invalid_index_params)
    def test_array_getitem_raises(self, inserted_array: Array, index_exp: Slice):
        """Test array raise IndexError on invalid slice expression (None, True, False, too many indexes).

        :param inserted_array: Pre created array
        :param index_exp: slicing params list
        """
        with pytest.raises(IndexError):
            assert inserted_array[index_exp]

    @pytest.mark.parametrize(
        "index_exp",
        [10, -11, (10,), (0, 10), (0, 0, 10), (10, 0), (-11, 0), (0, -11, 0), (..., 10), (..., -11), (0, ..., 10), 11],
    )
    def test_array_getitem_raises_on_a_position_out_of_range(self, inserted_array: Array, index_exp):
        """An integer index beyond the axis raises IndexError, as it does in numpy; nothing is read as fill values.

        :param inserted_array: Pre created array of shape (10, 10, 10)
        :param index_exp: index expression with a position out of range
        """
        with pytest.raises(IndexError, match="out of range"):
            inserted_array[index_exp]

    @pytest.mark.parametrize(
        ("index_exp", "shape"),
        [
            (9, (10, 10)),
            (-10, (10, 10)),
            (0, (10, 10)),
            ((0, 9), (10,)),
            ((-10, -10, -10), ()),
            ((..., 9), (10, 10)),
            ((9, ..., -10), (10,)),
        ],
    )
    def test_array_getitem_accepts_the_positions_within_range(self, inserted_array: Array, index_exp, shape: tuple):
        """Every position from -size to size - 1 is accepted, on every axis, with and without an ellipsis.

        :param inserted_array: Pre created array of shape (10, 10, 10)
        :param index_exp: index expression within the range
        :param shape: shape of the subset
        """
        assert inserted_array[index_exp].shape == shape

    def test_array_getitem_raises_memory_error(self, client, root_path):
        """Test array raise MemoryError on too big slice expression and too low memory limit."""
        schema = ArraySchema(
            dimensions=[
                DimensionSchema(name="x", size=10000),
                DimensionSchema(name="y", size=10000),
            ],
            dtype=float,
        )
        col_name = "memory_excess"
        with Client(embedded_uri(root_path), memory_limit=100, loglevel="CRITICAL") as extra_client:
            big_collection = client.create_collection(col_name, schema)
            big_collection.create()

            collection = extra_client.get_collection(col_name)
            try:
                with pytest.raises(DekerMemoryError):
                    for array in collection:
                        assert array[:]
            finally:
                collection.delete()

    @pytest.mark.parametrize("index_exp", valid_index_exp_params)
    def test_array_getitem_ok(self, inserted_array: Array, array_data: ndarray, index_exp: Slice):
        """Tests subset creation from array __getitem__.

        :param inserted_array: Pre created array
        :param array_data: Data of array
        :param index_exp: slicing params list
        """
        subset = inserted_array[index_exp]
        assert subset
        data = subset.read()
        assert (data == array_data[index_exp]).all()

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
    def test_array_fancy_getitem_ok(self, scaled_array, index_exp: Slice, fancy_index_exp: FancySlice):
        """Tests subset fancy creation from array __getitem__.

        :param scaled_array: Pre created array
        :param index_exp: slicing params list
        :param fancy_index_exp: slicing params list
        """
        fancy_subset = scaled_array[fancy_index_exp]
        assert fancy_subset
        subset = scaled_array[index_exp]
        assert subset
        fancy_data = fancy_subset.read()
        data = subset.read()
        assert (fancy_data == data).all()

    @pytest.mark.parametrize(
        "scale_index_exp",
        [
            # "index",
            # datetime.get_utc(),
            # datetime.get_utc(tz=timezone.utc),
            # 0.1,
            # 0.2,
            # 0.5000001,
            # -0.0000001,
            # 90.5,
            # -90.5,
            # np.index_exp[0.2:-90.0, 0.0:179.5],
            # np.index_exp[0.5:-90.5, 0.0:179.5],
            # np.index_exp[0.5:-90.0, 0.1:179.5],
            # np.index_exp[0.5:-90.0, 0.0:180.0],
            # np.index_exp[0.5:-90.0, 0.0:179.999999999999999],
            # np.index_exp[0.0:-90.0:1.5],
            # np.index_exp[0.0:-90.0:-1],
            # np.index_exp[0.0:-90.0, 0.0:179.5:-1],
            # np.index_exp[0.0:-90.0, 0.0:179.5:2],
            np.index_exp[:, :, "а"],  # cyrillic
            np.index_exp[:, :, "aa"],
            np.index_exp[:, :, " a"],
            np.index_exp[:, :, "a "],
            np.index_exp[:, :, " a "],
            np.index_exp[:, :, "a":"z"],
            # np.index_exp[:, :, 0.1],
            # np.index_exp[:, :, -0.1],
        ],
    )
    def test_array_scale_getitem_raises(self, scaled_array: Array, scale_index_exp: FancySlice):
        """Tests subset raises errors from array __getitem__.

        :param scaled_array: Pre created array
        :param scale_index_exp: slicing params list
        """
        with pytest.raises(IndexError):
            scaled_array[scale_index_exp]

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
    def test_timed_array_fancy_getitem_ok(self, timed_array, index_exp: Slice, fancy_index_exp: FancySlice):
        """Tests subset fancy creation from array __getitem__.

        :param timed_array: Pre created array
        :param index_exp: slicing params list
        :param fancy_index_exp: slicing params list
        """
        fancy_subset = timed_array[fancy_index_exp]
        assert fancy_subset
        subset = timed_array[index_exp]
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
    def test_array_time_getitem_raises(self, timed_array: Array, scale_index_exp: FancySlice):
        """Tests subset raises errors from array __getitem__.

        :param timed_array: Pre created array
        :param scale_index_exp: slicing params list
        """
        with pytest.raises(IndexError):
            timed_array[scale_index_exp]

    def test_array_as_dict(self, array: Array):
        array_dict = {
            "id": array.id,
            "collection": array.collection,
            "dimensions": tuple(dim.as_dict for dim in array.dimensions),
            "shape": array.shape,
            "named_shape": array.named_shape,
            "primary_attributes": array.primary_attributes,
            "custom_attributes": array.custom_attributes,
        }
        assert array.as_dict == array_dict

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
    def test_array_raises_on_invalid_id(self, array_collection: Collection, factory, id_):
        with pytest.raises(DekerValidationError):
            assert Array(
                array_collection,
                factory.get_array_adapter(array_collection.path, storage_adapter=HDF5StorageAdapter),
                id_=id_,
            )

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"id_": "not-a-uuid"}, id="invalid-id"),
            pytest.param({"primary_attributes": "not-a-dict"}, id="invalid-primary-attributes"),
            pytest.param({"custom_attributes": ["not-a-dict"]}, id="invalid-custom-attributes"),
        ],
    )
    def test_array_rejected_by_validation_is_dropped_silently(self, mocker: MockerFixture, kwargs: dict):
        """Tests that an Array rejected by the validation is destroyed without an error in its finalizer."""
        gc.collect()  # the garbage of other tests is not counted
        hook = mocker.patch.object(sys, "unraisablehook")
        with pytest.raises(DekerValidationError) as excinfo:
            Array(None, None, **kwargs)  # type: ignore[arg-type]
        del excinfo
        gc.collect()
        assert [call.args[0].exc_value for call in hook.call_args_list] == []

    @pytest.mark.parametrize("id_", [Sentinel, "c9b83915-95e7-5b8e-bf4a-65a7cf806edf"])
    def test_valid_array_is_dropped_silently(
        self,
        mocker: MockerFixture,
        array_collection: Collection,
        factory: AdaptersFactory,
        id_: Any,
    ):
        """Tests that a valid Array is destroyed without an error in its finalizer."""
        gc.collect()  # the garbage of other tests is not counted
        hook = mocker.patch.object(sys, "unraisablehook")
        array = Array(
            array_collection,
            factory.get_array_adapter(array_collection.path, storage_adapter=HDF5StorageAdapter),
            id_=id_,
        )
        assert array.id
        del array
        gc.collect()
        assert [call.args[0].exc_value for call in hook.call_args_list] == []

    def test_array_str(self, array: Array, array_with_attributes: Array):
        arrays = (array, array_with_attributes)
        for ar in arrays:
            s = f"{ar.__class__.__name__}({ar.id!r}"  # noqa: F841  # the test asserts nothing

    def test_array_repr(self, array: Array, array_with_attributes: Array):
        arrays = (array, array_with_attributes)
        for ar in arrays:
            s = f"{ar.__class__.__name__}(id={ar.id!r}, collection={ar.collection!r})"
            if ar.primary_attributes:
                s = s[:-1] + f", primary_attributes={ar.primary_attributes!r})"
            if ar.custom_attributes:
                s = s[:-1] + f", custom_attributes={ar.custom_attributes!r})"
            assert repr(ar) == s

    @pytest.mark.parametrize("index_exp", invalid_step_index_exp_params)
    def test_step_validator(self, array: Array, index_exp):
        """Tests if a zero or a boolean step is rejected."""
        with pytest.raises(IndexError):
            array[index_exp]

    def test_create_from_meta_ordered(
        self,
        array_collection_with_attributes: Collection,
        local_array_adapter: LocalArrayAdapter,
        local_varray_adapter: LocalVArrayAdapter,
        array_with_attributes: Array,
    ):
        meta: ArrayMeta = array_with_attributes.as_dict

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

        array = Array._create_from_meta(
            array_collection_with_attributes,
            meta=meta,
            array_adapter=local_array_adapter,
            varray_adapter=None,
        )
        assert array.primary_attributes == array_with_attributes.primary_attributes
        assert array.custom_attributes == array_with_attributes.custom_attributes


# Described dimensions used by the step tests: every axis has its own kind of fancy values
STEP_START = datetime(2024, 1, 1, tzinfo=timezone.utc)
STEP_LABELS = ["a", "b", "c", "d", "e"]


def _step_dimensions():
    """Dimensions of the array for the step tests: time (24 x 1h), scale down (10 x -0.5), scale up (10 x 0.5), labels.

    :returns: dimension schemas
    """
    return [
        TimeDimensionSchema(name="t", size=24, start_value=STEP_START, step=timedelta(hours=1)),
        DimensionSchema(name="y", size=10, scale=Scale(start_value=90.0, step=-0.5)),
        DimensionSchema(name="x", size=10, scale=Scale(start_value=-180.0, step=0.5)),
        DimensionSchema(name="layers", size=5, labels=STEP_LABELS),
    ]


# Fancy steps of the described array and the plain integer slices they convert to
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
    pytest.param(
        np.index_exp[STEP_START + timedelta(hours=1) : STEP_START + timedelta(hours=23) : 3],
        (slice(1, 23, 3),),
        id="time-int-step",
    ),
    pytest.param(
        np.index_exp[:, 89.0:86.0:-1.0],
        (slice(None), slice(2, 8, 2)),
        id="scale-down-value-step-against-the-scale",
    ),
    pytest.param(
        np.index_exp[:, 86.0:89.0:1.0],
        (slice(None), slice(8, 2, -2)),
        id="scale-down-value-step-along-the-scale",
    ),
    pytest.param(
        np.index_exp[:, :, -179.0:-176.0:1.0],
        (slice(None), slice(None), slice(2, 8, 2)),
        id="scale-up-value-step",
    ),
    pytest.param(
        np.index_exp[:, :, -176.0:-179.0:-1.5],
        (slice(None), slice(None), slice(8, 2, -3)),
        id="scale-up-negative-value-step",
    ),
    pytest.param(
        np.index_exp[:, :, :, "e":"a":-1],
        (slice(None), slice(None), slice(None), slice(4, 0, -1)),
        id="labels-backward",
    ),
    pytest.param(
        np.index_exp[:, :, :, "a":"e":2],
        (slice(None), slice(None), slice(None), slice(0, 4, 2)),
        id="labels-every-other",
    ),
    pytest.param(
        np.index_exp[STEP_START + timedelta(hours=3) :: timedelta(hours=4), 89.0::-1.0, ::3, "b"::2],
        (slice(3, None, 4), slice(2, None, 2), slice(None, None, 3), slice(1, None, 2)),
        id="every-axis-at-once",
    ),
    # the steps 3, -2 and -3 in the units of every axis, without bounds: hours on the time axis, scale units on the
    # scales (the sign is relative to the direction of the scale: on the scale down -1.0 walks up the positions),
    # positions on the labels, and the four at once
    pytest.param(np.index_exp[:: timedelta(hours=3)], (slice(None, None, 3),), id="time-3h"),
    pytest.param(np.index_exp[:: -timedelta(hours=2)], (slice(None, None, -2),), id="time-minus-2h"),
    pytest.param(np.index_exp[:: -timedelta(hours=3)], (slice(None, None, -3),), id="time-minus-3h"),
    pytest.param(np.index_exp[:, ::1.5], (slice(None), slice(None, None, -3)), id="scale-down-1.5"),
    pytest.param(np.index_exp[:, ::-1.0], (slice(None), slice(None, None, 2)), id="scale-down-minus-1.0"),
    pytest.param(np.index_exp[:, ::-1.5], (slice(None), slice(None, None, 3)), id="scale-down-minus-1.5"),
    pytest.param(np.index_exp[:, :, ::1.5], (slice(None), slice(None), slice(None, None, 3)), id="scale-up-1.5"),
    pytest.param(
        np.index_exp[:, :, ::-1.0], (slice(None), slice(None), slice(None, None, -2)), id="scale-up-minus-1.0"
    ),
    pytest.param(
        np.index_exp[:, :, ::-1.5], (slice(None), slice(None), slice(None, None, -3)), id="scale-up-minus-1.5"
    ),
    pytest.param(
        np.index_exp[:, :, :, ::3], (slice(None), slice(None), slice(None), slice(None, None, 3)), id="labels-3"
    ),
    pytest.param(
        np.index_exp[:, :, :, ::-2], (slice(None), slice(None), slice(None), slice(None, None, -2)), id="labels-minus-2"
    ),
    pytest.param(
        np.index_exp[:, :, :, ::-3], (slice(None), slice(None), slice(None), slice(None, None, -3)), id="labels-minus-3"
    ),
    pytest.param(
        np.index_exp[:: -timedelta(hours=3), ::-1.0, ::1.5, ::-2],
        (slice(None, None, -3), slice(None, None, 2), slice(None, None, 3), slice(None, None, -2)),
        id="every-axis-3-minus-2-minus-3",
    ),
]

# Integer positions with a fancy step: the integers are positions, as in numpy, the step converts
fancy_step_with_int_position_params = [
    pytest.param(np.index_exp[2 :: timedelta(hours=2)], (slice(2, None, 2),), id="time-int-start"),
    pytest.param(np.index_exp[: 10 : timedelta(hours=3)], (slice(None, 10, 3),), id="time-int-stop"),
    pytest.param(np.index_exp[:, :, 2::1.0], (slice(None), slice(None), slice(2, None, 2)), id="scale-int-start"),
    pytest.param(np.index_exp[:, :, :8:1.0], (slice(None), slice(None), slice(None, 8, 2)), id="scale-int-stop"),
    pytest.param(np.index_exp[:, 2::1.0], (slice(None), slice(2, None, -2)), id="scale-down-int-start"),
    pytest.param(np.index_exp[-5 :: timedelta(hours=1)], (slice(-5, None, 1),), id="time-negative-int-start"),
    pytest.param(np.index_exp[: -2 : timedelta(hours=2)], (slice(None, -2, 2),), id="time-negative-int-stop"),
    pytest.param(np.index_exp[:, :, -4::1.0], (slice(None), slice(None), slice(-4, None, 2)), id="scale-negative"),
]

# Steps that do not fit the kind of the axis
invalid_fancy_step_params = [
    pytest.param(np.index_exp[:, :, :, "a":"d":0], id="labels-zero-step"),
    pytest.param(np.index_exp[:, :, :, "a":"d":True], id="labels-boolean-step"),
    pytest.param(np.index_exp[STEP_START : STEP_START + timedelta(hours=5) : 0], id="time-zero-step-with-datetimes"),
    pytest.param(np.index_exp[STEP_START : STEP_START + timedelta(hours=5) : True], id="time-boolean-step"),
    pytest.param(np.index_exp[b"2024-01-01":], id="time-bytes-start"),
    pytest.param(np.index_exp[:, b"90.0":], id="scale-bytes-start"),
    pytest.param(np.index_exp[:, :, :, b"a":], id="labels-bytes-start"),
    pytest.param(np.index_exp[:, :, :, (1, 2) :], id="labels-tuple-start"),
    pytest.param(np.index_exp[:: timedelta(minutes=30)], id="time-step-not-a-multiple"),
    pytest.param(np.index_exp[:: timedelta(0)], id="time-zero-step"),
    pytest.param(np.index_exp[::1.0], id="time-float-step"),
    pytest.param(np.index_exp[::"a"], id="time-string-step"),
    pytest.param(np.index_exp[timedelta(hours=1) :: 2], id="time-timedelta-start"),
    pytest.param(np.index_exp[: timedelta(hours=1) : 2], id="time-timedelta-stop"),
    pytest.param(np.index_exp[:, ::0.3], id="scale-step-not-a-multiple"),
    pytest.param(np.index_exp[:, ::0.0], id="scale-zero-step"),
    pytest.param(np.index_exp[:, :: timedelta(hours=1)], id="scale-timedelta-step"),
    pytest.param(np.index_exp[:, ::"a"], id="scale-string-step"),
    pytest.param(np.index_exp[:, :, :, ::1.0], id="labels-float-step"),
    pytest.param(np.index_exp[:, :, :, ::"b"], id="labels-string-step"),
    pytest.param(np.index_exp[:, :, :, :: timedelta(hours=1)], id="labels-timedelta-step"),
    pytest.param(np.index_exp[:, 89.0:86.0:1.0], id="scale-step-walks-away-from-stop"),
    pytest.param(
        np.index_exp[STEP_START + timedelta(hours=2) : STEP_START + timedelta(hours=10) : -2],
        id="time-step-walks-away-from-stop",
    ),
]


class TestArrayStepIndexing:
    """Steps in the index expressions of an Array: integers everywhere, timedelta on time axes, floats on scales."""

    @pytest.fixture(scope="class")
    def described_array(self, client: Client) -> Array:
        collection = client.create_collection("array_steps", ArraySchema(dtype=float, dimensions=_step_dimensions()))
        try:
            array = collection.create()
            array[:].update(np.arange(24 * 10 * 10 * 5, dtype=float).reshape(24, 10, 10, 5))
            yield array
        finally:
            collection.delete()

    @pytest.mark.parametrize(("index_exp", "bounds"), fancy_step_params)
    def test_fancy_steps_convert_to_integer_slices(self, described_array: Array, index_exp: FancySlice, bounds: tuple):
        """Tests if a timedelta step on a time axis, a float step on a scale and a string slice on labels convert."""
        subset = described_array[index_exp]
        assert subset.bounds == bounds
        expected = described_array[:].read()[bounds]
        assert subset.shape == expected.shape
        assert np.array_equal(subset.read(), expected)

    @pytest.mark.parametrize(("index_exp", "bounds"), fancy_step_with_int_position_params)
    def test_int_positions_with_a_fancy_step(self, described_array: Array, index_exp: FancySlice, bounds: tuple):
        """Tests if integer start/stop next to a timedelta or float step stay positions and the step converts."""
        subset = described_array[index_exp]
        assert subset.bounds == bounds
        expected = described_array[:].read()[bounds]
        assert subset.shape == expected.shape
        assert np.array_equal(subset.read(), expected)

    @pytest.mark.parametrize("index_exp", invalid_fancy_step_params)
    def test_steps_of_a_wrong_kind_raise(self, described_array: Array, index_exp: FancySlice):
        """Tests if a step that does not fit the axis, or walks away from its stop, is rejected."""
        with pytest.raises(IndexError):
            described_array[index_exp]

    @pytest.mark.parametrize("index_exp", invalid_step_index_exp_params)
    def test_zero_and_boolean_steps_raise(self, described_array: Array, index_exp: Slice):
        """Tests if a zero or a boolean step is rejected on a described array too."""
        with pytest.raises(IndexError):
            described_array[index_exp]

    @pytest.mark.parametrize(
        ("step", "message"),
        [
            (True, "step shall not be a boolean"),
            (False, "step shall not be a boolean"),
            (0, "step shall not be zero"),
        ],
    )
    def test_convert_step_rejects_booleans_and_zero_itself(self, described_array: Array, step, message: str):
        """Tests the step converter refuses a boolean or a zero step on its own, before any slice check."""
        with pytest.raises(IndexError, match=message):
            described_array._convert_step(step, described_array.dimensions[0])

    @pytest.mark.parametrize("step", [None, 1, -3])
    def test_convert_step_keeps_integers(self, described_array: Array, step):
        assert described_array._convert_step(step, described_array.dimensions[0]) == step


if __name__ == "__main__":
    pytest.main()


class TestArrayTimeoutAndCancellation:
    """The timeout of an update of the attributes is the limit of the wait for the lock; both it and the deletion stop
    on a cancelled scope before the adapter.
    """

    @pytest.fixture
    def target(self, inserted_array_with_attributes):
        return inserted_array_with_attributes

    @pytest.mark.parametrize(
        ("operation", "method", "args"),
        [
            pytest.param(
                "update_custom_attributes",
                "update_meta_custom_attributes",
                ({"custom_attribute": 0.7},),
                id="attributes",
            ),
        ],
    )
    @pytest.mark.parametrize(
        ("kwargs", "expected"),
        [pytest.param({"timeout": 5}, 5, id="given"), pytest.param({}, Sentinel, id="not-given")],
    )
    def test_timeout_reaches_the_adapter(self, target, mocker, operation, method, args, kwargs, expected):
        adapter_call = mocker.patch.object(target._adapter, method)
        getattr(target, operation)(*args, **kwargs)
        assert adapter_call.call_args.kwargs["timeout"] == expected
        assert isinstance(adapter_call.call_args.kwargs["token"], AbstractToken)

    @pytest.mark.parametrize(
        ("operation", "method", "args"),
        [
            pytest.param(
                "update_custom_attributes",
                "update_meta_custom_attributes",
                ({"custom_attribute": 0.7},),
                id="attributes",
            ),
        ],
    )
    @pytest.mark.parametrize("timeout", [0, -1, 1.5, True, "10", None])
    def test_invalid_timeout_is_rejected_before_the_adapter(self, target, mocker, operation, method, args, timeout):
        adapter_call = mocker.patch.object(target._adapter, method)
        with pytest.raises(DekerValidationError):
            getattr(target, operation)(*args, timeout=timeout)
        adapter_call.assert_not_called()

    @pytest.mark.parametrize(
        ("operation", "method", "args"),
        [
            pytest.param(
                "update_custom_attributes",
                "update_meta_custom_attributes",
                ({"custom_attribute": 0.7},),
                id="attributes",
            ),
            pytest.param("delete", "delete", (), id="delete"),
        ],
    )
    def test_cancelled_scope_stops_before_the_adapter(self, target, mocker, operation, method, args):
        adapter_call = mocker.patch.object(target._adapter, method)
        token = SimpleToken()
        token.cancel()
        with cancellation_scope(token), pytest.raises(DekerCancelledError):
            getattr(target, operation)(*args)
        adapter_call.assert_not_called()


class TestArrayWhoseFileIsGone:
    """An Array whose main file is gone does not exist: its metadata calls raise DekerInstanceNotExistsError."""

    @pytest.mark.parametrize("operation", ["update_custom_attributes", "read_meta"])
    def test_file_gone_before_the_call(self, inserted_array_with_attributes: Array, tmp_path: Path, operation: str):
        array = inserted_array_with_attributes
        file = get_paths(array, array._adapter.collection_path).main / (array.id + array._adapter.file_ext)
        file.rename(tmp_path / file.name)
        attributes = dict(array.custom_attributes)
        args = ({"custom_attribute": 0.9},) if operation == "update_custom_attributes" else ()
        with pytest.raises(DekerInstanceNotExistsError) as error:
            getattr(array, operation)(*args)
        assert str(error.value) == f"{file} doesn't exist"
        assert array.custom_attributes == attributes


def trash_of(target: Array) -> Path:
    """Find the trash of the storage of an Array or a VArray.

    :param target: the Array or the VArray
    :returns: the trash
    """
    return Path(target._adapter.uri.path) / target._adapter.ctx.config.trash_directory


def symlinks_of_arrays(target: Array) -> list[Path]:
    """List the symlinks of the Arrays of a VArray; an Array has none.

    :param target: the Array or the VArray
    :returns: the symlinks
    """
    if isinstance(target, Array):
        return []
    return list(target._VArray__array_adapter._adapter_iter(target.id))


def held_file_of(target: Array) -> Path:
    """Find the file a reader or a writer of an Array or a VArray holds: the main file of the Array or of the first
    Array of the VArray.

    :param target: the Array or the VArray
    :returns: the file
    """
    if isinstance(target, Array):
        return target._adapter._get_main_path_to_file(target)
    return symlinks_of_arrays(target)[0].readlink()


def files_of(target: Array) -> list[Path]:
    """List the main files and the symlinks of an Array or a VArray with its Arrays.

    :param target: the Array or the VArray
    :returns: the files
    """
    adapter = target._adapter
    paths = get_paths(target, adapter.collection_path)
    name = target.id + adapter.file_ext
    arrays = [path for symlink in symlinks_of_arrays(target) for path in (symlink.readlink(), symlink)]
    return [paths.main / name, paths.symlink / name, *arrays]


class CancelScopeAndMove:
    """Stand for the move to the trash: cancel the scope, then move.

    :param scope: token of the cancellation scope
    :param real: the real move
    """

    def __init__(self, scope: SimpleToken, real) -> None:
        self.scope = scope
        self.real = real

    def __call__(self, *args, **kwargs):
        self.scope.cancel()
        return self.real(*args, **kwargs)


class HeldEmptying:
    """Stand for the emptying of the trash in its background thread: it waits for a go, then empties.

    :param real: the real emptying
    """

    def __init__(self, real) -> None:
        self.real = real
        self.started = threading.Event()
        self.go = threading.Event()
        self.threads: list[threading.Thread] = []

    def __call__(self, *args, **kwargs):
        self.threads.append(threading.current_thread())
        self.started.set()
        self.go.wait(30)
        return self.real(*args, **kwargs)


class TestDelete:
    """The deletion of an Array or a VArray: no lock, a move of its files to the trash, one emptying of the trash."""

    @pytest.fixture(
        params=["inserted_array_with_attributes", "inserted_varray_with_attributes"], ids=["array", "varray"]
    )
    def target(self, request) -> Array:
        """The Array or the VArray to delete, with its data; deleted again after the test.

        :param request: request of the fixture
        """
        target = request.getfixturevalue(request.param)
        assert list(trash_of(target).iterdir()) == []
        return target

    @staticmethod
    def assert_deleted(target: Array, files: list[Path]) -> None:
        """The files of the target are gone; it is found neither by its id nor by its primary attributes.

        :param target: the Array or the VArray
        :param files: its files before the deletion
        """
        assert [path for path in files if os.path.lexists(path)] == []
        collection = target._collection
        assert collection.filter({"id": target.id}).last() is None
        assert collection.filter(target.primary_attributes).last() is None

    @pytest.mark.timeout(60)
    @pytest.mark.parametrize("shared", [True, False], ids=["reader", "writer"])
    def test_held_file_does_not_stop_the_deletion(self, target: Array, shared: bool):
        files = files_of(target)
        with Flock(held_file_of(target), shared=shared):
            start = time.monotonic()
            assert target.delete() is None
            assert time.monotonic() - start < 0.5  # nobody is waited for
        self.assert_deleted(target, files)
        assert list(trash_of(target).iterdir()) == []

    def test_trash_is_emptied_once_before_the_return_by_default(self, target: Array, mocker: MockerFixture):
        files = files_of(target)
        empty = mocker.spy(deker_local_adapters.mixin, "empty_trash_directory")
        target.delete()
        empty.assert_called_once_with(trash_of(target), background=False, logger=target._adapter.logger)
        self.assert_deleted(target, files)
        assert list(trash_of(target).iterdir()) == []

    def test_deletion_of_a_deleted_one_is_not_an_error(self, target: Array):
        files = files_of(target)
        target.delete()
        assert target.delete() is None
        self.assert_deleted(target, files)
        assert list(trash_of(target).iterdir()) == []

    @pytest.mark.parametrize("background", [False, True])
    def test_without_emptying_the_trash_keeps_exactly_what_is_moved(
        self, target: Array, mocker: MockerFixture, background: bool, client: Client
    ):
        files = files_of(target)
        empty = mocker.spy(deker_local_adapters.mixin, "empty_trash_directory")
        thread = mocker.spy(threading.Thread, "start")
        assert target.delete(background=background, _empty_trash=False) is None
        empty.assert_not_called()
        thread.assert_not_called()
        self.assert_deleted(target, files)
        assert len(list(trash_of(target).iterdir())) == len(files)
        client.empty_trash()
        assert list(trash_of(target).iterdir()) == []

    @pytest.mark.timeout(60)
    def test_background_returns_while_the_trash_is_emptied(self, target: Array, mocker: MockerFixture):
        files = files_of(target)
        held = HeldEmptying(deker_local_adapters.collection_adapter._empty_trash_in_background)
        mocker.patch.object(deker_local_adapters.collection_adapter, "_empty_trash_in_background", side_effect=held)
        assert target.delete(background=True) is None
        assert held.started.wait(30)
        self.assert_deleted(target, files)
        assert len(list(trash_of(target).iterdir())) == len(files)  # returned while the emptying waits
        (thread,) = held.threads
        assert thread is not threading.current_thread()
        assert thread.daemon
        held.go.set()
        thread.join(30)
        assert list(trash_of(target).iterdir()) == []

    def test_failed_emptying_is_raised_after_the_deletion(self, target: Array, client: Client):
        files = files_of(target)
        trash = trash_of(target)
        locked = trash / "locked"
        locked.mkdir()
        (locked / "kept").write_text("file")
        locked.chmod(0o500)
        try:
            with pytest.raises(DekerTrashError) as error:
                target.delete()
            assert str(error.value) == (
                f"The trash of the storage is not emptied: 1 files are not removed from {trash}. "
                "Reasons: Permission denied"
            )
            self.assert_deleted(target, files)
            assert [path.name for path in trash.iterdir()] == ["locked"]
        finally:
            locked.chmod(0o700)
            client.empty_trash()
        assert list(trash.iterdir()) == []

    def test_cancelled_before_the_first_move_moves_nothing(self, target: Array):
        files = files_of(target)
        scope = SimpleToken()
        scope.cancel()
        with cancellation_scope(scope), pytest.raises(DekerCancelledError):
            target.delete()
        assert [path for path in files if not os.path.lexists(path)] == []
        assert list(trash_of(target).iterdir()) == []

    def test_cancelled_after_the_first_move_moves_everything(self, target: Array, mocker: MockerFixture):
        files = files_of(target)
        scope = SimpleToken()
        real = deker_local_adapters.mixin.move_to_trash
        move = mocker.patch.object(
            deker_local_adapters.mixin, "move_to_trash", side_effect=CancelScopeAndMove(scope, real)
        )
        with cancellation_scope(scope):
            assert target.delete() is None
        assert scope.cancelled
        assert move.call_count == len(files)
        self.assert_deleted(target, files)
        assert list(trash_of(target).iterdir()) == []

    def test_timeout_is_not_an_argument(self, target: Array):
        files = files_of(target)
        with pytest.raises(TypeError) as error:
            target.delete(timeout=5)
        assert str(error.value) == "BaseArray.delete() got an unexpected keyword argument 'timeout'"
        assert [path for path in files if not os.path.lexists(path)] == []


class TestDeleteInAStorageWithoutItsDirectories:
    """The deletion that does not find the trash or the collections directory of the storage is critical."""

    @pytest.fixture(params=["array", "varray"])
    def target(self, request, tmp_path: Path) -> Array:
        """An Array or a VArray with its Arrays, in a storage of the test's own.

        :param request: request of the fixture
        :param tmp_path: directory of the test
        """
        client = Client(embedded_uri(tmp_path / "storage"), loglevel="ERROR")
        dimensions = [DimensionSchema(name="x", size=4)]
        if request.param == "array":
            target = client.create_collection("own", ArraySchema(dimensions=dimensions, dtype=float)).create()
        else:
            schema = VArraySchema(dimensions=dimensions, dtype=float, vgrid=[2])
            target = client.create_collection("own", schema).create()
            target[:].update(np.zeros(4))
        yield target
        client.close()

    @pytest.mark.parametrize("directory", ["trash", "collections"])
    def test_missing_storage_directory_is_critical(
        self, target: Array, tmp_path: Path, mocker: MockerFixture, directory: str
    ):
        gone = tmp_path / "storage" / directory
        gone.rename(tmp_path / "away")
        critical = mocker.patch.object(logging.getLogger("Deker"), "critical")
        with pytest.raises(DekerCriticalError) as error:
            target.delete()
        assert str(error.value) == f"Storage directory {gone} doesn't exist"
        critical.assert_called_once_with(f"Storage directory {gone} doesn't exist")
        assert not gone.exists()

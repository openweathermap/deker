from datetime import datetime, timezone
from typing import TYPE_CHECKING

import numpy as np
import pytest

from deker_local_adapters import LocalCollectionAdapter

from tests.parameters.collection_params import CollectionParams

from deker.collection import Collection
from deker.errors import DekerInstanceNotExistsError, DekerMemoryError
from deker.tools import check_memory
from deker.tools.time import convert_datetime_attrs_to_iso, convert_iso_attrs_to_datetime


if TYPE_CHECKING:
    from deker.arrays import Array, VArray


def test_get_attributes_schema(collection_adapter: LocalCollectionAdapter, factory):
    """Tests if array schema returns correct attributes.

    :param collection_adapter: Collection adapter object
    """
    # Collection without varray_schema
    params_without_varray = CollectionParams.OK_params_for_collection_no_varray()
    collection_without_varray = Collection(
        adapter=collection_adapter,
        factory=factory,
        storage_adapter=collection_adapter.get_storage_adapter(),
        **params_without_varray,
    )

    array_schema_attrs = collection_without_varray.array_schema.attributes
    assert array_schema_attrs == params_without_varray["schema"].attributes

    varray_schema = collection_without_varray.varray_schema
    assert varray_schema is None

    # Collection with varray schema
    params_with_varray = CollectionParams.OK_params_for_collection_varray()
    collection_with_varray = Collection(
        adapter=collection_adapter,
        factory=factory,
        storage_adapter=collection_adapter.get_storage_adapter(),
        **params_with_varray,
    )

    varray_schema_attrs = collection_with_varray.varray_schema.attributes
    assert varray_schema_attrs == params_with_varray["schema"].attributes


def test_deleted_array_call(inserted_array: "Array"):
    """Test if deleted array raises error if called."""
    inserted_array.delete()

    with pytest.raises(DekerInstanceNotExistsError):
        inserted_array[:].read()

    with pytest.raises(DekerInstanceNotExistsError):
        inserted_array[:].clear()

    with pytest.raises(DekerInstanceNotExistsError):
        inserted_array[:].update(data=np.ones(shape=inserted_array.shape))


def test_deleted_varray_call(inserted_varray: "VArray"):
    """Test if deleted varray raises error if called."""
    inserted_varray.delete()
    subset = inserted_varray[:]
    with pytest.raises(DekerInstanceNotExistsError):
        subset.read()

    with pytest.raises(DekerInstanceNotExistsError):
        subset.clear()

    with pytest.raises(DekerInstanceNotExistsError):
        subset.update(data=np.ones(shape=inserted_varray.shape))


def test_deleted_collection_call(array_collection: "Collection"):
    """Test if deleted collection raises error if called."""
    array_collection.delete()

    with pytest.raises(DekerInstanceNotExistsError):
        array_collection.create()

    with pytest.raises(DekerInstanceNotExistsError):
        array_collection.clear()

    with pytest.raises(DekerInstanceNotExistsError):
        for _ in array_collection:
            pass


@pytest.mark.parametrize(
    ("shape", "dtype"),
    [
        ((720, 22, 721, 1440), np.float64),
        ((100, 40, 361, 720), float),
        ((1024, 1024, 1024), np.int8),
        ((10240, 10240), int),
        (
            (
                10240,
                10241,
            ),
            np.int8,
        ),
    ],
)
def test_check_memory_raises(config, shape, dtype):
    with pytest.raises(DekerMemoryError):
        check_memory(shape, dtype, config.memory_limit)


@pytest.mark.parametrize(
    ("attrs", "expected"),
    [
        (
            {"int": 1, "str": "1", "dt": datetime(2023, 1, 1, tzinfo=timezone.utc)},
            {"int": 1, "str": "1", "dt": "2023-01-01T00:00:00+00:00"},
        ),
        ({"int": 1, "str": "1", "dt": "dt"}, {"int": 1, "str": "1", "dt": "dt"}),
        (None, None),
        ({}, {}),
    ],
)
def test_convert_datetime_attrs(attrs, expected):
    res = convert_datetime_attrs_to_iso(attrs)
    assert res == expected
    if attrs is not None:
        assert res is not expected


@pytest.mark.parametrize(
    "attrs",
    [
        [
            {"int": 1, "str": "1", "dt": datetime(2023, 1, 1, tzinfo=timezone.utc)},
            {"int": 1, "str": "1", "dt": "2023-01-01T00:00:00+00:00"},
        ],
        '{"int": 1, "str": "1", "dt": "2023-01-01T00:00:00+00:00"}',
        1,
        0.0,
        "",
        {datetime(2023, 1, 1, tzinfo=timezone.utc)},
        (datetime(2023, 1, 1, tzinfo=timezone.utc),),
    ],
)
def test_convert_datetime_attrs(attrs):
    with pytest.raises(TypeError):
        assert convert_datetime_attrs_to_iso(attrs)


@pytest.mark.parametrize(
    ("attrs", "expected"),
    [
        (
            {"int": 1, "str": "1", "dt": datetime(2023, 1, 1, tzinfo=timezone.utc)},
            {"int": 1, "str": "1", "dt": "2023-01-01T00:00:00+00:00"},
        ),
        (
            {"int": 1, "str": "1", "dt": datetime(2023, 1, 1)},
            {"int": 1, "str": "1", "dt": "2023-01-01T00:00:00"},
        ),
        ({"int": 1, "str": "1", "dt": "dt"}, {"int": 1, "str": "1", "dt": "dt"}),
        (None, None),
        ({}, {}),
    ],
)
def test_convert_datetime_attrs_raises(attrs, expected):
    res = convert_datetime_attrs_to_iso(attrs)
    assert res == expected
    if attrs is not None:
        assert res is not expected


@pytest.mark.parametrize(
    ("attrs", "expected"),
    [
        (
            {"int": 1, "str": "1", "dt": "2023-01-01T00:00:00+00:00"},
            {"int": 1, "str": "1", "dt": datetime(2023, 1, 1, tzinfo=timezone.utc)},
        ),
        (
            {"int": 1, "str": "1", "dt": "2023-01-01T00:00:00"},
            {"int": 1, "str": "1", "dt": datetime(2023, 1, 1)},
        ),
        ({"int": 1, "str": "1", "dt": "dt"}, {"int": 1, "str": "1", "dt": "dt"}),
        (None, None),
        ({}, {}),
    ],
)
def test_convert_isoformat_attrs(attrs, expected):
    res = convert_iso_attrs_to_datetime(attrs)
    assert res == expected
    if attrs is not None:
        assert res is not expected


@pytest.mark.parametrize(
    "attrs",
    [
        [
            {"int": 1, "str": "1", "dt": datetime(2023, 1, 1, tzinfo=timezone.utc)},
            {"int": 1, "str": "1", "dt": "2023-01-01T00:00:00+00:00"},
        ],
        '{"int": 1, "str": "1", "dt": "2023-01-01T00:00:00+00:00"}',
        1,
        0.0,
        "",
        {datetime(2023, 1, 1, tzinfo=timezone.utc)},
        (datetime(2023, 1, 1, tzinfo=timezone.utc),),
    ],
)
def test_convert_isoformat_attrs_raises(attrs):
    with pytest.raises(TypeError):
        assert convert_iso_attrs_to_datetime(attrs)


if __name__ == "__main__":
    pytest.main()

import logging

from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pytest

from deker_local_adapters import LocalCollectionAdapter

from tests.parameters.collection_params import CollectionParams

from deker.collection import Collection
from deker.errors import DekerCriticalError, DekerInstanceNotExistsError, DekerMemoryError, DekerValidationError
from deker.tools import array as array_tools
from deker.tools import check_memory, convert_human_memory_to_bytes, get_default_fill_value, get_paths
from deker.tools.array import UNCHECKED_SHARE_OF_LIMIT
from deker.tools.path import check_storage_directory
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


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        (int, np.iinfo(np.int64).min),
        (np.int8, np.iinfo(np.int8).min),
        (np.uint16, 0),
        (float, np.nan),
        (np.float32, np.nan),
        (complex, np.nan),
        (np.complex128, np.nan),
    ],
)
def test_get_default_fill_value(dtype, expected):
    """Tests default fill values: minimum for integers, nan for the others."""
    value = get_default_fill_value(dtype)
    assert value == expected or (np.isnan(expected) and np.isnan(value))


@pytest.mark.parametrize("dtype", [str, bytes, object])
def test_get_default_fill_value_raises(dtype):
    """Tests if non-numeric dtypes have no default fill value."""
    with pytest.raises(DekerValidationError):
        get_default_fill_value(dtype)


def test_get_paths_raises_on_wrong_object(tmp_path):
    """Tests if paths can be built only for arrays."""
    with pytest.raises(TypeError):
        get_paths("not an array", tmp_path)


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


class TestCheckMemorySmallRequests:
    """A request within a small share of the limit of the client fits for sure: the system is not asked."""

    LIMIT = 100 * 1024 * 1024

    def test_a_small_request_does_not_query_the_system(self, mocker):
        virtual = mocker.spy(array_tools, "virtual_memory")
        swap = mocker.spy(array_tools, "swap_memory")
        check_memory((100, 100), np.float64, self.LIMIT)  # 80 KB of 100 MB
        assert virtual.call_count == 0
        assert swap.call_count == 0

    @pytest.mark.parametrize(
        ("cells", "queried"),
        [
            pytest.param(LIMIT * UNCHECKED_SHARE_OF_LIMIT, False, id="exactly-the-share"),
            pytest.param(LIMIT * UNCHECKED_SHARE_OF_LIMIT + 1, True, id="one-byte-above-the-share"),
        ],
    )
    def test_the_share_decides_whether_the_system_is_queried(self, mocker, cells: float, queried: bool):
        virtual = mocker.spy(array_tools, "virtual_memory")
        swap = mocker.spy(array_tools, "swap_memory")
        check_memory((int(cells),), np.int8, self.LIMIT)
        assert (virtual.call_count == 1) is queried
        assert (swap.call_count == 1) is queried

    def test_a_big_request_queries_the_system_once(self, mocker):
        """One query of the memory and one of the swap per check, not one per number in the message."""
        virtual = mocker.spy(array_tools, "virtual_memory")
        swap = mocker.spy(array_tools, "swap_memory")
        check_memory((10 * 1024 * 1024,), np.int8, self.LIMIT)  # 10 MB of 100 MB
        assert virtual.call_count == 1
        assert swap.call_count == 1

    def test_a_request_above_the_limit_raises_without_the_system(self, mocker):
        """What does not fit the limit of the client is rejected before the system is asked."""
        virtual = mocker.spy(array_tools, "virtual_memory")
        with pytest.raises(DekerMemoryError, match="Can not allocate"):
            check_memory((self.LIMIT + 1,), np.int8, self.LIMIT)
        assert virtual.call_count == 0

    def test_a_request_above_the_free_memory_raises(self, mocker):
        """What fits the limit of the client but not the memory free right now is rejected too."""
        mocker.patch.object(array_tools, "virtual_memory", return_value=mocker.Mock(total=self.LIMIT, available=1024))
        mocker.patch.object(array_tools, "swap_memory", return_value=mocker.Mock(total=0, free=0))
        with pytest.raises(DekerMemoryError, match="Current available free memory"):
            check_memory((10 * 1024 * 1024,), np.int8, self.LIMIT)


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
def test_convert_datetime_attrs_rejects_wrong_type(attrs):
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


@pytest.mark.parametrize(
    ("params", "result", "error"),
    [
        ("1K", 1024, None),
        ("1M", 1024**2, None),
        ("1G", 1024**3, None),
        ("0", 0, None),
        ("Foo", None, DekerValidationError),
        (1.5, None, DekerValidationError),
        (None, None, DekerValidationError),
        (["1G"], None, DekerValidationError),
        (b"1G", None, DekerValidationError),
    ],
)
def test_convert_human_to_bytes(params, result, error):
    if error:
        with pytest.raises(error):
            convert_human_memory_to_bytes(params)
    else:
        assert convert_human_memory_to_bytes(params) == result


if __name__ == "__main__":
    pytest.main()


class TestCheckStorageDirectory:
    """A directory of the storage that is missing is critical: logged as such and raised; one in place passes."""

    def test_directory_in_place_passes_quietly(self, tmp_path: Path, deker_log: pytest.LogCaptureFixture):
        assert check_storage_directory(tmp_path) is None
        assert deker_log.records == []

    def test_missing_directory_is_logged_and_raised(self, tmp_path: Path, deker_log: pytest.LogCaptureFixture):
        missing = tmp_path / "trash"
        message = f"Storage directory {missing} doesn't exist"
        with pytest.raises(DekerCriticalError) as error:
            check_storage_directory(missing)
        assert str(error.value) == message
        assert [(record.levelno, record.getMessage()) for record in deker_log.records] == [(logging.CRITICAL, message)]
        assert not missing.exists()

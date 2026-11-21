"""Tests of the adapter interfaces: data validation shared by every array adapter."""

import inspect

from typing import Any

import numpy as np
import pytest

from deker_local_adapters import LocalCollectionAdapter
from deker_local_adapters.factory import AdaptersFactory

from tests.parameters.fresh_copy import load_fresh_copy

from deker.ABC.base_adapters import BaseArrayAdapter, BaseCollectionAdapter, IArrayAdapter
from deker.errors import DekerArrayTypeError, DekerMetaDataError
from deker.tools import schema as schema_module
from deker.types.private import enums as enums_module

# Python numbers are converted to the array dtype when every value fits it; the result equals the input.
# (dtype, data, expected values)
PYTHON_NUMBERS_THAT_FIT = [
    pytest.param(np.uint8, [0, 255], [0, 255], id="uint8-list-bounds"),
    pytest.param(np.uint8, (1, 2), [1, 2], id="uint8-tuple"),
    pytest.param(np.uint8, [[1, 2], [3, 4]], [[1, 2], [3, 4]], id="uint8-nested-list"),
    pytest.param(np.uint8, ((1, 2), [3, 4]), [[1, 2], [3, 4]], id="uint8-mixed-containers"),
    pytest.param(np.uint8, [1.0, 2.0], [1, 2], id="uint8-whole-floats"),
    pytest.param(np.int8, [-128, 127], [-128, 127], id="int8-bounds"),
    pytest.param(np.int16, [-32768, 32767], [-32768, 32767], id="int16-bounds"),
    pytest.param(np.int32, [-(2**31), 2**31 - 1], [-(2**31), 2**31 - 1], id="int32-bounds"),
    pytest.param(np.int64, [-(2**63), 2**63 - 1], [-(2**63), 2**63 - 1], id="int64-bounds"),
    pytest.param(np.uint64, [0, 2**64 - 1], [0, 2**64 - 1], id="uint64-bounds"),
    pytest.param(np.float32, [0.1, -0.1], [0.1, -0.1], id="float32-rounded-fractions"),
    pytest.param(np.float32, [3.0e38, -3.0e38], [3.0e38, -3.0e38], id="float32-near-bounds"),
    pytest.param(np.float32, [1, 2.5], [1.0, 2.5], id="float32-ints-and-floats"),
    pytest.param(np.float32, [2**70], [2**70], id="float32-big-int-fits"),
    pytest.param(
        np.float32, [float("nan"), float("inf"), float("-inf")], [np.nan, np.inf, -np.inf], id="float32-nan-inf"
    ),
    pytest.param(np.float64, [1.7e308, -1.7e308], [1.7e308, -1.7e308], id="float64-near-bounds"),
    pytest.param(np.float64, [2**53, 1e-300], [2**53, 1e-300], id="float64-precision"),
    pytest.param(np.float16, [1.0, 65504.0], [1.0, 65504.0], id="float16-bounds"),
    pytest.param(np.complex64, [1 + 2j, -1e38 + 1e38j], [1 + 2j, -1e38 + 1e38j], id="complex64"),
    pytest.param(np.complex64, [1, 2.5], [1 + 0j, 2.5 + 0j], id="complex64-from-real"),
    pytest.param(np.complex128, [1e308 + 1e308j], [1e308 + 1e308j], id="complex128"),
]

# the same values as scalars
PYTHON_SCALARS_THAT_FIT = [
    pytest.param(np.uint8, 255, id="uint8-max"),
    pytest.param(np.uint8, 0, id="uint8-min"),
    pytest.param(np.uint8, 7.0, id="uint8-whole-float"),
    pytest.param(np.int8, -128, id="int8-min"),
    pytest.param(np.int32, 2**31 - 1, id="int32-max"),
    pytest.param(np.float32, 0.1, id="float32-fraction"),
    pytest.param(np.float32, 3, id="float32-int"),
    pytest.param(np.float32, float("nan"), id="float32-nan"),
    pytest.param(np.float64, 1e308, id="float64-big"),
    pytest.param(np.complex64, 1 + 2j, id="complex64"),
    pytest.param(np.complex128, 2.5, id="complex128-from-float"),
]

# Python numbers that do not fit the array dtype: nothing is converted, the error says so.
PYTHON_NUMBERS_THAT_DO_NOT_FIT = [
    pytest.param(np.uint8, [-1], id="uint8-negative"),
    pytest.param(np.uint8, [256], id="uint8-overflow"),
    pytest.param(np.uint8, [[1, 2], [3, 300]], id="uint8-nested-overflow"),
    pytest.param(np.uint8, [1.5], id="uint8-fraction"),
    pytest.param(np.uint8, [float("nan")], id="uint8-nan"),
    pytest.param(np.uint8, [float("inf")], id="uint8-inf"),
    pytest.param(np.uint8, [1 + 0j], id="uint8-complex"),
    pytest.param(np.uint8, [2**70], id="uint8-huge-int"),
    pytest.param(np.int8, [128], id="int8-overflow"),
    pytest.param(np.int8, [-129], id="int8-underflow"),
    pytest.param(np.int32, [2**31], id="int32-overflow"),
    pytest.param(np.int32, [-(2**31) - 1], id="int32-underflow"),
    pytest.param(np.int64, [2**63], id="int64-overflow"),
    pytest.param(np.int64, [2**100], id="int64-huge-int"),
    pytest.param(np.uint64, [-1], id="uint64-negative"),
    pytest.param(np.float32, [1e39], id="float32-overflow"),
    pytest.param(np.float32, [-1e39], id="float32-underflow"),
    pytest.param(np.float32, [2**130], id="float32-huge-int"),
    pytest.param(np.float32, [1 + 1j], id="float32-complex"),
    pytest.param(np.float16, [70000.0], id="float16-overflow"),
    pytest.param(np.float64, [1 + 1j], id="float64-complex"),
    pytest.param(np.float64, [2**1100], id="float64-huge-int"),
    pytest.param(np.complex64, [1e39 + 0j], id="complex64-real-overflow"),
    pytest.param(np.complex64, [1e39j], id="complex64-imag-overflow"),
]

PYTHON_SCALARS_THAT_DO_NOT_FIT = [
    pytest.param(np.uint8, -1, id="uint8-negative"),
    pytest.param(np.uint8, 256, id="uint8-overflow"),
    pytest.param(np.uint8, 1.5, id="uint8-fraction"),
    pytest.param(np.uint8, 2**70, id="uint8-huge-int"),
    pytest.param(np.int8, 128, id="int8-overflow"),
    pytest.param(np.int32, 2**31, id="int32-overflow"),
    pytest.param(np.int64, 2**63, id="int64-overflow"),
    pytest.param(np.int64, float("nan"), id="int64-nan"),
    pytest.param(np.float32, 1e39, id="float32-overflow"),
    pytest.param(np.float32, 1 + 1j, id="float32-complex"),
    pytest.param(np.float64, 2**1100, id="float64-huge-int"),
    pytest.param(np.complex64, 1e39j, id="complex64-imag-overflow"),
]

# numpy data is never converted: its dtype must be exactly the array dtype
NUMPY_DATA_OF_ANOTHER_DTYPE = [
    pytest.param(np.int32, np.array([1], dtype=np.uint8), id="array-uint8-into-int32"),
    pytest.param(np.int64, np.array([1], dtype=np.int32), id="array-int32-into-int64"),
    pytest.param(np.float64, np.array([1.0], dtype=np.float32), id="array-float32-into-float64"),
    pytest.param(np.float32, np.array([1.0], dtype=np.float64), id="array-float64-into-float32"),
    pytest.param(np.float16, np.array([1], dtype=np.uint8), id="array-uint8-into-float16"),
    pytest.param(np.int64, np.array([1.0], dtype=np.float64), id="array-float64-into-int64"),
    pytest.param(np.complex128, np.array([1.0], dtype=np.float64), id="array-float64-into-complex128"),
    pytest.param(np.int64, np.array([True]), id="array-bool-into-int64"),
    pytest.param(np.int32, np.uint8(1), id="scalar-uint8-into-int32"),
    pytest.param(np.int64, np.int32(1), id="scalar-int32-into-int64"),
    pytest.param(np.float32, np.float64(0.5), id="scalar-float64-into-float32"),
    pytest.param(np.float64, np.float32(0.5), id="scalar-float32-into-float64"),
    pytest.param(np.complex128, np.complex64(1 + 1j), id="scalar-complex64-into-complex128"),
]

NUMPY_DATA_OF_THE_SAME_DTYPE = [
    pytest.param(np.uint8, np.array([1, 255], dtype=np.uint8), id="array-uint8"),
    pytest.param(np.float32, np.array([0.1], dtype=np.float32), id="array-float32"),
    pytest.param(np.complex64, np.array([1 + 1j], dtype=np.complex64), id="array-complex64"),
    pytest.param(np.uint8, np.uint8(255), id="scalar-uint8"),
    pytest.param(np.float32, np.float32(0.1), id="scalar-float32"),
    pytest.param(np.int64, np.int64(-1), id="scalar-int64"),
]

NOT_DATA_AT_ALL = [
    pytest.param(np.int64, "1,2,3", id="string"),
    pytest.param(np.int64, ["1", "2"], id="list-of-strings"),
    pytest.param(np.int64, [[1, 2], ["a", "b"]], id="nested-list-with-strings"),
    pytest.param(np.int64, [None], id="list-with-none"),
    pytest.param(np.int64, None, id="none"),
    pytest.param(np.int64, {"a": 1}, id="dict"),
    pytest.param(np.int64, {1, 2}, id="set"),
    pytest.param(np.int64, b"\x01", id="bytes"),
    pytest.param(np.int64, [[1, 2], [3]], id="ragged-list"),
]


def _shape_of(data: Any) -> tuple[int, ...]:
    """Shape of a list, tuple, ndarray or scalar."""
    return np.asarray(data).shape


class TestProcessData:
    """``_process_data`` converts Python numbers that fit the dtype and rejects everything else."""

    @pytest.mark.parametrize(("dtype", "data", "expected"), PYTHON_NUMBERS_THAT_FIT)
    def test_python_numbers_that_fit_are_converted(self, dtype: type, data: Any, expected: list):
        result = IArrayAdapter._process_data(dtype, _shape_of(data), data)
        assert isinstance(result, np.ndarray)
        assert result.dtype == dtype
        assert result.shape == _shape_of(data)
        assert np.array_equal(result, np.asarray(expected, dtype=dtype), equal_nan=True)

    @pytest.mark.parametrize(("dtype", "data"), PYTHON_SCALARS_THAT_FIT)
    def test_python_scalars_that_fit_are_converted(self, dtype: type, data: Any):
        result = IArrayAdapter._process_data(dtype, (2, 2), data, np.index_exp[0, 0])
        assert isinstance(result, dtype)
        assert np.array_equal(np.asarray(result), np.asarray(data, dtype=dtype), equal_nan=True)

    @pytest.mark.parametrize(("dtype", "data"), PYTHON_NUMBERS_THAT_DO_NOT_FIT)
    def test_python_numbers_that_do_not_fit_are_rejected(self, dtype: type, data: Any):
        with pytest.raises(DekerArrayTypeError):
            IArrayAdapter._process_data(dtype, _shape_of(data), data)

    @pytest.mark.parametrize(("dtype", "data"), PYTHON_SCALARS_THAT_DO_NOT_FIT)
    def test_python_scalars_that_do_not_fit_are_rejected(self, dtype: type, data: Any):
        with pytest.raises(DekerArrayTypeError):
            IArrayAdapter._process_data(dtype, (2, 2), data, np.index_exp[0, 0])

    @pytest.mark.parametrize(("dtype", "data"), NUMPY_DATA_OF_ANOTHER_DTYPE)
    def test_numpy_data_is_never_converted(self, dtype: type, data: Any):
        with pytest.raises(DekerArrayTypeError):
            IArrayAdapter._process_data(dtype, _shape_of(data), data, np.index_exp[...])

    @pytest.mark.parametrize(("dtype", "data"), NUMPY_DATA_OF_THE_SAME_DTYPE)
    def test_numpy_data_of_the_array_dtype_is_returned_as_is(self, dtype: type, data: Any):
        result = IArrayAdapter._process_data(dtype, _shape_of(data), data, np.index_exp[...])
        assert result is data

    @pytest.mark.parametrize(("dtype", "data"), NOT_DATA_AT_ALL)
    def test_non_numeric_data_is_rejected(self, dtype: type, data: Any):
        with pytest.raises(DekerArrayTypeError):
            IArrayAdapter._process_data(dtype, (2, 2), data)

    @pytest.mark.parametrize(
        ("shape", "bounds", "data"),
        [
            pytest.param((2, 2), np.index_exp[:], [1, 2, 3], id="list-wrong-shape"),
            pytest.param((2, 2), np.index_exp[0], [[1, 2], [3, 4]], id="list-too-many-cells"),
            pytest.param((2, 2), np.index_exp[:], np.zeros((3, 2), dtype=np.int64), id="array-wrong-shape"),
        ],
    )
    def test_data_of_a_wrong_shape_is_rejected(self, shape: tuple, bounds: Any, data: Any):
        with pytest.raises(IndexError):
            IArrayAdapter._process_data(np.int64, shape, data, bounds)

    @pytest.mark.parametrize(("dtype", "data"), PYTHON_NUMBERS_THAT_DO_NOT_FIT[:3])
    def test_rejection_message_names_the_dtype(self, dtype: type, data: Any):
        with pytest.raises(DekerArrayTypeError, match=dtype.__name__):
            IArrayAdapter._process_data(dtype, _shape_of(data), data)


class TestCreateCollectionFromMeta:
    METADATA = {
        "name": "long_double",
        "metadata_version": "0.2",
        "storage_adapter": "HDF5StorageAdapter",
        "type": "array",
        "options": None,
        "schema": {
            "dtype": "numpy.float128",
            "fill_value": None,
            "attributes": [],
            "dimensions": [{"type": "generic", "name": "x", "size": 3, "labels": None, "scale": None}],
        },
    }

    def test_dtype_absent_on_this_platform_raises_a_clear_error(
        self, collection_adapter: LocalCollectionAdapter, factory: AdaptersFactory, monkeypatch: pytest.MonkeyPatch
    ):
        """A collection written with float128 elsewhere is not corrupted: the error names the platform."""
        monkeypatch.delattr(np, "float128", raising=False)
        enums = load_fresh_copy(enums_module, "deker.types.private.enums_without_long_types")
        monkeypatch.setattr(schema_module, "DTypeEnum", enums.DTypeEnum)
        with pytest.raises(DekerMetaDataError, match="numpy.float128 is not available") as info:
            collection_adapter.create_collection_from_meta(self.METADATA, factory)
        assert "invalid/corrupted" not in str(info.value)

    def test_dtype_present_in_numpy_reads_the_collection(
        self, collection_adapter: LocalCollectionAdapter, factory: AdaptersFactory, monkeypatch: pytest.MonkeyPatch
    ):
        """With float128 in NumPy (Linux, Intel macOS) the metadata reads like any other."""
        monkeypatch.setattr(np, "float128", np.longdouble, raising=False)
        enums = load_fresh_copy(enums_module, "deker.types.private.enums_with_long_types")
        monkeypatch.setattr(schema_module, "DTypeEnum", enums.DTypeEnum)
        collection = collection_adapter.create_collection_from_meta(self.METADATA, factory)
        assert collection.array_schema.dtype is np.longdouble

    def test_unknown_dtype_is_corrupted_metadata(
        self, collection_adapter: LocalCollectionAdapter, factory: AdaptersFactory
    ):
        metadata = {**self.METADATA, "schema": {**self.METADATA["schema"], "dtype": "numpy.int128"}}
        with pytest.raises(DekerMetaDataError, match="invalid/corrupted"):
            collection_adapter.create_collection_from_meta(metadata, factory)


@pytest.mark.parametrize(
    ("interface", "method"),
    [
        *[
            (IArrayAdapter, name)
            for name in ("create", "update_meta_custom_attributes", "delete", "read_data", "update", "clear")
        ],
        (BaseArrayAdapter, "delete_all_by_vid"),
        *[(BaseCollectionAdapter, name) for name in ("create", "clear", "delete")],
    ],
)
def test_operations_of_the_adapters_take_the_timeout_and_the_token(interface, method):
    parameters = inspect.signature(getattr(interface, method)).parameters
    assert parameters["timeout"].kind is inspect.Parameter.KEYWORD_ONLY
    assert parameters["token"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize(
    ("interface", "method"),
    [(IArrayAdapter, "read_meta"), (IArrayAdapter, "get_by_id"), (BaseCollectionAdapter, "read")],
)
def test_reads_of_the_metadata_take_no_token(interface, method):
    parameters = inspect.signature(getattr(interface, method)).parameters
    assert "timeout" not in parameters
    assert "token" not in parameters


def test_storage_adapter_not_given_is_the_default():
    from deker_local_adapters import HDF5StorageAdapter

    from deker.ABC.base_adapters import BaseCollectionAdapter as Adapter

    assert Adapter.get_storage_adapter() is HDF5StorageAdapter

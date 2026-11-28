"""Tests of the subset interface shared by Subset and VSubset."""

import inspect

from unittest.mock import patch

import numpy as np
import pytest

from deker.ABC import base_subset
from deker.ABC.base_subset import BaseSubset
from deker.arrays import Array, VArray
from deker.subset import Subset, VSubset


class TestBaseSubsetProperties:
    def test_fill_value_is_the_array_fill_value(self, array: Array):
        subset = array[:]
        assert subset.fill_value is array.fill_value
        assert np.isnan(subset.fill_value)

    def test_fill_value_of_a_vsubset_is_the_varray_fill_value(self, varray: VArray):
        subset = varray[:]
        assert subset.fill_value is varray.fill_value
        assert np.isnan(subset.fill_value)


class TestAbstractSignatures:
    """The abstract methods of BaseSubset declare the parameters their implementations take."""

    @pytest.mark.parametrize("implementation", [Subset, VSubset], ids=["Subset", "VSubset"])
    @pytest.mark.parametrize("method", ["update", "clear"])
    def test_abstract_method_has_the_signature_of_the_implementation(
        self, implementation: type[BaseSubset], method: str
    ):
        abstract = inspect.signature(getattr(BaseSubset, method))
        implemented = inspect.signature(getattr(implementation, method))
        assert [(param.name, param.kind, param.default) for param in abstract.parameters.values()] == [
            (param.name, param.kind, param.default) for param in implemented.parameters.values()
        ]
        assert abstract.parameters["timeout"].kind is inspect.Parameter.KEYWORD_ONLY
        assert abstract == implemented


class TestReadXArrayWithoutXArray:
    """Without xarray installed, read_xarray raises the import error captured at import time."""

    @pytest.mark.parametrize("fixture", ["inserted_array", "inserted_varray"])
    def test_read_xarray_raises_the_captured_import_error(self, request: pytest.FixtureRequest, fixture: str):
        array = request.getfixturevalue(fixture)
        error = ImportError("No module named 'xarray' found")
        with patch.object(base_subset, "xarray_import_error", error):
            with pytest.raises(ImportError) as info:
                array[:].read_xarray()
        assert info.value is error

    def test_read_xarray_works_when_xarray_is_there(self, inserted_array: Array):
        assert base_subset.xarray_import_error is None
        data = inserted_array[:].read_xarray()
        assert np.array_equal(data.values, inserted_array[:].read())

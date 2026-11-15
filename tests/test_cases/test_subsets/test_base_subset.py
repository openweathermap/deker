"""Tests of the subset interface shared by Subset and VSubset."""

from unittest.mock import patch

import numpy as np
import pytest

from deker.ABC import base_subset
from deker.arrays import Array, VArray


class TestBaseSubsetProperties:
    def test_fill_value_is_the_array_fill_value(self, array: Array):
        subset = array[:]
        assert subset.fill_value is array.fill_value
        assert np.isnan(subset.fill_value)

    def test_fill_value_of_a_vsubset_is_the_varray_fill_value(self, varray: VArray):
        subset = varray[:]
        assert subset.fill_value is varray.fill_value
        assert np.isnan(subset.fill_value)


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

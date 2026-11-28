"""Tests of the private typings: the numeric types depend on what NumPy provides on the platform."""

import sys

from typing import get_args

import numpy as np
import pytest

from tests.parameters.fresh_copy import load_fresh_copy

from deker.types.private import typings

LONG_TYPES = {"float128": np.longdouble, "complex256": np.clongdouble}


class TestNumericLongTypes:
    def test_types_match_numpy_of_this_platform(self):
        """The 128-bit types are in NumericDtypes and in Numeric exactly when NumPy provides them here."""
        for name, fallback in LONG_TYPES.items():
            long_type = getattr(np, name, fallback)
            if hasattr(np, name):
                assert long_type in typings.NumericDtypes
                assert long_type in get_args(typings.Numeric)
            else:
                assert name not in {t.__name__ for t in typings.NumericDtypes}

    def test_numeric_is_built_from_numeric_dtypes(self):
        """At runtime the union has every type of the list and nothing else."""
        assert set(get_args(typings.Numeric)) == set(typings.NumericDtypes)

    @pytest.mark.parametrize("name", list(LONG_TYPES))
    def test_type_is_absent_when_numpy_has_no_such_type(self, name: str, monkeypatch: pytest.MonkeyPatch):
        """Without the type in NumPy (Apple silicon, Windows) the module imports and the lists do not name it."""
        monkeypatch.delattr(np, name, raising=False)
        copy = load_fresh_copy(typings, "deker.types.private.typings_without_long_types")
        assert name not in {t.__name__ for t in copy.NumericDtypes}
        assert name not in {t.__name__ for t in get_args(copy.Numeric)}
        assert np.longdouble in copy.NumericDtypes
        assert np.clongdouble in get_args(copy.Numeric)

    @pytest.mark.parametrize("name", list(LONG_TYPES))
    def test_type_is_present_when_numpy_has_the_type(self, name: str, monkeypatch: pytest.MonkeyPatch):
        """With the type in NumPy (Linux, Intel macOS) both the list and the union contain it."""
        monkeypatch.setattr(np, name, LONG_TYPES[name], raising=False)
        copy = load_fresh_copy(typings, "deker.types.private.typings_with_long_types")
        assert LONG_TYPES[name] in copy.NumericDtypes
        assert LONG_TYPES[name] in get_args(copy.Numeric)

    def test_fresh_copy_leaves_the_imported_module_alone(self):
        """The copies of the tests are not registered: the typings the package exports are the ones in sys.modules."""
        assert sys.modules["deker.types.private.typings"] is typings
        assert "deker.types.private.typings_without_long_types" not in sys.modules
        assert "deker.types.private.typings_with_long_types" not in sys.modules

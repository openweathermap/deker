"""Tests of the private enums: the data types depend on what NumPy provides on the platform."""

import sys

import numpy as np
import pytest

from tests.parameters.fresh_copy import load_fresh_copy

from deker.types.private import enums

LONG_TYPES = {"float128": np.longdouble, "complex256": np.clongdouble}


class TestDTypeEnumLongTypes:
    def test_members_match_numpy_of_this_platform(self):
        """The 128-bit types are members exactly when NumPy provides them here."""
        for name in LONG_TYPES:
            assert (name in enums.DTypeEnum.__members__) == hasattr(np, name)

    @pytest.mark.parametrize("name", list(LONG_TYPES))
    def test_member_is_absent_when_numpy_has_no_such_type(self, name: str, monkeypatch: pytest.MonkeyPatch):
        """Without the type in NumPy (Apple silicon, Windows) the enum has no such member and is still importable."""
        monkeypatch.delattr(np, name, raising=False)
        copy = load_fresh_copy(enums, "deker.types.private.enums_without_long_types")
        assert name not in copy.DTypeEnum.__members__
        with pytest.raises(KeyError):
            copy.DTypeEnum[name]
        assert "longdouble" in copy.DTypeEnum.__members__
        assert copy.DTypeEnum.longdouble.value is np.longdouble

    @pytest.mark.parametrize("name", list(LONG_TYPES))
    def test_member_is_present_when_numpy_has_the_type(self, name: str, monkeypatch: pytest.MonkeyPatch):
        """With the type in NumPy (Linux, Intel macOS) the enum maps the name to it."""
        monkeypatch.setattr(np, name, LONG_TYPES[name], raising=False)
        copy = load_fresh_copy(enums, "deker.types.private.enums_with_long_types")
        assert copy.DTypeEnum[name].value is LONG_TYPES[name]

    def test_stored_name_of_longdouble_is_float128_where_numpy_has_it(self, monkeypatch: pytest.MonkeyPatch):
        """The first member with the value decides the name in the metadata: float128, as the files of 1.x have it.

        The order of the members matters: the longdouble block is the last in the class so that ssort keeps it.
        """
        monkeypatch.setattr(np, "float128", np.longdouble, raising=False)
        copy = load_fresh_copy(enums, "deker.types.private.enums_with_long_types")
        assert copy.DTypeEnum(np.longdouble) is copy.DTypeEnum.float128
        assert copy.DTypeEnum.get_name(copy.DTypeEnum(np.longdouble)) == "numpy.float128"

    def test_stored_name_of_longdouble_is_longdouble_where_numpy_has_no_float128(self, monkeypatch: pytest.MonkeyPatch):
        """Without float128 the member longdouble gives the name; longfloat stays an alias."""
        monkeypatch.delattr(np, "float128", raising=False)
        copy = load_fresh_copy(enums, "deker.types.private.enums_without_long_types")
        assert copy.DTypeEnum(np.longdouble) is copy.DTypeEnum.longdouble
        assert copy.DTypeEnum.get_name(copy.DTypeEnum(np.longdouble)) == "numpy.longdouble"
        assert copy.DTypeEnum["longfloat"] is copy.DTypeEnum.longdouble

    @pytest.mark.parametrize("present", [True, False])
    def test_stored_name_of_clongdouble_is_clongfloat(self, present: bool, monkeypatch: pytest.MonkeyPatch):
        """complex256 is an alias of clongfloat where it exists, as in 1.x: the stored name does not depend on it."""
        if present:
            monkeypatch.setattr(np, "complex256", np.clongdouble, raising=False)
        else:
            monkeypatch.delattr(np, "complex256", raising=False)
        copy = load_fresh_copy(enums, "deker.types.private.enums_clongdouble")
        assert copy.DTypeEnum(np.clongdouble) is copy.DTypeEnum.clongfloat
        assert copy.DTypeEnum.get_name(copy.DTypeEnum(np.clongdouble)) == "numpy.clongfloat"

    def test_fresh_copy_leaves_the_imported_module_alone(self):
        """The copies of the tests are not registered: the enum the package exports is the one in sys.modules."""
        assert sys.modules["deker.types.private.enums"] is enums
        assert "deker.types.private.enums_without_long_types" not in sys.modules
        assert "deker.types.private.enums_with_long_types" not in sys.modules


class TestDTypeEnumGetName:
    @pytest.mark.parametrize(
        ("member", "expected"),
        [
            (enums.DTypeEnum.int, "int"),
            (enums.DTypeEnum.float, "float"),
            (enums.DTypeEnum.complex, "complex"),
            (enums.DTypeEnum.string, "string"),
            (enums.DTypeEnum.datetime, "datetime"),
            (enums.DTypeEnum.tuple, "tuple"),
            (enums.DTypeEnum.int8, "numpy.int8"),
            (enums.DTypeEnum.float32, "numpy.float32"),
            (enums.DTypeEnum.int16, "numpy.int16"),
            (enums.DTypeEnum.complex64, "numpy.complex64"),
        ],
    )
    def test_builtin_types_are_bare_and_numpy_types_are_prefixed(self, member: enums.DTypeEnum, expected: str):
        """The metadata stores the Python types by name and the NumPy types with the numpy prefix.

        The members are the canonical ones: an alias (``short`` for ``int16``) is stored under the canonical name.
        """
        assert enums.DTypeEnum.get_name(member) == expected
